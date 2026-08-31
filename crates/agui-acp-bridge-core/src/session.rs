//! ACP session actor.
//!
//! Owns the `connect_with(...)` future for one ACP session and bridges
//! between an external [`AcpSessionHandle`] (mpsc commands in,
//! per-prompt [`BridgeStreamItem`] streams out) and the in-protocol
//! `cx: ConnectionTo<Agent>`.
//!
//! # Per-prompt event routing
//!
//! ACP `session/update` notifications arrive on a single connection-level
//! callback installed at `Client::builder().on_receive_notification(...)`
//! time. To route them to the **current prompt's** event channel, the actor
//! holds a shared `Arc<Mutex<Option<mpsc::Sender<BridgeStreamItem>>>>` slot.
//! On `Prompt`, it installs the per-prompt sender; when the prompt completes
//! (success or error) it clears the slot. Notifications that arrive while
//! the slot is empty are logged and dropped — they would be ACP protocol
//! violations (notification outside any active turn).
//!
//! # Request handling
//!
//! The actor registers `on_receive_request` handlers for:
//! - `RequestPermissionRequest` — consults the configured `PermissionPolicy`.
//!   `Allow`/`Deny` decisions respond inline; `Defer` decisions emit a
//!   `BridgeStreamItem::Interrupt` and await an external resolution via
//!   `AcpSessionHandle::resolve_permission` with `permission_timeout`.
//! - Filesystem and terminal request handlers are capability-gated by the
//!   configured [`PermissionPolicy`] and the initialized platform backend.

use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
#[cfg(feature = "unstable_session_model")]
use agent_client_protocol::schema::v1::SessionConfigSelectOptions;
use agent_client_protocol::schema::v1::{
    BooleanConfigOptionCapabilities, ClientCapabilities, ClientSessionCapabilities,
    CloseSessionRequest, ContentBlock, CreateTerminalRequest, DeleteSessionRequest,
    FileSystemCapabilities, HttpHeader, InitializeRequest, KillTerminalRequest, McpServer,
    McpServerHttp, NewSessionRequest, NewSessionResponse, PromptRequest, ReadTextFileRequest,
    ReadTextFileResponse, ReleaseTerminalRequest, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome,
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigOptionValue,
    SessionConfigOptionsCapabilities, SessionId, SessionMode, SessionModeState,
    SessionNotification, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    SetSessionModeRequest, StopReason, TerminalOutputRequest, WaitForTerminalExitRequest,
    WriteTextFileRequest, WriteTextFileResponse,
};
use agent_client_protocol::{Agent, Client, ConnectTo, ConnectionTo, RequestCancellation};
use dashmap::DashMap;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::acp::{
    AcpSessionHandle, PendingPermissions, SessionCommand, SessionConfig, SessionInitState,
    SessionTurnQueue, TurnState,
};
use crate::echo_agent;
use crate::error::BridgeError;
use crate::policy::{PermissionDecision, PermissionPolicy};
use crate::stream::{BridgeStreamItem, ModeOffering, SessionModesInit, SessionSummary};
#[cfg(feature = "unstable_session_model")]
use crate::stream::{ModelOffering, SessionModelsInit};
use crate::terminal::TerminalRegistry;

/// MCP server name advertised on `NewSessionRequest.mcp_servers`. Agents
/// typically prefix the tool names they surface to their LLM with this
/// (e.g. opencode renders our `say_hello` tool as
/// `agui-acp-bridge_say_hello`). Exported so the handler can compute the
/// prefixed variants for the translator's suppression filter.
pub const MCP_SERVER_NAME: &str = "agui-acp-bridge";

const COMMAND_BUFFER: usize = 8;
const IN_PROCESS_DUPLEX_BUFFER: usize = 65_536;
// ponytail: keep transient history/list budgets local until the existing
// BridgeConfig surface grows dedicated values for these operations.
const MAX_LOAD_HISTORY_EVENTS: usize = 4096;
const MAX_LOAD_HISTORY_BYTES: usize = 16 * 1024 * 1024;
const MAX_LIST_SESSIONS: usize = 10_000;
const MAX_LIST_BYTES: usize = 16 * 1024 * 1024;
const MAX_LIST_PAGES: usize = 1000;

type EventSlot = Arc<Mutex<Option<mpsc::Sender<BridgeStreamItem>>>>;

/// Captures `session/update` notifications replayed by the agent during a
/// `session/load` call. The agent streams the conversation history as
/// notifications *before* any prompt is active, so they would otherwise be
/// dropped ("no active prompt"). When `Some`, the notification handler
/// appends each update here instead; the actor flushes the buffer onto the
/// first prompt's stream so the resuming client sees its prior conversation.
#[derive(Debug, Default)]
struct LoadHistory {
    updates: Vec<agent_client_protocol::schema::v1::SessionUpdate>,
    bytes: usize,
    exceeded: bool,
}

impl LoadHistory {
    fn append(
        &mut self,
        update: agent_client_protocol::schema::v1::SessionUpdate,
        bytes: usize,
    ) -> Result<(), ()> {
        if self.exceeded
            || self.updates.len() >= MAX_LOAD_HISTORY_EVENTS
            || bytes > MAX_LOAD_HISTORY_BYTES
            || self.bytes > MAX_LOAD_HISTORY_BYTES - bytes
        {
            self.exceeded = true;
            return Err(());
        }
        self.bytes += bytes;
        self.updates.push(update);
        Ok(())
    }
}

type LoadBuffer = Arc<Mutex<Option<LoadHistory>>>;

#[derive(Debug, Default)]
struct BoundedSessionList {
    summaries: Vec<SessionSummary>,
    bytes: usize,
}

impl BoundedSessionList {
    fn push(&mut self, summary: SessionSummary) -> Result<(), BridgeError> {
        let bytes = serde_json::to_vec(&summary)
            .map_err(BridgeError::Json)?
            .len();
        self.push_with_size(summary, bytes)
    }

    fn push_with_size(&mut self, summary: SessionSummary, bytes: usize) -> Result<(), BridgeError> {
        if self.summaries.len() >= MAX_LIST_SESSIONS {
            return Err(session_limit_error(
                "session/list result exceeds the entry limit",
            ));
        }
        if bytes > MAX_LIST_BYTES || self.bytes > MAX_LIST_BYTES - bytes {
            return Err(session_limit_error(
                "session/list result exceeds the byte limit",
            ));
        }
        self.bytes += bytes;
        self.summaries.push(summary);
        Ok(())
    }

    fn len(&self) -> usize {
        self.summaries.len()
    }

    fn into_summaries(self) -> Vec<SessionSummary> {
        self.summaries
    }
}

fn next_list_cursor(pages: usize, next: Option<String>) -> Result<Option<String>, BridgeError> {
    match next {
        Some(next) if pages < MAX_LIST_PAGES => Ok(Some(next)),
        Some(_) => Err(session_limit_error(
            "session/list exceeded the page limit before the final page",
        )),
        None => Ok(None),
    }
}

fn session_limit_error(message: &'static str) -> BridgeError {
    BridgeError::Acp(agent_client_protocol::Error::request_cancelled().data(message))
}

fn load_history_limit_error() -> agent_client_protocol::Error {
    agent_client_protocol::Error::request_cancelled()
        .data("session/load history exceeds the bridge event or byte limit")
}

pub(crate) async fn spawn_in_process_echo_session(
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError> {
    spawn_in_process_session_with(cfg, |s| Box::pin(echo_agent::run_echo_agent(s))).await
}

/// Spawn an in-process ACP session backed by a caller-provided agent runner.
///
/// `agent_runner` receives one half of a tokio duplex stream and is expected
/// to drive the ACP `Agent` builder loop until the stream is dropped. Used by
/// integration tests to plug in custom agents (image, failing, etc.) without
/// duplicating the duplex+transport plumbing.
#[doc(hidden)]
pub async fn spawn_in_process_session_with<F>(
    cfg: SessionConfig,
    agent_runner: F,
) -> Result<AcpSessionHandle, BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    let mut agent_guard = AbortOnDrop::new(tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process test agent terminated with error");
        }
    }));

    let (read, write) = tokio::io::split(client_stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    let result = spawn_session(transport, cfg).await;
    if result.is_ok() {
        agent_guard.disarm();
    }
    result
}

/// In-process equivalent of [`list_sessions_via`]: drive `session/list`
/// against a caller-provided agent over an in-memory duplex. Exercises the
/// **real** listing code path (the same one subprocess clients use) so tests
/// don't have to shortcut around it.
#[doc(hidden)]
pub async fn list_sessions_in_process_with<F>(
    cfg: SessionConfig,
    agent_runner: F,
) -> Result<Vec<SessionSummary>, BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process list agent terminated with error");
        }
    });

    let (read, write) = tokio::io::split(client_stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    list_sessions_via(transport, cfg).await
}

/// In-process equivalent of [`delete_session_via`].
#[doc(hidden)]
pub async fn delete_session_in_process_with<F>(
    cfg: SessionConfig,
    session_id: SessionId,
    agent_runner: F,
) -> Result<(), BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process delete agent terminated with error");
        }
    });

    let (read, write) = tokio::io::split(client_stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    delete_session_via(transport, cfg, session_id).await
}

pub(crate) async fn spawn_session<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(COMMAND_BUFFER);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<SessionReady, BridgeError>>();
    let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
    let turn_queue = Arc::new(SessionTurnQueue::new(cfg.config.max_queued_turns));
    let unusable = Arc::new(AtomicBool::new(false));
    let init_state = Arc::new(Mutex::new(SessionInitState::default()));

    let handle_pending = pending_permissions.clone();
    let handle_turn_queue = turn_queue.clone();
    let handle_unusable = unusable.clone();
    let handle_init_state = init_state.clone();
    let event_buffer = cfg.config.event_buffer;
    let actor_state = SessionActorState {
        pending_permissions,
        turn_queue,
        unusable,
        init_state,
    };

    let mut actor_guard = AbortOnDrop::new(tokio::spawn(run_actor(
        connector,
        cfg,
        cmd_rx,
        ready_tx,
        actor_state,
    )));

    match ready_rx.await {
        Ok(Ok(ready)) => {
            actor_guard.disarm();
            Ok(AcpSessionHandle::new(
                cmd_tx,
                handle_pending,
                handle_turn_queue,
                handle_unusable,
                ready.session_id,
                ready.supports_close,
                handle_init_state,
                event_buffer,
            ))
        }
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BridgeError::SessionClosed),
    }
}

/// Keeps a newly spawned actor attached to its opener until the readiness
/// handshake succeeds. Dropping the opener future otherwise detaches the
/// actor, allowing a timed-out handshake to keep a connector/subprocess alive.
struct AbortOnDrop<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn disarm(&mut self) {
        // Dropping a JoinHandle detaches the task. That is intentional only
        // after the actor has reported a successful ready handshake.
        self.handle.take();
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Open a short-lived ACP connection, confirm the agent advertises
/// `session/list`, fetch all session summaries (following `nextCursor`
/// pagination), and tear the connection down.
///
/// Stateless: the bridge stores nothing — this is a pass-through of what the
/// agent persists. Returns [`BridgeError::Unsupported`] when the agent does
/// not advertise `sessionCapabilities.list`.
pub(crate) async fn list_sessions_via<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<Vec<SessionSummary>, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel::<Result<Vec<SessionSummary>, BridgeError>>();
    let cwd = cfg.cwd.clone();
    let request_timeout = cfg.config.set_session_timeout;

    // A minimal client: we issue requests from the connection task and never
    // receive notifications/requests we care about, so the builder only needs
    // a dispatch handler to route responses back to their awaiters.
    let result_tx = std::sync::Mutex::new(Some(result_tx));
    let connect_result = agent_client_protocol::Client
        .builder()
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::util::internal_error(
                            "unhandled request",
                        )),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let result_slot = result_tx;
            async move {
                let outcome = list_sessions_inner(&cx, cwd, request_timeout).await;
                if let Some(tx) = result_slot.lock().expect("result slot poisoned").take() {
                    let _ = tx.send(outcome);
                }
                Ok(())
            }
        })
        .await;

    // If the connection itself failed (spawn/handshake transport error),
    // surface that; otherwise return whatever the inner task produced.
    match result_rx.await {
        Ok(res) => res,
        Err(_) => match connect_result {
            Ok(()) => Err(BridgeError::SessionClosed),
            Err(err) => Err(BridgeError::Acp(err)),
        },
    }
}

/// Open a short-lived ACP connection, confirm the agent advertises
/// `session/delete`, delete the supplied persisted session, and tear the
/// connection down.
pub(crate) async fn delete_session_via<T>(
    connector: T,
    cfg: SessionConfig,
    session_id: SessionId,
) -> Result<(), BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel::<Result<(), BridgeError>>();
    let request_timeout = cfg.config.set_session_timeout;

    let result_tx = std::sync::Mutex::new(Some(result_tx));
    let connect_result = agent_client_protocol::Client
        .builder()
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::util::internal_error(
                            "unhandled request",
                        )),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let result_slot = result_tx;
            async move {
                let outcome = delete_session_inner(&cx, session_id, request_timeout).await;
                if let Some(tx) = result_slot.lock().expect("result slot poisoned").take() {
                    let _ = tx.send(outcome);
                }
                Ok(())
            }
        })
        .await;

    match result_rx.await {
        Ok(res) => res,
        Err(_) => match connect_result {
            Ok(()) => Err(BridgeError::SessionClosed),
            Err(err) => Err(BridgeError::Acp(err)),
        },
    }
}

/// Inner body of [`list_sessions_via`]: initialize, capability-gate, then
/// page through `session/list`.
async fn list_sessions_inner(
    cx: &ConnectionTo<Agent>,
    _cwd: PathBuf,
    request_timeout: Duration,
) -> Result<Vec<SessionSummary>, BridgeError> {
    use agent_client_protocol::schema::v1::ListSessionsRequest;

    let init = tokio::time::timeout(
        request_timeout,
        cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map_err(BridgeError::Acp)?;

    require_protocol_v1(init.protocol_version)?;

    if init.agent_capabilities.session_capabilities.list.is_none() {
        return Err(BridgeError::Unsupported("session/list".into()));
    }

    let mut summaries = BoundedSessionList::default();
    let mut cursor: Option<String> = None;
    // Guard against a misbehaving agent returning an endless cursor chain.
    let mut pages = 0usize;
    loop {
        // Intentionally do NOT filter by `cwd`: the history UI wants every
        // conversation the agent persists, and an exact-path filter is
        // fragile across canonicalization differences (Windows `\\?\`
        // extended-length prefixes, trailing slashes, symlinks). Listing
        // unfiltered and letting the client decide is both more useful and
        // more robust.
        let mut req = ListSessionsRequest::new();
        if let Some(c) = cursor.take() {
            req = req.cursor(c);
        }
        let resp = tokio::time::timeout(request_timeout, cx.send_request(req).block_task())
            .await
            .map_err(|_| BridgeError::Timeout(request_timeout))?
            .map_err(BridgeError::Acp)?;

        tracing::debug!(
            page = pages,
            count = resp.sessions.len(),
            has_next = resp.next_cursor.is_some(),
            "session/list page received"
        );

        if resp.sessions.len() > MAX_LIST_SESSIONS.saturating_sub(summaries.len()) {
            return Err(session_limit_error(
                "session/list result exceeds the entry limit",
            ));
        }
        for info in resp.sessions {
            summaries.push(SessionSummary {
                session_id: info.session_id.0.to_string(),
                cwd: info.cwd.to_string_lossy().into_owned(),
                title: info.title,
                updated_at: info.updated_at,
            })?;
        }

        pages += 1;
        match next_list_cursor(pages, resp.next_cursor)? {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    tracing::info!(total = summaries.len(), "session/list complete");
    Ok(summaries.into_summaries())
}

async fn delete_session_inner(
    cx: &ConnectionTo<Agent>,
    session_id: SessionId,
    request_timeout: Duration,
) -> Result<(), BridgeError> {
    let init = tokio::time::timeout(
        request_timeout,
        cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map_err(BridgeError::Acp)?;

    require_protocol_v1(init.protocol_version)?;

    if init
        .agent_capabilities
        .session_capabilities
        .delete
        .is_none()
    {
        return Err(BridgeError::Unsupported("session/delete".into()));
    }

    tokio::time::timeout(
        request_timeout,
        cx.send_request(DeleteSessionRequest::new(session_id))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map(|_| ())
    .map_err(BridgeError::Acp)
}

struct SessionActorState {
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    unusable: Arc<AtomicBool>,
    init_state: Arc<Mutex<SessionInitState>>,
}

struct SessionReady {
    session_id: SessionId,
    supports_close: bool,
}

/// Last-resort cleanup for actor cancellation or panic. The explicit cleanup
/// at the normal return site runs before error-event backpressure, while this
/// guard covers task aborts that never reach that site.
struct ActorExitGuard {
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    unusable: Arc<AtomicBool>,
}

impl Drop for ActorExitGuard {
    fn drop(&mut self) {
        self.unusable.store(true, Ordering::Release);
        self.turn_queue.clear();
        drain_pending_permissions(&self.pending_permissions);
    }
}

async fn run_actor<T>(
    connector: T,
    cfg: SessionConfig,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    ready_tx: oneshot::Sender<Result<SessionReady, BridgeError>>,
    state: SessionActorState,
) where
    T: ConnectTo<Client> + Send + 'static,
{
    let SessionActorState {
        pending_permissions,
        turn_queue,
        unusable,
        init_state,
    } = state;
    let _exit_guard = ActorExitGuard {
        pending_permissions: pending_permissions.clone(),
        turn_queue: turn_queue.clone(),
        unusable: unusable.clone(),
    };
    let event_slot: EventSlot = Arc::new(Mutex::new(None));
    let event_slot_for_notif = event_slot.clone();
    let event_slot_for_perm = event_slot.clone();
    let event_slot_for_session = event_slot.clone();
    let load_buffer: LoadBuffer = Arc::new(Mutex::new(None));
    let load_buffer_for_notif = load_buffer.clone();
    let load_buffer_for_session = load_buffer.clone();
    let SessionConfig {
        cwd,
        policy,
        config,
        mcp_url,
        mcp_headers,
        load_session_id,
    } = cfg;
    let permission_timeout = config.permission_timeout;
    let filesystem_capabilities = policy.filesystem_capabilities();
    let read_filesystem_enabled =
        filesystem_capabilities.read_text_file && crate::file_ops::read_text_file_supported();
    let write_filesystem_enabled =
        filesystem_capabilities.write_text_file && crate::file_ops::write_text_file_supported();
    let filesystem_capabilities = filesystem_capabilities
        .read_text_file(read_filesystem_enabled)
        .write_text_file(write_filesystem_enabled);
    // Do not advertise terminal access unless the policy opts in and the
    // platform-specific cwd/process backend initialized successfully.
    let terminal_backend = if policy.terminal_capability() {
        TerminalRegistry::new(cwd.clone()).ok()
    } else {
        None
    };
    let terminal_capability = terminal_backend.is_some();
    let cwd = Arc::new(cwd);
    let filesystem_lock = Arc::new(AsyncMutex::new(()));
    let (terminal_registry, terminal_registry_guard) = terminal_backend
        .map(|(registry, guard)| (Some(registry), Some(guard)))
        .unwrap_or((None, None));

    let pending_perms_for_handler = pending_permissions.clone();
    let pending_perms_for_drain = pending_permissions.clone();
    let turns_for_handler = turn_queue.clone();
    let turns_for_session = turn_queue.clone();
    let unusable_for_session = unusable.clone();
    let read_filesystem_lock = filesystem_lock.clone();
    let write_filesystem_lock = filesystem_lock.clone();
    let ready_tx = std::sync::Mutex::new(Some(ready_tx));

    let result = agent_client_protocol::Client
        .builder()
        .on_receive_notification(
            {
                let init_state = init_state.clone();
                async move |notification: SessionNotification, _cx| {
                    let load_state = load_buffer_for_notif
                        .lock()
                        .expect("load buffer poisoned")
                        .as_ref()
                        .map(|history| history.exceeded);
                    if load_state == Some(true) {
                        return Err(load_history_limit_error());
                    }
                    let load_bytes = if load_state == Some(false) {
                        Some(
                            serde_json::to_vec(&notification)
                                .map_err(agent_client_protocol::Error::into_internal_error)?
                                .len(),
                        )
                    } else {
                        None
                    };
                    // Keep the cached init state in sync with autonomous mode
                    // changes so the next prompt's SessionInit emission shows
                    // the right current mode. We still forward the
                    // notification verbatim — the translator already turns
                    // CurrentModeUpdate into an `agent:mode_update` CUSTOM
                    // event for live UI updates.
                    if let agent_client_protocol::schema::v1::SessionUpdate::CurrentModeUpdate(ref m) =
                        notification.update
                    {
                        let new_id = m.current_mode_id.0.to_string();
                        let mut guard = init_state.lock().expect("init_state poisoned");
                        if let Some(modes) = guard.modes.as_mut() {
                            modes.current_mode_id = new_id;
                        }
                    } else if let agent_client_protocol::schema::v1::SessionUpdate::ConfigOptionUpdate(
                        ref update,
                    ) = notification.update
                    {
                        let options = update.config_options.clone();
                        let mut guard = init_state.lock().expect("init_state poisoned");
                        guard.config_options = Some(options.clone());
                        sync_legacy_picker_state(&mut guard, &options);
                    }
                    // If a session/load is in progress, the agent is replaying
                    // history. Capture those updates into the load buffer (no
                    // prompt is active yet) so the actor can flush them onto
                    // the first prompt's stream.
                    {
                        let mut buf = load_buffer_for_notif.lock().expect("load buffer poisoned");
                        if let Some(history) = buf.as_mut()
                            && let Some(bytes) = load_bytes
                        {
                            if history.append(notification.update, bytes).is_err() {
                                return Err(load_history_limit_error());
                            }
                            return Ok(());
                        }
                    }
                    let sender = event_slot_for_notif
                        .lock()
                        .expect("event slot poisoned")
                        .clone();
                    if let Some(tx) = sender {
                        let _ = tx.send(BridgeStreamItem::Update(notification.update)).await;
                    } else {
                        tracing::warn!(
                            "received session/update notification with no active prompt; dropping"
                        );
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            {
                let policy = policy.clone();
                let event_slot_for_perm = event_slot_for_perm.clone();
                let pending_perms = pending_perms_for_handler.clone();
                let turns = turns_for_handler.clone();
                async move |req: RequestPermissionRequest,
                            responder: agent_client_protocol::Responder<
                    RequestPermissionResponse,
                >,
                            _cx| {
                    handle_permission_request(
                        req,
                        responder,
                        policy.clone(),
                        event_slot_for_perm.clone(),
                        pending_perms.clone(),
                        turns.clone(),
                        permission_timeout,
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cwd = cwd.clone();
                let filesystem_lock = read_filesystem_lock;
                async move |req: ReadTextFileRequest, responder, cx| {
                    if !read_filesystem_enabled {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }

                    let cancellation = responder.cancellation();
                    let cwd = cwd.clone();
                    let filesystem_lock = filesystem_lock.clone();
                    if let Err(error) = cx.spawn(async move {
                        let result = cancellation
                            .run_until_cancelled(read_file_request(
                                req,
                                cwd,
                                filesystem_lock,
                                cancellation.clone(),
                            ))
                            .await;
                        let result = if cancellation.is_cancelled() {
                            Err(agent_client_protocol::Error::request_cancelled())
                        } else {
                            result
                        };
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "filesystem read response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "filesystem read task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cwd = cwd.clone();
                let filesystem_lock = write_filesystem_lock;
                async move |req: WriteTextFileRequest, responder, cx| {
                    if !write_filesystem_enabled {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }

                    let cancellation = responder.cancellation();
                    let cwd = cwd.clone();
                    let filesystem_lock = filesystem_lock.clone();
                    if let Err(error) = cx.spawn(async move {
                        let result = write_file_request(
                            req,
                            cwd,
                            filesystem_lock,
                            cancellation.clone(),
                        )
                        .await;
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "filesystem write response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "filesystem write task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let registry = terminal_registry.clone();
                async move |req: CreateTerminalRequest, responder, cx| {
                    if !terminal_capability {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    let Some(registry) = registry.clone() else {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        if let Err(error) = crate::terminal::create_request(
                            req,
                            registry,
                            cancellation,
                            responder,
                        )
                        .await
                        {
                            tracing::debug!(?error, "terminal create response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "terminal create task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let registry = terminal_registry.clone();
                async move |req: TerminalOutputRequest, responder, cx| {
                    if !terminal_capability {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    let Some(registry) = registry.clone() else {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let result = crate::terminal::output_request(req, registry, cancellation).await;
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "terminal output response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "terminal output task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let registry = terminal_registry.clone();
                async move |req: WaitForTerminalExitRequest, responder, cx| {
                    if !terminal_capability {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    let Some(registry) = registry.clone() else {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let result = crate::terminal::wait_request(req, registry, cancellation).await;
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "terminal wait response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "terminal wait task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let registry = terminal_registry.clone();
                async move |req: KillTerminalRequest, responder, cx| {
                    if !terminal_capability {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    let Some(registry) = registry.clone() else {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let result = crate::terminal::kill_request(req, registry, cancellation).await;
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "terminal kill response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "terminal kill task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let registry = terminal_registry.clone();
                async move |req: ReleaseTerminalRequest, responder, cx| {
                    if !terminal_capability {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    let Some(registry) = registry.clone() else {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let result = crate::terminal::release_request(req, registry, cancellation).await;
                        if let Err(error) = responder.respond_with_result(result) {
                            tracing::debug!(?error, "terminal release response could not be sent");
                        }
                        Ok(())
                    }) {
                        tracing::debug!(?error, "terminal release task could not be spawned");
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let ready_slot = ready_tx;
            let event_slot = event_slot_for_session;
            let pending_for_drain = pending_permissions.clone();
            let turn_queue = turns_for_session.clone();
            let unusable = unusable_for_session.clone();
            let mut cmd_rx = cmd_rx;
            let mcp_url = mcp_url.clone();
            let mcp_headers = mcp_headers.clone();
            let init_state = init_state.clone();
            let load_session_id = load_session_id.clone();
            let load_buffer = load_buffer_for_session;
            async move {
                let (session_id, supports_close) = match initialize(
                    &cx,
                    cwd.as_ref().clone(),
                    mcp_url,
                    mcp_headers,
                    load_session_id,
                    &load_buffer,
                    &init_state,
                    filesystem_capabilities,
                    terminal_capability,
                )
                .await
                {
                    Ok((id, init, supports_close)) => {
                        *init_state.lock().expect("init_state poisoned") = init.clone();
                        if let Some(tx) = ready_slot.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Ok(SessionReady {
                                session_id: id.clone(),
                                supports_close,
                            }));
                        }
                        (id, supports_close)
                    }
                    Err(err) => {
                        if let Some(tx) = ready_slot.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Err(err));
                        }
                        return Ok(());
                    }
                };

                while let Some(cmd) = cmd_rx.recv().await {
                    match cmd {
                        SessionCommand::Prompt {
                            prompt,
                            events_tx,
                            finished_tx,
                            turn,
                        } => {
                            *event_slot.lock().expect("event slot poisoned") =
                                Some(events_tx.clone());

                            // Surface the cached SessionInit at the start of
                            // every prompt so reconnecting clients still see
                            // the picker before the first agent text.
                            let snapshot = init_state.lock().expect("init_state poisoned").clone();
                            let _ = events_tx
                                .send(BridgeStreamItem::SessionInit {
                                    modes: snapshot.modes,
                                    models: snapshot.models,
                                    config_options: snapshot.config_options,
                                })
                                .await;

                            // If this session was resumed via session/load,
                            // the agent replayed its history into the load
                            // buffer during initialize. Flush that history
                            // onto the stream now (once), ahead of the new
                            // turn, so the resuming client sees the prior
                            // conversation. Disarm the buffer afterwards so
                            // subsequent prompts behave normally.
                            let replay = load_buffer.lock().expect("load buffer poisoned").take();
                            if let Some(history) = replay {
                                for update in history.updates {
                                    if events_tx
                                        .send(BridgeStreamItem::Update(update))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }

                            // Run the prompt while concurrently watching for
                            // a cancel notification (via the turn state) and for
                            // the SSE consumer dropping. A naive serial
                            // `await` would let cancel signals miss the
                            // window — see audit P0 "session.cancel()
                            // cannot interrupt".
                            let res = run_prompt_with_cancel(
                                &cx,
                                &session_id,
                                prompt,
                                &events_tx,
                                turn.clone(),
                                pending_permissions.clone(),
                                config.cancel_grace_timeout,
                            )
                            .await;

                            let grace_expired = matches!(
                                &res,
                                Err(BridgeError::Timeout(timeout))
                                    if *timeout == config.cancel_grace_timeout
                            ) && turn.is_cancelled();

                            *event_slot.lock().expect("event slot poisoned") = None;

                            let _ = finished_tx.send(res);
                            drop(events_tx);
                            turn_queue.remove(&turn);
                            if grace_expired {
                                // The agent did not acknowledge cancellation
                                // within the grace window. Its session state
                                // is now unknowable; close the actor and make
                                // the cache evict this handle rather than
                                // reusing it for a later AG-UI run.
                                unusable.store(true, Ordering::Release);
                                break;
                            }
                        }
                        SessionCommand::SetMode { mode_id, ack } => {
                            // Settings are deliberately actor-serial. A
                            // prompt owns the ACP turn until it finishes, so
                            // a queued setting cannot race a later setting or
                            // overwrite its newer cached snapshot.
                            //
                            // The HTTP caller may time out while this command
                            // waits behind a prompt. Do not apply a command
                            // whose responder has already gone away.
                            let mut ack = ack;
                            if ack.is_closed() {
                                continue;
                            }
                            let res = tokio::select! {
                                _ = ack.closed() => {
                                    // The caller disconnected while the ACP
                                    // request was in flight. Its eventual
                                    // response cannot safely update a session
                                    // that may be reused, so close this actor.
                                    unusable.store(true, Ordering::Release);
                                    break;
                                }
                                result = tokio::time::timeout(
                                    config.set_session_timeout,
                                    send_set_mode(&cx, &session_id, &mode_id, init_state.clone()),
                                ) => match result {
                                    Ok(result) => result,
                                    Err(_) => {
                                        unusable.store(true, Ordering::Release);
                                        let _ = send_setting_ack(
                                            ack,
                                            Err(BridgeError::Timeout(config.set_session_timeout)),
                                            &unusable,
                                        );
                                        break;
                                    }
                                },
                            };
                            if ack.is_closed() && res.is_ok() {
                                unusable.store(true, Ordering::Release);
                                break;
                            }
                            if !send_setting_ack(ack, res, &unusable) {
                                break;
                            }
                        }
                        SessionCommand::SetConfigOption {
                            config_id,
                            value,
                            ack,
                        } => {
                            let mut ack = ack;
                            if ack.is_closed() {
                                continue;
                            }
                            let res = tokio::select! {
                                _ = ack.closed() => {
                                    // The caller disconnected while the ACP
                                    // request was in flight. Do not keep a
                                    // state-uncertain session alive.
                                    unusable.store(true, Ordering::Release);
                                    break;
                                }
                                result = tokio::time::timeout(
                                    config.set_session_timeout,
                                    send_set_config_option(
                                        &cx,
                                        &session_id,
                                        &config_id,
                                        &value,
                                        init_state.clone(),
                                    ),
                                ) => match result {
                                    Ok(result) => result,
                                    Err(_) => {
                                        unusable.store(true, Ordering::Release);
                                        let _ = send_setting_ack(
                                            ack,
                                            Err(BridgeError::Timeout(config.set_session_timeout)),
                                            &unusable,
                                        );
                                        break;
                                    }
                                },
                            };
                            if ack.is_closed() && res.is_ok() {
                                unusable.store(true, Ordering::Release);
                                break;
                            }
                            if !send_setting_ack(ack, res, &unusable) {
                                break;
                            }
                        }
                        SessionCommand::Close { ack } => {
                            if !supports_close {
                                let _ = ack.send(Err(BridgeError::Unsupported(
                                    "session/close".into(),
                                )));
                                continue;
                            }

                            // Closing is a terminal actor operation. The
                            // request uses the real ACP SessionId returned by
                            // initialize, never the bridge thread/run ids.
                            let result = match tokio::time::timeout(
                                config.set_session_timeout,
                                cx.send_request(CloseSessionRequest::new(session_id.clone()))
                                    .block_task(),
                            )
                            .await
                            {
                                Ok(Ok(_)) => Ok(()),
                                Ok(Err(error)) => Err(BridgeError::Acp(error)),
                                Err(_) => Err(BridgeError::Timeout(config.set_session_timeout)),
                            };

                            // A close response error/timeout leaves the ACP
                            // state uncertain, so all terminal close outcomes
                            // make this actor unusable and release local work.
                            unusable.store(true, Ordering::Release);
                            turn_queue.clear();
                            drain_pending_permissions(&pending_for_drain);
                            let _ = ack.send(result);
                            break;
                        }
                        SessionCommand::DrainHistory {
                            events_tx,
                            finished_tx,
                        } => {
                            // Emit a SessionInit so the resuming client's
                            // picker is populated, then flush any loaded
                            // history, then finish — without prompting.
                            let snapshot = init_state.lock().expect("init_state poisoned").clone();
                            let _ = events_tx
                                .send(BridgeStreamItem::SessionInit {
                                    modes: snapshot.modes,
                                    models: snapshot.models,
                                    config_options: snapshot.config_options,
                                })
                                .await;
                            let replay = load_buffer.lock().expect("load buffer poisoned").take();
                            if let Some(history) = replay {
                                for update in history.updates {
                                    if events_tx
                                        .send(BridgeStreamItem::Update(update))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                            let _ = finished_tx.send(Ok(StopReason::EndTurn));
                            drop(events_tx);
                        }
                    }
                }
                // Connection closing — fail any pending permission waiters
                // (their spawned tasks will respond Cancelled to the agent
                // and exit) so memory and tasks don't linger past idle
                // reaping.
                drain_pending_permissions(&pending_for_drain);
                Ok(())
            }
        })
        .await;

    if let Some(registry) = terminal_registry.as_ref() {
        registry.shutdown();
    }
    drop(terminal_registry_guard);

    // Every exit path below represents a dead actor. Mark the shared handle
    // first so new prompts fail closed, then release every queued admission
    // before any potentially backpressured error event is sent.
    unusable.store(true, Ordering::Release);
    turn_queue.clear();

    if let Err(err) = result {
        let sender = event_slot.lock().expect("event slot poisoned").clone();
        if let Some(tx) = sender {
            let _ = tx
                .send(BridgeStreamItem::RunError {
                    message: format!("acp connection terminated: {err}"),
                })
                .await;
        }
        // Drain any pending permission waiters so their spawned timeout
        // tasks don't dangle for `permission_timeout` after the actor
        // exits. Sending `Deny` causes them to respond `Cancelled` to the
        // agent (which is moot since the connection is gone) and to
        // remove themselves from the pending map.
        drain_pending_permissions(&pending_perms_for_drain);
    }
}

/// Resolve every pending permission with `Deny` and remove its map entry.
///
/// Called when the session is shutting down (either via a normal
/// `cmd_rx` close or after a connection error) so the spawn tasks
/// awaiting `resolve_rx` exit immediately rather than after
/// `permission_timeout`. Safe to call from any sync context — the
/// oneshot send is non-blocking.
fn drain_pending_permissions(pending: &PendingPermissions) {
    let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
    for key in keys {
        if let Some((_, p)) = pending.remove(&key) {
            // The receiver may already be gone (timeout fired first).
            // We don't care about the send result.
            let _ = p.resolver.send(PermissionDecision::Deny);
        }
    }
}

/// Deliver a setting result or make the session unusable when the caller has
/// gone away between the closed check and `send`.
fn send_setting_ack(
    ack: oneshot::Sender<Result<(), BridgeError>>,
    result: Result<(), BridgeError>,
    unusable: &AtomicBool,
) -> bool {
    if ack.send(result).is_err() {
        unusable.store(true, Ordering::Release);
        false
    } else {
        true
    }
}

fn file_operation_error(error: BridgeError) -> agent_client_protocol::Error {
    match crate::file_ops::error_kind(&error) {
        crate::file_ops::FileErrorKind::InvalidParams => {
            agent_client_protocol::Error::invalid_params()
        }
        crate::file_ops::FileErrorKind::ResourceNotFound => {
            agent_client_protocol::Error::resource_not_found(None)
        }
        crate::file_ops::FileErrorKind::Internal => agent_client_protocol::Error::internal_error(),
    }
}

fn request_path(path: &std::path::Path) -> Result<&str, agent_client_protocol::Error> {
    path.to_str()
        .ok_or_else(agent_client_protocol::Error::invalid_params)
}

fn request_line(value: Option<u32>) -> Result<Option<usize>, agent_client_protocol::Error> {
    value
        .map(|value| {
            usize::try_from(value).map_err(|_| agent_client_protocol::Error::invalid_params())
        })
        .transpose()
}

async fn read_file_request(
    request: ReadTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
) -> Result<ReadTextFileResponse, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }

    let path = request_path(&request.path)?;
    let line = request_line(request.line)?;
    let limit = request_line(request.limit)?;
    let _guard = filesystem_lock.lock().await;
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }

    crate::file_ops::read_text_file_range(cwd.as_path(), path, line, limit)
        .await
        .map(ReadTextFileResponse::new)
        .map_err(file_operation_error)
}

async fn write_file_request(
    request: WriteTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
) -> Result<WriteTextFileResponse, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    if request.content.len() > crate::file_ops::MAX_TEXT_FILE_BYTES {
        return Err(agent_client_protocol::Error::invalid_params());
    }

    let path = request_path(&request.path)?;
    let _guard = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err(agent_client_protocol::Error::request_cancelled());
        }
        guard = filesystem_lock.lock() => guard,
    };
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }

    crate::file_ops::write_text_file(cwd.as_path(), path, &request.content)
        .await
        .map(|_| WriteTextFileResponse::new())
        .map_err(file_operation_error)
}

#[allow(clippy::too_many_arguments)]
async fn initialize(
    cx: &ConnectionTo<Agent>,
    cwd: PathBuf,
    mcp_url: Option<String>,
    mcp_headers: Vec<HttpHeader>,
    load_session_id: Option<SessionId>,
    load_buffer: &LoadBuffer,
    init_state: &Mutex<SessionInitState>,
    filesystem_capabilities: FileSystemCapabilities,
    terminal_capability: bool,
) -> Result<(SessionId, SessionInitState, bool), BridgeError> {
    // Send the agent a conventional absolute cwd (no Windows `\\?\` verbatim
    // prefix) so its persisted session directory matches what other tools use
    // and directory-scoped `session/list` can find it later.
    let cwd = crate::file_ops::acp_cwd(&cwd);
    let init_response = cx
        .send_request(
            InitializeRequest::new(ProtocolVersion::V1).client_capabilities(
                ClientCapabilities::new()
                    .fs(filesystem_capabilities)
                    .terminal(terminal_capability)
                    .session(
                        ClientSessionCapabilities::new().config_options(
                            SessionConfigOptionsCapabilities::new()
                                .boolean(BooleanConfigOptionCapabilities::new()),
                        ),
                    ),
            ),
        )
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    // ACP negotiates a wire protocol version, not a schema/package version.
    // Do not issue any session request until the agent explicitly accepts v1.
    require_protocol_v1(init_response.protocol_version)?;
    let supports_close = init_response
        .agent_capabilities
        .session_capabilities
        .close
        .is_some();

    // Compose the optional MCP server entry once; both new and load requests
    // carry it so frontend tools work on resumed sessions too.
    let mcp_servers: Vec<McpServer> = match mcp_url {
        Some(url) if init_response.agent_capabilities.mcp_capabilities.http => {
            vec![mcp_http_server(url, mcp_headers)]
        }
        Some(_) => {
            tracing::warn!(
                "agent did not advertise mcpCapabilities.http=true; \
                 frontend tools will not be injected"
            );
            vec![]
        }
        None => vec![],
    };

    // Strict resume path: load an existing ACP session only when the caller
    // asked for it and the agent advertises `loadSession`. Unsupported load or
    // a failed `session/load` is returned to the caller; it never becomes a
    // fresh `session/new`. The agent replays history as `session/update`
    // notifications during the call; those are captured into `load_buffer`
    // (see the notification handler) so the handler can surface them on the
    // resume run's stream.
    if let Some(sid) = load_session_id {
        if !init_response.agent_capabilities.load_session {
            return Err(BridgeError::ResumeUnsupported(
                "agent does not advertise loadSession".into(),
            ));
        }
        if sid.0.trim().is_empty() {
            return Err(BridgeError::ResumeFailed(
                "session/load requires a non-empty session id".into(),
            ));
        }

        use agent_client_protocol::schema::v1::LoadSessionRequest;
        let session_id = sid;
        // Arm the buffer so history notifications are captured rather
        // than dropped ("no active prompt").
        load_buffer
            .lock()
            .expect("load buffer poisoned")
            .replace(LoadHistory::default());
        let mut req = LoadSessionRequest::new(session_id.clone(), cwd.clone());
        if !mcp_servers.is_empty() {
            req = req.mcp_servers(mcp_servers.clone());
        }
        let load = cx.send_request(req).block_task().await;
        match load {
            Ok(resp) => {
                let exceeded = load_buffer
                    .lock()
                    .expect("load buffer poisoned")
                    .as_ref()
                    .is_some_and(|history| history.exceeded);
                if exceeded {
                    load_buffer.lock().expect("load buffer poisoned").take();
                    return Err(BridgeError::ResumeFailed(
                        "session/load history exceeds the bridge event or byte limit".into(),
                    ));
                }
                let current = init_state.lock().expect("init_state poisoned").clone();
                let init = init_state_from_load(&resp, &current);
                return Ok((session_id, init, supports_close));
            }
            Err(error) => {
                // A strict resume never falls through to session/new. Clear
                // any replay notifications before surfacing the load error.
                let exceeded = load_buffer
                    .lock()
                    .expect("load buffer poisoned")
                    .as_ref()
                    .is_some_and(|history| history.exceeded);
                load_buffer.lock().expect("load buffer poisoned").take();
                if exceeded {
                    return Err(BridgeError::ResumeFailed(
                        "session/load history exceeds the bridge event or byte limit".into(),
                    ));
                }
                return Err(BridgeError::ResumeFailed(format!(
                    "session/load failed: {error}"
                )));
            }
        }
    }

    let mut new_session = NewSessionRequest::new(cwd);
    if !mcp_servers.is_empty() {
        new_session = new_session.mcp_servers(mcp_servers);
    }

    let session = cx
        .send_request(new_session)
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    let init = extract_init_state(&session);
    Ok((session.session_id, init, supports_close))
}

fn mcp_http_server(url: String, headers: Vec<HttpHeader>) -> McpServer {
    McpServer::Http(McpServerHttp::new(MCP_SERVER_NAME, url).headers(headers))
}

/// Merge the fields supplied by a `LoadSessionResponse` into state collected
/// while replaying the loaded session. Absent response fields do not clear the
/// replayed capability snapshot.
fn init_state_from_load(
    resp: &agent_client_protocol::schema::v1::LoadSessionResponse,
    current: &SessionInitState,
) -> SessionInitState {
    let mut init = current.clone();
    if let Some(modes) = resp.modes.as_ref() {
        init.modes = Some(modes_from_state(modes));
    }
    if let Some(config_options) = resp.config_options.as_ref() {
        init.config_options = Some(config_options.clone());
        #[cfg(feature = "unstable_session_model")]
        {
            init.models = models_from_config_options(config_options);
        }
    }
    init
}

/// Convert the stable model config option into the bridge's legacy serializable
/// model mirror. Returns `None` when the agent does not advertise a model
/// select option.
fn extract_init_state(resp: &NewSessionResponse) -> SessionInitState {
    SessionInitState {
        modes: resp.modes.as_ref().map(modes_from_state),
        #[cfg(feature = "unstable_session_model")]
        models: resp
            .config_options
            .as_deref()
            .and_then(models_from_config_options),
        #[cfg(not(feature = "unstable_session_model"))]
        models: None,
        config_options: resp.config_options.clone(),
    }
}

fn require_protocol_v1(actual: ProtocolVersion) -> Result<(), BridgeError> {
    if actual == ProtocolVersion::V1 {
        Ok(())
    } else {
        Err(BridgeError::ProtocolVersionMismatch {
            expected: ProtocolVersion::V1,
            actual,
        })
    }
}

fn modes_from_state(state: &SessionModeState) -> SessionModesInit {
    SessionModesInit {
        current_mode_id: state.current_mode_id.0.to_string(),
        available_modes: state
            .available_modes
            .iter()
            .map(|m: &SessionMode| ModeOffering {
                id: m.id.0.to_string(),
                name: m.name.clone(),
                description: m.description.clone(),
            })
            .collect(),
    }
}

#[cfg(feature = "unstable_session_model")]
fn models_from_config_options(options: &[SessionConfigOption]) -> Option<SessionModelsInit> {
    let option = options
        .iter()
        .find(|option| option.category.as_ref() == Some(&SessionConfigOptionCategory::Model))?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    let available_models = match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options
            .iter()
            .map(|model| ModelOffering {
                id: model.value.0.to_string(),
                name: model.name.clone(),
                description: model.description.clone(),
            })
            .collect(),
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .map(|model| ModelOffering {
                id: model.value.0.to_string(),
                name: model.name.clone(),
                description: model.description.clone(),
            })
            .collect(),
        _ => return None,
    };
    Some(SessionModelsInit {
        current_model_id: select.current_value.0.to_string(),
        available_models,
    })
}

/// Send `session/set_mode` and, on success, update the cached init state's
/// `current_mode_id`. Returns the agent's error if the request was rejected
/// (e.g. unknown `mode_id`).
async fn send_set_mode(
    cx: &ConnectionTo<Agent>,
    session_id: &SessionId,
    mode_id: &str,
    init_state: Arc<Mutex<SessionInitState>>,
) -> Result<(), BridgeError> {
    let mode_id_arc: agent_client_protocol::schema::v1::SessionModeId = mode_id.to_string().into();
    cx.send_request(SetSessionModeRequest::new(
        session_id.clone(),
        mode_id_arc.clone(),
    ))
    .block_task()
    .await
    .map_err(BridgeError::Acp)?;
    let mut guard = init_state.lock().expect("init_state poisoned");
    if let Some(modes) = guard.modes.as_mut() {
        modes.current_mode_id = mode_id.to_string();
    }
    Ok(())
}

fn sync_legacy_picker_state(
    init_state: &mut SessionInitState,
    config_options: &[SessionConfigOption],
) {
    let current_value = |category: SessionConfigOptionCategory| {
        config_options
            .iter()
            .find(|option| option.category.as_ref() == Some(&category))
            .and_then(|option| match &option.kind {
                SessionConfigKind::Select(select) => Some(select.current_value.0.to_string()),
                _ => None,
            })
    };

    if let Some(current_mode_id) = current_value(SessionConfigOptionCategory::Mode)
        && let Some(modes) = init_state.modes.as_mut()
    {
        modes.current_mode_id = current_mode_id;
    }
    #[cfg(feature = "unstable_session_model")]
    if let Some(current_model_id) = current_value(SessionConfigOptionCategory::Model)
        && let Some(models) = init_state.models.as_mut()
    {
        models.current_model_id = current_model_id;
    }
}

/// Send the stable ACP `session/set_config_option` request and replace the
/// cached config-option snapshot with the response's complete list.
async fn send_set_config_option(
    cx: &ConnectionTo<Agent>,
    session_id: &SessionId,
    config_id: &str,
    value: &SessionConfigOptionValue,
    init_state: Arc<Mutex<SessionInitState>>,
) -> Result<(), BridgeError> {
    let response: SetSessionConfigOptionResponse = cx
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            config_id.to_string(),
            value.clone(),
        ))
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;
    let mut guard = init_state.lock().expect("init_state poisoned");
    guard.config_options = Some(response.config_options.clone());
    sync_legacy_picker_state(&mut guard, &response.config_options);
    Ok(())
}

/// Run a prompt while concurrently watching for a cancel notification and
/// the prompt-event receiver being dropped.
///
/// On cancel **or** disconnect we send the ACP `session/cancel` notification
/// to the agent and **continue awaiting** the prompt response: the agent is
/// expected to wind down and respond with `StopReason::Cancelled`. Awaiting
/// the response is what lets the `Cancelled` envelope reach the caller's
/// `finished` oneshot.
///
/// Subsequent cancel signals while we're already awaiting are absorbed
/// silently — the first cancel did the work.
async fn run_prompt_with_cancel(
    cx: &ConnectionTo<Agent>,
    session_id: &agent_client_protocol::schema::v1::SessionId,
    prompt: Vec<ContentBlock>,
    events_tx: &mpsc::Sender<BridgeStreamItem>,
    turn: Arc<TurnState>,
    pending_permissions: PendingPermissions,
    cancel_grace_timeout: Duration,
) -> Result<StopReason, BridgeError> {
    // Register the waiter before checking the flag. `enable()` closes the
    // check/register gap: a notify_one that races this setup remains queued
    // for this future instead of being lost.
    let cancelled = turn.cancel_notify.notified();
    tokio::pin!(cancelled);
    cancelled.as_mut().enable();

    // A queued turn can be cancelled before the actor starts it. Do not send
    // an ACP prompt for a caller that has already disconnected.
    if turn.is_cancelled() {
        turn.cancel_and_drain(&pending_permissions);
        let _ = events_tx
            .send(BridgeStreamItem::Finished {
                stop_reason: StopReason::Cancelled,
            })
            .await;
        return Ok(StopReason::Cancelled);
    }

    let mut prompt_fut = std::pin::pin!(async {
        cx.send_request(PromptRequest::new(session_id.clone(), prompt))
            .block_task()
            .await
            .map_err(BridgeError::Acp)
    });

    let mut cancel_deadline = Box::pin(tokio::time::sleep(Duration::from_secs(
        100 * 365 * 24 * 60 * 60,
    )));
    let mut already_cancelled = false;
    let response = loop {
        tokio::select! {
            biased;

            res = &mut prompt_fut => break res,

            () = &mut cancelled, if !already_cancelled => {
                already_cancelled = true;
                turn.cancel_and_drain(&pending_permissions);
                let _ = cx.send_notification(
                    agent_client_protocol::schema::v1::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
                cancel_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + cancel_grace_timeout);
            }

            // If the SSE consumer drops, eagerly cancel so the agent
            // stops doing work nobody will read.
            () = events_tx.closed(), if !already_cancelled => {
                already_cancelled = true;
                turn.cancel_and_drain(&pending_permissions);
                let _ = cx.send_notification(
                    agent_client_protocol::schema::v1::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
                cancel_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + cancel_grace_timeout);
            }

            () = &mut cancel_deadline, if already_cancelled => {
                tracing::warn!(
                    ?cancel_grace_timeout,
                    "agent did not acknowledge session/cancel within grace window"
                );
                break Err(BridgeError::Timeout(cancel_grace_timeout));
            }
        }
    };

    let response = response?;
    let _ = events_tx
        .send(BridgeStreamItem::Finished {
            stop_reason: response.stop_reason,
        })
        .await;

    Ok(response.stop_reason)
}

/// Consult the configured policy and respond to a `requestPermission` request.
///
/// Decision flow:
/// - `Allow { option_id }` → respond inline with `Selected(option_id)`
/// - `Deny` → respond inline with `Cancelled`
/// - `Defer { interrupt_id }` → emit `BridgeStreamItem::Interrupt`, then
///   **spawn a task** to await an external resolution via
///   [`AcpSessionHandle::resolve_permission`] with the configured timeout
///   and respond from there. The handler returns immediately so the SDK
///   dispatch loop is **not** blocked while waiting for user approval —
///   a critical correctness property since the loop also delivers
///   `session/update` notifications and other concurrent work for the
///   same connection. Timeout / no-active-prompt fall back to `Cancelled`.
async fn handle_permission_request(
    req: RequestPermissionRequest,
    responder: agent_client_protocol::Responder<RequestPermissionResponse>,
    policy: Arc<dyn PermissionPolicy>,
    event_slot: EventSlot,
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    permission_timeout: Duration,
) -> Result<(), agent_client_protocol::Error> {
    let turn = turn_queue.current();
    let decision = policy.decide(&req).await;

    // A policy may have been awaiting its own async work when the turn was
    // cancelled. Do not let that late decision resurrect a cancelled ACP
    // permission request.
    if turn.as_ref().is_some_and(|turn| turn.is_cancelled()) {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    }

    match decision {
        PermissionDecision::Allow { option_id } => {
            responder.respond(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option_id)),
            ))
        }
        PermissionDecision::Deny => responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        )),
        PermissionDecision::Defer { interrupt_id } => {
            let Some(turn) = turn else {
                tracing::warn!("permission request arrived with no active turn; denying");
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            };

            // Look up the active prompt's event channel to emit the
            // Interrupt. Without an active prompt there's nobody listening,
            // so we deny rather than dangle the request.
            let sender = event_slot.lock().expect("event slot poisoned").clone();
            let Some(tx) = sender else {
                tracing::warn!("permission request arrived with no active prompt; denying");
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            };

            // Register the pending permission BEFORE sending the Interrupt,
            // so a resolve() call that arrives before our await on rx still
            // hits the map.
            let (resolve_tx, resolve_rx) = oneshot::channel::<PermissionDecision>();
            let valid_option_ids = req
                .options
                .iter()
                .map(|o| o.option_id.0.to_string())
                .collect::<std::collections::HashSet<_>>();
            let registered = turn.register_pending(
                &pending_permissions,
                interrupt_id.clone(),
                crate::acp::PendingPermission::new(resolve_tx, valid_option_ids, turn.clone()),
            );
            if !registered {
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            let interrupt = BridgeStreamItem::Interrupt {
                id: interrupt_id.clone(),
                request: req.clone(),
            };
            if tx.send(interrupt).await.is_err() {
                // Receiver dropped (client disconnected) — clean up and deny.
                turn.remove_pending(&pending_permissions, &interrupt_id);
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            // KEY CORRECTNESS POINT: spawn the wait off the dispatch loop.
            // Awaiting `resolve_rx` here would block every subsequent
            // notification/request the agent sends until the user
            // approves/denies (or we time out — minutes by default).
            let pending = pending_permissions.clone();
            let turn_for_wait = turn.clone();
            tokio::spawn(async move {
                let resolution = tokio::time::timeout(permission_timeout, resolve_rx).await;
                // Always remove from the pending map (even on timeout).
                turn_for_wait.remove_pending(&pending, &interrupt_id);
                let response = match resolution {
                    Ok(Ok(PermissionDecision::Allow { option_id })) => {
                        RequestPermissionResponse::new(RequestPermissionOutcome::Selected(
                            SelectedPermissionOutcome::new(option_id),
                        ))
                    }
                    Ok(Ok(PermissionDecision::Deny)) | Ok(Ok(PermissionDecision::Defer { .. })) => {
                        // A `Defer` arriving as a *resolution* makes no sense
                        // (it's the policy's initial verdict, not an answer).
                        // Treat as deny so we never leave the agent waiting.
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                    Ok(Err(_)) => {
                        // Sender dropped without resolving — treat as deny.
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                    Err(_) => {
                        tracing::warn!(
                            interrupt_id = %interrupt_id,
                            timeout_secs = permission_timeout.as_secs(),
                            "permission request timed out; denying"
                        );
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                };
                if let Err(e) = responder.respond(response) {
                    tracing::warn!(error = %e, "failed to send deferred permission response");
                }
            });
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct DenyPolicy;

    #[async_trait::async_trait]
    impl crate::policy::PermissionPolicy for DenyPolicy {
        async fn decide(
            &self,
            _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
        ) -> PermissionDecision {
            PermissionDecision::Deny
        }
    }

    async fn run_unsupported_client_methods_agent(
        stream: tokio::io::DuplexStream,
        unsupported: Arc<AtomicUsize>,
        capabilities_ok: Arc<AtomicBool>,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, CreateTerminalRequest, InitializeResponse, KillTerminalRequest,
            NewSessionRequest, NewSessionResponse, PromptResponse, ReadTextFileRequest,
            ReleaseTerminalRequest, TerminalId, TerminalOutputRequest, WaitForTerminalExitRequest,
            WriteTextFileRequest,
        };

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

        Agent
            .builder()
            .name("agui-bridge-unsupported-client-methods-test")
            .on_receive_request(
                {
                    let capabilities_ok = capabilities_ok.clone();
                    async move |req: InitializeRequest, responder, _cx| {
                        capabilities_ok.store(
                            !req.client_capabilities.fs.read_text_file
                                && !req.client_capabilities.fs.write_text_file
                                && !req.client_capabilities.terminal,
                            Ordering::SeqCst,
                        );
                        responder.respond(
                            InitializeResponse::new(req.protocol_version)
                                .agent_capabilities(AgentCapabilities::new()),
                        )
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(NewSessionResponse::new(SessionId::from("test-session")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let unsupported_for_handler = unsupported.clone();
                    async move |req: PromptRequest,
                                responder,
                                cx: ConnectionTo<agent_client_protocol::Client>| {
                        let session_id = req.session_id;
                        let terminal_id = TerminalId::new("unsupported-test-terminal");
                        let unsupported = unsupported_for_handler.clone();
                        let cx_for_requests = cx.clone();
                        cx.spawn(async move {
                            let read = cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    "/tmp/unsupported.txt",
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                read,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let write = cx_for_requests
                                .send_request(WriteTextFileRequest::new(
                                    session_id.clone(),
                                    "/tmp/unsupported.txt",
                                    "probe",
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                write,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let create = cx_for_requests
                                .send_request(CreateTerminalRequest::new(
                                    session_id.clone(),
                                    "true",
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                create,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let output = cx_for_requests
                                .send_request(TerminalOutputRequest::new(
                                    session_id.clone(),
                                    terminal_id.clone(),
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                output,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let wait = cx_for_requests
                                .send_request(WaitForTerminalExitRequest::new(
                                    session_id.clone(),
                                    terminal_id.clone(),
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                wait,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let kill = cx_for_requests
                                .send_request(KillTerminalRequest::new(
                                    session_id.clone(),
                                    terminal_id.clone(),
                                ))
                                .block_task()
                                .await;
                            if matches!(
                                kill,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            let release = cx_for_requests
                                .send_request(ReleaseTerminalRequest::new(session_id, terminal_id))
                                .block_task()
                                .await;
                            if matches!(
                                release,
                                Err(error)
                                    if matches!(
                                        error.code,
                                        agent_client_protocol::ErrorCode::MethodNotFound
                                    )
                            ) {
                                unsupported.fetch_add(1, Ordering::SeqCst);
                            }

                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    match message {
                        agent_client_protocol::Dispatch::Response(result, router) => {
                            router.route_with_result(result)
                        }
                        agent_client_protocol::Dispatch::Request(_, responder) => responder
                            .respond_with_error(agent_client_protocol::util::internal_error(
                                "unhandled request",
                            )),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_to(transport)
            .await
            .map_err(BridgeError::Acp)
    }

    #[derive(Debug)]
    struct FilesystemPolicy {
        capabilities: FileSystemCapabilities,
    }

    #[async_trait::async_trait]
    impl crate::policy::PermissionPolicy for FilesystemPolicy {
        async fn decide(
            &self,
            _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
        ) -> PermissionDecision {
            PermissionDecision::Deny
        }

        fn filesystem_capabilities(&self) -> FileSystemCapabilities {
            self.capabilities.clone()
        }
    }

    #[derive(Debug, Default, Clone)]
    struct FilesystemProbe {
        capabilities: Option<FileSystemCapabilities>,
        read: Option<Result<String, i32>>,
        write: Option<Result<(), i32>>,
        read_after_write: Option<Result<String, i32>>,
    }

    async fn run_filesystem_probe(capabilities: FileSystemCapabilities) -> FilesystemProbe {
        let raw =
            std::env::temp_dir().join(format!("agui-filesystem-session-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&raw).unwrap();
        let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
        std::fs::write(cwd.join("roundtrip.txt"), "before\n世界\n").unwrap();

        let probe = Arc::new(Mutex::new(FilesystemProbe::default()));
        let probe_for_agent = probe.clone();
        let path = cwd.join("roundtrip.txt").to_string_lossy().into_owned();
        let cfg = SessionConfig {
            cwd,
            policy: Arc::new(FilesystemPolicy { capabilities }),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        };

        let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(run_filesystem_probe_agent(stream, path, probe_for_agent))
        })
        .await
        .expect("filesystem session opens");
        let mut prompt = handle.prompt("probe").await.expect("prompt opens");
        while let Some(item) = prompt.events.recv().await {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                break;
            }
        }
        assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
        drop(handle);
        let result = probe.lock().unwrap().clone();
        let _ = std::fs::remove_dir_all(raw);
        result
    }

    async fn run_filesystem_probe_agent(
        stream: tokio::io::DuplexStream,
        path: String,
        probe: Arc<Mutex<FilesystemProbe>>,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
            PromptResponse,
        };

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

        Agent
            .builder()
            .name("agui-bridge-filesystem-test")
            .on_receive_request(
                {
                    let probe = probe.clone();
                    async move |req: InitializeRequest, responder, _cx| {
                        probe.lock().unwrap().capabilities = Some(req.client_capabilities.fs);
                        responder.respond(
                            InitializeResponse::new(req.protocol_version)
                                .agent_capabilities(AgentCapabilities::new()),
                        )
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(NewSessionResponse::new(SessionId::from("filesystem-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let probe = probe.clone();
                    async move |req: PromptRequest,
                                responder,
                                cx: ConnectionTo<agent_client_protocol::Client>| {
                        let session_id = req.session_id;
                        let path = path.clone();
                        let probe = probe.clone();
                        let cx_for_requests = cx.clone();
                        cx.spawn(async move {
                            let read = cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    path.clone(),
                                ))
                                .block_task()
                                .await;
                            probe.lock().unwrap().read = Some(match read {
                                Ok(response) => Ok(response.content),
                                Err(error) => Err(error.code.into()),
                            });

                            let write = cx_for_requests
                                .send_request(WriteTextFileRequest::new(
                                    session_id.clone(),
                                    path.clone(),
                                    "after\n",
                                ))
                                .block_task()
                                .await;
                            let write_ok = write.is_ok();
                            probe.lock().unwrap().write = Some(match write {
                                Ok(_) => Ok(()),
                                Err(error) => Err(error.code.into()),
                            });

                            if write_ok {
                                let read_after = cx_for_requests
                                    .send_request(ReadTextFileRequest::new(session_id, path))
                                    .block_task()
                                    .await;
                                probe.lock().unwrap().read_after_write = Some(match read_after {
                                    Ok(response) => Ok(response.content),
                                    Err(error) => Err(error.code.into()),
                                });
                            }
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    match message {
                        agent_client_protocol::Dispatch::Response(result, router) => {
                            router.route_with_result(result)
                        }
                        agent_client_protocol::Dispatch::Request(_, responder) => responder
                            .respond_with_error(agent_client_protocol::util::internal_error(
                                "unhandled request",
                            )),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_to(transport)
            .await
            .map_err(BridgeError::Acp)
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, Default, Clone)]
    struct CancellationProbe {
        first_write: Option<Result<(), i32>>,
        second_write: Option<Result<(), i32>>,
    }

    #[cfg(target_os = "linux")]
    async fn run_deterministic_write_cancellation_probe()
    -> (CancellationProbe, (bool, bool), (bool, bool)) {
        let raw =
            std::env::temp_dir().join(format!("agui-filesystem-cancel-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&raw).unwrap();
        let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
        let first_target = cwd.join("first.txt");
        let second_target = cwd.join("second.txt");
        let first_path = first_target.to_string_lossy().into_owned();
        let second_path = second_target.to_string_lossy().into_owned();
        let gate = Arc::new(crate::file_ops::WriteGate {
            path: first_target.clone(),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        crate::file_ops::install_write_gate(gate.clone());
        let started = gate.started.notified();
        let probe = Arc::new(Mutex::new(CancellationProbe::default()));
        let probe_for_agent = probe.clone();
        let (cancel_done_tx, cancel_done_rx) = oneshot::channel();
        let gate_for_agent = gate.clone();

        let cfg = SessionConfig {
            cwd,
            policy: Arc::new(FilesystemPolicy {
                capabilities: FileSystemCapabilities::new().write_text_file(true),
            }),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        };
        let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(run_cancellation_agent(
                stream,
                first_path,
                second_path,
                gate_for_agent,
                probe_for_agent,
                cancel_done_tx,
            ))
        })
        .await
        .expect("filesystem cancellation session opens");

        let mut prompt = handle.prompt("cancel").await.expect("prompt opens");
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .expect("first write must reach the in-flight gate");
        tokio::time::timeout(std::time::Duration::from_secs(5), cancel_done_rx)
            .await
            .expect("cancellation must be sent")
            .expect("cancellation signal must remain connected");

        let before_release = (first_target.exists(), second_target.exists());
        gate.release.notify_waiters();

        while let Some(item) = prompt.events.recv().await {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                break;
            }
        }
        assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
        drop(handle);
        let result = probe.lock().unwrap().clone();
        crate::file_ops::clear_write_gate();
        let after_release = (first_target.exists(), second_target.exists());
        let _ = std::fs::remove_dir_all(raw);
        (result, before_release, after_release)
    }

    #[cfg(target_os = "linux")]
    async fn run_cancellation_agent(
        stream: tokio::io::DuplexStream,
        first_path: String,
        second_path: String,
        gate: Arc<crate::file_ops::WriteGate>,
        probe: Arc<Mutex<CancellationProbe>>,
        cancel_done_tx: oneshot::Sender<()>,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
            PromptResponse,
        };

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

        Agent
            .builder()
            .name("agui-bridge-filesystem-cancellation-test")
            .on_receive_request(
                async move |req: InitializeRequest, responder, _cx| {
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(NewSessionResponse::new(SessionId::from("cancel-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let first_path = first_path.clone();
                    let second_path = second_path.clone();
                    let gate = gate.clone();
                    let probe = probe.clone();
                    let cancel_done_tx = Arc::new(Mutex::new(Some(cancel_done_tx)));
                    async move |req: PromptRequest,
                                responder,
                                cx: ConnectionTo<agent_client_protocol::Client>| {
                        let session_id = req.session_id;
                        let first_path = first_path.clone();
                        let second_path = second_path.clone();
                        let gate = gate.clone();
                        let probe = probe.clone();
                        let cancel_done_tx = cancel_done_tx.clone();
                        let cx_for_requests = cx.clone();
                        cx.spawn(async move {
                            let first = cx_for_requests.send_request(WriteTextFileRequest::new(
                                session_id.clone(),
                                first_path,
                                "first",
                            ));
                            gate.started.notified().await;

                            let second = cx_for_requests.send_request(WriteTextFileRequest::new(
                                session_id,
                                second_path,
                                "second",
                            ));
                            let _ = first.cancel();
                            let _ = second.cancel();
                            if let Some(tx) = cancel_done_tx.lock().unwrap().take() {
                                let _ = tx.send(());
                            }

                            let second_result = second.block_task().await;
                            probe.lock().unwrap().second_write = Some(match second_result {
                                Ok(_) => Ok(()),
                                Err(error) => Err(error.code.into()),
                            });
                            let first_result = first.block_task().await;
                            probe.lock().unwrap().first_write = Some(match first_result {
                                Ok(_) => Ok(()),
                                Err(error) => Err(error.code.into()),
                            });
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    match message {
                        agent_client_protocol::Dispatch::Response(result, router) => {
                            router.route_with_result(result)
                        }
                        agent_client_protocol::Dispatch::Request(_, responder) => responder
                            .respond_with_error(agent_client_protocol::util::internal_error(
                                "unhandled request",
                            )),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_to(transport)
            .await
            .map_err(BridgeError::Acp)
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, Clone)]
    struct BoundaryPaths {
        outside: String,
        missing: String,
        invalid_utf8: String,
        ordinary_io: String,
        oversized_write: String,
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, Default, Clone)]
    struct BoundaryProbe {
        codes: Vec<i32>,
    }

    #[cfg(target_os = "linux")]
    async fn run_filesystem_boundary_probe() -> (BoundaryProbe, bool) {
        let raw =
            std::env::temp_dir().join(format!("agui-filesystem-boundary-{}", uuid::Uuid::new_v4()));
        let outside_raw = std::env::temp_dir().join(format!(
            "agui-filesystem-boundary-outside-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&raw).unwrap();
        std::fs::create_dir_all(&outside_raw).unwrap();
        let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
        let outside = crate::file_ops::canonicalize_cwd(&outside_raw).unwrap();
        std::fs::write(outside.join("outside.txt"), "outside").unwrap();
        std::fs::write(cwd.join("invalid.txt"), [0xff, 0xfe]).unwrap();
        std::fs::write(cwd.join("not-a-directory"), "file").unwrap();
        let paths = BoundaryPaths {
            outside: outside.join("outside.txt").to_string_lossy().into_owned(),
            missing: cwd.join("missing.txt").to_string_lossy().into_owned(),
            invalid_utf8: cwd.join("invalid.txt").to_string_lossy().into_owned(),
            ordinary_io: cwd
                .join("not-a-directory")
                .join("child.txt")
                .to_string_lossy()
                .into_owned(),
            oversized_write: cwd.join("oversized.txt").to_string_lossy().into_owned(),
        };
        let probe = Arc::new(Mutex::new(BoundaryProbe::default()));
        let probe_for_agent = probe.clone();
        let cfg = SessionConfig {
            cwd,
            policy: Arc::new(FilesystemPolicy {
                capabilities: FileSystemCapabilities::new()
                    .read_text_file(true)
                    .write_text_file(true),
            }),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        };
        let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(run_boundary_agent(stream, paths, probe_for_agent))
        })
        .await
        .expect("filesystem boundary session opens");
        let mut prompt = handle.prompt("boundary").await.expect("prompt opens");
        while let Some(item) = prompt.events.recv().await {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                break;
            }
        }
        assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
        drop(handle);
        let result = probe.lock().unwrap().clone();
        let oversized_exists = raw.join("oversized.txt").exists();
        let _ = std::fs::remove_dir_all(raw);
        let _ = std::fs::remove_dir_all(outside_raw);
        (result, oversized_exists)
    }

    #[cfg(target_os = "linux")]
    async fn run_boundary_agent(
        stream: tokio::io::DuplexStream,
        paths: BoundaryPaths,
        probe: Arc<Mutex<BoundaryProbe>>,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
            PromptResponse,
        };

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
        Agent
            .builder()
            .name("agui-bridge-filesystem-boundary-test")
            .on_receive_request(
                async move |req: InitializeRequest, responder, _cx| {
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(NewSessionResponse::new(SessionId::from("boundary-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let probe = probe.clone();
                    async move |req: PromptRequest,
                                responder,
                                cx: ConnectionTo<agent_client_protocol::Client>| {
                        let session_id = req.session_id;
                        let paths = paths.clone();
                        let probe = probe.clone();
                        let cx_for_requests = cx.clone();
                        cx.spawn(async move {
                            let code = |result: Result<
                                agent_client_protocol::schema::v1::ReadTextFileResponse,
                                agent_client_protocol::Error,
                            >| match result {
                                Ok(_) => 0,
                                Err(error) => error.code.into(),
                            };
                            let mut codes = Vec::with_capacity(6);
                            codes.push(
                                code(cx_for_requests
                                    .send_request(ReadTextFileRequest::new(
                                        session_id.clone(),
                                        "relative.txt",
                                    ))
                                    .block_task()
                                    .await),
                            );
                            codes.push(
                                code(cx_for_requests
                                    .send_request(ReadTextFileRequest::new(
                                        session_id.clone(),
                                        paths.outside,
                                    ))
                                    .block_task()
                                    .await),
                            );
                            codes.push(
                                code(cx_for_requests
                                    .send_request(ReadTextFileRequest::new(
                                        session_id.clone(),
                                        paths.missing,
                                    ))
                                    .block_task()
                                    .await),
                            );
                            codes.push(
                                code(cx_for_requests
                                    .send_request(ReadTextFileRequest::new(
                                        session_id.clone(),
                                        paths.invalid_utf8,
                                    ))
                                    .block_task()
                                    .await),
                            );

                            let ordinary_io = cx_for_requests
                                .send_request(WriteTextFileRequest::new(
                                    session_id.clone(),
                                    paths.ordinary_io,
                                    "ordinary I/O error",
                                ))
                                .block_task()
                                .await;
                            codes.push(match ordinary_io {
                                Ok(_) => 0,
                                Err(error) => error.code.into(),
                            });

                            let oversized = cx_for_requests
                                .send_request(WriteTextFileRequest::new(
                                    session_id,
                                    paths.oversized_write,
                                    "x".repeat(crate::file_ops::MAX_TEXT_FILE_BYTES + 1),
                                ))
                                .block_task()
                                .await;
                            codes.push(match oversized {
                                Ok(_) => 0,
                                Err(error) => error.code.into(),
                            });
                            probe.lock().unwrap().codes = codes;
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    }
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    match message {
                        agent_client_protocol::Dispatch::Response(result, router) => {
                            router.route_with_result(result)
                        }
                        agent_client_protocol::Dispatch::Request(_, responder) => responder
                            .respond_with_error(agent_client_protocol::util::internal_error(
                                "unhandled request",
                            )),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_to(transport)
            .await
            .map_err(BridgeError::Acp)
    }

    #[test]
    fn successful_setting_ack_send_failure_marks_session_unusable() {
        let unusable = AtomicBool::new(false);
        let (ack, receiver) = oneshot::channel();
        drop(receiver);

        // `Ok(())` models a completed ACP setting RPC whose caller vanished
        // before the actor could deliver the result.
        assert!(!send_setting_ack(ack, Ok(()), &unusable));
        assert!(unusable.load(Ordering::Acquire));
    }

    #[test]
    fn load_init_state_merges_replayed_capabilities_with_partial_response() {
        use agent_client_protocol::schema::v1::{
            LoadSessionResponse, SessionConfigKind, SessionConfigOption,
            SessionConfigOptionCategory, SessionConfigSelect, SessionConfigSelectOption,
            SessionMode,
        };

        let replayed_options = vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "replayed-mode",
                    vec![SessionConfigSelectOption::new(
                        "replayed-mode",
                        "Replayed mode",
                    )],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "replayed-model",
                    vec![SessionConfigSelectOption::new(
                        "replayed-model",
                        "Replayed model",
                    )],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ];
        let replayed_mode = SessionModeState::new(
            "replayed-mode",
            vec![SessionMode::new("replayed-mode", "Replayed mode")],
        );
        let replayed = SessionInitState {
            modes: Some(modes_from_state(&replayed_mode)),
            #[cfg(feature = "unstable_session_model")]
            models: models_from_config_options(&replayed_options),
            #[cfg(not(feature = "unstable_session_model"))]
            models: None,
            config_options: Some(replayed_options.clone()),
        };

        let response_mode = SessionModeState::new(
            "response-mode",
            vec![SessionMode::new("response-mode", "Response mode")],
        );
        let merged =
            init_state_from_load(&LoadSessionResponse::new().modes(response_mode), &replayed);
        assert_eq!(
            merged
                .modes
                .as_ref()
                .map(|modes| modes.current_mode_id.as_str()),
            Some("response-mode")
        );
        assert_eq!(merged.config_options, Some(replayed_options.clone()));
        #[cfg(feature = "unstable_session_model")]
        assert_eq!(
            merged
                .models
                .as_ref()
                .map(|models| models.current_model_id.as_str()),
            Some("replayed-model")
        );

        let response_options = vec![
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "response-model",
                    vec![SessionConfigSelectOption::new(
                        "response-model",
                        "Response model",
                    )],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ];
        let merged = init_state_from_load(
            &LoadSessionResponse::new().config_options(response_options.clone()),
            &replayed,
        );
        assert_eq!(
            merged
                .modes
                .as_ref()
                .map(|modes| modes.current_mode_id.as_str()),
            Some("replayed-mode")
        );
        assert_eq!(merged.config_options, Some(response_options));
        #[cfg(feature = "unstable_session_model")]
        assert_eq!(
            merged
                .models
                .as_ref()
                .map(|models| models.current_model_id.as_str()),
            Some("response-model")
        );
    }

    #[test]
    fn mcp_http_server_propagates_authorization_header() {
        let server = mcp_http_server(
            "http://127.0.0.1:8080/mcp/thread".into(),
            vec![HttpHeader::new("Authorization", "Bearer test-token")],
        );
        let McpServer::Http(server) = server else {
            unreachable!("helper must build an HTTP MCP server");
        };
        assert_eq!(server.headers.len(), 1);
        assert_eq!(server.headers[0].name, "Authorization");
        assert_eq!(server.headers[0].value, "Bearer test-token");
    }

    #[tokio::test]
    async fn unadvertised_filesystem_and_terminal_methods_are_not_found() {
        let unsupported = Arc::new(AtomicUsize::new(0));
        let capabilities_ok = Arc::new(AtomicBool::new(false));
        let unsupported_for_agent = unsupported.clone();
        let capabilities_for_agent = capabilities_ok.clone();
        let cfg = SessionConfig {
            cwd: PathBuf::from("/"),
            policy: Arc::new(DenyPolicy),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        };
        let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(run_unsupported_client_methods_agent(
                stream,
                unsupported_for_agent,
                capabilities_for_agent,
            ))
        })
        .await
        .expect("session opens");

        let mut prompt =
            tokio::time::timeout(std::time::Duration::from_secs(5), handle.prompt("probe"))
                .await
                .expect("prompt opens before timeout")
                .expect("prompt opens");
        while let Some(item) =
            tokio::time::timeout(std::time::Duration::from_secs(5), prompt.events.recv())
                .await
                .expect("prompt event arrives before timeout")
        {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                break;
            }
        }
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), prompt.finished)
                .await
                .expect("finished result arrives before timeout")
                .expect("finished sender remains")
                .expect("prompt succeeds"),
            StopReason::EndTurn
        );
        assert!(capabilities_ok.load(Ordering::SeqCst));
        assert_eq!(unsupported.load(Ordering::SeqCst), 7);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn read_and_write_capabilities_are_independent_and_roundtrip() {
        if !crate::file_ops::read_text_file_supported() {
            let unsupported = run_filesystem_probe(
                FileSystemCapabilities::new()
                    .read_text_file(true)
                    .write_text_file(true),
            )
            .await;
            assert_eq!(
                unsupported.capabilities,
                Some(FileSystemCapabilities::default())
            );
            assert_eq!(unsupported.read, Some(Err(-32601)));
            assert_eq!(unsupported.write, Some(Err(-32601)));
            assert_eq!(unsupported.read_after_write, None);
            return;
        }

        let both = run_filesystem_probe(
            FileSystemCapabilities::new()
                .read_text_file(true)
                .write_text_file(true),
        )
        .await;
        assert_eq!(
            both.capabilities,
            Some(
                FileSystemCapabilities::new()
                    .read_text_file(true)
                    .write_text_file(true)
            )
        );
        assert_eq!(both.read, Some(Ok("before\n世界\n".into())));
        assert_eq!(both.write, Some(Ok(())));
        assert_eq!(both.read_after_write, Some(Ok("after\n".into())));

        let read_only =
            run_filesystem_probe(FileSystemCapabilities::new().read_text_file(true)).await;
        assert_eq!(
            read_only.capabilities,
            Some(FileSystemCapabilities::new().read_text_file(true))
        );
        assert_eq!(read_only.read, Some(Ok("before\n世界\n".into())));
        assert_eq!(read_only.write, Some(Err(-32601)));
        assert_eq!(read_only.read_after_write, None);

        let write_only =
            run_filesystem_probe(FileSystemCapabilities::new().write_text_file(true)).await;
        assert_eq!(
            write_only.capabilities,
            Some(FileSystemCapabilities::new().write_text_file(true))
        );
        assert_eq!(write_only.read, Some(Err(-32601)));
        assert_eq!(write_only.write, Some(Ok(())));
        assert_eq!(write_only.read_after_write, Some(Err(-32601)));
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn unsupported_filesystem_capabilities_are_not_advertised() {
        let probe = run_filesystem_probe(
            FileSystemCapabilities::new()
                .read_text_file(true)
                .write_text_file(true),
        )
        .await;
        assert_eq!(probe.capabilities, Some(FileSystemCapabilities::default()));
        assert_eq!(probe.read, Some(Err(-32601)));
        assert_eq!(probe.write, Some(Err(-32601)));
        assert_eq!(probe.read_after_write, None);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn filesystem_requests_return_exact_boundary_error_codes() {
        if !crate::file_ops::read_text_file_supported() {
            return;
        }
        let (probe, oversized_exists) = run_filesystem_boundary_probe().await;
        assert_eq!(
            probe.codes,
            vec![-32602, -32602, -32002, -32603, -32603, -32602]
        );
        assert!(
            !oversized_exists,
            "an oversized write must be rejected before touching disk"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn write_cancellation_is_deterministic_before_and_after_start() {
        if !crate::file_ops::read_text_file_supported() {
            return;
        }
        let (probe, before_release, after_release) =
            run_deterministic_write_cancellation_probe().await;
        assert_eq!(probe.first_write, Some(Ok(())));
        assert_eq!(probe.second_write, Some(Err(-32800)));
        assert_eq!(before_release, (false, false));
        assert_eq!(after_release, (true, false));
    }

    #[test]
    fn filesystem_handler_error_constructors_use_exact_acp_codes() {
        assert_eq!(
            i32::from(agent_client_protocol::Error::method_not_found().code),
            -32601
        );
        assert_eq!(
            i32::from(agent_client_protocol::Error::invalid_params().code),
            -32602
        );
        assert_eq!(
            i32::from(agent_client_protocol::Error::resource_not_found(None).code),
            -32002
        );
        assert_eq!(
            i32::from(agent_client_protocol::Error::internal_error().code),
            -32603
        );
        assert_eq!(
            i32::from(agent_client_protocol::Error::request_cancelled().code),
            -32800
        );
    }

    #[test]
    fn load_history_limits_events_and_bytes_without_reordering() {
        use agent_client_protocol::schema::v1::{CurrentModeUpdate, SessionUpdate};

        let first = SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("first"));
        let second = SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("second"));
        let mut history = LoadHistory::default();
        history.append(first.clone(), 1).expect("first fits");
        history.append(second.clone(), 1).expect("second fits");
        assert_eq!(history.updates, vec![first, second]);

        for _ in 2..MAX_LOAD_HISTORY_EVENTS {
            history
                .append(
                    SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("event")),
                    1,
                )
                .expect("event fits");
        }
        assert!(
            history
                .append(
                    SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("over")),
                    1,
                )
                .is_err()
        );
        assert_eq!(history.updates.len(), MAX_LOAD_HISTORY_EVENTS);
        assert!(history.exceeded);

        let mut bytes = LoadHistory::default();
        bytes
            .append(
                SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("bytes")),
                MAX_LOAD_HISTORY_BYTES,
            )
            .expect("byte limit itself fits");
        assert!(
            bytes
                .append(
                    SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("over")),
                    1,
                )
                .is_err()
        );
        assert_eq!(bytes.bytes, MAX_LOAD_HISTORY_BYTES);
    }

    #[test]
    fn session_list_limits_entries_bytes_and_pages_with_errors() {
        let summary = |id: &str| SessionSummary {
            session_id: id.into(),
            cwd: "/".into(),
            title: None,
            updated_at: None,
        };
        let mut entries = BoundedSessionList::default();
        for index in 0..MAX_LIST_SESSIONS {
            entries
                .push_with_size(summary(&index.to_string()), 1)
                .expect("entry fits");
        }
        let entry_error = entries
            .push_with_size(summary("over"), 1)
            .expect_err("entry limit must be reported");
        assert_eq!(
            i32::from(match entry_error {
                BridgeError::Acp(error) => error.code,
                other => panic!("unexpected error: {other:?}"),
            }),
            -32800
        );
        assert_eq!(entries.len(), MAX_LIST_SESSIONS);

        let mut bytes = BoundedSessionList::default();
        bytes
            .push_with_size(summary("bytes"), MAX_LIST_BYTES)
            .expect("byte limit itself fits");
        assert!(bytes.push_with_size(summary("over"), 1).is_err());
        assert_eq!(bytes.len(), 1);

        assert!(next_list_cursor(MAX_LIST_PAGES - 1, Some("next".into())).is_ok());
        assert!(next_list_cursor(MAX_LIST_PAGES, Some("next".into())).is_err());
        assert!(next_list_cursor(MAX_LIST_PAGES, None).is_ok());
    }
}
