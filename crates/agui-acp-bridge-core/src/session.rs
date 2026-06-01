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
//! - `ReadTextFileRequest` / `WriteTextFileRequest` — sandboxed file I/O.
//! - `CreateTerminalRequest` / `TerminalOutputRequest` — stub responses.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::{
    ContentBlock, CreateTerminalRequest, CreateTerminalResponse, InitializeRequest, McpServer,
    McpServerHttp, NewSessionRequest, NewSessionResponse, PromptRequest, ProtocolVersion,
    ReadTextFileRequest, ReadTextFileResponse, RequestPermissionOutcome, RequestPermissionRequest,
    RequestPermissionResponse, SelectedPermissionOutcome, SessionId, SessionMode, SessionModeState,
    SessionNotification, SetSessionModeRequest, StopReason, TerminalOutputRequest,
    TerminalOutputResponse, TextContent, WriteTextFileRequest, WriteTextFileResponse,
};
use agent_client_protocol::{Agent, Client, ConnectTo, ConnectionTo};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::acp::{
    AcpSessionHandle, PendingPermissions, SessionCommand, SessionConfig, SessionInitState,
};
use crate::echo_agent;
use crate::error::BridgeError;
use crate::file_ops;
use crate::policy::{PermissionDecision, PermissionPolicy};
use crate::stream::{BridgeStreamItem, ModeOffering, SessionModesInit, SessionSummary};
#[cfg(feature = "unstable_session_model")]
use crate::stream::{ModelOffering, SessionModelsInit};

/// MCP server name advertised on `NewSessionRequest.mcp_servers`. Agents
/// typically prefix the tool names they surface to their LLM with this
/// (e.g. opencode renders our `say_hello` tool as
/// `agui-acp-bridge_say_hello`). Exported so the handler can compute the
/// prefixed variants for the translator's suppression filter.
pub const MCP_SERVER_NAME: &str = "agui-acp-bridge";

const COMMAND_BUFFER: usize = 8;
const IN_PROCESS_DUPLEX_BUFFER: usize = 65_536;

type EventSlot = Arc<Mutex<Option<mpsc::Sender<BridgeStreamItem>>>>;

/// Captures `session/update` notifications replayed by the agent during a
/// `session/load` call. The agent streams the conversation history as
/// notifications *before* any prompt is active, so they would otherwise be
/// dropped ("no active prompt"). When `Some`, the notification handler
/// appends each update here instead; the actor flushes the buffer onto the
/// first prompt's stream so the resuming client sees its prior conversation.
type LoadBuffer = Arc<Mutex<Option<Vec<agent_client_protocol::schema::SessionUpdate>>>>;

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

    tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process test agent terminated with error");
        }
    });

    let (read, write) = tokio::io::split(client_stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    spawn_session(transport, cfg).await
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

pub(crate) async fn spawn_session<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(COMMAND_BUFFER);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<SessionInitState, BridgeError>>();
    let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
    let cancel_notify = Arc::new(tokio::sync::Notify::new());
    let init_state = Arc::new(Mutex::new(SessionInitState::default()));

    let handle_pending = pending_permissions.clone();
    let handle_cancel_notify = cancel_notify.clone();
    let handle_init_state = init_state.clone();
    let event_buffer = cfg.config.event_buffer;

    tokio::spawn(run_actor(
        connector,
        cfg,
        cmd_rx,
        ready_tx,
        pending_permissions,
        cancel_notify,
        init_state,
    ));

    match ready_rx.await {
        Ok(Ok(_)) => Ok(AcpSessionHandle::new(
            cmd_tx,
            handle_cancel_notify,
            handle_pending,
            handle_init_state,
            event_buffer,
        )),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BridgeError::SessionClosed),
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

    // A minimal client: we issue requests from the connection task and never
    // receive notifications/requests we care about, so the builder only needs
    // a dispatch handler to route responses back to their awaiters.
    let result_tx = std::sync::Mutex::new(Some(result_tx));
    let connect_result = agent_client_protocol::Client
        .builder()
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.respond_with_result(result)
                    }
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let result_slot = result_tx;
            async move {
                let outcome = list_sessions_inner(&cx, cwd).await;
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

/// Inner body of [`list_sessions_via`]: initialize, capability-gate, then
/// page through `session/list`.
async fn list_sessions_inner(
    cx: &ConnectionTo<Agent>,
    _cwd: PathBuf,
) -> Result<Vec<SessionSummary>, BridgeError> {
    use agent_client_protocol::schema::ListSessionsRequest;

    let init = cx
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    if init.agent_capabilities.session_capabilities.list.is_none() {
        return Err(BridgeError::Unsupported("session/list".into()));
    }

    let mut summaries: Vec<SessionSummary> = Vec::new();
    let mut cursor: Option<String> = None;
    // Guard against a misbehaving agent returning an endless cursor chain.
    let mut pages = 0u32;
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
        let resp = cx
            .send_request(req)
            .block_task()
            .await
            .map_err(BridgeError::Acp)?;

        tracing::debug!(
            page = pages,
            count = resp.sessions.len(),
            has_next = resp.next_cursor.is_some(),
            "session/list page received"
        );

        for info in resp.sessions {
            summaries.push(SessionSummary {
                session_id: info.session_id.0.to_string(),
                cwd: info.cwd.to_string_lossy().into_owned(),
                title: info.title,
                updated_at: info.updated_at,
            });
        }

        pages += 1;
        match resp.next_cursor {
            Some(next) if pages < 1000 => cursor = Some(next),
            _ => break,
        }
    }

    tracing::info!(total = summaries.len(), "session/list complete");
    Ok(summaries)
}

async fn run_actor<T>(
    connector: T,
    cfg: SessionConfig,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    ready_tx: oneshot::Sender<Result<SessionInitState, BridgeError>>,
    pending_permissions: PendingPermissions,
    cancel_notify: Arc<tokio::sync::Notify>,
    init_state: Arc<Mutex<SessionInitState>>,
) where
    T: ConnectTo<Client> + Send + 'static,
{
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
        load_session_id,
    } = cfg;
    let permission_timeout = config.permission_timeout;
    let cwd = Arc::new(cwd);
    let cwd_for_read = cwd.clone();
    let cwd_for_write = cwd.clone();

    let pending_perms_for_handler = pending_permissions.clone();
    let pending_perms_for_drain = pending_permissions.clone();

    let ready_tx = std::sync::Mutex::new(Some(ready_tx));

    let result = agent_client_protocol::Client
        .builder()
        .on_receive_notification(
            {
                let init_state = init_state.clone();
                async move |notification: SessionNotification, _cx| {
                    // Keep the cached init state in sync with autonomous mode
                    // changes so the next prompt's SessionInit emission shows
                    // the right current mode. We still forward the
                    // notification verbatim — the translator already turns
                    // CurrentModeUpdate into an `agent:mode_update` CUSTOM
                    // event for live UI updates.
                    if let agent_client_protocol::schema::SessionUpdate::CurrentModeUpdate(ref m) =
                        notification.update
                    {
                        let new_id = m.current_mode_id.0.to_string();
                        let mut guard = init_state.lock().expect("init_state poisoned");
                        if let Some(modes) = guard.modes.as_mut() {
                            modes.current_mode_id = new_id;
                        }
                    }
                    // If a session/load is in progress, the agent is replaying
                    // history. Capture those updates into the load buffer (no
                    // prompt is active yet) so the actor can flush them onto
                    // the first prompt's stream.
                    {
                        let mut buf = load_buffer_for_notif.lock().expect("load buffer poisoned");
                        if let Some(history) = buf.as_mut() {
                            history.push(notification.update);
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
                        permission_timeout,
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: ReadTextFileRequest, responder, _cx| {
                let path = req.path.to_string_lossy();
                let limit = req.limit.map(|l| l as usize);
                match file_ops::read_text_file(&cwd_for_read, &path, limit).await {
                    Ok(content) => responder.respond(ReadTextFileResponse::new(content)),
                    Err(e) => {
                        responder.respond_with_error(agent_client_protocol::util::internal_error(
                            format!("read_text_file failed: {e}"),
                        ))
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: WriteTextFileRequest, responder, _cx| {
                let path = req.path.to_string_lossy();
                let content = &req.content;
                match file_ops::write_text_file(&cwd_for_write, &path, content).await {
                    Ok(()) => responder.respond(WriteTextFileResponse::new()),
                    Err(e) => {
                        responder.respond_with_error(agent_client_protocol::util::internal_error(
                            format!("write_text_file failed: {e}"),
                        ))
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: CreateTerminalRequest, responder, _cx| {
                let terminal_id = uuid::Uuid::new_v4().to_string();
                responder.respond(CreateTerminalResponse::new(terminal_id))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: TerminalOutputRequest, responder, _cx| {
                responder.respond(TerminalOutputResponse::new("", false))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let ready_slot = ready_tx;
            let event_slot = event_slot_for_session;
            let pending_for_drain = pending_permissions.clone();
            let cancel_notify = cancel_notify.clone();
            let mut cmd_rx = cmd_rx;
            let mcp_url = mcp_url.clone();
            let init_state = init_state.clone();
            let load_session_id = load_session_id.clone();
            let load_buffer = load_buffer_for_session;
            async move {
                let session_id = match initialize(
                    &cx,
                    cwd.as_ref().clone(),
                    mcp_url,
                    load_session_id,
                    &load_buffer,
                )
                .await
                {
                    Ok((id, init)) => {
                        *init_state.lock().expect("init_state poisoned") = init.clone();
                        if let Some(tx) = ready_slot.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Ok(init));
                        }
                        id
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
                            text,
                            events_tx,
                            finished_tx,
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
                                for update in history {
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
                            // a cancel notification (via Notify) and for
                            // the SSE consumer dropping. A naive serial
                            // `await` would let cancel signals miss the
                            // window — see audit P0 "session.cancel()
                            // cannot interrupt".
                            let res = run_prompt_with_cancel(
                                &cx,
                                &session_id,
                                text,
                                &events_tx,
                                cancel_notify.as_ref(),
                            )
                            .await;

                            *event_slot.lock().expect("event slot poisoned") = None;

                            let _ = finished_tx.send(res);
                            drop(events_tx);
                        }
                        SessionCommand::SetMode { mode_id, ack } => {
                            // Run set_mode off the main loop so a long
                            // in-flight prompt does NOT block mode
                            // switches. ConnectionTo<Agent> is Clone +
                            // Send and the SDK's request layer is
                            // multiplexed by id, so concurrent
                            // session/set_mode and session/prompt are
                            // safe to dispatch.
                            let cx2 = cx.clone();
                            let sid = session_id.clone();
                            let init_state = init_state.clone();
                            tokio::spawn(async move {
                                let res = send_set_mode(&cx2, &sid, &mode_id, init_state).await;
                                let _ = ack.send(res);
                            });
                        }
                        #[cfg(feature = "unstable_session_model")]
                        SessionCommand::SetModel { model_id, ack } => {
                            let cx2 = cx.clone();
                            let sid = session_id.clone();
                            let init_state = init_state.clone();
                            tokio::spawn(async move {
                                let res = send_set_model(&cx2, &sid, &model_id, init_state).await;
                                let _ = ack.send(res);
                            });
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
                                })
                                .await;
                            let replay = load_buffer.lock().expect("load buffer poisoned").take();
                            if let Some(history) = replay {
                                for update in history {
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

async fn initialize(
    cx: &ConnectionTo<Agent>,
    cwd: PathBuf,
    mcp_url: Option<String>,
    load_session_id: Option<String>,
    load_buffer: &LoadBuffer,
) -> Result<(SessionId, SessionInitState), BridgeError> {
    // Send the agent a conventional absolute cwd (no Windows `\\?\` verbatim
    // prefix) so its persisted session directory matches what other tools use
    // and directory-scoped `session/list` can find it later.
    let cwd = crate::file_ops::acp_cwd(&cwd);
    let init_response = cx
        .send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    // Compose the optional MCP server entry once; both new and load requests
    // carry it so frontend tools work on resumed sessions too.
    let mcp_servers: Vec<McpServer> = match mcp_url {
        Some(url) if init_response.agent_capabilities.mcp_capabilities.http => {
            vec![McpServer::Http(McpServerHttp::new(MCP_SERVER_NAME, url))]
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

    // Resume path: load an existing session if the caller asked for it AND
    // the agent advertises the `loadSession` capability. The agent replays
    // the conversation history as `session/update` notifications during the
    // call; those are captured into `load_buffer` (see the notification
    // handler) so the handler can surface them on the resume run's stream.
    if let Some(sid) = load_session_id {
        if init_response.agent_capabilities.load_session {
            use agent_client_protocol::schema::LoadSessionRequest;
            let session_id = SessionId::from(sid);
            // Arm the buffer so history notifications are captured rather
            // than dropped ("no active prompt").
            load_buffer
                .lock()
                .expect("load buffer poisoned")
                .replace(Vec::new());
            let mut req = LoadSessionRequest::new(session_id.clone(), cwd.clone());
            if !mcp_servers.is_empty() {
                req = req.mcp_servers(mcp_servers.clone());
            }
            let load = cx
                .send_request(req)
                .block_task()
                .await
                .map_err(BridgeError::Acp);
            match load {
                Ok(resp) => {
                    let init = init_state_from_load(&resp);
                    return Ok((session_id, init));
                }
                Err(e) => {
                    // Disarm the buffer and fall through to a fresh session
                    // so a stale/invalid id doesn't hard-fail the run.
                    load_buffer.lock().expect("load buffer poisoned").take();
                    tracing::warn!(error = %e, "session/load failed; creating a fresh session");
                }
            }
        } else {
            tracing::warn!(
                "agent does not advertise loadSession capability; \
                 creating a fresh session instead of resuming"
            );
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
    Ok((session.session_id, init))
}

/// Extract init state from a `LoadSessionResponse` (mirrors
/// [`extract_init_state`] for `NewSessionResponse`).
fn init_state_from_load(
    resp: &agent_client_protocol::schema::LoadSessionResponse,
) -> SessionInitState {
    SessionInitState {
        modes: resp.modes.as_ref().map(modes_from_state),
        #[cfg(feature = "unstable_session_model")]
        models: resp.models.as_ref().map(models_from_state),
        #[cfg(not(feature = "unstable_session_model"))]
        models: None,
    }
}

/// Convert ACP-schema `SessionModeState` / `SessionModelState` into
/// the bridge's serializable mirrors. Returns `None` when the agent
/// did not advertise the corresponding capability.
fn extract_init_state(resp: &NewSessionResponse) -> SessionInitState {
    SessionInitState {
        modes: resp.modes.as_ref().map(modes_from_state),
        #[cfg(feature = "unstable_session_model")]
        models: resp.models.as_ref().map(models_from_state),
        #[cfg(not(feature = "unstable_session_model"))]
        models: None,
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
fn models_from_state(
    state: &agent_client_protocol::schema::SessionModelState,
) -> SessionModelsInit {
    SessionModelsInit {
        current_model_id: state.current_model_id.0.to_string(),
        available_models: state
            .available_models
            .iter()
            .map(|m| ModelOffering {
                id: m.model_id.0.to_string(),
                name: m.name.clone(),
                description: m.description.clone(),
            })
            .collect(),
    }
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
    let mode_id_arc: agent_client_protocol::schema::SessionModeId = mode_id.to_string().into();
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

/// Send `session/set_model` and, on success, update the cached init state's
/// `current_model_id`. Only compiled when `unstable_session_model` is on.
#[cfg(feature = "unstable_session_model")]
async fn send_set_model(
    cx: &ConnectionTo<Agent>,
    session_id: &SessionId,
    model_id: &str,
    init_state: Arc<Mutex<SessionInitState>>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::{ModelId, SetSessionModelRequest};
    let model_id_arc: ModelId = model_id.to_string().into();
    cx.send_request(SetSessionModelRequest::new(
        session_id.clone(),
        model_id_arc,
    ))
    .block_task()
    .await
    .map_err(BridgeError::Acp)?;
    let mut guard = init_state.lock().expect("init_state poisoned");
    if let Some(models) = guard.models.as_mut() {
        models.current_model_id = model_id.to_string();
    }
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
    session_id: &agent_client_protocol::schema::SessionId,
    text: String,
    events_tx: &mpsc::Sender<BridgeStreamItem>,
    cancel_notify: &tokio::sync::Notify,
) -> Result<StopReason, BridgeError> {
    // Capture a notified() future BEFORE issuing the prompt, so we don't
    // miss a cancel that arrives between issuing the request and reaching
    // the select! below.
    let cancelled = cancel_notify.notified();
    tokio::pin!(cancelled);

    let mut prompt_fut = std::pin::pin!(async {
        cx.send_request(PromptRequest::new(
            session_id.clone(),
            vec![ContentBlock::Text(TextContent::new(text))],
        ))
        .block_task()
        .await
        .map_err(BridgeError::Acp)
    });

    let mut already_cancelled = false;
    let response = loop {
        tokio::select! {
            biased;

            res = &mut prompt_fut => break res,

            () = &mut cancelled, if !already_cancelled => {
                already_cancelled = true;
                let _ = cx.send_notification(
                    agent_client_protocol::schema::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
            }

            // If the SSE consumer drops, eagerly cancel so the agent
            // stops doing work nobody will read.
            () = events_tx.closed(), if !already_cancelled => {
                already_cancelled = true;
                let _ = cx.send_notification(
                    agent_client_protocol::schema::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
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
    permission_timeout: Duration,
) -> Result<(), agent_client_protocol::Error> {
    let decision = policy.decide(&req).await;

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
            pending_permissions.insert(
                interrupt_id.clone(),
                crate::acp::PendingPermission::new(resolve_tx, valid_option_ids),
            );

            let interrupt = BridgeStreamItem::Interrupt {
                id: interrupt_id.clone(),
                request: req.clone(),
            };
            if tx.send(interrupt).await.is_err() {
                // Receiver dropped (client disconnected) — clean up and deny.
                pending_permissions.remove(&interrupt_id);
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            // KEY CORRECTNESS POINT: spawn the wait off the dispatch loop.
            // Awaiting `resolve_rx` here would block every subsequent
            // notification/request the agent sends until the user
            // approves/denies (or we time out — minutes by default).
            let pending = pending_permissions.clone();
            tokio::spawn(async move {
                let resolution = tokio::time::timeout(permission_timeout, resolve_rx).await;
                // Always remove from the pending map (even on timeout).
                pending.remove(&interrupt_id);
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

// Subprocess-driven tests live in `core/tests/process_echo.rs`.
// The in-process echo path is covered by `core/tests/in_process_echo.rs`
// and by the server crate's HTTP/SSE roundtrip suite. Adding focused unit
// tests directly on the actor would require leaking internals; we prefer
// the integration-test approach.
