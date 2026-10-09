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
//! the slot is empty are spilled into a bounded buffer (capacity
//! [`SPILL_CAPACITY`]) that the next prompt drains ahead of its own events;
//! overflow warns and drops — they would be ACP protocol violations
//! (notification outside any active turn).
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

use std::future::Future;
use std::io::Write;
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
#[cfg(test)]
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
const EVENT_DRIVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EVENT_ITEM_LIMIT: usize = 4096;
const EVENT_BYTE_LIMIT: usize = 16 * 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = EVENT_ITEM_LIMIT + 1;
const FAILED_EVENT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
const SPAWNED_WORK_ITEMS: usize = 128;
const SPAWNED_WORK_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct WorkUsage {
    items: usize,
    bytes: usize,
}

struct WorkAdmission {
    usage: Mutex<WorkUsage>,
    limits: (usize, usize),
    retire_tx: tokio::sync::watch::Sender<Option<agent_client_protocol::Error>>,
}

#[derive(Clone)]
pub(crate) struct WorkPermit {
    _lease: Arc<WorkLease>,
}

struct WorkLease {
    admission: Arc<WorkAdmission>,
    bytes: usize,
}

impl Drop for WorkLease {
    fn drop(&mut self) {
        let mut usage = self.admission.usage.lock().expect("work usage poisoned");
        usage.items -= 1;
        usage.bytes -= self.bytes;
    }
}

impl WorkAdmission {
    fn new() -> Arc<Self> {
        Self::with_limits(SPAWNED_WORK_ITEMS, SPAWNED_WORK_BYTES)
    }

    fn with_limits(items: usize, bytes: usize) -> Arc<Self> {
        let (retire_tx, _) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            usage: Mutex::new(WorkUsage::default()),
            limits: (items, bytes),
            retire_tx,
        })
    }

    fn try_acquire<T: serde::Serialize, I: serde::Serialize + ?Sized>(
        self: &Arc<Self>,
        request: &T,
        id: &I,
    ) -> Result<WorkPermit, ()> {
        let bytes = json_size_bounded(&(request, id), self.limits.1);
        let mut usage = self.usage.lock().expect("work usage poisoned");
        if self.retire_tx.borrow().is_some()
            || usage.items >= self.limits.0
            || bytes == usize::MAX
            || bytes > self.limits.1
            || usage.bytes > self.limits.1 - bytes
        {
            drop(usage);
            self.retire_tx.send_if_modified(|current| {
                if current.is_none() {
                    *current = Some(agent_client_protocol::Error::internal_error().data(serde_json::json!({"limit":"ACP_SPAWNED_WORK","items":self.limits.0,"bytes":self.limits.1})));
                    true
                } else { false }
            });
            return Err(());
        }
        usage.items += 1;
        usage.bytes += bytes;
        Ok(WorkPermit {
            _lease: Arc::new(WorkLease {
                admission: self.clone(),
                bytes,
            }),
        })
    }
}

type EventSlot = Arc<Mutex<Option<Arc<EventRoute>>>>;

struct EventRoute {
    events_tx: mpsc::Sender<BridgeStreamItem>,
    turn: Arc<TurnState>,
    state: Mutex<RouteState>,
}

#[derive(Default)]
struct RouteState {
    terminal: bool,
    failed: bool,
    limit: Option<EventLimit>,
}

struct EventMailbox {
    tx: mpsc::Sender<QueuedEvent>,
    budget: Arc<Mutex<EventBudget>>,
    item_limit: usize,
    byte_limit: usize,
}

#[derive(Debug, Clone, Copy)]
enum EventLimit {
    Count,
    Bytes,
}

impl EventLimit {
    fn name(self) -> &'static str {
        match self {
            Self::Count => "item_count",
            Self::Bytes => "serialized_bytes",
        }
    }
}

impl EventMailbox {
    fn new() -> (EventMailboxTx, mpsc::Receiver<QueuedEvent>) {
        Self::with_limits(EVENT_ITEM_LIMIT, EVENT_BYTE_LIMIT)
    }

    fn with_limits(
        item_limit: usize,
        byte_limit: usize,
    ) -> (EventMailboxTx, mpsc::Receiver<QueuedEvent>) {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        (
            Arc::new(Self {
                tx,
                budget: Arc::new(Mutex::new(EventBudget::default())),
                item_limit,
                byte_limit,
            }),
            rx,
        )
    }

    fn enqueue_data(
        &self,
        route: &Arc<EventRoute>,
        item: BridgeStreamItem,
    ) -> Result<(), EventLimit> {
        let bytes = event_payload_bytes(&item);
        let mut route_state = route.state.lock().expect("event route poisoned");
        if route_state.terminal || route_state.failed {
            return Err(EventLimit::Count);
        }
        let mut budget = self.budget.lock().expect("event budget poisoned");
        let limit = if budget.items >= self.item_limit {
            Some(EventLimit::Count)
        } else if bytes > self.byte_limit || budget.bytes > self.byte_limit - bytes {
            Some(EventLimit::Bytes)
        } else {
            None
        };
        if let Some(limit) = limit {
            route_state.failed = true;
            route_state.limit = Some(limit);
            route.turn.fail();
            return Err(limit);
        }
        budget.items += 1;
        budget.bytes += bytes;
        drop(budget);
        let credit = EventCredit {
            budget: self.budget.clone(),
            bytes,
        };
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => {
                permit.send(QueuedEvent {
                    route: route.clone(),
                    item,
                    credit: Some(credit),
                    terminal_ack: None,
                });
                Ok(())
            }
            Err(_) => {
                drop(credit);
                route_state.failed = true;
                route_state.limit = Some(EventLimit::Count);
                route.turn.fail();
                Err(EventLimit::Count)
            }
        }
    }

    fn enqueue_terminal(
        &self,
        route: &Arc<EventRoute>,
        item: BridgeStreamItem,
    ) -> oneshot::Receiver<Result<(), ()>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let mut state = route.state.lock().expect("event route poisoned");
        if state.terminal {
            state.failed = true;
            state.limit.get_or_insert(EventLimit::Count);
            route.turn.fail();
            let _ = ack_tx.send(Err(()));
            return ack_rx;
        }
        state.terminal = true;
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => {
                permit.send(QueuedEvent {
                    route: route.clone(),
                    item,
                    credit: None,
                    terminal_ack: Some(ack_tx),
                });
            }
            Err(_) => {
                state.failed = true;
                state.limit.get_or_insert(EventLimit::Count);
                route.turn.fail();
                let _ = ack_tx.send(Err(()));
            }
        }
        ack_rx
    }
}

fn event_payload_bytes(item: &BridgeStreamItem) -> usize {
    match item {
        BridgeStreamItem::Update(update) => json_size_bounded(update, EVENT_BYTE_LIMIT),
        BridgeStreamItem::Interrupt { id, request } => {
            let remaining = EVENT_BYTE_LIMIT.saturating_sub(id.len());
            let size = json_size_bounded(request, remaining);
            if size == usize::MAX {
                usize::MAX
            } else {
                size.saturating_add(id.len())
            }
        }
        _ => 0,
    }
}

fn json_size_bounded(value: &impl serde::Serialize, limit: usize) -> usize {
    struct Counter {
        size: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.size) {
                return Err(std::io::Error::other("serialized size limit exceeded"));
            }
            self.size += bytes.len();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { size: 0, limit };
    if serde_json::to_writer(&mut counter, value).is_err() {
        usize::MAX
    } else {
        counter.size
    }
}

#[derive(Default)]
struct EventBudget {
    items: usize,
    bytes: usize,
}

struct EventCredit {
    budget: Arc<Mutex<EventBudget>>,
    bytes: usize,
}

impl Drop for EventCredit {
    fn drop(&mut self) {
        let mut budget = self.budget.lock().expect("event budget poisoned");
        budget.items -= 1;
        budget.bytes -= self.bytes;
    }
}

/// One [`BridgeStreamItem`] queued by a handler running on the ACP SDK's
/// single dispatch loop, awaiting off-loop delivery to the per-prompt
/// channel named by `events_tx`.
struct QueuedEvent {
    route: Arc<EventRoute>,
    item: BridgeStreamItem,
    credit: Option<EventCredit>,
    terminal_ack: Option<oneshot::Sender<Result<(), ()>>>,
}

type EventMailboxTx = Arc<EventMailbox>;

/// Sole consumer of the dispatch-loop event mailbox.
///
/// Items are enqueued strictly in dispatch order by the single-threaded
/// dispatch loop and delivered by this single driver in the same order, so
/// per-turn message ordering is exact — which a naive `cx.spawn`-per-send
/// (concurrent `FuturesUnordered` tasks racing an unordered lock) could not
/// guarantee for adjacent `AgentMessageChunk`s. The driver, not the dispatch
/// loop, absorbs back-pressure: it awaits the bounded per-prompt channel.
///
/// The turn's TERMINAL item (`Finished`/`RunError`) travels through this
/// same FIFO: because this driver is the mailbox's only consumer, FIFO
/// order alone guarantees every update enqueued before the terminal has
/// been pushed into the per-prompt channel before the consumer can observe
/// the terminal. Without this barrier the `session/prompt` response (routed
/// on the dispatch loop while the mailbox still holds undelivered updates)
/// lets the actor finish the turn early — the SSE stream ends on
/// `Finished` and the mailbox tail is lost.
///
/// The mailbox outlives individual turns (one connection = one driver), so
/// a send failure — meaning that turn's consumer dropped and the turn is
/// terminating via the `events_tx.closed()` watcher in
/// `run_prompt_with_cancel` — skips only that item; queued items of the
/// dead turn fail against its dead sender one by one, while items of any
/// later turn carry their own live sender and deliver normally. The driver
/// itself must NOT exit here: doing so would permanently kill event
/// delivery for every future turn on the connection.
async fn run_event_mailbox(
    mut rx: mpsc::Receiver<QueuedEvent>,
    unusable: Arc<AtomicBool>,
) -> Result<(), agent_client_protocol::Error> {
    let mut failed_deadline: Option<(crate::acp::TurnId, tokio::time::Instant)> = None;
    while let Some(event) = rx.recv().await {
        let QueuedEvent {
            route,
            item,
            credit,
            terminal_ack,
        } = event;
        let mut failed = route.turn.is_failed();
        let mut permit = None;
        if !failed {
            let notified = route.turn.failure_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if route.turn.is_failed() {
                failed = true;
            } else {
                tokio::select! {
                    result = route.events_tx.reserve() => {
                        permit = result.ok();
                    }
                    () = &mut notified => failed = true,
                }
            }
        }

        if failed {
            let turn_id = route.turn.id();
            let deadline = match failed_deadline {
                Some((id, deadline)) if id == turn_id => deadline,
                _ => {
                    let deadline = tokio::time::Instant::now() + FAILED_EVENT_DELIVERY_TIMEOUT;
                    failed_deadline = Some((turn_id, deadline));
                    deadline
                }
            };
            permit = match tokio::time::timeout_at(deadline, route.events_tx.reserve()).await {
                Ok(Ok(permit)) => Some(permit),
                _ => None,
            };
            if permit.is_none() {
                unusable.store(true, Ordering::Release);
                drop(credit);
                if let Some(ack) = terminal_ack {
                    let _ = ack.send(Err(()));
                }
                while let Ok(mut queued) = rx.try_recv() {
                    if let Some(ack) = queued.terminal_ack.take() {
                        let _ = ack.send(Err(()));
                    }
                    drop(queued);
                }
                continue;
            }
            permit.expect("reserved event slot").send(item);
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        } else if let Some(permit) = permit {
            permit.send(item);
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        } else {
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        }
    }
    Ok(())
}

/// Bounded spill for `session/update` notifications that arrive with no
/// active prompt. ACP treats out-of-turn updates as protocol violations, but
/// they can also precede the actor's slot install by a scheduling hair —
/// dropping them silently violates the "no silent drops" contract. The next
/// prompt drains this buffer ahead of its own events; overflow warns and
/// drops (the session state is already beyond recovery at that point).
///
/// ponytail: spill lives on the actor's flush path, not a dedicated side
/// channel; if out-of-turn volume ever matters, replace with an unbounded
/// dedicated stream wired through `BridgeStreamItem`.
const SPILL_CAPACITY: usize = 32;

type SpillBuffer = Arc<Mutex<Vec<agent_client_protocol::schema::v1::SessionUpdate>>>;

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
            return Err(session_limit_error("MAX_LIST_SESSIONS", MAX_LIST_SESSIONS));
        }
        if bytes > MAX_LIST_BYTES || self.bytes > MAX_LIST_BYTES - bytes {
            return Err(session_limit_error("MAX_LIST_BYTES", MAX_LIST_BYTES));
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
        Some(_) => Err(session_limit_error("MAX_LIST_PAGES", MAX_LIST_PAGES)),
        None => Ok(None),
    }
}

/// Bridge budget violation for `session/list`. Reports `-32603`
/// (internal error) with structured data naming the actual limit —
/// `-32800` (`request_cancelled`) would falsely imply the caller cancelled.
fn session_limit_error(limit: &'static str, cap: usize) -> BridgeError {
    BridgeError::Acp(
        agent_client_protocol::Error::internal_error()
            .data(serde_json::json!({ "limit": limit, "cap": cap })),
    )
}

fn mailbox_limit_error(limit: EventLimit) -> BridgeError {
    BridgeError::Acp(
        agent_client_protocol::Error::internal_error().data(serde_json::json!({
            "limit": limit.name(),
            "items": EVENT_ITEM_LIMIT,
            "bytes": EVENT_BYTE_LIMIT,
        })),
    )
}

fn load_history_limit_error() -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(serde_json::json!({
        "limit": "MAX_LOAD_HISTORY",
        "cap": { "events": MAX_LOAD_HISTORY_EVENTS, "bytes": MAX_LOAD_HISTORY_BYTES }
    }))
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
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

    let result = spawn_session(transport, cfg).await;
    if result.is_ok() {
        agent_guard.disarm();
    }
    result
}

#[cfg(all(test, target_os = "linux"))]
async fn spawn_in_process_session_with_work<F>(
    cfg: SessionConfig,
    work: Arc<WorkAdmission>,
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
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);
    let result = spawn_session_with_work(transport, cfg, work).await;
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
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

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
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

    delete_session_via(transport, cfg, session_id).await
}

pub(crate) async fn spawn_session<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    spawn_session_with_work(connector, cfg, WorkAdmission::new()).await
}

async fn spawn_session_with_work<T>(
    connector: T,
    cfg: SessionConfig,
    work_admission: Arc<WorkAdmission>,
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
        work_admission,
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
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
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
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
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

        // Bound enforcement lives in `BoundedSessionList::push_with_size`.
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
    work_admission: Arc<WorkAdmission>,
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
    let error_slot: EventSlot = Arc::new(Mutex::new(None));
    let event_slot_for_notif = event_slot.clone();
    let event_slot_for_perm = event_slot.clone();
    let event_slot_for_session = event_slot.clone();
    let error_slot_for_session = error_slot.clone();
    let (event_mailbox_tx, event_mailbox_rx) = EventMailbox::new();
    let mut work_retired = work_admission.retire_tx.subscribe();
    let mut event_driver = AbortOnDrop::new(tokio::spawn(run_event_mailbox(
        event_mailbox_rx,
        unusable.clone(),
    )));
    let mailbox_for_notif = event_mailbox_tx.clone();
    let mailbox_for_perm = event_mailbox_tx.clone();
    let mailbox_for_prompt = event_mailbox_tx.clone();
    let spill: SpillBuffer = Arc::new(Mutex::new(Vec::new()));
    let spill_for_notif = spill.clone();
    let spill_for_teardown = spill.clone();
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
    let (filesystem_root, filesystem_root_guard) =
        if read_filesystem_enabled || write_filesystem_enabled {
            match tokio::task::spawn_blocking({
                let cwd = cwd.clone();
                move || {
                    let root = std::fs::canonicalize(cwd)?;
                    let guard = crate::file_ops::pin_filesystem_root(&root)?;
                    Ok::<_, std::io::Error>((root, guard))
                }
            })
            .await
            {
                Ok(Ok((root, guard))) => (Arc::new(root), Some(guard)),
                Ok(Err(error)) => {
                    let ready_tx = ready_tx;
                    let _ = ready_tx.send(Err(BridgeError::Io(error)));
                    return;
                }
                Err(error) => {
                    let ready_tx = ready_tx;
                    let _ = ready_tx.send(Err(BridgeError::Io(std::io::Error::other(error))));
                    return;
                }
            }
        } else {
            (Arc::new(cwd.clone()), None)
        };
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
    let pending_perms_for_events = pending_permissions.clone();
    let pending_perms_for_drain = pending_permissions.clone();
    let turns_for_session = turn_queue.clone();
    let unusable_for_session = unusable.clone();
    let read_filesystem_lock = filesystem_lock.clone();
    let write_filesystem_lock = filesystem_lock.clone();
    let read_filesystem_root = filesystem_root.clone();
    let write_filesystem_root = filesystem_root.clone();
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
                    let route = event_slot_for_notif
                        .lock()
                        .expect("event slot poisoned")
                        .clone();
                    if let Some(route) = route {
                        // The SDK runs this handler on its single sequential
                        // dispatch loop, so a blocking `tx.send(..).await` on
                        // the full per-prompt channel would stall every
                        // response, request, and notification on this
                        // connection — including `session/prompt`'s own
                        // response, deadlocking the turn. Queue for the
                        // off-loop mailbox driver instead (see
                        // `run_event_mailbox`); enqueue order preserves the
                        // agent's update order exactly.
                        if let Err(limit) = mailbox_for_notif.enqueue_data(
                            &route,
                            BridgeStreamItem::Update(notification.update),
                        ) && route.turn.is_failed()
                        {
                            route.turn.cancel_and_drain(&pending_perms_for_events);
                            tracing::warn!(?limit, "session event mailbox limit exceeded");
                        }
                    } else {
                        // No active prompt. The SDK's dispatch loop only logs
                        // errors from notification handlers (it cannot reply
                        // to a notification), so returning Err would neither
                        // surface the loss nor preserve the update — spill
                        // into the bounded buffer for the next run instead.
                        //
                        // ponytail: slot check and spill push are not atomic —
                        // an update observed here just before a prompt installs
                        // its slot stays spilled and is delivered at the start
                        // of the NEXT run. ACP treats out-of-turn updates as a
                        // protocol violation anyway, so this seam is accepted.
                        let mut spill = spill_for_notif.lock().expect("spill buffer poisoned");
                        if spill.len() >= SPILL_CAPACITY {
                            tracing::warn!(
                                capacity = SPILL_CAPACITY,
                                "out-of-turn session/update spill overflow; dropping update"
                            );
                        } else {
                            spill.push(notification.update);
                        }
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
                let event_mailbox_for_perm = mailbox_for_perm.clone();
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
                        event_mailbox_for_perm.clone(),
                        pending_perms.clone(),
                        permission_timeout,
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cwd = read_filesystem_root;
                let filesystem_lock = read_filesystem_lock;
                let work = work_admission.clone();
                async move |req: ReadTextFileRequest, responder, cx| {
                    if !read_filesystem_enabled {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }

                    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
                        Ok(permit) => permit,
                        Err(()) => {
                            let mut retired = work.retire_tx.subscribe();
                            return tokio::select! { biased; changed = retired.changed() => { let _ = changed; Ok(()) }, () = std::future::pending() => Ok(()) };
                        }
                    };

                    let cancellation = responder.cancellation();
                    let cwd = cwd.clone();
                    let filesystem_lock = filesystem_lock.clone();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit.clone();
                        let result = cancellation
                            .run_until_cancelled(read_file_request_with_work(
                                req,
                                cwd,
                                filesystem_lock,
                                cancellation.clone(),
                                Some(permit.clone()),
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
                let cwd = write_filesystem_root;
                let filesystem_lock = write_filesystem_lock;
                let work = work_admission.clone();
                async move |req: WriteTextFileRequest, responder, cx| {
                    if !write_filesystem_enabled {
                        return responder.respond_with_error(
                            agent_client_protocol::Error::method_not_found(),
                        );
                    }
                    if let Some(error) = write_content_validation_error(&req.content) {
                        return responder.respond_with_error(error);
                    }
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
                        Ok(permit) => permit,
                        Err(()) => {
                            let mut retired = work.retire_tx.subscribe();
                            return tokio::select! { biased; changed = retired.changed() => { let _ = changed; Ok(()) }, () = std::future::pending() => Ok(()) };
                        }
                    };

                    let cancellation = responder.cancellation();
                    let cwd = cwd.clone();
                    let filesystem_lock = filesystem_lock.clone();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit.clone();
                        let result = write_file_request_with_work(
                            req,
                            cwd,
                            filesystem_lock,
                            cancellation.clone(),
                            Some(permit.clone()),
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
                let work = work_admission.clone();
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
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) { Ok(p) => p, Err(()) => { let mut rx=work.retire_tx.subscribe(); return tokio::select! { biased; _=rx.changed()=>Ok(()), ()=std::future::pending()=>Ok(()) }; } };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit.clone();
                        if let Err(error) = crate::terminal::create_request(
                            req,
                            registry,
                            cancellation,
                            responder,
                            Some(permit.clone()),
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
                let work = work_admission.clone();
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
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) { Ok(p) => p, Err(()) => { let mut rx=work.retire_tx.subscribe(); return tokio::select! { biased; _=rx.changed()=>Ok(()), ()=std::future::pending()=>Ok(()) }; } };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit;
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
                let work = work_admission.clone();
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
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) { Ok(p) => p, Err(()) => { let mut rx=work.retire_tx.subscribe(); return tokio::select! { biased; _=rx.changed()=>Ok(()), ()=std::future::pending()=>Ok(()) }; } };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit;
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
                let work = work_admission.clone();
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
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) { Ok(p) => p, Err(()) => { let mut rx=work.retire_tx.subscribe(); return tokio::select! { biased; _=rx.changed()=>Ok(()), ()=std::future::pending()=>Ok(()) }; } };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit;
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
                let work = work_admission.clone();
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
                    let permit = match work.try_acquire(&req, &responder.id().to_string()) { Ok(p) => p, Err(()) => { let mut rx=work.retire_tx.subscribe(); return tokio::select! { biased; _=rx.changed()=>Ok(()), ()=std::future::pending()=>Ok(()) }; } };
                    let cancellation = responder.cancellation();
                    if let Err(error) = cx.spawn(async move {
                        let _permit = permit;
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
        // Keep unknown methods out of the SDK's fallback pending-request
        // queue. Typed handlers above retain their normal dispatch path.
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => {
                        responder.respond_with_error(agent_client_protocol::Error::method_not_found())
                    }
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let ready_slot = Arc::new(ready_tx);
            let event_slot = event_slot_for_session;
            let error_slot = error_slot_for_session;
            let pending_for_drain = pending_permissions.clone();
            let turn_queue = turns_for_session.clone();
            let unusable = unusable_for_session.clone();
            let mut cmd_rx = cmd_rx;
            let mcp_url = mcp_url.clone();
            let mcp_headers = mcp_headers.clone();
            let init_state = init_state.clone();
            let load_session_id = load_session_id.clone();
            let load_buffer = load_buffer_for_session;
            let spill = spill;
            let event_mailbox_tx = mailbox_for_prompt;
            async move {
                let ready_slot_for_foreground = ready_slot.clone();
                let foreground = async {
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
                        if let Some(tx) = ready_slot_for_foreground.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Ok(SessionReady {
                                session_id: id.clone(),
                                supports_close,
                            }));
                        }
                        (id, supports_close)
                    }
                    Err(err) => {
                        if let Some(tx) = ready_slot_for_foreground.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Err(err));
                        }
                        return Ok(());
                    }
                };

                loop {
                    let cmd = tokio::select! {
                        biased;
                        () = cx.incoming_closed() => break,
                        cmd = cmd_rx.recv() => match cmd { Some(cmd) => cmd, None => break },
                    };
                    match cmd {
                        SessionCommand::Prompt {
                            prompt,
                            events_tx,
                            finished_tx,
                            turn,
                        } => {
                            let route = Arc::new(EventRoute {
                                events_tx: events_tx.clone(),
                                turn: turn.clone(),
                                state: Mutex::new(RouteState::default()),
                            });
                            *error_slot.lock().expect("error route slot poisoned") =
                                Some(route.clone());
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

                            // Drain out-of-turn updates spilled by the
                            // notification handler ahead of this turn's own
                            // events (ordered capture, see SpillBuffer).
                            let spilled: Vec<_> =
                                std::mem::take(&mut *spill.lock().expect("spill buffer poisoned"));
                            for update in spilled {
                                if events_tx
                                    .send(BridgeStreamItem::Update(update))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }

                            // Do not expose the live route until bootstrap
                            // history and spill have drained, or live updates
                            // could overtake that prefix.
                            *event_slot.lock().expect("event slot poisoned") = Some(route.clone());

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

                            // Capture retirement from the raw prompt outcome before a
                            // mailbox-limit presentation error can replace it.
                            let grace_expired = matches!(
                                &res,
                                Err(BridgeError::CancelGraceExpired(_))
                            ) && turn.is_cancelled();
                            let peer_closed = matches!(&res, Err(BridgeError::SessionClosed));
                            let failure_limit = route
                                .state
                                .lock()
                                .expect("event route poisoned")
                                .limit;
                            let mut res = if turn.is_failed() {
                                Err(mailbox_limit_error(
                                    failure_limit.unwrap_or(EventLimit::Count),
                                ))
                            } else {
                                res
                            };
                            // A completed turn owns all of its deferred permission
                            // callbacks; deny and unregister them before admitting the
                            // next turn, without sending an ACP cancel notification.
                            turn.cancel_and_drain(&pending_for_drain);

                            let terminal = match &res {
                                Ok(stop_reason) => BridgeStreamItem::Finished {
                                    stop_reason: *stop_reason,
                                },
                                Err(error) => BridgeStreamItem::RunError {
                                    message: if turn.is_failed() {
                                        format!(
                                            "session event mailbox {} limit exceeded",
                                            failure_limit.unwrap_or(EventLimit::Count).name()
                                        )
                                    } else {
                                        error.to_string()
                                    },
                                },
                            };
                            let terminal_ack = event_mailbox_tx.enqueue_terminal(&route, terminal);
                            if turn.is_failed() && res.is_ok() {
                                let limit = route
                                    .state
                                    .lock()
                                    .expect("event route poisoned")
                                    .limit
                                    .unwrap_or(EventLimit::Count);
                                res = Err(mailbox_limit_error(limit));
                            }

                            *event_slot.lock().expect("event slot poisoned") = None;

                            if grace_expired || peer_closed {
                                unusable.store(true, Ordering::Release);
                            }

                            let _ = finished_tx.send(res);
                            drop(events_tx);
                            turn_queue.remove(&turn);
                            if !matches!(terminal_ack.await, Ok(Ok(()))) {
                                unusable.store(true, Ordering::Release);
                                let _ = error_slot
                                    .lock()
                                    .expect("error route slot poisoned")
                                    .take();
                                break;
                            }
                            let _ = error_slot
                                .lock()
                                .expect("error route slot poisoned")
                                .take();
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
                            let kept = run_setting_command(
                                ack,
                                config.set_session_timeout,
                                &unusable,
                                send_set_mode(&cx, &session_id, &mode_id, init_state.clone()),
                            )
                            .await;
                            if !kept {
                                break;
                            }
                        }
                        SessionCommand::SetConfigOption {
                            config_id,
                            value,
                            ack,
                        } => {
                            let kept = run_setting_command(
                                ack,
                                config.set_session_timeout,
                                &unusable,
                                send_set_config_option(
                                    &cx,
                                    &session_id,
                                    &config_id,
                                    &value,
                                    init_state.clone(),
                                ),
                            )
                            .await;
                            if !kept {
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
                            let route = Arc::new(EventRoute {
                                events_tx: events_tx.clone(),
                                turn: Arc::new(TurnState::new()),
                                state: Mutex::new(RouteState::default()),
                            });
                            *error_slot.lock().expect("error route slot poisoned") =
                                Some(route);
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

                            // Mirror the Prompt arm: drain out-of-turn updates
                            // spilled between `session/load` and this drain so
                            // they are neither misattributed to a later turn
                            // nor silently lost.
                            let spilled: Vec<_> =
                                std::mem::take(&mut *spill.lock().expect("spill buffer poisoned"));
                            for update in spilled {
                                if events_tx
                                    .send(BridgeStreamItem::Update(update))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            let _ = finished_tx.send(Ok(StopReason::EndTurn));
                            drop(events_tx);
                            let _ = error_slot
                                .lock()
                                .expect("error route slot poisoned")
                                .take();
                        }
                    }
                }
                // Connection closing — fail any pending permission waiters
                // (their spawned tasks will respond Cancelled to the agent
                // and exit) so memory and tasks don't linger past idle
                // reaping.
                drain_pending_permissions(&pending_for_drain);
                Ok(())
                };
                tokio::select! {
                    biased;
                    changed = work_retired.changed() => {
                        let _ = changed;
                        unusable.store(true, Ordering::Release);
                        drain_pending_permissions(&pending_for_drain);
                        let error = work_retired.borrow().clone().unwrap_or_else(agent_client_protocol::Error::internal_error);
                        if let Some(tx) = ready_slot.lock().expect("ready slot poisoned").take() {
                            let _ = tx.send(Err(BridgeError::Acp(error.clone())));
                        }
                        Err(error)
                    }
                    result = foreground => result,
                }
            }
        })
        .await;

    if let Some(registry) = terminal_registry.as_ref() {
        registry.shutdown();
    }
    drop(terminal_registry_guard);
    // SDK callback closures (and their in-flight operations) are gone after
    // connect_with returns; release the session lease before mailbox draining.
    #[cfg(target_os = "linux")]
    drop(filesystem_root_guard);
    #[cfg(not(target_os = "linux"))]
    let _filesystem_root_guard = filesystem_root_guard;

    // Every exit path below represents a dead actor. Mark the shared handle
    // first so new prompts fail closed, then release every queued admission
    // before any potentially backpressured error event is sent.
    unusable.store(true, Ordering::Release);
    turn_queue.clear();

    // Actor shutdown with a non-empty spill buffer means those out-of-turn
    // updates will never be drained by a future prompt — surface the loss
    // instead of discarding silently.
    let remaining_spill = spill_for_teardown
        .lock()
        .expect("spill buffer poisoned")
        .len();
    if remaining_spill > 0 {
        tracing::warn!(
            count = remaining_spill,
            "session actor terminated with non-empty out-of-turn update spill buffer; \
             buffered updates discarded"
        );
    }

    drain_pending_permissions(&pending_perms_for_drain);
    if let Err(err) = result {
        let route = error_slot
            .lock()
            .expect("error route slot poisoned")
            .take()
            .or_else(|| event_slot.lock().expect("event slot poisoned").take());
        if let Some(route) = route {
            route.turn.fail();
            route.turn.cancel_and_drain(&pending_perms_for_drain);
            let terminal_ack = event_mailbox_tx.enqueue_terminal(
                &route,
                BridgeStreamItem::RunError {
                    message: if work_admission.retire_tx.borrow().is_some() {
                        "ACP_SPAWNED_WORK quota exceeded".into()
                    } else {
                        format!("acp connection terminated: {err}")
                    },
                },
            );
            let _ = terminal_ack.await;
        }
    }
    // Release every producer before closing the FIFO; the connection error,
    // when present, is queued behind all accepted events above.
    drop(event_slot.lock().expect("event slot poisoned").take());
    drop(event_mailbox_tx);
    if let Some(handle) = event_driver.handle.as_mut()
        && tokio::time::timeout(EVENT_DRIVER_SHUTDOWN_TIMEOUT, handle)
            .await
            .is_ok()
    {
        event_driver.disarm();
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

/// Shared body of `SetMode`/`SetConfigOption`: run one actor-serial setting
/// RPC and reconcile every caller-gone path.
///
/// Returns `false` only when the actor must shut down (the caller vanished at
/// any point). All three poison cases — the responder closed while the ACP
/// request raced it, the request itself timed out, and the acknowledgement
/// failing to deliver — mark the session unusable before exiting, because a
/// half-applied setting leaves the cached snapshot untrustworthy for reuse.
async fn run_setting_command(
    mut ack: oneshot::Sender<Result<(), BridgeError>>,
    timeout: Duration,
    unusable: &AtomicBool,
    request: impl Future<Output = Result<(), BridgeError>>,
) -> bool {
    // Settings are deliberately actor-serial. A prompt owns the ACP turn
    // until it finishes, so a queued setting cannot race a later setting or
    // overwrite its newer cached snapshot. The HTTP caller may time out while
    // this command waits behind a prompt; do not apply a command whose
    // responder has already gone away.
    if ack.is_closed() {
        return true;
    }

    let res = tokio::select! {
        _ = ack.closed() => {
            // The caller disconnected while the ACP request was in flight.
            // Its eventual response cannot safely update a session that may
            // be reused, so close this actor.
            unusable.store(true, Ordering::Release);
            return false;
        }
        result = tokio::time::timeout(timeout, request) => match result {
            Ok(result) => result,
        Err(_) => {
            // A timed-out setting leaves ACP state uncertain: report the
            // timeout to the caller if it can still receive it, then always
            // shut the actor down.
            unusable.store(true, Ordering::Release);
            let _ = send_setting_ack(ack, Err(BridgeError::Timeout(timeout)), unusable);
            return false;
        }
        },
    };
    if ack.is_closed() && res.is_ok() {
        unusable.store(true, Ordering::Release);
        return false;
    }
    send_setting_ack(ack, res, unusable)
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

fn write_content_validation_error(content: &str) -> Option<agent_client_protocol::Error> {
    (content.len() > crate::file_ops::MAX_TEXT_FILE_BYTES)
        .then(agent_client_protocol::Error::invalid_params)
}

async fn read_file_request_with_work(
    request: ReadTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
    permit: Option<WorkPermit>,
) -> Result<ReadTextFileResponse, agent_client_protocol::Error> {
    let path = request_path(&request.path)?;
    let line = request_line(request.line)?;
    let limit = request_line(request.limit)?;
    let _guard = filesystem_lock.lock().await;
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    crate::file_ops::read_text_file_range_with_work(cwd.as_path(), path, line, limit, permit)
        .await
        .map(ReadTextFileResponse::new)
        .map_err(file_operation_error)
}

async fn write_file_request_with_work(
    request: WriteTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
    permit: Option<WorkPermit>,
) -> Result<WriteTextFileResponse, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    if let Some(error) = write_content_validation_error(&request.content) {
        return Err(error);
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

    crate::file_ops::write_text_file_with_work(cwd.as_path(), path, &request.content, permit)
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
#[allow(clippy::too_many_arguments)]
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
    let incoming_closed = cx.incoming_closed();
    tokio::pin!(incoming_closed);
    let response = loop {
        tokio::select! {
            biased;

            res = &mut prompt_fut => break res,

            // Prefer a response already queued by the dispatch loop over EOF,
            // while still terminating an outstanding RPC when the peer closes.
            () = &mut incoming_closed => break Err(BridgeError::SessionClosed),

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
                break Err(BridgeError::CancelGraceExpired(cancel_grace_timeout));
            }
        }
    };

    let response = response?;
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
#[allow(clippy::too_many_arguments)]
async fn handle_permission_request(
    req: RequestPermissionRequest,
    responder: agent_client_protocol::Responder<RequestPermissionResponse>,
    policy: Arc<dyn PermissionPolicy>,
    event_slot: EventSlot,
    event_mailbox: EventMailboxTx,
    pending_permissions: PendingPermissions,
    permission_timeout: Duration,
) -> Result<(), agent_client_protocol::Error> {
    let route = event_slot.lock().expect("event slot poisoned").clone();
    let Some(route) = route else {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    };
    let turn = route.turn.clone();
    let request_bytes =
        json_size_bounded(&req, crate::acp::MAX_PENDING_PERMISSION_BYTES_PER_REQUEST);
    let Some(permission_budget) = turn.reserve_permission(request_bytes) else {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    };
    let decision = policy.decide(&req).await;

    // A policy may have been awaiting its own async work when the turn was
    // cancelled. Do not let that late decision resurrect a cancelled ACP
    // permission request.
    if turn.is_cancelled() || turn.is_failed() {
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
                crate::acp::PendingPermission::new(
                    resolve_tx,
                    valid_option_ids,
                    turn.clone(),
                    permission_budget,
                ),
            );
            if !registered {
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            // Same dispatch-loop constraint as the notification handler: a
            // blocking send here would stall `session/prompt`'s response and
            // deadlock the turn when the channel is full. Enqueue off-loop.
            if event_mailbox
                .enqueue_data(
                    &route,
                    BridgeStreamItem::Interrupt {
                        id: interrupt_id.clone(),
                        request: req.clone(),
                    },
                )
                .is_err()
            {
                if turn.is_failed() {
                    turn.cancel_and_drain(&pending_permissions);
                }
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

    #[cfg(target_os = "linux")]
    static FILESYSTEM_WRITE_GATE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(target_os = "linux")]
    struct WriteGateReset;

    #[cfg(target_os = "linux")]
    impl Drop for WriteGateReset {
        fn drop(&mut self) {
            crate::file_ops::clear_write_gate();
        }
    }

    #[tokio::test]
    async fn work_admission_latches_once_and_blocking_clone_keeps_credit() {
        let work = WorkAdmission::with_limits(2, 4096);
        let retired = work.retire_tx.subscribe();
        let first = work.try_acquire(&"first", "id-1").unwrap();
        let second = work.try_acquire(&"second", "id-2").unwrap();
        let blocking_lease = first.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking = tokio::task::spawn_blocking(move || {
            let _lease = blocking_lease;
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        drop(first);
        assert!(work.try_acquire(&"third", "id-3").is_err());
        let error = retired.borrow().clone().expect("quota error is latched");
        assert_eq!(
            error.data,
            Some(serde_json::json!({"limit":"ACP_SPAWNED_WORK","items":2,"bytes":4096}))
        );
        assert!(work.try_acquire(&"fourth", "id-4").is_err());
        assert_eq!(
            retired.borrow().as_ref(),
            Some(&error),
            "first quota error remains authoritative"
        );
        drop(second);
        assert!(
            work.try_acquire(&"fifth", "id-5").is_err(),
            "blocking clone retains the first credit"
        );
        release_tx.send(()).unwrap();
        blocking.await.unwrap();
        assert_eq!(work.usage.lock().unwrap().items, 0);
    }

    fn test_route(events_tx: mpsc::Sender<BridgeStreamItem>) -> Arc<EventRoute> {
        Arc::new(EventRoute {
            events_tx,
            turn: Arc::new(TurnState::new()),
            state: Mutex::new(RouteState::default()),
        })
    }

    #[tokio::test]
    async fn mailbox_limits_fail_closed_without_poisoning_later_turns() {
        use agent_client_protocol::schema::v1::{CurrentModeUpdate, SessionUpdate};

        let (mailbox, rx) = EventMailbox::with_limits(1, 1024);
        let (events_tx, mut events_rx) = mpsc::channel(1);
        events_tx
            .send(BridgeStreamItem::SessionInit {
                modes: None,
                models: None,
                config_options: None,
            })
            .await
            .unwrap();
        let route = test_route(events_tx.clone());
        let driver = tokio::spawn(run_event_mailbox(rx, Arc::new(AtomicBool::new(false))));
        let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
        let (permission_tx, permission_rx) = oneshot::channel();
        assert!(route.turn.register_pending(
            &pending_permissions,
            "mailbox-permission".into(),
            crate::acp::PendingPermission::new(
                permission_tx,
                std::collections::HashSet::new(),
                route.turn.clone(),
                route.turn.reserve_permission(16).unwrap(),
            ),
        ));
        let first = BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(
            CurrentModeUpdate::new("prefix"),
        ));
        mailbox.enqueue_data(&route, first).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while mailbox.tx.capacity() != EVENT_CHANNEL_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver dequeued the event and is blocked on the full turn channel");
        assert_eq!(
            mailbox.budget.lock().unwrap().items,
            1,
            "driver in-flight event remains charged"
        );
        assert!(matches!(
            mailbox.enqueue_data(
                &route,
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "overflow"
                ),)),
            ),
            Err(EventLimit::Count)
        ));
        route.turn.cancel_and_drain(&pending_permissions);
        assert!(matches!(permission_rx.await, Ok(PermissionDecision::Deny)));
        let terminal_ack = mailbox.enqueue_terminal(
            &route,
            BridgeStreamItem::RunError {
                message: "event item limit".into(),
            },
        );
        assert!(matches!(
            events_rx.recv().await,
            Some(BridgeStreamItem::SessionInit { .. })
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(BridgeStreamItem::Update(_))
        ));
        assert!(matches!(
            events_rx.recv().await,
            Some(BridgeStreamItem::RunError { .. })
        ));
        assert!(
            events_rx.try_recv().is_err(),
            "one terminal only; no Finished follows"
        );
        assert_eq!(terminal_ack.await.unwrap(), Ok(()));
        drop(mailbox);
        driver.await.unwrap().unwrap();

        let (mailbox, _rx) = EventMailbox::with_limits(8, 8);
        let (events_tx, _events_rx) = mpsc::channel(1);
        let route = test_route(events_tx);
        assert!(matches!(
            mailbox.enqueue_data(
                &route,
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "payload too large"
                ),)),
            ),
            Err(EventLimit::Bytes)
        ));

        let (mailbox, rx) = EventMailbox::with_limits(1, 1024);
        let (events_tx, _events_rx) = mpsc::channel(1);
        let route = test_route(events_tx.clone());
        events_tx
            .send(BridgeStreamItem::SessionInit {
                modes: None,
                models: None,
                config_options: None,
            })
            .await
            .unwrap();
        let unusable = Arc::new(AtomicBool::new(false));
        let driver = tokio::spawn(run_event_mailbox(rx, unusable.clone()));
        mailbox
            .enqueue_data(
                &route,
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "accepted",
                ))),
            )
            .unwrap();
        assert!(
            mailbox
                .enqueue_data(
                    &route,
                    BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(
                        CurrentModeUpdate::new("overflow"),
                    )),
                )
                .is_err()
        );
        let terminal_ack = mailbox.enqueue_terminal(
            &route,
            BridgeStreamItem::RunError {
                message: "event item limit".into(),
            },
        );
        assert_eq!(
            tokio::time::timeout(
                FAILED_EVENT_DELIVERY_TIMEOUT + Duration::from_secs(1),
                terminal_ack
            )
            .await
            .expect("failed route must retire within its independent deadline")
            .unwrap(),
            Err(())
        );
        assert!(unusable.load(Ordering::Acquire));
        drop(mailbox);
        driver.await.unwrap().unwrap();

        // An ordinary dead-turn receiver is item-local: later route events
        // remain deliverable on the same driver.
        let (mailbox, rx) = EventMailbox::with_limits(4, 1024);
        let driver = tokio::spawn(run_event_mailbox(rx, Arc::new(AtomicBool::new(false))));
        let (dead_tx, dead_rx) = mpsc::channel(1);
        drop(dead_rx);
        mailbox
            .enqueue_data(
                &test_route(dead_tx),
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "dead",
                ))),
            )
            .unwrap();
        let (healthy_tx, mut healthy_rx) = mpsc::channel(1);
        mailbox
            .enqueue_data(
                &test_route(healthy_tx),
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "healthy",
                ))),
            )
            .unwrap();
        assert!(matches!(
            healthy_rx.recv().await,
            Some(BridgeStreamItem::Update(_))
        ));
        drop(mailbox);
        driver.await.unwrap().unwrap();
    }

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
        let _gate_reset = WriteGateReset;
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
    #[tokio::test]
    async fn spawned_write_quota_retires_actor_while_mutex_work_is_blocked() {
        let _gate_lock = FILESYSTEM_WRITE_GATE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let raw =
            std::env::temp_dir().join(format!("agui-work-admission-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&raw).unwrap();
        let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
        let paths: Vec<_> = ["first.txt", "second.txt", "third.txt"]
            .into_iter()
            .map(|name| cwd.join(name))
            .collect();
        let gate = Arc::new(crate::file_ops::WriteGate {
            path: paths[0].clone(),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        crate::file_ops::install_write_gate(gate.clone());
        let _gate_reset = WriteGateReset;
        let started = gate.started.notified();
        let work = WorkAdmission::with_limits(2, 4096);
        let mut retired = work.retire_tx.subscribe();
        let (third_tx, third_rx) = oneshot::channel();
        let agent_gate = gate.clone();
        let agent_paths: Vec<_> = paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
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
        let handle = spawn_in_process_session_with_work(cfg, work.clone(), move |stream| {
            Box::pin(run_quota_write_agent(
                stream,
                agent_paths,
                agent_gate,
                third_rx,
            ))
        })
        .await
        .expect("filesystem quota session opens");
        let mut prompt = handle.prompt("quota").await.expect("prompt opens");
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .expect("first write reaches existing filesystem gate");
        tokio::time::timeout(Duration::from_secs(5), async {
            while work.usage.lock().unwrap().items != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first gated and second mutex-queued requests are both charged");
        assert_eq!(work.usage.lock().unwrap().items, 2);
        third_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), retired.changed())
            .await
            .expect("third RPC retires work admission")
            .expect("work admission sender remains alive");

        let mut run_errors = 0;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(5), prompt.events.recv())
            .await
            .expect("quota error reaches the active prompt")
        {
            match item {
                BridgeStreamItem::RunError { message } => {
                    run_errors += 1;
                    assert!(message.contains("ACP_SPAWNED_WORK"), "{message}");
                }
                BridgeStreamItem::Finished { .. } => panic!("quota retirement emitted Finished"),
                _ => {}
            }
        }
        assert_eq!(run_errors, 1, "exactly one terminal quota error is emitted");
        assert!(
            prompt.finished.await.is_err(),
            "retired prompt is not successful"
        );
        tokio::time::timeout(Duration::from_secs(5), handle.closed())
            .await
            .expect("quota retirement closes the session handle");

        gate.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), async {
            while work.usage.lock().unwrap().items != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both queued and blocking work credits release");
        assert!(
            !paths[2].exists(),
            "third request never reaches a write executor"
        );
        crate::file_ops::clear_write_gate();
        let _ = std::fs::remove_dir_all(raw);
    }

    #[cfg(target_os = "linux")]
    async fn run_quota_write_agent(
        stream: tokio::io::DuplexStream,
        paths: Vec<String>,
        gate: Arc<crate::file_ops::WriteGate>,
        third_trigger: oneshot::Receiver<()>,
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
            .name("agui-bridge-work-admission-test")
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
                    responder.respond(NewSessionResponse::new(SessionId::from("quota-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let paths = paths.clone();
                    let gate = gate.clone();
                    let third_trigger = Arc::new(Mutex::new(Some(third_trigger)));
                    async move |req: PromptRequest, responder, cx: ConnectionTo<agent_client_protocol::Client>| {
                        let paths = paths.clone();
                        let gate = gate.clone();
                        let third_trigger = third_trigger.lock().expect("test trigger poisoned").take().unwrap();
                        let spawn_cx = cx.clone();
                        spawn_cx.spawn(async move {
                            let session_id = req.session_id;
                            let mut tasks = Vec::new();
                            let mut third_trigger = Some(third_trigger);
                            let first_started = gate.started.notified();
                            tokio::pin!(first_started);
                            for (index, path) in paths.into_iter().enumerate() {
                                let request_cx = cx.clone();
                                let request_spawn = request_cx.clone();
                                let session_id = session_id.clone();
                                let (done_tx, done_rx) = oneshot::channel();
                                tasks.push(done_rx);
                                request_spawn.spawn(async move {
                                    let request = WriteTextFileRequest::new(
                                        session_id,
                                        path,
                                        format!("write-{index}"),
                                    );
                                    let _ = request_cx.send_request(request).block_task().await;
                                    let _ = done_tx.send(());
                                    Ok(())
                                })
                                .expect("agent outbound write task starts");
                                if index == 0 {
                                    first_started.as_mut().await;
                                }
                                if index == 1 {
                                    let _ = third_trigger.take().unwrap().await;
                                }
                            }
                            for task in tasks {
                                let _ = task.await;
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
                            .respond_with_error(agent_client_protocol::Error::method_not_found()),
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
    }

    #[cfg(target_os = "linux")]
    #[derive(Debug, Default, Clone)]
    struct BoundaryProbe {
        codes: Vec<i32>,
    }

    #[cfg(target_os = "linux")]
    async fn run_filesystem_boundary_probe() -> BoundaryProbe {
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
        let _ = std::fs::remove_dir_all(raw);
        let _ = std::fs::remove_dir_all(outside_raw);
        result
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
                            let mut codes = Vec::with_capacity(5);
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

    #[cfg(target_os = "linux")]
    async fn run_oversized_frame_agent(
        stream: tokio::io::DuplexStream,
        target_path: String,
        result_tx: oneshot::Sender<Option<String>>,
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
            .name("agui-bridge-oversized-wire-frame-test")
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
                    responder.respond(NewSessionResponse::new(SessionId::from("wire-limit-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                {
                    let target_path = Arc::new(target_path);
                    let result_tx = Arc::new(Mutex::new(Some(result_tx)));
                    async move |req: PromptRequest,
                                responder,
                                cx: ConnectionTo<agent_client_protocol::Client>| {
                        let target_path = target_path.clone();
                        let result_tx = result_tx.clone();
                        let spawn_cx = cx.clone();
                        spawn_cx.spawn(async move {
                            let request = WriteTextFileRequest::new(
                                req.session_id,
                                target_path.as_str(),
                                "x".repeat(16 * 1024 * 1024 + 1),
                            );
                            let result = cx.send_request(request).block_task().await;
                            let result = match result {
                                Ok(_) => None,
                                Err(error) => Some(error.to_string()),
                            };
                            if let Some(tx) =
                                result_tx.lock().expect("wire result poisoned").take()
                            {
                                let _ = tx.send(result);
                            }
                            let _ = responder.respond(PromptResponse::new(StopReason::EndTurn));
                            Ok(())
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
                            .respond_with_error(agent_client_protocol::Error::method_not_found()),
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
        let probe = run_filesystem_boundary_probe().await;
        assert_eq!(probe.codes, vec![-32602, -32602, -32002, -32603, -32603]);
    }

    #[test]
    fn oversized_write_rpc_preflight_returns_invalid_params_without_creating_file() {
        let raw = std::env::temp_dir().join(format!(
            "agui-oversized-write-preflight-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&raw).unwrap();
        let target = raw.join("oversized.txt");
        let request = WriteTextFileRequest::new(
            SessionId::from("oversized-preflight-test"),
            target.to_string_lossy().into_owned(),
            "x".repeat(crate::file_ops::MAX_TEXT_FILE_BYTES + 1),
        );
        let error = write_content_validation_error(&request.content)
            .expect("oversized content must fail before filesystem access");
        assert_eq!(i32::from(error.code), -32602);
        assert!(!target.exists(), "invalid request cannot create its target");
        let _ = std::fs::remove_dir_all(raw);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn oversized_wire_frame_is_rejected_by_guarded_inprocess_transport() {
        let raw = std::env::temp_dir().join(format!(
            "agui-oversized-wire-frame-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&raw).unwrap();
        let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
        let target = cwd.join("must-not-exist.txt");
        let target_path = target.to_string_lossy().into_owned();
        let (agent_result_tx, agent_result_rx) = oneshot::channel();
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
            Box::pin(run_oversized_frame_agent(
                stream,
                target_path,
                agent_result_tx,
            ))
        })
        .await
        .expect("wire-boundary session opens");
        let mut prompt = handle.prompt("oversized-wire").await.expect("prompt opens");
        let run_error = tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(item) = prompt.events.recv().await {
                match item {
                    BridgeStreamItem::RunError { message } => return message,
                    BridgeStreamItem::Finished { .. } => {
                        panic!("oversized frame must not produce RunFinished")
                    }
                    _ => {}
                }
            }
            panic!("oversized frame ended the stream without a terminal RunError")
        })
        .await
        .expect("guarded wire frame rejection finishes promptly");
        assert!(handle.is_unusable(), "oversized frame poisons the session");
        tokio::time::timeout(Duration::from_secs(5), handle.closed())
            .await
            .expect("oversized frame closes the session handle");
        assert!(
            !target.exists(),
            "oversized frame must be rejected before the write handler executes"
        );
        let agent_error = tokio::time::timeout(Duration::from_secs(10), agent_result_rx)
            .await
            .expect("agent outbound request observes the closed transport")
            .expect("agent reports its send result");
        assert!(
            agent_error.is_some(),
            "oversized write has no successful RPC reply"
        );
        assert!(
            run_error.contains("frame bytes limit exceeded")
                || agent_error
                    .as_deref()
                    .is_some_and(|error| error.contains("frame bytes limit exceeded")),
            "expected guarded transport frame diagnostic; run error={run_error:?}, agent error={agent_error:?}"
        );
        let _ = std::fs::remove_dir_all(raw);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn write_cancellation_is_deterministic_before_and_after_start() {
        let _gate_lock = FILESYSTEM_WRITE_GATE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        // Budget violations are internal errors with structured limit data,
        // not -32800 (request_cancelled) — the caller did not cancel.
        let (code, data) = match entry_error {
            BridgeError::Acp(error) => (i32::from(error.code), error.data),
            other => panic!("unexpected error: {other:?}"),
        };
        assert_eq!(code, -32603);
        assert_eq!(
            data,
            Some(serde_json::json!({"limit": "MAX_LIST_SESSIONS", "cap": MAX_LIST_SESSIONS}))
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

    /// Policy that defers every permission request to the AG-UI client.
    #[derive(Debug)]
    struct DeferPolicy;

    #[async_trait::async_trait]
    impl crate::policy::PermissionPolicy for DeferPolicy {
        async fn decide(
            &self,
            _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
        ) -> PermissionDecision {
            PermissionDecision::Defer {
                interrupt_id: "test-interrupt".into(),
            }
        }
    }

    /// Regression agent for the dispatch-loop stall (BUG 1): on prompt it
    /// floods more `session/update` notifications than the per-prompt event
    /// channel holds and then issues a `requestPermission` request. Before
    /// the fix, the notification handler's blocking send filled the channel
    /// and hung the SDK's single dispatch loop, so neither the
    /// `session/prompt` response nor the permission request was ever routed.
    async fn run_flooding_permission_agent(
        stream: tokio::io::DuplexStream,
        update_count: usize,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, ContentChunk, InitializeResponse, NewSessionRequest,
            NewSessionResponse, PermissionOption, PermissionOptionId, PermissionOptionKind,
            PromptResponse, SessionUpdate, StopReason, TextContent, ToolCallId, ToolCallUpdate,
            ToolCallUpdateFields,
        };

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

        Agent
            .builder()
            .name("agui-bridge-flooding-permission-test")
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
                    responder.respond(NewSessionResponse::new(SessionId::from("flood-test")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |req: PromptRequest,
                            responder: agent_client_protocol::Responder<
                    agent_client_protocol::schema::v1::PromptResponse,
                >,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let session_id = req.session_id;
                    // Spawn: awaiting the permission response inline here
                    // would block the agent's own dispatch loop (which must
                    // stay free to read that very response).
                    let cx_for_task = cx.clone();
                    let _ = cx.spawn(async move {
                        for i in 0..update_count {
                            cx_for_task.send_notification(SessionNotification::new(
                                session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(format!("chunk-{i} "))),
                                )),
                            ))?;
                        }
                        // Then ask for permission — this must be readable by
                        // the client even while the event channel is full.
                        cx_for_task
                            .send_request(RequestPermissionRequest::new(
                                session_id.clone(),
                                ToolCallUpdate::new(
                                    ToolCallId::new("flood-tc"),
                                    ToolCallUpdateFields::new().title("Flood permission"),
                                ),
                                vec![PermissionOption::new(
                                    PermissionOptionId::new("allow-once"),
                                    "Allow once",
                                    PermissionOptionKind::AllowOnce,
                                )],
                            ))
                            .block_task()
                            .await?;
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    });
                    Ok(())
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

    /// Regression test for the dispatch-loop stall (BUG 1): the agent floods
    /// 4x the per-prompt event channel's capacity with `session/update`s and
    /// then issues a `requestPermission` mid-turn. Before the fix, the
    /// notification handler's blocking send filled the channel and hung the
    /// SDK's single sequential dispatch loop — the `session/prompt` response
    /// and the permission request were never routed, so the turn never
    /// completed. Now the mailbox delivers everything and the turn finishes.
    #[tokio::test]
    async fn flooding_updates_do_not_stall_dispatch_loop_and_permission_is_answered() {
        use agent_client_protocol::schema::v1::PermissionOptionId;

        // Default event_buffer is 64; flood well past it.
        const UPDATE_COUNT: usize = 256;

        let handle = spawn_in_process_session_with(
            SessionConfig {
                cwd: PathBuf::from("/"),
                policy: Arc::new(DeferPolicy),
                config: crate::config::BridgeConfig::default(),
                mcp_url: None,
                mcp_headers: Vec::new(),
                load_session_id: None,
            },
            move |stream| Box::pin(run_flooding_permission_agent(stream, UPDATE_COUNT)),
        )
        .await
        .expect("flooding session opens");

        let mut prompt = handle.prompt("flood").await.expect("prompt opens");

        // Consume like a slow-but-connected SSE consumer: drain events and,
        // when the deferred permission interrupt surfaces, answer it from the
        // "frontend". Before the fix the first recv() already timed out — the
        // stalled dispatch loop could not even route the prompt response.
        let mut updates = 0usize;
        let mut interrupts = 0usize;
        loop {
            let item =
                tokio::time::timeout(std::time::Duration::from_secs(10), prompt.events.recv())
                    .await
                    .expect("event arrives before timeout (dispatch loop must not stall)");
            let Some(item) = item else {
                break;
            };
            match item {
                BridgeStreamItem::Update(_) => updates += 1,
                BridgeStreamItem::Interrupt { .. } => {
                    interrupts += 1;
                    assert!(
                        handle.resolve_permission(
                            "test-interrupt",
                            PermissionDecision::Allow {
                                option_id: PermissionOptionId::new("allow-once"),
                            },
                        ),
                        "interrupt resolution must be accepted mid-turn"
                    );
                }
                BridgeStreamItem::Finished { .. } => break,
                _ => {}
            }
        }
        assert!(
            interrupts >= 1,
            "the agent's requestPermission must surface as an Interrupt"
        );
        assert_eq!(updates, UPDATE_COUNT);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(10), prompt.finished)
                .await
                .expect("finished arrives before timeout")
                .expect("finished sender remains")
                .expect("prompt succeeds"),
            StopReason::EndTurn
        );
    }

    #[tokio::test]
    async fn completed_turn_drains_deferred_permissions_before_next_turn() {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
            PermissionOption, PermissionOptionId, PermissionOptionKind, PromptResponse, ToolCallId,
            ToolCallUpdate, ToolCallUpdateFields,
        };

        // The current fixture uses a constant interrupt ID; unique request IDs
        // are needed here because all 80 callbacks remain pending together.
        #[derive(Debug)]
        struct UniqueDeferPolicy;
        #[async_trait::async_trait]
        impl crate::policy::PermissionPolicy for UniqueDeferPolicy {
            async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
                PermissionDecision::Defer {
                    interrupt_id: request.tool_call.fields.title.clone().unwrap(),
                }
            }
        }
        let (start_tx, mut start_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();
        let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel::<(usize, usize, bool)>();
        let cfg = SessionConfig {
            cwd: PathBuf::from("/"),
            policy: Arc::new(UniqueDeferPolicy),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        };
        let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(async move {
                let (read, write) = tokio::io::split(stream);
                let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
                Agent.builder().name("completed-turn-permissions-test")
                    .on_receive_request(async move |req: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(req.protocol_version).agent_capabilities(AgentCapabilities::new()))
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_request(async move |_req: NewSessionRequest, responder, _cx| {
                        responder.respond(NewSessionResponse::new(SessionId::from("drain-test")))
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_request({
                        let start_tx = start_tx.clone();
                        let cancelled_tx = cancelled_tx.clone();
                        async move |req: PromptRequest, responder, cx: ConnectionTo<agent_client_protocol::Client>| {
                            let start_tx = start_tx.clone();
                            let cancelled_tx = cancelled_tx.clone();
                            let round = req.prompt.iter().filter_map(|b| match b { ContentBlock::Text(t) => t.text.parse::<usize>().ok(), _ => None }).next().unwrap();
                            let (go_tx, go_rx) = oneshot::channel(); start_tx.send(go_tx).unwrap();
                            let cx = cx.clone();
                            let spawn_cx = cx.clone();
                            spawn_cx.spawn(async move {
                                for i in 0..80 {
                                    let req = RequestPermissionRequest::new(req.session_id.clone(), ToolCallUpdate::new(ToolCallId::new(format!("{round}-{i}")), ToolCallUpdateFields::new().title(format!("{round}-{i}"))), vec![PermissionOption::new(PermissionOptionId::new("allow"), "Allow", PermissionOptionKind::AllowOnce)]);
                                    let request_cx = cx.clone(); let request_spawn = request_cx.clone();
                                    let cancelled_tx = cancelled_tx.clone();
                                    let _ = request_cx.spawn(async move {
                                        let result = request_spawn.send_request(req).block_task().await;
                                        let cancelled = matches!(result, Ok(response) if response.outcome == RequestPermissionOutcome::Cancelled);
                                        let _ = cancelled_tx.send((round, i, cancelled));
                                        Ok(())
                                    });
                                }
                                let _ = go_rx.await;
                                responder.respond(PromptResponse::new(StopReason::EndTurn))
                            }).expect("agent prompt task spawned"); Ok(())
                        }
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_dispatch(async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| match message {
                        agent_client_protocol::Dispatch::Response(result, router) => router.route_with_result(result),
                        agent_client_protocol::Dispatch::Request(_, responder) => responder.respond_with_error(agent_client_protocol::Error::method_not_found()),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }, agent_client_protocol::on_receive_dispatch!())
                    .connect_to(transport).await.map_err(BridgeError::Acp)
            })
        }).await.expect("session opens");
        for round in 0..2 {
            let mut prompt = handle.prompt(round.to_string()).await.unwrap();
            let release = start_rx.recv().await.unwrap();
            let mut ids = Vec::new();
            while ids.len() < 80 {
                if let BridgeStreamItem::Interrupt { id, .. } = prompt.events.recv().await.unwrap()
                {
                    ids.push(id);
                }
            }
            release.send(()).unwrap();
            let mut finished = false;
            while let Some(item) = prompt.events.recv().await {
                if matches!(item, BridgeStreamItem::Finished { .. }) {
                    finished = true;
                    break;
                }
                assert!(!matches!(item, BridgeStreamItem::RunError { .. }));
            }
            assert!(finished);
            assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
            assert!(handle.pending_permissions().is_empty());
            assert!(ids.iter().all(|id| !handle.resolve_permission(
                id,
                PermissionDecision::Allow {
                    option_id: PermissionOptionId::new("allow")
                }
            )));
            let mut cancelled_count = 0;
            for _ in 0..80 {
                let (actual_round, index, cancelled) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), cancelled_rx.recv())
                        .await
                        .expect("permission response arrives")
                        .expect("agent response channel remains connected");
                assert_eq!(actual_round, round);
                assert!(index < 80);
                assert!(
                    cancelled,
                    "request permission response must be Cancelled, not approval or another error"
                );
                cancelled_count += 1;
            }
            assert_eq!(cancelled_count, 80);
        }
        let mut prompt = handle.prompt("2").await.unwrap();
        let release = start_rx.recv().await.unwrap();
        let mut count = 0;
        while count < 80 {
            if matches!(
                prompt.events.recv().await.unwrap(),
                BridgeStreamItem::Interrupt { .. }
            ) {
                count += 1;
            }
        }
        release.send(()).unwrap();
        while let Some(item) = prompt.events.recv().await {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                break;
            }
        }
        assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
        assert_eq!(count, 80);
        assert!(handle.pending_permissions().is_empty());
    }
}
