//! Public surface for ACP client adapters.
//!
//! An [`AcpClient`] knows how to open an ACP session backed by some transport
//! (subprocess, in-process, future variants). Each call to [`AcpClient::open_session`]
//! returns an [`AcpSessionHandle`] — a mpsc-style handle that lets the bridge:
//!
//! 1. Submit prompts and receive a [`PromptStream`] for that single turn:
//!    a bounded mpsc receiver of [`BridgeStreamItem`]s plus a oneshot delivering
//!    the terminal `StopReason` (or [`BridgeError`]).
//! 2. Cancel the in-flight turn.
//! 3. Close an agent-advertised ACP session with the real ACP `SessionId`.
//!
//! The handle is a thin facade over a tokio actor task that owns the ACP
//! `connect_with(...)` future for the entire lifetime of the session. Dropping
//! the handle aborts the actor, which in turn drops the underlying subprocess
//! guard owned by `agent-client-protocol::AcpAgent` and kills the child.
//!
//! # Why per-prompt channels?
//!
//! AG-UI runs map 1:1 to a single HTTP POST and a single SSE response. Each run
//! must emit `RunStarted` → updates → `RunFinished|RunError` and then end
//! cleanly. Routing every notification through a session-lifetime channel makes
//! it ambiguous which prompt a notification belongs to (especially after
//! interrupt + resume in M1.2). Per-prompt channels make the boundary explicit
//! and let `BridgeHandler::handle` produce a stream that terminates exactly
//! when the agent's turn does.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use agent_client_protocol::schema::v1::{
    ContentBlock, HttpHeader, SessionConfigOption, SessionConfigOptionValue, SessionId, StopReason,
    TextContent,
};
use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::config::BridgeConfig;
use crate::error::BridgeError;
use crate::policy::{PermissionDecision, PermissionPolicy};
use crate::stream::{BridgeStreamItem, SessionModelsInit, SessionModesInit, SessionSummary};

use crate::session::spawn_in_process_echo_session;

/// One pending permission request awaiting external resolution.
///
/// The bridge surfaces these via `BridgeStreamItem::Interrupt` (when a
/// `Defer` policy decision needs the AG-UI client to participate). The REST
/// `/approval` endpoint resolves the request by interrupt id; we store the
/// set of legal `option_id`s alongside the oneshot so the endpoint can
/// reject mismatched values instead of silently passing them through to the
/// agent (which would make the agent fail with a confusing error).
#[derive(Debug)]
pub struct PendingPermission {
    pub(crate) resolver: oneshot::Sender<PermissionDecision>,
    /// `option_id`s the agent advertised in the original request. Only an
    /// `Allow` decision is checked against this set; `Deny` always passes.
    pub(crate) valid_option_ids: HashSet<String>,
    pub(crate) turn: Arc<TurnState>,
}

impl PendingPermission {
    pub(crate) fn new(
        resolver: oneshot::Sender<PermissionDecision>,
        valid_option_ids: HashSet<String>,
        turn: Arc<TurnState>,
    ) -> Self {
        Self {
            resolver,
            valid_option_ids,
            turn,
        }
    }

    /// `true` if `option_id` is one of the choices the agent offered.
    #[must_use]
    pub fn allows_option(&self, option_id: &str) -> bool {
        self.valid_option_ids.contains(option_id)
    }

    pub(crate) fn turn(&self) -> Arc<TurnState> {
        self.turn.clone()
    }
}

/// Shared map of pending permission requests awaiting external resolution.
pub type PendingPermissions = Arc<DashMap<String, PendingPermission>>;

/// Shared cancellation state for one queued/in-flight prompt turn.
///
/// The cancellation flag and the pending-id set are guarded together while a
/// permission is registered or drained. That makes a cancel racing with a
/// late `requestPermission` deterministic: either registration happens first
/// and the entry is drained, or it observes the flag and responds cancelled
/// without entering the map.
static NEXT_TURN_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque identity for one queued or in-flight prompt turn.
///
/// A [`PromptStream`] carries its own identity so a consumer disconnect can
/// cancel that turn without accidentally cancelling an older turn that is
/// currently running ahead of it in the session queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TurnId(u64);

#[derive(Debug)]
pub(crate) struct TurnState {
    id: TurnId,
    cancelled: AtomicBool,
    pending_ids: StdMutex<HashSet<String>>,
    pub(crate) cancel_notify: tokio::sync::Notify,
}

impl TurnState {
    pub(crate) fn new() -> Self {
        Self {
            id: TurnId(NEXT_TURN_ID.fetch_add(1, Ordering::Relaxed)),
            cancelled: AtomicBool::new(false),
            pending_ids: StdMutex::new(HashSet::new()),
            cancel_notify: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn id(&self) -> TurnId {
        self.id
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn register_pending(
        &self,
        pending_permissions: &PendingPermissions,
        interrupt_id: String,
        pending: PendingPermission,
    ) -> bool {
        let mut ids = self.pending_ids.lock().expect("turn state poisoned");
        if self.is_cancelled() {
            return false;
        }
        pending_permissions.insert(interrupt_id.clone(), pending);
        ids.insert(interrupt_id);
        true
    }

    pub(crate) fn remove_pending(
        &self,
        pending_permissions: &PendingPermissions,
        interrupt_id: &str,
    ) {
        let mut ids = self.pending_ids.lock().expect("turn state poisoned");
        pending_permissions.remove(interrupt_id);
        ids.remove(interrupt_id);
    }

    /// Mark this turn cancelled and resolve every permission registered for it
    /// with `Deny`. The registration lock is held across the flag check and
    /// drain, so a permission callback cannot register after the drain.
    pub(crate) fn cancel_and_drain(&self, pending_permissions: &PendingPermissions) {
        let mut ids = self.pending_ids.lock().expect("turn state poisoned");
        self.cancelled.store(true, Ordering::Release);
        // There is one prompt waiter per turn. `notify_one` pairs with the
        // `Notified::enable` registration in `run_prompt_with_cancel`, so a
        // cancellation that lands between the flag check and `select!` is
        // retained rather than lost.
        self.cancel_notify.notify_one();
        let keys: Vec<String> = ids.drain().collect();
        drop(ids);

        for key in keys {
            if let Some((_, pending)) = pending_permissions.remove(&key) {
                let _ = pending.resolver.send(PermissionDecision::Deny);
            }
        }
    }
}

/// Queue of prompt turns in actor order. Keeping the turn in this shared
/// queue before sending the actor command closes the small window where a
/// caller cancels immediately after `prompt()` but before the actor receives
/// the command.
#[derive(Debug)]
pub(crate) struct SessionTurnQueue {
    turns: StdMutex<VecDeque<Arc<TurnState>>>,
    max_queued_turns: usize,
}

impl SessionTurnQueue {
    pub(crate) fn new(max_queued_turns: usize) -> Self {
        Self {
            turns: StdMutex::new(VecDeque::new()),
            max_queued_turns,
        }
    }

    /// Development-only explicit unlimited mode uses `max_queued_turns = 0`.
    pub(crate) fn try_enqueue(&self) -> Result<Arc<TurnState>, usize> {
        let turn = Arc::new(TurnState::new());
        let mut turns = self.turns.lock().expect("turn queue poisoned");
        if self.max_queued_turns != 0 && turns.len() >= self.max_queued_turns {
            return Err(self.max_queued_turns);
        }
        turns.push_back(turn.clone());
        Ok(turn)
    }

    pub(crate) fn current(&self) -> Option<Arc<TurnState>> {
        self.turns
            .lock()
            .expect("turn queue poisoned")
            .front()
            .cloned()
    }

    pub(crate) fn find(&self, id: TurnId) -> Option<Arc<TurnState>> {
        self.turns
            .lock()
            .expect("turn queue poisoned")
            .iter()
            .find(|turn| turn.id() == id)
            .cloned()
    }

    pub(crate) fn remove(&self, target: &Arc<TurnState>) {
        let mut turns = self.turns.lock().expect("turn queue poisoned");
        if let Some(index) = turns.iter().position(|turn| Arc::ptr_eq(turn, target)) {
            turns.remove(index);
        }
    }

    pub(crate) fn clear(&self) {
        self.turns.lock().expect("turn queue poisoned").clear();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.turns.lock().expect("turn queue poisoned").is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.turns.lock().expect("turn queue poisoned").len()
    }
}

impl Default for SessionTurnQueue {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Per-session configuration handed to [`AcpClient::open_session`].
///
/// Bundles the working directory the session will use, the permission policy
/// applied to ACP `requestPermission` requests, the runtime tuning knobs
/// (timeouts, buffer sizes), and (optionally) an MCP HTTP URL the agent
/// should connect to for client-injected tools (`useFrontendTool`). Cloning
/// is cheap (everything heavy is `Arc`-wrapped).
#[derive(Clone)]
pub struct SessionConfig {
    /// Initial working directory passed to ACP's `session/new`.
    pub cwd: PathBuf,
    /// Permission policy consulted for every `requestPermission` request.
    pub policy: Arc<dyn PermissionPolicy>,
    /// Runtime tuning knobs (timeouts, buffer sizes).
    pub config: BridgeConfig,
    /// Optional MCP HTTP server URL to advertise in the session's
    /// `mcp_servers` field. If `Some`, the bridge expects the agent's
    /// `mcpCapabilities.http` to be true; the URL is opaque (typically
    /// `http://<bridge-host>/mcp/<thread-token>`).
    ///
    /// `None` disables frontend-tool injection for this session.
    pub mcp_url: Option<String>,
    /// HTTP headers sent by the agent to the advertised MCP server.
    ///
    /// The bridge uses this for the optional bearer token. Values are kept out
    /// of [`Debug`] output so credentials cannot leak through configuration
    /// logging.
    pub mcp_headers: Vec<HttpHeader>,
    /// When `Some(session_id)`, the session is opened by **loading** an
    /// existing ACP session (`session/load`) rather than creating a fresh
    /// one (`session/new`). The agent replays the conversation history as
    /// `session/update` notifications, which the bridge streams back as
    /// AG-UI events. Requires the agent's `loadSession` capability; if it is
    /// absent, or if `session/load` fails, opening the session fails without
    /// falling back to `session/new`.
    ///
    /// `None` (the default) always creates a new session.
    pub load_session_id: Option<SessionId>,
}

impl std::fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionConfig")
            .field("cwd", &self.cwd)
            .field("policy", &format_args!("{:?}", self.policy))
            .field("config", &self.config)
            .field("mcp_url", &self.mcp_url)
            .field("load_session_id", &self.load_session_id)
            .finish()
    }
}

/// A factory that opens ACP sessions on demand.
///
/// Implementations are expected to be cheap to clone / share (`Arc<dyn AcpClient>`).
/// The actual heavy lifting (spawning a subprocess, performing the JSON-RPC
/// handshake, creating the session) happens inside [`AcpClient::open_session`].
#[async_trait]
pub trait AcpClient: Send + Sync + 'static {
    /// Open a fresh ACP session with the supplied configuration.
    ///
    /// The returned handle owns the live connection. Dropping it terminates
    /// the session (and, for subprocess-backed clients, kills the child).
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError>;

    /// List persisted sessions via ACP `session/list`.
    ///
    /// Opens a short-lived ACP connection, performs the `initialize`
    /// handshake to confirm the agent advertises
    /// `sessionCapabilities.list`, issues `session/list`, and tears the
    /// connection down. The bridge holds no state of its own — this is a
    /// pass-through view of what the agent persists.
    ///
    /// Returns [`BridgeError::Unsupported`] if the agent does not advertise
    /// the `session/list` capability. The default implementation returns
    /// `Unsupported` so clients that cannot list (e.g. the in-process echo)
    /// degrade gracefully.
    async fn list_sessions(&self, _cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        Err(BridgeError::Unsupported("session/list".into()))
    }

    /// Delete a persisted ACP session using a short-lived connection.
    ///
    /// The default implementation returns [`BridgeError::Unsupported`] so
    /// existing clients that do not expose transient ACP connections remain
    /// source-compatible.
    async fn delete_session(
        &self,
        _cfg: SessionConfig,
        _session_id: SessionId,
    ) -> Result<(), BridgeError> {
        Err(BridgeError::Unsupported("session/delete".into()))
    }
}

/// An ACP client that runs an embedded echo agent over `tokio::io::duplex`.
///
/// Useful as a fixture for HTTP/SSE round-trip tests and for local development
/// without a real ACP agent binary. Each `open_session` call spawns a fresh
/// in-process agent task; dropping the returned handle drops the duplex stream
/// and the agent task terminates naturally.
#[derive(Debug, Default, Clone, Copy)]
pub struct InProcessAcpClient;

impl InProcessAcpClient {
    /// Create a new in-process client backed by an embedded echo agent.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AcpClient for InProcessAcpClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        spawn_in_process_echo_session(cfg).await
    }
}

/// Test-only `AcpClient` that hands each `open_session` call a freshly spawned
/// in-process agent produced by the supplied factory. Used by the bridge's
/// integration tests to drive non-text and error paths through the full HTTP
/// stack without a subprocess.
#[doc(hidden)]
pub struct CustomAgentInProcessClient<F> {
    factory: Arc<F>,
}

impl<F> CustomAgentInProcessClient<F> {
    #[doc(hidden)]
    pub fn new(factory: F) -> Self {
        Self {
            factory: Arc::new(factory),
        }
    }
}

impl<F> std::fmt::Debug for CustomAgentInProcessClient<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAgentInProcessClient")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<F, Fut> AcpClient for CustomAgentInProcessClient<F>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), BridgeError>> + Send + 'static,
{
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let factory = self.factory.clone();
        crate::session::spawn_in_process_session_with(cfg, move |s| Box::pin(factory(s))).await
    }

    async fn delete_session(
        &self,
        cfg: SessionConfig,
        session_id: SessionId,
    ) -> Result<(), BridgeError> {
        let factory = self.factory.clone();
        crate::session::delete_session_in_process_with(cfg, session_id, move |s| {
            Box::pin(factory(s))
        })
        .await
    }
}

/// The streaming side of one prompt turn.
///
/// `events` carries every `session/update` (and any mid-turn interrupt) emitted
/// by the agent for **this** prompt only. The actor closes the channel as soon
/// as the prompt's `StopReason` is delivered, so iterating to `None` is a safe
/// way to drain the turn.
///
/// `finished` resolves exactly once with the terminal outcome of the prompt:
/// either `Ok(StopReason)` from the agent's `prompt` response or a
/// `BridgeError` if the session/transport failed mid-turn.
///
/// Both halves are independent: callers can `tokio::select!` on `finished` and
/// `events.recv()`, or drain `events` to completion and then await `finished`.
#[derive(Debug)]
#[must_use = "PromptStream owns the events for the turn; dropping it cancels nothing but discards events"]
pub struct PromptStream {
    /// Per-prompt event channel. Closes when the prompt completes.
    pub events: mpsc::Receiver<BridgeStreamItem>,
    /// Resolves with the terminal `StopReason` (or `BridgeError`) for this turn.
    pub finished: oneshot::Receiver<Result<StopReason, BridgeError>>,
}

/// A live ACP session.
///
/// Use [`AcpSessionHandle::prompt`] to submit a turn and receive its [`PromptStream`].
/// Use [`AcpSessionHandle::cancel`] to abort the in-flight turn.
/// Use [`AcpSessionHandle::resolve_permission`] to resolve a pending permission request.
/// Use [`AcpSessionHandle::set_mode`] and
/// [`AcpSessionHandle::set_config_option`] to change session settings.
/// Use [`AcpSessionHandle::close`] for an agent-advertised ACP
/// `session/close`; dropping the handle remains the local fallback cleanup.
///
/// Dropping the handle terminates the underlying actor and subprocess.
#[derive(Debug)]
pub struct AcpSessionHandle {
    cmd_tx: mpsc::Sender<SessionCommand>,
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    unusable: Arc<AtomicBool>,
    session_id: SessionId,
    supports_close: bool,
    /// Snapshot of `SessionModeState` returned by `session/new`, kept in sync
    /// with subsequent `session/set_mode` responses and `CurrentModeUpdate`
    /// notifications. The handler reads this on each new prompt to emit a
    /// fresh `SessionInit` so reconnecting clients still see the picker.
    init_state: Arc<StdMutex<SessionInitState>>,
    event_buffer: usize,
}

/// Removes a turn admission if the command send is cancelled or fails before
/// the actor takes ownership of it. Once the command is delivered, the actor
/// owns removal on every terminal path.
struct EnqueuedTurnGuard {
    queue: Arc<SessionTurnQueue>,
    turn: Option<Arc<TurnState>>,
}

impl EnqueuedTurnGuard {
    fn new(queue: Arc<SessionTurnQueue>, turn: Arc<TurnState>) -> Self {
        Self {
            queue,
            turn: Some(turn),
        }
    }

    fn disarm(&mut self) {
        self.turn = None;
    }
}

impl Drop for EnqueuedTurnGuard {
    fn drop(&mut self) {
        if let Some(turn) = &self.turn {
            self.queue.remove(turn);
        }
    }
}

/// Per-session ACP-level capability snapshot the handle keeps cached so the
/// HTTP layer can serve picker UIs without round-tripping to the agent on
/// every request. The actor updates this when:
///
/// 1. `session/new` returns (initial set).
/// 2. `session/set_mode` / `session/set_config_option` succeeds.
/// 3. A `session/update` `CurrentModeUpdate` notification arrives (agent
///    autonomously switched mode).
#[derive(Debug, Default, Clone)]
pub struct SessionInitState {
    pub modes: Option<SessionModesInit>,
    pub models: Option<SessionModelsInit>,
    /// Complete config-option snapshot returned by `session/new` or
    /// `session/load`, then replaced by config update/set responses.
    pub config_options: Option<Vec<SessionConfigOption>>,
}

impl AcpSessionHandle {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<SessionCommand>,
        pending_permissions: PendingPermissions,
        turn_queue: Arc<SessionTurnQueue>,
        unusable: Arc<AtomicBool>,
        session_id: SessionId,
        supports_close: bool,
        init_state: Arc<StdMutex<SessionInitState>>,
        event_buffer: usize,
    ) -> Self {
        Self {
            cmd_tx,
            pending_permissions,
            turn_queue,
            unusable,
            session_id,
            supports_close,
            init_state,
            event_buffer: event_buffer.max(1),
        }
    }

    /// Submit a text prompt and receive a [`PromptStream`] scoped to that turn.
    ///
    /// The returned channels are created fresh per call; previous prompts'
    /// channels are unaffected. The actor processes prompts sequentially, so
    /// concurrent calls on the same session will queue.
    pub async fn prompt(&self, text: impl Into<String>) -> Result<PromptStream, BridgeError> {
        self.prompt_blocks(vec![ContentBlock::Text(TextContent::new(text))])
            .await
    }

    /// Submit an ordered ACP content-block prompt and receive a [`PromptStream`]
    /// scoped to that turn. Every block is forwarded to ACP unchanged and in
    /// the supplied order.
    pub async fn prompt_blocks(
        &self,
        prompt: Vec<ContentBlock>,
    ) -> Result<PromptStream, BridgeError> {
        self.prompt_blocks_with_turn(prompt)
            .await
            .map(|(prompt, _turn_id)| prompt)
    }

    /// Submit a text prompt and return its stream together with the opaque
    /// identity used to cancel exactly this queued/in-flight turn.
    ///
    /// This additive API keeps [`PromptStream`] structurally compatible for
    /// downstream struct literals and exhaustive destructuring; callers that
    /// need disconnect-scoped cancellation can opt into the turn identity.
    pub async fn prompt_with_turn(
        &self,
        text: impl Into<String>,
    ) -> Result<(PromptStream, TurnId), BridgeError> {
        self.prompt_blocks_with_turn(vec![ContentBlock::Text(TextContent::new(text))])
            .await
    }

    /// Submit an ordered ACP content-block prompt and return its stream
    /// together with the opaque identity used to cancel exactly this
    /// queued/in-flight turn.
    pub async fn prompt_blocks_with_turn(
        &self,
        prompt: Vec<ContentBlock>,
    ) -> Result<(PromptStream, TurnId), BridgeError> {
        if self.cmd_tx.is_closed() || self.is_unusable() {
            return Err(BridgeError::SessionClosed);
        }
        let (events_tx, events_rx) = mpsc::channel(self.event_buffer);
        let (finished_tx, finished_rx) = oneshot::channel();
        let turn = self
            .turn_queue
            .try_enqueue()
            .map_err(|max_queued_turns| BridgeError::QueueCapacity { max_queued_turns })?;
        let mut enqueue_guard = EnqueuedTurnGuard::new(self.turn_queue.clone(), turn.clone());
        self.cmd_tx
            .send(SessionCommand::Prompt {
                prompt,
                events_tx,
                finished_tx,
                turn: turn.clone(),
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        enqueue_guard.disarm();
        Ok((
            PromptStream {
                events: events_rx,
                finished: finished_rx,
            },
            turn.id(),
        ))
    }

    /// Cancel the in-flight turn (if any).
    ///
    /// This signals the session actor to send an ACP `session/cancel`
    /// notification to the agent. It is fire-and-forget: if no prompt is in
    /// flight the call is a no-op. The corresponding turn's [`PromptStream`]
    /// will subsequently emit `Finished { stop_reason: Cancelled }` once the
    /// agent acks.
    pub fn cancel(&self) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        if let Some(turn) = self.turn_queue.current() {
            self.cancel_turn(turn.id())?;
        }
        Ok(())
    }

    /// Cancel one specific queued or in-flight turn.
    ///
    /// The identity must have come from a [`PromptStream`] owned by this
    /// session. A turn from another session is ignored, which prevents a
    /// stale SSE cleanup task from cancelling unrelated work.
    pub fn cancel_turn(&self, turn_id: TurnId) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        if let Some(turn) = self.turn_queue.find(turn_id) {
            turn.cancel_and_drain(&self.pending_permissions);
        }
        Ok(())
    }

    /// Resolve a pending permission request by its interrupt ID.
    ///
    /// The decision must satisfy:
    /// - For `PermissionDecision::Allow { option_id }`, the `option_id` must
    ///   match one of the choices the agent advertised in the original
    ///   `RequestPermissionRequest`. If it does not, the resolution is
    ///   rejected and `false` is returned (the pending request stays in the
    ///   map until it is resolved correctly or times out).
    /// - `Deny` is always accepted.
    /// - `Defer` is rejected (the policy has already decided to defer; this
    ///   value would loop the bridge).
    ///
    /// Returns `true` if the permission was found, validated, and resolved;
    /// `false` if no pending permission with that ID exists or the decision
    /// was rejected by validation.
    #[must_use]
    pub fn resolve_permission(&self, interrupt_id: &str, decision: PermissionDecision) -> bool {
        // Reject `Defer` early without touching the map.
        if matches!(decision, PermissionDecision::Defer { .. }) {
            return false;
        }
        // Validate `Allow.option_id` against the stored set before consuming
        // the oneshot — leaving the entry in place for retry if invalid.
        if let PermissionDecision::Allow { ref option_id } = decision {
            if let Some(entry) = self.pending_permissions.get(interrupt_id) {
                if !entry.allows_option(option_id.0.as_ref()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        let turn = self
            .pending_permissions
            .get(interrupt_id)
            .map(|entry| entry.turn());
        let Some(turn) = turn else {
            return false;
        };
        let mut turn_guard = turn.pending_ids.lock().expect("turn state poisoned");
        if turn.is_cancelled() {
            return false;
        }
        // Validation passed (or it's a Deny) — consume the entry.
        let Some((_, pending)) = self.pending_permissions.remove(interrupt_id) else {
            return false;
        };
        turn_guard.remove(interrupt_id);
        pending.resolver.send(decision).is_ok()
    }

    /// Access the pending permissions map (for external resolution via REST).
    pub fn pending_permissions(&self) -> &PendingPermissions {
        &self.pending_permissions
    }

    /// Snapshot the cached init state (modes / models) for HTTP discovery.
    ///
    /// The snapshot reflects the most recent state known to the bridge:
    /// the initial offering from `session/new`, plus any updates applied
    /// after a successful `session/set_mode` / `session/set_config_option` or a
    /// `CurrentModeUpdate` notification from the agent.
    #[must_use]
    pub fn init_state(&self) -> SessionInitState {
        self.init_state.lock().expect("init_state poisoned").clone()
    }

    /// Send an ACP `session/set_mode` request and await the agent's
    /// acknowledgement. Returns `Err(BridgeError::SessionClosed)` if the
    /// actor is gone, `Err(BridgeError::Acp)` if the agent rejected the
    /// mode_id (typically because it isn't in `availableModes`).
    pub async fn set_mode(&self, mode_id: impl Into<String>) -> Result<(), BridgeError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetMode {
                mode_id: mode_id.into(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Send an ACP `session/set_config_option` request with a select/value-id
    /// payload and await the agent's acknowledgement.
    pub async fn set_config_option(
        &self,
        config_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), BridgeError> {
        self.set_config_option_value(config_id, SessionConfigOptionValue::value_id(value.into()))
            .await
    }

    /// Send an ACP `session/set_config_option` request with its typed payload
    /// and await the agent's acknowledgement.
    pub async fn set_config_option_value(
        &self,
        config_id: impl Into<String>,
        value: SessionConfigOptionValue,
    ) -> Result<(), BridgeError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetConfigOption {
                config_id: config_id.into(),
                value,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Whether cancellation exceeded its grace window and this ACP session
    /// must not be reused.
    #[must_use]
    pub fn is_unusable(&self) -> bool {
        self.unusable.load(Ordering::Acquire)
    }

    /// The real ACP session identifier returned by `session/new` or
    /// `session/load`.
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Whether the agent advertised `sessionCapabilities.close`.
    #[must_use]
    pub fn supports_close(&self) -> bool {
        self.supports_close
    }

    /// Whether no prompt is active or queued for this session.
    #[must_use]
    pub fn turn_queue_empty(&self) -> bool {
        self.turn_queue.is_empty()
    }

    /// Ask the agent to close this ACP session when it advertises
    /// `sessionCapabilities.close`.
    ///
    /// The actor bounds the request with `set_session_timeout`. A successful,
    /// failed, or timed-out close makes the handle unusable; dropping the
    /// handle remains the local cleanup mechanism. Unsupported close leaves
    /// the actor usable and sends no ACP request.
    pub async fn close(&self) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Close { ack: ack_tx })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Flush any history captured from a `session/load` onto a fresh
    /// [`PromptStream`] **without** prompting the agent.
    ///
    /// Used for "resume bootstrap" runs: the AG-UI client opens a previously
    /// persisted thread and expects to see its prior conversation, but is not
    /// submitting a new turn. The returned stream replays the loaded history
    /// (if any) and then finishes immediately. If the session was not resumed
    /// (no buffered history), the stream simply finishes with no updates.
    pub async fn drain_history(&self) -> Result<PromptStream, BridgeError> {
        let (events_tx, events_rx) = mpsc::channel(self.event_buffer);
        let (finished_tx, finished_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::DrainHistory {
                events_tx,
                finished_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        Ok(PromptStream {
            events: events_rx,
            finished: finished_rx,
        })
    }
}

/// Commands sent from [`AcpSessionHandle`] to the actor task.
#[derive(Debug)]
pub(crate) enum SessionCommand {
    Prompt {
        prompt: Vec<ContentBlock>,
        events_tx: mpsc::Sender<BridgeStreamItem>,
        finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
        turn: Arc<TurnState>,
    },
    SetMode {
        mode_id: String,
        ack: oneshot::Sender<Result<(), BridgeError>>,
    },
    SetConfigOption {
        config_id: String,
        value: SessionConfigOptionValue,
        ack: oneshot::Sender<Result<(), BridgeError>>,
    },
    Close {
        ack: oneshot::Sender<Result<(), BridgeError>>,
    },
    /// Flush `session/load` history onto a fresh stream without prompting.
    DrainHistory {
        events_tx: mpsc::Sender<BridgeStreamItem>,
        finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_queue_rejects_at_capacity_without_disturbing_existing_turn() {
        let queue = SessionTurnQueue::new(1);
        let active = queue.try_enqueue().expect("first turn fits");
        assert!(matches!(queue.try_enqueue(), Err(1)));
        assert!(queue.find(active.id()).is_some());

        queue.remove(&active);
        assert!(queue.try_enqueue().is_ok());
    }

    #[test]
    fn zero_turn_queue_limit_is_explicitly_unlimited() {
        let queue = SessionTurnQueue::new(0);
        for _ in 0..128 {
            assert!(queue.try_enqueue().is_ok());
        }
    }

    #[test]
    fn clear_releases_all_turn_slots() {
        let queue = SessionTurnQueue::new(2);
        let _first = queue.try_enqueue().expect("first turn fits");
        let _second = queue.try_enqueue().expect("second turn fits");
        assert_eq!(queue.len(), 2);

        queue.clear();

        assert_eq!(queue.len(), 0);
        assert!(queue.try_enqueue().is_ok());
    }

    #[tokio::test]
    async fn cancelled_prompt_send_releases_turn_slot() {
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let queue = Arc::new(SessionTurnQueue::new(1));
        let handle = Arc::new(AcpSessionHandle::new(
            cmd_tx.clone(),
            Arc::new(DashMap::new()),
            queue.clone(),
            Arc::new(AtomicBool::new(false)),
            SessionId::from("test-session"),
            false,
            Arc::new(StdMutex::new(SessionInitState::default())),
            1,
        ));
        let (ack_tx, _ack_rx) = oneshot::channel();
        cmd_tx
            .send(SessionCommand::SetMode {
                mode_id: "blocked".into(),
                ack: ack_tx,
            })
            .await
            .expect("fill command channel");

        let prompt = tokio::spawn({
            let handle = handle.clone();
            async move { handle.prompt_with_turn("blocked prompt").await }
        });
        for _ in 0..16 {
            if queue.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            queue.len(),
            1,
            "prompt must enqueue before send backpressure"
        );

        prompt.abort();
        let _ = prompt.await;
        assert_eq!(queue.len(), 0, "cancelled enqueue must release its slot");
    }
}
