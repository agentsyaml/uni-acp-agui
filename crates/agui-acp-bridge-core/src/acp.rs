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
//!
//! The handle is a thin facade over a tokio actor task that owns the ACP
//! `connect_with(...)` future for the entire lifetime of the session. Dropping
//! the handle aborts the actor, which in turn drops the underlying `ChildGuard`
//! inside `agent-client-protocol-tokio` and kills the subprocess.
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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use agent_client_protocol::schema::StopReason;
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
}

impl PendingPermission {
    pub(crate) fn new(
        resolver: oneshot::Sender<PermissionDecision>,
        valid_option_ids: HashSet<String>,
    ) -> Self {
        Self {
            resolver,
            valid_option_ids,
        }
    }

    /// `true` if `option_id` is one of the choices the agent offered.
    #[must_use]
    pub fn allows_option(&self, option_id: &str) -> bool {
        self.valid_option_ids.contains(option_id)
    }
}

/// Shared map of pending permission requests awaiting external resolution.
pub type PendingPermissions = Arc<DashMap<String, PendingPermission>>;

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
    /// When `Some(session_id)`, the session is opened by **loading** an
    /// existing ACP session (`session/load`) rather than creating a fresh
    /// one (`session/new`). The agent replays the conversation history as
    /// `session/update` notifications, which the bridge streams back as
    /// AG-UI events. Requires the agent's `loadSession` capability; if it
    /// is absent the open falls back to `session/new` with a fresh id.
    ///
    /// `None` (the default) always creates a new session.
    pub load_session_id: Option<String>,
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
/// Use [`AcpSessionHandle::set_mode`] / [`AcpSessionHandle::set_model`] to switch
/// the session's mode or model via ACP `session/set_mode` / `session/set_model`.
///
/// Dropping the handle terminates the underlying actor and subprocess.
#[derive(Debug)]
pub struct AcpSessionHandle {
    cmd_tx: mpsc::Sender<SessionCommand>,
    cancel_notify: Arc<tokio::sync::Notify>,
    pending_permissions: PendingPermissions,
    /// Snapshot of `SessionModeState` returned by `session/new`, kept in sync
    /// with subsequent `session/set_mode` responses and `CurrentModeUpdate`
    /// notifications. The handler reads this on each new prompt to emit a
    /// fresh `SessionInit` so reconnecting clients still see the picker.
    init_state: Arc<StdMutex<SessionInitState>>,
    event_buffer: usize,
}

/// Per-session ACP-level capability snapshot the handle keeps cached so the
/// HTTP layer can serve picker UIs without round-tripping to the agent on
/// every request. The actor updates this when:
///
/// 1. `session/new` returns (initial set).
/// 2. `session/set_mode` / `session/set_model` succeeds (current id moves).
/// 3. A `session/update` `CurrentModeUpdate` notification arrives (agent
///    autonomously switched mode).
#[derive(Debug, Default, Clone)]
pub struct SessionInitState {
    pub modes: Option<SessionModesInit>,
    pub models: Option<SessionModelsInit>,
}

impl AcpSessionHandle {
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<SessionCommand>,
        cancel_notify: Arc<tokio::sync::Notify>,
        pending_permissions: PendingPermissions,
        init_state: Arc<StdMutex<SessionInitState>>,
        event_buffer: usize,
    ) -> Self {
        Self {
            cmd_tx,
            cancel_notify,
            pending_permissions,
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
        let (events_tx, events_rx) = mpsc::channel(self.event_buffer);
        let (finished_tx, finished_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Prompt {
                text: text.into(),
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
        self.cancel_notify.notify_waiters();
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
        // Validation passed (or it's a Deny) — consume the entry.
        let Some((_, pending)) = self.pending_permissions.remove(interrupt_id) else {
            return false;
        };
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
    /// after a successful `session/set_mode` / `session/set_model` or a
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

    /// Send an ACP `session/set_model` request and await the agent's
    /// acknowledgement. Only available when the `unstable_session_model`
    /// feature is enabled (default-on for this crate).
    #[cfg(feature = "unstable_session_model")]
    pub async fn set_model(&self, model_id: impl Into<String>) -> Result<(), BridgeError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetModel {
                model_id: model_id.into(),
                ack: ack_tx,
            })
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
        text: String,
        events_tx: mpsc::Sender<BridgeStreamItem>,
        finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
    },
    SetMode {
        mode_id: String,
        ack: oneshot::Sender<Result<(), BridgeError>>,
    },
    #[cfg(feature = "unstable_session_model")]
    SetModel {
        model_id: String,
        ack: oneshot::Sender<Result<(), BridgeError>>,
    },
    /// Flush `session/load` history onto a fresh stream without prompting.
    DrainHistory {
        events_tx: mpsc::Sender<BridgeStreamItem>,
        finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
    },
}
