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

use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    ContentBlock, HttpHeader, SessionConfigOption, SessionConfigOptionValue, SessionId, StopReason,
};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use crate::config::BridgeConfig;
use crate::error::BridgeError;
use crate::policy::PermissionPolicy;
use crate::stream::{BridgeStreamItem, SessionModelsInit, SessionModesInit, SessionSummary};

use crate::session::spawn_in_process_echo_session;

mod handle;
#[cfg(test)]
mod tests;
mod turns;

pub use handle::{AcpSessionHandle, SessionInitState};
#[cfg(test)]
use turns::MAX_PENDING_PERMISSIONS_PER_TURN;
pub(crate) use turns::{MAX_PENDING_PERMISSION_BYTES_PER_REQUEST, SessionTurnQueue, TurnState};
pub use turns::{PendingPermission, PendingPermissions, TurnId};

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
