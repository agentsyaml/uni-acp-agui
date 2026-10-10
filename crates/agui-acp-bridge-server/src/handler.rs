//! AG-UI `RunHandler` implementation that bridges into ACP sessions.
//!
//! Responsibilities:
//! 1. Maintain the `thread_id → AcpSessionHandle` map (sessions are created
//!    lazily on first use and reused across runs sharing a thread_id).
//! 2. Extract the user's prompt text from `RunAgentInput.messages`.
//! 3. Drive the per-prompt [`PromptStream`] and translate each
//!    [`BridgeStreamItem`] into AG-UI [`Event`]s, framed by `RUN_STARTED` /
//!    `RUN_FINISHED|RUN_ERROR`.
//! 4. Surface deferred permission requests to the AG-UI client as
//!    `STATE_SNAPSHOT` events with an `approval` payload, and accept
//!    decisions back via `POST /approval`.
//! 5. Reap idle sessions: a background task gracefully closes and then drops
//!    sessions whose `last_used` timestamp is older than
//!    `BridgeConfig.idle_timeout`.
//!
//! Concurrency: distinct `thread_id`s are independent; concurrent runs on the
//! **same** `thread_id` are rejected at the AG-UI admission boundary. The
//! DashMaps protect the run claim and lazy-creation races.

use std::cell::Cell;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use agent_client_protocol::schema::v1::{
    HttpHeader, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigOptionValue, SessionConfigSelectOptions, SessionId, StopReason,
};
use agui_rs_core::events::{Event, factory};
use agui_rs_core::types::{Message, RunAgentInput, UserMessageContent};
use agui_rs_server::error::{AgUiError, Result as AgUiResult};
use agui_rs_server::handler::RunHandler;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use futures::stream::{self, BoxStream, StreamExt};
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use agui_acp_bridge_core::acp::{
    AcpClient, AcpSessionHandle, PromptStream, SessionConfig, SessionInitState, TurnId,
};
use agui_acp_bridge_core::frontend_tools::{
    FrontendToolDef, FrontendToolRegistry, FrontendToolResponse,
};
use agui_acp_bridge_core::policy::PermissionPolicy;
use agui_acp_bridge_core::translation::{Translator, session_init_event};
use agui_acp_bridge_core::{BridgeConfig, BridgeError, BridgeStreamItem, canonicalize_cwd};
use agui_acp_bridge_policy::AutoDeny;

mod admission;
mod cache;
mod capacity;
mod frontend;
mod history_stream;
mod input;
mod lifecycle;
mod opening;
mod prompt_stream;
mod router;
mod run;
mod security;
mod settings;
mod sse_send;
mod state;
mod types;

use self::admission::{LifecycleGuard, RunAdmissionGuard, SessionCreateGate, ThreadClaim};
pub(crate) use self::cache::PromptGuard;
use self::cache::{
    McpCredential, SessionEntry, SessionsListCacheEntry, graceful_close_removed,
    remove_session_if_handle, remove_session_if_same, revoke_mcp_credential,
};
use self::frontend::{ClearOnDrop, OpeningMcpCredential};
use self::history_stream::build_history_stream;
#[cfg(test)]
use self::input::keepalive_interval_for_tests;
use self::input::{
    CAPACITY_REJECTED, DEFAULT_BODY_LIMIT_BYTES, SSE_KEEPALIVE_INTERVAL, TrailingUser,
    acp_failure_run_error, acp_resume_session_id, agui_input_boundary, extract_trailing_user_text,
    guarded_event_stream, keepalive_event, run_error_with_code, session_init_event_with_config,
    stop_reason_terminal_event,
};
#[cfg(test)]
use self::opening::resume_cwd;
use self::opening::{SessionAdmissionError, semaphore_for};
use self::prompt_stream::{
    EventStreamContext, build_event_stream_with_keepalive, install_frontend_sender,
};
pub use self::router::{build_router, build_router_inner};
pub(crate) use self::run::BridgeHandler;
use self::security::{bridge_security_middleware, has_valid_bearer, validate_bearer_token};
#[cfg(test)]
use self::sse_send::{RETIRED_SSE_DRAIN_GRACE, SseSendError, send_sse_with_timeout};
use self::sse_send::{RetiredSseDrain, send_history_sse, send_prompt_sse};
pub use self::state::BridgeAppStateBuilder;
pub(crate) use self::types::{
    CancelSessionBody, CloseSessionBody, DeleteSessionBody, SetSessionConfigOptionBody,
};
pub use self::types::{CloseSessionStatus, DeleteSessionStatus, ResolveOutcome, SetSessionStatus};
use self::types::{
    typed_config_option_value, validate_discovered_config_option, validate_legacy_mode,
};

fn build_event_stream(
    thread_id: String,
    run_id: String,
    prompt_stream: PromptStream,
    context: EventStreamContext,
    translated_buffer: usize,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    build_event_stream_with_keepalive(
        thread_id,
        run_id,
        prompt_stream,
        context,
        translated_buffer,
        prompt_guard,
        run_guard,
        SSE_KEEPALIVE_INTERVAL,
    )
}

/// Shared bridge state: ACP client factory + session map + cwd for new sessions
/// + bridge configuration + permission policy.
///
/// Constructed via [`BridgeAppState::new`] (uses [`AutoDeny`] +
/// [`BridgeConfig::default`]) or via [`BridgeAppState::builder`] for full control.
///
/// `Clone` is cheap (`Arc` bump). The background reaper holds a `Weak` so it
/// auto-exits when the last clone is dropped, and the inner `Drop` aborts
/// any spawned task lest it leak a worker on graceful shutdown.
#[derive(Clone)]
pub struct BridgeAppState {
    inner: Arc<Inner>,
}

struct Inner {
    sessions: DashMap<String, Arc<SessionEntry>>,
    mcp_credentials: DashMap<String, Arc<McpCredential>>,
    /// One mutually-exclusive run/lifecycle claim per thread. Run claims carry
    /// the AG-UI run id; lifecycle claims carry their short operation reason.
    /// The owning guard removes its exact claim conditionally on drop.
    active_runs: DashMap<String, ThreadClaim>,
    /// In-flight session setting RPCs. They are separate from AG-UI run claims
    /// because settings intentionally queue behind an active prompt, while
    /// close/reaper/LRU must still treat the setting as busy.
    active_settings: DashMap<String, usize>,
    /// Reference-counted per-thread gates for the lazy session-creation
    /// critical section. A gate remains mapped while queued callers still
    /// hold it, including after a failed creation.
    create_locks: DashMap<String, Arc<SessionCreateGate>>,
    /// Serializes the short semaphore/idle-victim selection section. This is
    /// never held across ACP actor creation or handshake.
    capacity_gate: tokio::sync::Mutex<()>,
    /// Caches `GET /sessions` results so bursts collapse into one
    /// `session/list` spawn (each one forks a subprocess). Single-slot and
    /// TTL-bounded; only successes are cached — errors return uncached. The
    /// `sessions_list_gate` mutex also caps concurrent uncached listings at
    /// one, which is the actual spawn bound.
    sessions_list_cache: tokio::sync::Mutex<Option<SessionsListCacheEntry>>,
    /// Fairness gate for the sessions-list cache; held across the ACP
    /// `session/list` call (which has its own `open_session_timeout` bound).
    sessions_list_gate: tokio::sync::Mutex<()>,
    /// Real live-session capacity. `None` is the explicit unlimited mode.
    session_capacity: Option<Arc<Semaphore>>,
    client: Arc<dyn AcpClient>,
    cwd: PathBuf,
    config: BridgeConfig,
    policy: Arc<dyn PermissionPolicy>,
    /// Registry powering frontend-tool injection (`useFrontendTool`).
    /// One entry per AG-UI thread; each entry stores the latest tool list,
    /// the live SSE sender, and the pending-call map keyed by tool_call_id.
    frontend_tools: FrontendToolRegistry,
    /// Public base URL the agent will connect to for MCP. When `Some`,
    /// per-session `mcp_url` is computed as `<self_url>/mcp/<thread-token>`.
    /// `None` disables frontend-tool injection (sessions get no mcp_servers
    /// entry); the bridge still serves AG-UI normally.
    self_url: Option<String>,
    /// Optional bearer credential protecting every route except exact
    /// `GET/HEAD /health`. The value is never included in debug output.
    bearer_token: Option<Arc<str>>,
    /// Canonical origins allowed on requests to the MCP endpoint. An empty
    /// set allows requests without an Origin header but rejects every present
    /// Origin header.
    mcp_allowed_origins: HashSet<String>,
    /// Handle to the background reaper task; aborted when `Inner` drops so
    /// graceful shutdown doesn't leak a tokio worker.
    reaper: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[cfg(test)]
mod tests;
