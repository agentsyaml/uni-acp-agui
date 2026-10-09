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
use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_stream::wrappers::ReceiverStream;

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

/// Outcome of resolving a deferred permission request.
///
/// Surfaced through `BridgeAppState::resolve_permission` so HTTP handlers can
/// distinguish "no such interrupt" (404) from "invalid option" (422) from
/// "ok" (200) without losing the pending entry on validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Permission was found, validated, and delivered to the session actor.
    Resolved,
    /// The supplied `option_id` is not one of the choices the agent offered.
    /// The pending request stays in the map so the caller can retry.
    InvalidOption,
    /// No pending permission with that id (already resolved, timed out, or
    /// never existed).
    NotFound,
}

/// Outcome of the server-facing session setting operations. The HTTP routes
/// map these to the status codes documented on the route itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetSessionStatus {
    /// No session for the supplied thread id (caller must create one first
    /// by issuing a normal AG-UI run).
    NotFound,
    /// Another lifecycle operation owns the thread claim.
    Busy,
    /// Agent rejected the request, or no matching discovered config option was
    /// available for a mode/model alias.
    Acp(String),
    /// Agent did not respond within `BridgeConfig.set_session_timeout`.
    /// The session is left intact and the caller can retry.
    Timeout,
    /// Underlying session actor terminated; cache entry is evicted.
    SessionClosed,
}

/// JSON body for the server-facing config-option route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetSessionConfigOptionBody {
    pub thread_id: String,
    pub config_id: String,
    #[serde(deserialize_with = "deserialize_string_or_boolean")]
    pub value: serde_json::Value,
}

fn deserialize_string_or_boolean<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
    match value {
        serde_json::Value::Bool(_) | serde_json::Value::String(_) => Ok(value),
        _ => Err(serde::de::Error::custom(
            "value must be a JSON string or boolean",
        )),
    }
}

fn typed_config_option_value(value: serde_json::Value) -> SessionConfigOptionValue {
    match value {
        serde_json::Value::Bool(value) => SessionConfigOptionValue::boolean(value),
        serde_json::Value::String(value) => SessionConfigOptionValue::value_id(value),
        _ => unreachable!("config option input was validated during deserialization"),
    }
}

fn config_option_value_type_matches(
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> bool {
    matches!(
        (&option.kind, value),
        (
            SessionConfigKind::Boolean(_),
            SessionConfigOptionValue::Boolean { .. }
        ) | (
            SessionConfigKind::Select(_),
            SessionConfigOptionValue::ValueId { .. }
        )
    )
}

fn config_option_value_matches(
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> bool {
    if !config_option_value_type_matches(option, value) {
        return false;
    }
    match (&option.kind, value) {
        (SessionConfigKind::Boolean(_), SessionConfigOptionValue::Boolean { .. }) => true,
        (SessionConfigKind::Select(select), SessionConfigOptionValue::ValueId { value }) => {
            match &select.options {
                SessionConfigSelectOptions::Ungrouped(options) => options
                    .iter()
                    .any(|option| option.value.0.as_ref() == value.0.as_ref()),
                SessionConfigSelectOptions::Grouped(groups) => groups.iter().any(|group| {
                    group
                        .options
                        .iter()
                        .any(|option| option.value.0.as_ref() == value.0.as_ref())
                }),
                _ => false,
            }
        }
        _ => false,
    }
}

fn validate_discovered_config_option(
    config_id: &str,
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> Result<(), SetSessionStatus> {
    if !config_option_value_type_matches(option, value) {
        Err(SetSessionStatus::Acp(format!(
            "config option `{config_id}` does not accept this value type"
        )))
    } else if config_option_value_matches(option, value) {
        Ok(())
    } else {
        Err(SetSessionStatus::Acp(format!(
            "config option `{config_id}` does not advertise this value"
        )))
    }
}

fn validate_legacy_mode(
    modes: Option<&agui_acp_bridge_core::SessionModesInit>,
    mode_id: &str,
) -> Result<(), SetSessionStatus> {
    let Some(modes) = modes else {
        return Err(SetSessionStatus::Acp(
            "agent did not advertise a legacy mode capability".into(),
        ));
    };
    if !modes.available_modes.iter().any(|mode| mode.id == mode_id) {
        return Err(SetSessionStatus::Acp(format!(
            "mode `{mode_id}` is not advertised"
        )));
    }
    Ok(())
}

/// JSON body for the server-facing cancel route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelSessionBody {
    pub thread_id: String,
}

/// JSON body for the explicit ACP session-close route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CloseSessionBody {
    pub thread_id: String,
}

/// JSON body for the explicit ACP session-delete route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteSessionBody {
    pub thread_id: String,
}

/// Outcome of closing a cached ACP session. The route maps each variant to a
/// stable HTTP status without collapsing unsupported close into a destructive
/// local drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseSessionStatus {
    /// No cached session exists for the supplied thread id.
    NotFound,
    /// A run, setting, queued turn, or pending frontend/permission operation
    /// prevents a terminal close.
    Busy,
    /// The agent did not advertise `sessionCapabilities.close`.
    Unsupported,
    /// The bounded ACP close request timed out.
    Timeout,
    /// The agent rejected the close request.
    Acp(String),
    /// The local actor was already closed.
    SessionClosed,
}

/// Outcome of deleting a persisted ACP session mapped to an exact AG-UI
/// thread. A missing bridge mapping is a local not-found response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteSessionStatus {
    /// The request did not contain a non-empty AG-UI thread id.
    InvalidInput,
    /// No bridge-owned session is mapped to the supplied AG-UI thread id.
    NotFound,
    /// A run, setting, queued turn, or pending frontend/permission operation
    /// prevents deletion.
    Busy,
    /// The agent did not advertise `sessionCapabilities.delete`.
    Unsupported,
    /// The bounded ACP delete request timed out.
    Timeout,
    /// The agent or transient transport rejected the request.
    Acp(String),
}

/// Keep the existing `agent:session_init` event shape and append the ACP
/// config-option snapshot without changing the general AG-UI translator.
fn session_init_event_with_config(
    modes: Option<&agui_acp_bridge_core::SessionModesInit>,
    models: Option<&agui_acp_bridge_core::stream::SessionModelsInit>,
    config_options: Option<&[agui_acp_bridge_core::SessionConfigOption]>,
) -> Event {
    let mut event = session_init_event(modes, models);
    if let Event::Custom(custom) = &mut event
        && let serde_json::Value::Object(payload) = &mut custom.value
    {
        payload.insert(
            "configOptions".to_string(),
            config_options
                .and_then(|options| serde_json::to_value(options).ok())
                .unwrap_or(serde_json::Value::Null),
        );
    }
    event
}

fn run_error_with_code(code: &'static str, message: impl Into<String>) -> Event {
    Event::RunError(agui_rs_core::events::RunErrorEvent {
        message: message.into(),
        code: Some(code.to_string()),
        base: agui_rs_core::events::BaseEventFields::default(),
    })
}

/// Map an ACP-origin `BridgeError` to a machine-readable RUN_ERROR code.
///
/// The cancel-grace expiry is the one timeout a client can act on (it
/// cancelled and the agent refused to stop within the grace window), so it
/// gets its own code via the dedicated [`BridgeError::CancelGraceExpired`]
/// variant. Every other ACP/connection failure collapses to `ACP_ERROR` with
/// the error string preserved in the message.
fn acp_failure_run_error(error: &BridgeError) -> Event {
    let is_cancel_grace = matches!(error, BridgeError::CancelGraceExpired(_));
    if is_cancel_grace {
        run_error_with_code("ACP_CANCEL_GRACE_EXPIRED", error.to_string())
    } else {
        run_error_with_code("ACP_ERROR", error.to_string())
    }
}

const MIN_BEARER_TOKEN_LEN: usize = 16;

tokio::task_local! {
    static CAPACITY_REJECTED: Cell<bool>;
}

fn validate_bearer_token(token: &str) -> Result<(), String> {
    if token.is_empty() {
        return Err("AGUI_ACP_BRIDGE_TOKEN must not be empty".into());
    }
    if token.len() < MIN_BEARER_TOKEN_LEN {
        return Err(format!(
            "bearer token must be at least {MIN_BEARER_TOKEN_LEN} bytes"
        ));
    }
    if !token.is_ascii() || token.chars().any(char::is_whitespace) {
        return Err("bearer token must contain only non-whitespace ASCII characters".into());
    }
    if token.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("bearer token must not contain control characters".into());
    }
    Ok(())
}

fn constant_time_eq(expected: &[u8], provided: &[u8]) -> bool {
    let mut difference = (expected.len() ^ provided.len()) as u64;
    for index in 0..expected.len().max(provided.len()) {
        difference |= u64::from(
            expected.get(index).copied().unwrap_or_default()
                ^ provided.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn has_valid_bearer(headers: &HeaderMap, expected: &[u8]) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some((scheme, credentials)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("Bearer")
        || credentials.is_empty()
        || credentials.chars().any(char::is_whitespace)
    {
        return false;
    }
    constant_time_eq(expected, credentials.as_bytes())
}

fn is_anonymous_health_probe(request: &Request<Body>) -> bool {
    matches!(request.method(), &Method::GET | &Method::HEAD)
        && request
            .uri()
            .path_and_query()
            .is_some_and(|value| value.as_str() == "/health")
}

fn unauthorized() -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

async fn bridge_security_middleware(
    allowed_origins: Arc<HashSet<String>>,
    bearer_token: Option<Arc<str>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if crate::mcp_endpoint::is_mcp_path(request.uri().path())
        && !crate::mcp_endpoint::origin_is_allowed(request.headers(), &allowed_origins)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let scoped_mcp_post = request.method() == Method::POST
        && request
            .uri()
            .path()
            .strip_prefix("/mcp/")
            .is_some_and(|tail| !tail.is_empty() && !tail.contains('/'));
    if scoped_mcp_post
        || bearer_token.is_none()
        || is_anonymous_health_probe(&request)
        || has_valid_bearer(
            request.headers(),
            bearer_token.as_deref().unwrap_or_default().as_bytes(),
        )
    {
        next.run(request).await
    } else {
        unauthorized()
    }
}

/// Map one ACP prompt stop reason to the bridge's single AG-UI terminal event.
///
/// ACP's enum is non-exhaustive, so a future stop reason fails closed as an
/// AG-UI run error rather than being reported as a successful turn.
fn stop_reason_terminal_event(thread_id: String, run_id: String, stop_reason: StopReason) -> Event {
    match stop_reason {
        StopReason::EndTurn => factory::run_finished(thread_id, run_id),
        StopReason::Cancelled => run_error_with_code("ACP_CANCELLED", "ACP prompt was cancelled"),
        StopReason::MaxTokens => {
            run_error_with_code("ACP_MAX_TOKENS", "ACP prompt reached the token limit")
        }
        StopReason::MaxTurnRequests => run_error_with_code(
            "ACP_MAX_TURN_REQUESTS",
            "ACP prompt reached the turn-request limit",
        ),
        StopReason::Refusal => run_error_with_code("ACP_REFUSAL", "ACP agent refused the prompt"),
        _ => run_error_with_code("ACP_STOP_REASON", "ACP returned an unknown stop reason"),
    }
}

fn guarded_event_stream(
    events: Vec<AgUiResult<Event>>,
    run_guard: RunAdmissionGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    stream::unfold((events.into_iter(), run_guard), |(mut events, run_guard)| async move {
        events
            .next()
            .map(|event| (event, (events, run_guard)))
    })
    .boxed()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SseSendError {
    Closed,
    ActorClosed,
    TimedOut,
}

const RETIRED_SSE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Default)]
struct RetiredSseDrain {
    deadline: Option<tokio::time::Instant>,
}

/// Send one item to the downstream SSE channel without allowing a stalled
/// consumer to pin the stream task forever. A zero duration is the explicit
/// legacy/unlimited mode documented on `BridgeConfig`.
async fn send_sse_with_timeout<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    timeout: std::time::Duration,
) -> Result<(), SseSendError> {
    if timeout.is_zero() {
        return tx.send(item).await.map_err(|_| SseSendError::Closed);
    }
    match tokio::time::timeout(timeout, tx.send(item)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(SseSendError::Closed),
        Err(_) => Err(SseSendError::TimedOut),
    }
}

async fn send_sse_while_session<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    timeout: std::time::Duration,
    session: &AcpSessionHandle,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    if !timeout.is_zero() {
        return send_sse_with_timeout(tx, item, timeout).await;
    }
    let send = tx.send(item);
    tokio::pin!(send);
    loop {
        if let Some(deadline) = retired.deadline {
            tokio::select! {
                biased;
                result = &mut send => return result.map_err(|_| SseSendError::Closed),
                () = tokio::time::sleep_until(deadline) => return Err(SseSendError::ActorClosed),
            }
        }
        tokio::select! {
            biased;
            result = &mut send => return result.map_err(|_| SseSendError::Closed),
            () = session.closed() => retired.deadline = Some(tokio::time::Instant::now() + RETIRED_SSE_DRAIN_GRACE),
        }
    }
}

fn log_sse_send_failure(
    failure: SseSendError,
    thread_id: &str,
    run_id: &str,
    timeout: std::time::Duration,
    stream_kind: &'static str,
) {
    match failure {
        SseSendError::TimedOut => {
            tracing::warn!(
                thread_id,
                run_id,
                timeout_ms = timeout.as_millis() as u64,
                stream_kind,
                "SSE slow consumer timeout"
            );
        }
        SseSendError::Closed => {
            tracing::debug!(thread_id, run_id, stream_kind, "SSE receiver closed");
        }
        SseSendError::ActorClosed => {
            tracing::debug!(
                thread_id,
                run_id,
                stream_kind,
                "ACP actor closed during SSE send"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_prompt_sse(
    tx: &mpsc::Sender<AgUiResult<Event>>,
    item: AgUiResult<Event>,
    timeout: std::time::Duration,
    session: &Arc<AcpSessionHandle>,
    thread_id: &str,
    run_id: &str,
    turn_id: TurnId,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    let result = send_sse_while_session(tx, item, timeout, session, retired).await;
    if let Err(failure) = result {
        log_sse_send_failure(failure, thread_id, run_id, timeout, "prompt");
        if let Err(error) = session.cancel_turn(turn_id) {
            tracing::debug!(
                thread_id,
                run_id,
                error = %error,
                "SSE teardown could not cancel ACP turn"
            );
        }
    }
    result
}

async fn send_history_sse(
    tx: &mpsc::Sender<AgUiResult<Event>>,
    item: AgUiResult<Event>,
    timeout: std::time::Duration,
    thread_id: &str,
    run_id: &str,
    session: &AcpSessionHandle,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    let result = send_sse_while_session(tx, item, timeout, session, retired).await;
    if let Err(failure) = result {
        log_sse_send_failure(failure, thread_id, run_id, timeout, "history");
    }
    result
}

/// Internal session record holding the handle plus a last-used timestamp.
#[derive(Debug)]
struct SessionEntry {
    handle: Arc<AcpSessionHandle>,
    /// Counts this actor against `max_sessions` until the entry and every
    /// external `Arc<SessionEntry>` holding it are dropped.
    _capacity_permit: Option<OwnedSemaphorePermit>,
    last_used: std::sync::Mutex<Instant>,
    /// Number of in-flight prompts on this session. The reaper refuses to
    /// drop entries with `active_prompts > 0` even if their `last_used` is
    /// stale: a long-running prompt would otherwise be killed mid-flight.
    active_prompts: std::sync::atomic::AtomicUsize,
    mcp_credential: Option<Arc<McpCredential>>,
}

struct McpCredential(String);

impl std::fmt::Debug for McpCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("McpCredential([redacted])")
    }
}

#[derive(Debug)]
enum SessionAdmissionError {
    Http(AgUiError),
    Capacity(String),
    ResumeUnsupported(String),
    ResumeFailed(String),
    ResumeMappingMismatch(String),
}

fn resume_open_error(error: BridgeError) -> SessionAdmissionError {
    match error {
        BridgeError::ResumeUnsupported(message) => {
            SessionAdmissionError::ResumeUnsupported(message)
        }
        BridgeError::Unsupported(message) => SessionAdmissionError::ResumeUnsupported(message),
        BridgeError::ResumeFailed(message) => SessionAdmissionError::ResumeFailed(message),
        other => SessionAdmissionError::ResumeFailed(format!("acp open_session failed: {other}")),
    }
}

fn semaphore_for(max_sessions: usize) -> Option<Arc<Semaphore>> {
    (max_sessions != 0).then(|| Arc::new(Semaphore::new(max_sessions)))
}

/// Validate a listed ACP session's cwd for resume, returning the spelling to
/// hand back to the agent.
///
/// Containment is checked against the **canonicalized** path, so a symlink
/// pointing outside the bridge root is rejected. But the path returned is the
/// agent's own spelling: `session/load` compares the cwd it is given against
/// the cwd it persisted, and on a symlinked prefix (macOS `/var` ->
/// `private/var`, which every `std::env::temp_dir()` path goes through) the
/// canonical form differs textually, so returning it would fail every resume
/// with "session/load cwd does not match persisted cwd".
fn resume_cwd(reported: &Path, root: &Path) -> Result<PathBuf, SessionAdmissionError> {
    if !reported.is_absolute() {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session has a non-absolute cwd".into(),
        ));
    }
    let canonical = std::fs::canonicalize(reported).map_err(|error| {
        SessionAdmissionError::ResumeFailed(format!(
            "listed ACP session cwd could not be canonicalized: {error}"
        ))
    })?;
    if !canonical.is_absolute() {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session has a non-absolute cwd".into(),
        ));
    }
    if !canonical.starts_with(root) {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session cwd is outside the bridge root".into(),
        ));
    }
    Ok(reported.to_path_buf())
}

/// One cached `GET /sessions` snapshot. Bounded by construction: a single
/// slot holding at most one snapshot. Only successes are cached — errors
/// return uncached (they still hit the gate, so concurrency stays bounded).
struct SessionsListCacheEntry {
    summaries: Vec<agui_acp_bridge_core::SessionSummary>,
    fetched_at: Instant,
}

/// How long a `GET /sessions` snapshot may be reused before the next request
/// re-queries the agent (which forks a subprocess).
const SESSIONS_LIST_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

impl Inner {
    /// Return the sessions list, serving a fresh-enough cache entry when
    /// available.
    ///
    /// Burst protection: all concurrent requests on a cache miss queue behind
    /// `sessions_list_gate`, then re-check the cache after taking it — so a
    /// burst of N requests produces at most one concurrent agent
    /// subprocess (`session/list` forks one). The gate IS the concurrency
    /// bound; there is no separate semaphore. The call itself is additionally
    /// bounded by `open_session_timeout`.
    async fn sessions_list(
        &self,
        cfg: SessionConfig,
    ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, BridgeError> {
        if let Some(entry) = self.sessions_list_cache.lock().await.as_ref()
            && entry.fetched_at.elapsed() < SESSIONS_LIST_CACHE_TTL
        {
            return Ok(entry.summaries.clone());
        }
        let _gate = self.sessions_list_gate.lock().await;
        // Re-check under the gate: another waiter likely just fetched.
        if let Some(entry) = self.sessions_list_cache.lock().await.as_ref()
            && entry.fetched_at.elapsed() < SESSIONS_LIST_CACHE_TTL
        {
            return Ok(entry.summaries.clone());
        }
        let client = self.client.clone();
        let timeout = self.config.open_session_timeout;
        let summaries = tokio::time::timeout(timeout, client.list_sessions(cfg))
            .await
            .map_err(|_| BridgeError::Timeout(timeout))??;
        *self.sessions_list_cache.lock().await = Some(SessionsListCacheEntry {
            summaries: summaries.clone(),
            fetched_at: Instant::now(),
        });
        Ok(summaries)
    }

    /// Drop any cached `GET /sessions` snapshot so the next request
    /// re-queries the agent. Called whenever the set of persisted sessions
    /// can change (open/close/delete) so create-then-list flows stay fresh;
    /// bursts still collapse because only mutations clear the slot.
    ///
    /// Takes `sessions_list_gate` so an invalidation cannot be overtaken by
    /// an in-flight query writing its PRE-mutation snapshot afterwards (the
    /// stale-until-TTL hole). Deadlock-safe: `sessions_list` holds the gate
    /// only across the ACP query and never calls back into any path that
    /// invalidates, and invalidation itself performs no awaits while holding
    /// the gate beyond the cache-mutex swap.
    async fn invalidate_sessions_list_cache(&self) {
        let _gate = self.sessions_list_gate.lock().await;
        *self.sessions_list_cache.lock().await = None;
    }
}

impl SessionEntry {
    #[cfg(test)]
    fn new(handle: Arc<AcpSessionHandle>, capacity_permit: Option<OwnedSemaphorePermit>) -> Self {
        Self::new_scoped(handle, capacity_permit, None)
    }

    fn new_scoped(
        handle: Arc<AcpSessionHandle>,
        capacity_permit: Option<OwnedSemaphorePermit>,
        mcp_credential: Option<Arc<McpCredential>>,
    ) -> Self {
        Self {
            handle,
            _capacity_permit: capacity_permit,
            last_used: std::sync::Mutex::new(Instant::now()),
            active_prompts: std::sync::atomic::AtomicUsize::new(0),
            mcp_credential,
        }
    }

    fn touch(&self) {
        *self.last_used.lock().expect("session entry mutex poisoned") = Instant::now();
    }

    fn last_used(&self) -> Instant {
        *self.last_used.lock().expect("session entry mutex poisoned")
    }

    fn active_prompts(&self) -> usize {
        self.active_prompts
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn enter_prompt(self: &Arc<Self>) -> PromptGuard {
        self.active_prompts
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.touch();
        PromptGuard {
            entry: self.clone(),
        }
    }
}

struct OpeningMcpCredential {
    inner: Arc<Inner>,
    thread_id: String,
    credential: Arc<McpCredential>,
    committed: bool,
}

impl Drop for OpeningMcpCredential {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self
                .inner
                .mcp_credentials
                .remove_if(&self.thread_id, |_, current| {
                    Arc::ptr_eq(current, &self.credential)
                });
        }
    }
}

/// Per-thread gate for lazy session creation.
///
/// `users` counts callers that have retained this gate, including callers
/// queued on `lock`. A gate is only removed after its last user releases it;
/// otherwise a failed creator could remove the map entry while an older
/// waiter still held an Arc to the old mutex, allowing a new caller to create
/// a second gate for the same thread.
struct SessionCreateGate {
    lock: tokio::sync::Mutex<()>,
    users: AtomicUsize,
    closing: AtomicBool,
}

impl SessionCreateGate {
    fn new() -> Self {
        Self {
            lock: tokio::sync::Mutex::new(()),
            users: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
        }
    }

    /// Retain the gate unless its last user has started removing it.
    fn try_retain(&self) -> bool {
        if self.closing.load(Ordering::Acquire) {
            return false;
        }
        self.users.fetch_add(1, Ordering::AcqRel);
        if self.closing.load(Ordering::Acquire) {
            self.users.fetch_sub(1, Ordering::AcqRel);
            false
        } else {
            true
        }
    }
}

/// One retained reference to a [`SessionCreateGate`].
struct SessionCreateGuard {
    inner: Arc<Inner>,
    thread_id: String,
    gate: Arc<SessionCreateGate>,
}

impl Drop for SessionCreateGuard {
    fn drop(&mut self) {
        if self.gate.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.gate.closing.store(true, Ordering::Release);
        }
        self.inner
            .remove_session_create_gate(&self.thread_id, &self.gate);
    }
}

/// RAII guard decrementing `active_prompts` when the prompt scope exits.
pub(crate) struct PromptGuard {
    entry: Arc<SessionEntry>,
}

impl Drop for PromptGuard {
    fn drop(&mut self) {
        self.entry
            .active_prompts
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.entry.touch();
    }
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

/// The mutually-exclusive claims that protect one thread's cached ACP entry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ThreadClaim {
    Run(String),
    Lifecycle(&'static str),
}

/// RAII ownership of one run or lifecycle admission claim.
///
/// The conditional removal matters if a stale teardown races a replacement
/// claim: an old operation must never release a newer operation's slot.
struct ThreadClaimGuard {
    inner: Arc<Inner>,
    thread_id: String,
    claim: ThreadClaim,
}

type RunAdmissionGuard = ThreadClaimGuard;
type LifecycleGuard = ThreadClaimGuard;

impl ThreadClaimGuard {
    fn claim(&self) -> &ThreadClaim {
        &self.claim
    }
}

impl Drop for ThreadClaimGuard {
    fn drop(&mut self) {
        let claim = self.claim.clone();
        let _ = self
            .inner
            .active_runs
            .remove_if(&self.thread_id, |_, current| current == &claim);
    }
}

impl Inner {
    async fn acquire_session_create_gate(self: &Arc<Self>, thread_id: &str) -> SessionCreateGuard {
        loop {
            let gate = self
                .create_locks
                .entry(thread_id.to_string())
                .or_insert_with(|| Arc::new(SessionCreateGate::new()))
                .clone();
            if gate.try_retain() {
                return SessionCreateGuard {
                    inner: self.clone(),
                    thread_id: thread_id.to_string(),
                    gate,
                };
            }

            // A closing gate is removed by its last user. Help complete that
            // cleanup here, then retry against the replacement gate.
            self.remove_session_create_gate(thread_id, &gate);
            tokio::task::yield_now().await;
        }
    }

    fn remove_session_create_gate(&self, thread_id: &str, expected: &Arc<SessionCreateGate>) {
        if !expected.closing.load(Ordering::Acquire) || expected.users.load(Ordering::Acquire) != 0
        {
            return;
        }
        let _ = self
            .create_locks
            .remove_if(thread_id, |_, current| Arc::ptr_eq(current, expected));
    }

    fn try_claim(
        self: &Arc<Self>,
        thread_id: &str,
        claim: ThreadClaim,
    ) -> Option<ThreadClaimGuard> {
        match self.active_runs.entry(thread_id.to_string()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(claim.clone());
                Some(ThreadClaimGuard {
                    inner: self.clone(),
                    thread_id: thread_id.to_string(),
                    claim,
                })
            }
            dashmap::mapref::entry::Entry::Occupied(_) => None,
        }
    }

    fn try_claim_run(self: &Arc<Self>, thread_id: &str, run_id: &str) -> Option<RunAdmissionGuard> {
        self.try_claim(thread_id, ThreadClaim::Run(run_id.to_string()))
    }

    fn try_claim_lifecycle(
        self: &Arc<Self>,
        thread_id: &str,
        reason: &'static str,
    ) -> Option<LifecycleGuard> {
        self.try_claim(thread_id, ThreadClaim::Lifecycle(reason))
    }

    fn has_lifecycle_claim(&self, thread_id: &str) -> bool {
        self.active_runs
            .get(thread_id)
            .is_some_and(|claim| matches!(claim.value(), ThreadClaim::Lifecycle(_)))
    }

    fn owns_claim(&self, thread_id: &str, claim: &ThreadClaim) -> bool {
        self.active_runs
            .get(thread_id)
            .is_some_and(|current| current.value() == claim)
    }
}

/// RAII marker for an in-flight session setting RPC. Settings are serialized by
/// the actor but still count as busy for close/reaper/LRU admission.
struct SettingGuard {
    inner: Arc<Inner>,
    thread_id: String,
}

impl Drop for SettingGuard {
    fn drop(&mut self) {
        match self.inner.active_settings.entry(self.thread_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if *entry.get() > 1 {
                    *entry.get_mut() -= 1;
                } else {
                    entry.remove();
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => {}
        }
    }
}

/// Remove a cached session only if the caller still owns the same entry that
/// it observed. A replacement session may already be installed under the
/// same thread id; an unconditional `remove(thread_id)` would delete that
/// newer session.
fn remove_session_if_same(inner: &Inner, thread_id: &str, expected: &Arc<SessionEntry>) -> bool {
    let removed = inner
        .sessions
        .remove_if(thread_id, |_, current| Arc::ptr_eq(current, expected));
    if removed.is_some() {
        revoke_mcp_credential(inner, thread_id, expected);
        inner.frontend_tools.drop_thread(thread_id);
        true
    } else {
        false
    }
}

/// Handle-based variant used by the SSE task, which intentionally keeps only
/// the handle Arc rather than the surrounding session entry.
fn remove_session_if_handle(
    inner: &Inner,
    thread_id: &str,
    expected: &Arc<AcpSessionHandle>,
) -> bool {
    let removed = inner.sessions.remove_if(thread_id, |_, current| {
        Arc::ptr_eq(&current.handle, expected)
    });
    if let Some((_, entry)) = removed {
        revoke_mcp_credential(inner, thread_id, &entry);
        inner.frontend_tools.drop_thread(thread_id);
        true
    } else {
        false
    }
}

fn revoke_mcp_credential(inner: &Inner, thread_id: &str, entry: &SessionEntry) {
    if let Some(expected) = &entry.mcp_credential {
        let _ = inner
            .mcp_credentials
            .remove_if(thread_id, |_, current| Arc::ptr_eq(current, expected));
    }
}

/// Gracefully close an entry that has already been removed from the cache.
///
/// Callers remove by pointer identity before entering this async primitive, so
/// a replacement under the same thread id can never be closed accidentally.
/// The lifecycle guard blocks a replacement run or lifecycle operation until
/// registry cleanup, the bounded ACP close attempt, and entry drop all finish.
/// Unsupported close deliberately becomes local drop for eviction paths and
/// never sends an illegal request.
async fn graceful_close_removed(
    inner: &Inner,
    thread_id: &str,
    entry: Arc<SessionEntry>,
    lifecycle_guard: LifecycleGuard,
    reason: &'static str,
) -> Result<(), BridgeError> {
    inner.frontend_tools.drop_thread(thread_id);
    let result = entry.handle.close().await;
    match &result {
        Ok(()) => tracing::debug!(thread_id, reason, "ACP session closed before local drop"),
        Err(BridgeError::Unsupported(_)) => {
            tracing::debug!(thread_id, reason, "ACP close unsupported; dropping locally")
        }
        Err(error) => tracing::warn!(
            thread_id,
            reason,
            error = %error,
            "ACP session close failed; dropping locally"
        ),
    }
    drop(entry);
    drop(lifecycle_guard);
    result
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(h) = self.reaper.lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
    }
}

impl BridgeAppState {
    /// Construct with default `BridgeConfig` and an `AutoDeny` policy.
    /// Suitable for local development; production should use
    /// [`BridgeAppState::builder`] to configure the bearer token.
    ///
    /// `cwd` is canonicalized at construction time. If it does not exist or
    /// cannot be canonicalized, [`std::path::absolute`] is used as a
    /// fallback so the sandbox always has an absolute reference path. A
    /// non-existent cwd will fail closed for any agent path that touches
    /// the filesystem (`safe_resolve` requires the existing ancestor to be
    /// canonicalizable).
    ///
    /// Frontend-tool injection (`useFrontendTool`) is **disabled** in this
    /// constructor — `self_url` is `None`. Use the builder's `with_self_url`
    /// to enable it.
    #[must_use]
    pub fn new(client: Arc<dyn AcpClient>, cwd: PathBuf) -> Self {
        let cwd = canonicalize_cwd(&cwd).unwrap_or_else(|err| {
            tracing::warn!(error = %err, cwd = %cwd.display(),
                "cwd canonicalize failed; using path as-is");
            cwd
        });
        let config = BridgeConfig::default();
        Self {
            inner: Arc::new(Inner {
                sessions: DashMap::new(),
                mcp_credentials: DashMap::new(),
                active_runs: DashMap::new(),
                active_settings: DashMap::new(),
                create_locks: DashMap::new(),
                capacity_gate: tokio::sync::Mutex::new(()),
                sessions_list_cache: tokio::sync::Mutex::new(None),
                sessions_list_gate: tokio::sync::Mutex::new(()),
                session_capacity: semaphore_for(config.max_sessions),
                client,
                cwd,
                config,
                policy: Arc::new(AutoDeny),
                frontend_tools: FrontendToolRegistry::new(),
                self_url: None,
                bearer_token: None,
                mcp_allowed_origins: HashSet::new(),
                reaper: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Builder for full control over config + policy + frontend-tool
    /// injection.
    ///
    /// `cwd` is canonicalized at [`BridgeAppStateBuilder::build`] time; see
    /// [`BridgeAppState::new`] for the cwd resolution semantics.
    #[must_use]
    pub fn builder(client: Arc<dyn AcpClient>, cwd: PathBuf) -> BridgeAppStateBuilder {
        BridgeAppStateBuilder {
            client,
            cwd,
            config: BridgeConfig::default(),
            policy: Arc::new(AutoDeny),
            self_url: None,
            bearer_token: None,
            mcp_allowed_origins: HashSet::new(),
        }
    }

    /// Bridge configuration in effect for new sessions.
    #[must_use]
    pub fn config(&self) -> &BridgeConfig {
        &self.inner.config
    }

    /// Permission policy applied to ACP `requestPermission` requests.
    #[must_use]
    pub fn policy(&self) -> &Arc<dyn PermissionPolicy> {
        &self.inner.policy
    }

    /// How many sessions are currently cached. Test/observability hook.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.inner.sessions.len()
    }

    fn try_claim_run(&self, thread_id: &str, run_id: &str) -> Option<RunAdmissionGuard> {
        self.inner.try_claim_run(thread_id, run_id)
    }

    fn try_claim_lifecycle(&self, thread_id: &str, reason: &'static str) -> Option<LifecycleGuard> {
        self.inner.try_claim_lifecycle(thread_id, reason)
    }

    /// List persisted sessions via ACP `session/list`.
    ///
    /// Each underlying query opens a short-lived ACP connection (which forks
    /// a subprocess agent), so results are cached for
    /// [`SESSIONS_LIST_CACHE_TTL`] and concurrent requests collapse into one
    /// query. Only successes are cached; errors return uncached but still
    /// share the single-query concurrency gate. Returns
    /// [`BridgeError::Unsupported`] when the agent does not advertise the
    /// `session/list` capability. The HTTP layer maps that to `501`.
    pub async fn list_sessions(
        &self,
    ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, BridgeError> {
        // Use a synthetic thread token for the transient connection's
        // (unused) MCP URL slot — listing issues no prompts, so no MCP
        // endpoint is needed.
        let cfg = self.session_config_for("__list__");
        self.inner.sessions_list(cfg).await
    }

    async fn validated_resume_cwd(
        &self,
        session_id: &SessionId,
    ) -> Result<PathBuf, SessionAdmissionError> {
        let summaries = match self.list_sessions().await {
            Ok(summaries) => summaries,
            Err(BridgeError::Unsupported(message)) => {
                return Err(SessionAdmissionError::ResumeUnsupported(message));
            }
            Err(error) => {
                return Err(SessionAdmissionError::ResumeFailed(format!(
                    "acp session/list failed: {error}"
                )));
            }
        };
        let session_id_text = session_id.0.to_string();
        let Some(summary) = summaries
            .into_iter()
            .find(|summary| summary.session_id == session_id_text)
        else {
            return Err(SessionAdmissionError::ResumeFailed(format!(
                "ACP session `{session_id_text}` was not found in session/list"
            )));
        };

        resume_cwd(Path::new(&summary.cwd), &self.inner.cwd)
    }

    /// Resolve a deferred permission request for the session bound to a
    /// specific thread.
    ///
    /// The route body supplies `thread_id` — the same thread id that produced
    /// the pending interrupt — so one client's stale/malicious interrupt id
    /// can never consume a pending permission parked on a *different* thread
    /// (the cross-session hole the previous global scan left open). Mirrors
    /// the thread-scoping of `FrontendToolRegistry::resolve_for_thread`.
    /// Validates the decision against the agent-advertised option set (see
    /// [`AcpSessionHandle::resolve_permission`] for details). Returns:
    /// - `ResolveOutcome::Resolved` — the decision was accepted and delivered.
    /// - `ResolveOutcome::InvalidOption` — an `Allow` decision named an
    ///   `option_id` the agent did not offer; the entry remains pending.
    /// - `ResolveOutcome::NotFound` — the thread has no live session, or no
    ///   pending permission with that id (already resolved, timed out, or
    ///   never existed).
    #[must_use]
    pub fn resolve_permission(
        &self,
        thread_id: &str,
        interrupt_id: &str,
        decision: agui_acp_bridge_core::PermissionDecision,
    ) -> ResolveOutcome {
        let Some(entry) = self.inner.sessions.get(thread_id).map(|e| e.clone()) else {
            return ResolveOutcome::NotFound;
        };
        // Quick check: is the entry there, and is the option valid? We do
        // this without consuming the entry first, so an `InvalidOption`
        // outcome leaves the request retryable.
        let pending = entry.handle.pending_permissions();
        let Some(record) = pending.get(interrupt_id) else {
            return ResolveOutcome::NotFound;
        };
        if let agui_acp_bridge_core::PermissionDecision::Allow { ref option_id } = decision
            && !record.allows_option(option_id.0.as_ref())
        {
            return ResolveOutcome::InvalidOption;
        }
        // Drop the read-guard before calling resolve (which takes a
        // write-guard via DashMap::remove) to avoid deadlock.
        drop(record);
        if entry.handle.resolve_permission(interrupt_id, decision) {
            ResolveOutcome::Resolved
        } else {
            // Lost a race against another resolver.
            ResolveOutcome::NotFound
        }
    }

    /// Snapshot the cached `SessionInitState` for an existing thread, or
    /// `None` if no session has been opened for it yet. Powers the
    /// `GET /session/init` discovery endpoint.
    #[must_use]
    pub fn session_init_state(&self, thread_id: &str) -> Option<SessionInitState> {
        let entry = self.inner.sessions.get(thread_id)?.clone();
        if entry.handle.is_unusable() {
            self.evict_unusable(thread_id, &entry);
            return None;
        }
        Some(entry.handle.init_state())
    }

    fn evict_unusable(&self, thread_id: &str, expected: &Arc<SessionEntry>) {
        let removed = self.inner.sessions.remove_if(thread_id, |_, entry| {
            Arc::ptr_eq(entry, expected) && entry.handle.is_unusable()
        });
        if removed.is_some() {
            revoke_mcp_credential(&self.inner, thread_id, expected);
            self.inner.frontend_tools.drop_thread(thread_id);
        }
    }

    fn enter_setting(&self, thread_id: &str) -> Result<SettingGuard, SetSessionStatus> {
        if self.inner.has_lifecycle_claim(thread_id) {
            return Err(SetSessionStatus::Busy);
        }
        self.inner
            .active_settings
            .entry(thread_id.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let guard = SettingGuard {
            inner: self.inner.clone(),
            thread_id: thread_id.to_string(),
        };
        if self.inner.has_lifecycle_claim(thread_id) {
            drop(guard);
            return Err(SetSessionStatus::Busy);
        }
        Ok(guard)
    }

    /// Send `session/set_mode` to the session bound to `thread_id`.
    ///
    /// Returns:
    /// - `Ok(())` — agent accepted the new mode.
    /// - `Err(SetSessionStatus::NotFound)` — no session exists for that thread.
    /// - `Err(SetSessionStatus::Busy)` — a lifecycle close/eviction owns the thread.
    /// - `Err(SetSessionStatus::Acp(_))` — agent rejected the request
    ///   (typically `mode_id` is not in `availableModes`).
    /// - `Err(SetSessionStatus::Timeout)` — agent did not respond within
    ///   `BridgeConfig.set_session_timeout`. The session is left intact;
    ///   the caller can retry.
    /// - `Err(SetSessionStatus::SessionClosed)` — actor died mid-flight; the
    ///   cache entry is evicted so a retry on the same `thread_id` rebuilds.
    pub async fn set_session_mode(
        &self,
        thread_id: &str,
        mode_id: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        let mode_id = mode_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        let snapshot = entry.handle.init_state();
        let result = if let Some(config_options) = snapshot.config_options {
            if let Some(option) = config_options
                .iter()
                .find(|option| option.category.as_ref() == Some(&SessionConfigOptionCategory::Mode))
            {
                let config_id = option.id.0.to_string();
                let value = SessionConfigOptionValue::value_id(mode_id.clone());
                validate_discovered_config_option(&config_id, option, &value)?;
                tokio::time::timeout(
                    timeout,
                    entry.handle.set_config_option_value(config_id, value),
                )
                .await
            } else if snapshot.modes.is_some() {
                // Mixed-capability agents may advertise an unrelated config
                // snapshot while retaining the legacy session/set_mode path.
                validate_legacy_mode(snapshot.modes.as_ref(), &mode_id)?;
                tokio::time::timeout(timeout, entry.handle.set_mode(mode_id)).await
            } else {
                return Err(SetSessionStatus::Acp(
                    "agent did not advertise a mode capability".into(),
                ));
            }
        } else {
            validate_legacy_mode(snapshot.modes.as_ref(), &mode_id)?;
            tokio::time::timeout(timeout, entry.handle.set_mode(mode_id)).await
        };
        match result {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Send the discovered model config option to the session bound to
    /// `thread_id`. This compatibility alias never emits ACP
    /// `session/set_model`.
    pub async fn set_session_model(
        &self,
        thread_id: &str,
        model_id: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        let model_id = model_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        let snapshot = entry.handle.init_state();
        let Some(config_options) = snapshot.config_options else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise a model config option".into(),
            ));
        };
        let Some(config_id) = config_options.iter().find_map(|option| {
            (option.category.as_ref() == Some(&SessionConfigOptionCategory::Model))
                .then(|| option.id.0.to_string())
        }) else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise a model config option".into(),
            ));
        };
        let option = config_options
            .iter()
            .find(|option| option.id.0.as_ref() == config_id.as_str())
            .expect("model config option was found by category");
        let value = SessionConfigOptionValue::value_id(model_id);
        validate_discovered_config_option(&config_id, option, &value)?;
        match tokio::time::timeout(
            timeout,
            entry.handle.set_config_option_value(config_id, value),
        )
        .await
        {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Send a select/value-id `session/set_config_option` request using the
    /// complete option list discovered during session initialization.
    pub async fn set_session_config_option(
        &self,
        thread_id: &str,
        config_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        self.set_session_config_option_value(
            thread_id,
            config_id,
            SessionConfigOptionValue::value_id(value.into()),
        )
        .await
    }

    /// Send a typed `session/set_config_option` request using the complete
    /// option list discovered during session initialization.
    pub async fn set_session_config_option_value(
        &self,
        thread_id: &str,
        config_id: impl Into<String>,
        value: SessionConfigOptionValue,
    ) -> Result<(), SetSessionStatus> {
        let config_id = config_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let Some(config_options) = entry.handle.init_state().config_options else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise config options".into(),
            ));
        };
        let Some(option) = config_options
            .iter()
            .find(|option| option.id.0.as_ref() == config_id.as_str())
        else {
            return Err(SetSessionStatus::Acp(format!(
                "agent did not advertise config option `{config_id}`"
            )));
        };
        validate_discovered_config_option(&config_id, option, &value)?;
        let timeout = self.inner.config.set_session_timeout;
        match tokio::time::timeout(
            timeout,
            entry.handle.set_config_option_value(config_id, value),
        )
        .await
        {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Cancel the current turn for a cached session. A session with no active
    /// turn is a successful no-op, matching ACP's notification semantics.
    pub fn cancel_session(&self, thread_id: &str) -> Result<(), SetSessionStatus> {
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        match entry.handle.cancel() {
            Ok(()) => {
                entry.touch();
                Ok(())
            }
            Err(BridgeError::SessionClosed) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Err(other) => Err(SetSessionStatus::Acp(other.to_string())),
        }
    }

    /// Explicitly close a cached ACP session using its agent-advertised
    /// `session/close` capability. The lifecycle claim prevents a concurrent
    /// AG-UI run or lifecycle operation from entering while the bounded ACP
    /// request is in flight; the queue/active checks reject work without
    /// waiting for it.
    pub async fn close_session(&self, thread_id: &str) -> Result<(), CloseSessionStatus> {
        let _lifecycle_guard = self
            .try_claim_lifecycle(thread_id, "session-close")
            .ok_or(CloseSessionStatus::Busy)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|entry| entry.clone())
            .ok_or(CloseSessionStatus::NotFound)?;

        if entry.handle.is_unusable() {
            remove_session_if_same(&self.inner, thread_id, &entry);
            return Err(CloseSessionStatus::NotFound);
        }
        if entry.active_prompts() > 0
            || !entry.handle.turn_queue_empty()
            || !entry.handle.pending_permissions().is_empty()
            || self.inner.active_settings.contains_key(thread_id)
            || self.inner.frontend_tools.pending_len(thread_id) > 0
        {
            return Err(CloseSessionStatus::Busy);
        }

        match entry.handle.close().await {
            Ok(()) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                self.inner.invalidate_sessions_list_cache().await;
                Ok(())
            }
            Err(BridgeError::Unsupported(_)) => {
                entry.touch();
                Err(CloseSessionStatus::Unsupported)
            }
            Err(BridgeError::Timeout(_)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(CloseSessionStatus::Timeout)
            }
            Err(BridgeError::SessionClosed) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(CloseSessionStatus::SessionClosed)
            }
            Err(BridgeError::Acp(error)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(CloseSessionStatus::Acp(error.to_string()))
            }
            Err(error) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(CloseSessionStatus::Acp(error.to_string()))
            }
        }
    }

    /// Delete a persisted ACP session through a transient connection.
    ///
    /// Delete only the ACP session mapped to the exact AG-UI thread id. A
    /// cache miss is local `404`; it never treats the thread id as an ACP id
    /// and never opens a transient ACP connection.
    pub async fn delete_session(&self, thread_id: &str) -> Result<(), DeleteSessionStatus> {
        if thread_id.trim().is_empty() {
            return Err(DeleteSessionStatus::InvalidInput);
        }

        let _lifecycle_guard = self
            .try_claim_lifecycle(thread_id, "session-delete")
            .ok_or(DeleteSessionStatus::Busy)?;
        let Some(entry) = self
            .inner
            .sessions
            .get(thread_id)
            .map(|entry| entry.clone())
        else {
            return Err(DeleteSessionStatus::NotFound);
        };

        if entry.active_prompts() > 0
            || !entry.handle.turn_queue_empty()
            || !entry.handle.pending_permissions().is_empty()
            || self.inner.active_settings.contains_key(thread_id)
            || self.inner.frontend_tools.pending_len(thread_id) > 0
        {
            return Err(DeleteSessionStatus::Busy);
        }

        let target_id = entry.handle.session_id().clone();
        let result = tokio::time::timeout(
            self.inner.config.set_session_timeout,
            self.inner
                .client
                .delete_session(self.session_config_for(thread_id), target_id),
        )
        .await;

        let status = match result {
            Ok(Ok(())) => None,
            Ok(Err(BridgeError::Unsupported(_))) => Some(DeleteSessionStatus::Unsupported),
            Ok(Err(BridgeError::Timeout(_))) | Err(_) => Some(DeleteSessionStatus::Timeout),
            Ok(Err(error)) => Some(DeleteSessionStatus::Acp(error.to_string())),
        };

        if let Some(status) = status {
            if matches!(status, DeleteSessionStatus::Unsupported) {
                entry.touch();
            } else {
                remove_session_if_same(&self.inner, thread_id, &entry);
                self.inner.invalidate_sessions_list_cache().await;
            }
            Err(status)
        } else {
            remove_session_if_same(&self.inner, thread_id, &entry);
            self.inner.invalidate_sessions_list_cache().await;
            Ok(())
        }
    }

    /// Spawn the background idle-session reaper.
    ///
    /// The reaper wakes every `idle_timeout / 4` (capped between 1s and 30s)
    /// and gracefully closes and removes any session whose `last_used` instant
    /// is older than `idle_timeout`. If the agent does not advertise close,
    /// the actor is still dropped after the unsupported result, which
    /// terminates the actor task and (for subprocess clients) kills the child.
    ///
    /// The reaper holds a `Weak<Inner>` so it auto-exits as soon as the
    /// last [`BridgeAppState`] clone is dropped — no manual cleanup needed.
    /// Its [`tokio::task::JoinHandle`] is stored on the `Inner` so
    /// `Drop for Inner` can abort it cleanly on the next tick instead of
    /// letting the worker hang around for a full polling cycle after
    /// shutdown.
    ///
    /// Calling `spawn_reaper` more than once on the same state will replace
    /// the old reaper (the previous task is aborted). In normal use the CLI
    /// calls it exactly once at startup.
    pub fn spawn_reaper(&self) {
        let weak = Arc::downgrade(&self.inner);
        let idle = self.inner.config.idle_timeout;
        let interval = std::cmp::min(idle / 4, std::time::Duration::from_secs(30))
            .max(std::time::Duration::from_secs(1));
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else {
                    // Last `BridgeAppState` clone has been dropped — exit.
                    return;
                };
                let now = Instant::now();
                let mut candidates = Vec::new();
                for entry in inner.sessions.iter() {
                    let value = entry.value().clone();
                    if value.active_prompts() > 0
                        || !value.handle.turn_queue_empty()
                        || !value.handle.pending_permissions().is_empty()
                        || inner.active_runs.contains_key(entry.key())
                        || inner.active_settings.contains_key(entry.key())
                        || inner.frontend_tools.pending_len(entry.key()) > 0
                    {
                        // Active prompts, queued turns, settings, close
                        // operations, and parked frontend work must survive
                        // idle-timeout windows.
                        continue;
                    }
                    let last = value.last_used();
                    if now.saturating_duration_since(last) >= idle {
                        candidates.push((entry.key().clone(), value));
                    }
                }
                let mut to_close = Vec::new();
                for (key, victim) in candidates {
                    let Some(lifecycle_guard) = inner.try_claim_lifecycle(&key, "idle-reaper")
                    else {
                        continue;
                    };
                    // remove_if avoids the iter→remove race: another caller
                    // may have touched the entry between our scan and now,
                    // or replaced it under the same key, in which case we
                    // leave the replacement alone for the next tick.
                    let removed = inner.sessions.remove_if(&key, |_, v| {
                        Arc::ptr_eq(v, &victim)
                            && v.active_prompts() == 0
                            && v.handle.turn_queue_empty()
                            && v.handle.pending_permissions().is_empty()
                            && inner.owns_claim(&key, lifecycle_guard.claim())
                            && !inner.active_settings.contains_key(&key)
                            && inner.frontend_tools.pending_len(&key) == 0
                            && now.saturating_duration_since(v.last_used()) >= idle
                    });
                    if removed.is_some() {
                        revoke_mcp_credential(&inner, &key, &victim);
                        tracing::info!(thread_id = %key, "reaping idle ACP session");
                        to_close.push((key, victim, lifecycle_guard));
                    }
                }
                for (key, victim, lifecycle_guard) in to_close {
                    let _ = graceful_close_removed(
                        &inner,
                        &key,
                        victim,
                        lifecycle_guard,
                        "idle-reaper",
                    )
                    .await;
                }
                drop(inner);
            }
        });
        if let Ok(mut slot) = self.inner.reaper.lock()
            && let Some(prev) = slot.replace(handle)
        {
            prev.abort();
        }
    }

    /// Frontend-tool registry. Used by the MCP HTTP endpoint to read the
    /// per-thread tool list, register pending calls, and route results
    /// back into the live SSE stream.
    pub fn frontend_tools(&self) -> &FrontendToolRegistry {
        &self.inner.frontend_tools
    }

    fn bearer_token(&self) -> Option<Arc<str>> {
        self.inner.bearer_token.clone()
    }

    pub(crate) fn mcp_origin_allowed(&self, headers: &HeaderMap) -> bool {
        crate::mcp_endpoint::origin_is_allowed(headers, &self.inner.mcp_allowed_origins)
    }

    pub(crate) fn mcp_allowed_origins(&self) -> Arc<HashSet<String>> {
        Arc::new(self.inner.mcp_allowed_origins.clone())
    }

    /// Headers handed to the ACP agent's MCP HTTP endpoint.
    ///
    /// MCP requests use a per-session opening credential, not the bridge's
    /// admin bearer token. The credential is scoped to this thread's MCP
    /// endpoint and is revoked when its session is removed.
    fn mcp_headers(&self, credential: Option<&McpCredential>) -> Vec<HttpHeader> {
        credential
            .map(|token| {
                vec![HttpHeader::new(
                    "Authorization",
                    format!("Bearer {}", token.0),
                )]
            })
            .unwrap_or_default()
    }

    pub(crate) fn mcp_credential_valid(&self, thread_id: &str, headers: &HeaderMap) -> bool {
        if self.inner.bearer_token.is_none() {
            return true;
        }
        let Some(expected) = self.inner.mcp_credentials.get(thread_id) else {
            return false;
        };
        has_valid_bearer(headers, expected.0.as_bytes())
    }

    /// Resolve a frontend tool call posted back from the browser in its
    /// owning thread. Returns `true` if a pending entry existed and was
    /// consumed.
    pub fn resolve_frontend_tool(
        &self,
        thread_id: &str,
        tool_call_id: &str,
        response: FrontendToolResponse,
    ) -> bool {
        self.inner
            .frontend_tools
            .resolve_for_thread(thread_id, tool_call_id, response)
    }

    fn session_config_for(&self, thread_token: &str) -> SessionConfig {
        self.session_config_for_with(thread_token, None)
    }

    fn encode_mcp_path_segment(segment: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut encoded = String::with_capacity(segment.len());
        for byte in segment.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                encoded.push(byte as char);
            } else {
                encoded.push('%');
                encoded.push(HEX[(byte >> 4) as usize] as char);
                encoded.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
        encoded
    }

    fn session_config_for_with(
        &self,
        thread_token: &str,
        load_session_id: Option<SessionId>,
    ) -> SessionConfig {
        self.session_config_for_with_cwd(thread_token, self.inner.cwd.clone(), load_session_id)
    }

    fn session_config_for_with_cwd(
        &self,
        thread_token: &str,
        cwd: PathBuf,
        load_session_id: Option<SessionId>,
    ) -> SessionConfig {
        let mcp_url = self
            .inner
            .self_url
            .as_ref()
            .map(|base| format!("{base}/mcp/{}", Self::encode_mcp_path_segment(thread_token)));
        let mcp_headers = mcp_url
            .as_ref()
            .map(|_| self.mcp_headers(None))
            .unwrap_or_default();
        SessionConfig {
            cwd,
            policy: self.inner.policy.clone(),
            config: self.inner.config.clone(),
            mcp_url,
            mcp_headers,
            load_session_id,
        }
    }

    fn session_config_for_open(
        &self,
        thread: &str,
        cwd: PathBuf,
        load: Option<SessionId>,
        credential: Option<&McpCredential>,
    ) -> SessionConfig {
        let mut config = self.session_config_for_with_cwd(thread, cwd, load);
        if config.mcp_url.is_some() {
            config.mcp_headers = self.mcp_headers(credential);
        }
        config
    }

    /// Try to reserve one real live-session slot. The short gate protects
    /// idle-victim selection and permit acquisition; it is released before
    /// the ACP handshake begins. An evicted entry may still hold its permit
    /// through an external `Arc`, so this method never assumes map removal
    /// means the actor is gone.
    async fn reserve_session_capacity(&self) -> Result<Option<OwnedSemaphorePermit>, &'static str> {
        let Some(semaphore) = self.inner.session_capacity.clone() else {
            return Ok(None);
        };
        // Fast path WITHOUT the gate or any eviction: if a free permit
        // exists, take it and leave every cached session alone. Only a pool
        // at genuine capacity falls through to LRU eviction — a permit-then-
        // validate caller must never cost an idle session its slot.
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => return Ok(Some(permit)),
            Err(TryAcquireError::Closed) => return Err("session capacity is unavailable"),
            Err(TryAcquireError::NoPermits) => {}
        }
        self.reserve_session_capacity_with_eviction(&semaphore)
            .await
    }

    /// The eviction fallback of [`Self::reserve_session_capacity`], reached
    /// only when no free permit exists. Removes the least-recently-used idle
    /// entry that is not claimed by a run, setting, or close operation, then
    /// retries acquisition.
    async fn reserve_session_capacity_with_eviction(
        &self,
        semaphore: &Arc<Semaphore>,
    ) -> Result<Option<OwnedSemaphorePermit>, &'static str> {
        let (key, victim, lifecycle_guard) = {
            // The gate protects only selection/removal. Never hold it across
            // the bounded ACP close below, or a slow agent would block every
            // unrelated capacity admission.
            let _capacity_gate = self.inner.capacity_gate.lock().await;

            // Re-check under the gate: another waiter may have freed a slot.
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => return Ok(Some(permit)),
                Err(TryAcquireError::Closed) => {
                    return Err("session capacity is unavailable");
                }
                Err(TryAcquireError::NoPermits) => {}
            }

            // All permits are held. Remove only the least-recently-used idle
            // entry that is not claimed by a run, setting, or close operation.
            let mut victim: Option<(String, Arc<SessionEntry>, Instant)> = None;
            for entry in self.inner.sessions.iter() {
                if entry.value().active_prompts() > 0
                    || !entry.value().handle.turn_queue_empty()
                    || !entry.value().handle.pending_permissions().is_empty()
                    || self.inner.active_runs.contains_key(entry.key())
                    || self.inner.active_settings.contains_key(entry.key())
                    || self.inner.frontend_tools.pending_len(entry.key()) > 0
                {
                    continue;
                }
                let last = entry.value().last_used();
                match &victim {
                    Some((_, _, best)) if *best <= last => {}
                    _ => victim = Some((entry.key().clone(), entry.value().clone(), last)),
                }
            }
            let Some((key, victim, last)) = victim else {
                return Err("session capacity reached: all cached sessions are busy");
            };
            let Some(lifecycle_guard) = self.inner.try_claim_lifecycle(&key, "lru-eviction") else {
                return Err("session capacity reached: all cached sessions are busy");
            };
            let removed = self.inner.sessions.remove_if(&key, |_, current| {
                Arc::ptr_eq(current, &victim)
                    && current.active_prompts() == 0
                    && current.handle.turn_queue_empty()
                    && current.handle.pending_permissions().is_empty()
                    && self.inner.owns_claim(&key, lifecycle_guard.claim())
                    && !self.inner.active_settings.contains_key(&key)
                    && self.inner.frontend_tools.pending_len(&key) == 0
                    && current.last_used() == last
            });
            if removed.is_none() {
                return Err("session capacity reached: all cached sessions are busy");
            }
            revoke_mcp_credential(&self.inner, &key, &victim);
            tracing::info!(thread_id = %key, "evicting LRU idle session to honour max_sessions");
            (key, victim, lifecycle_guard)
        };

        let _ = graceful_close_removed(&self.inner, &key, victim, lifecycle_guard, "lru-eviction")
            .await;

        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(permit)),
            Err(TryAcquireError::Closed | TryAcquireError::NoPermits) => {
                Err("session capacity reached: all cached sessions are busy")
            }
        }
    }
    async fn session_for(
        &self,
        thread_id: &str,
    ) -> Result<Arc<SessionEntry>, SessionAdmissionError> {
        self.session_for_resume(thread_id, None).await
    }

    /// Like [`session_for`] but, on a cache miss, opens the session by
    /// **loading** the existing ACP session named by `resume` (replaying its
    /// history) instead of creating a fresh one. When `resume` is `None`, a
    /// normal `session/new` is created; a requested resume that cannot load
    /// returns an error.
    async fn session_for_resume(
        &self,
        thread_id: &str,
        resume: Option<SessionId>,
    ) -> Result<Arc<SessionEntry>, SessionAdmissionError> {
        // Fast path: already cached.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            if existing.handle.is_unusable() {
                let expected = existing.clone();
                drop(existing);
                self.evict_unusable(thread_id, &expected);
            } else if resume
                .as_ref()
                .is_some_and(|session_id| existing.handle.session_id() != session_id)
            {
                return Err(SessionAdmissionError::ResumeMappingMismatch(
                    "requested ACP session does not match the cached thread mapping".into(),
                ));
            } else {
                existing.touch();
                return Ok(existing.clone());
            }
        }

        // Slow path: serialize concurrent first-time creators on the same
        // thread id behind a reference-counted per-key async mutex. The gate
        // stays in `create_locks` until every queued waiter has released it,
        // including waiters that follow a failed creation.
        let create_gate = self.inner.acquire_session_create_gate(thread_id).await;
        let _guard = create_gate.gate.lock.lock().await;

        // Re-check inside the critical section: another waiter may have
        // already populated the entry while we were queued for the lock.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            if existing.handle.is_unusable() {
                let expected = existing.clone();
                drop(existing);
                self.evict_unusable(thread_id, &expected);
            } else if resume
                .as_ref()
                .is_some_and(|session_id| existing.handle.session_id() != session_id)
            {
                return Err(SessionAdmissionError::ResumeMappingMismatch(
                    "requested ACP session does not match the cached thread mapping".into(),
                ));
            } else {
                existing.touch();
                return Ok(existing.clone());
            }
        }

        let resume_requested = resume.is_some();

        let load_cwd = match resume.as_ref() {
            Some(session_id) => match self.validated_resume_cwd(session_id).await {
                Ok(cwd) => cwd,
                Err(error) => {
                    self.inner.frontend_tools.drop_thread(thread_id);
                    return Err(error);
                }
            },
            None => self.inner.cwd.clone(),
        };

        let capacity_permit = match self.reserve_session_capacity().await {
            Ok(permit) => permit,
            Err(reason) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                return Err(SessionAdmissionError::Capacity(reason.to_string()));
            }
        };

        let mut opening_credential =
            if self.inner.bearer_token.is_some() && self.inner.self_url.is_some() {
                let credential = Arc::new(McpCredential(uuid::Uuid::new_v4().to_string()));
                self.inner
                    .mcp_credentials
                    .insert(thread_id.to_string(), credential.clone());
                Some(OpeningMcpCredential {
                    inner: self.inner.clone(),
                    thread_id: thread_id.to_string(),
                    credential,
                    committed: false,
                })
            } else {
                None
            };

        let handle_result = tokio::time::timeout(
            self.inner.config.open_session_timeout,
            self.inner.client.open_session(
                self.session_config_for_open(
                    thread_id,
                    load_cwd,
                    resume,
                    opening_credential
                        .as_ref()
                        .map(|guard| guard.credential.as_ref()),
                ),
            ),
        )
        .await;

        // A failed/timeout open drops `capacity_permit` here. A successful
        // open transfers it into the SessionEntry below.
        let handle = match handle_result {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                return Err(if resume_requested {
                    resume_open_error(e)
                } else {
                    SessionAdmissionError::Http(AgUiError::other(format!(
                        "acp open_session failed: {e}"
                    )))
                });
            }
            Err(_) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                let message = format!(
                    "acp open_session timed out after {:?}",
                    self.inner.config.open_session_timeout
                );
                return Err(if resume_requested {
                    SessionAdmissionError::ResumeFailed(message)
                } else {
                    SessionAdmissionError::Http(AgUiError::other(message))
                });
            }
        };
        let credential = opening_credential
            .as_ref()
            .map(|guard| guard.credential.clone());
        let entry = Arc::new(SessionEntry::new_scoped(
            Arc::new(handle),
            capacity_permit,
            credential,
        ));
        self.inner
            .sessions
            .insert(thread_id.to_string(), entry.clone());
        if let Some(guard) = opening_credential.as_mut() {
            guard.committed = true;
        }
        self.inner.invalidate_sessions_list_cache().await;
        Ok(entry)
    }
}

impl std::fmt::Debug for BridgeAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeAppState")
            .field("sessions_len", &self.inner.sessions.len())
            .field("cwd", &self.inner.cwd)
            .finish_non_exhaustive()
    }
}

/// `RunHandler` that translates each AG-UI POST into one ACP prompt turn.
#[derive(Clone, Debug)]
pub(crate) struct BridgeHandler {
    state: BridgeAppState,
}

impl BridgeHandler {
    #[must_use]
    pub fn new(state: BridgeAppState) -> Self {
        Self { state }
    }

    /// Build an SSE stream that replays a resumed session's loaded history
    /// (via [`AcpSessionHandle::drain_history`]) and then finishes, without
    /// prompting the agent. Used for "resume bootstrap" runs.
    async fn stream_resume_history(
        &self,
        thread_id: String,
        run_id: String,
        entry: Arc<SessionEntry>,
        run_guard: RunAdmissionGuard,
    ) -> AgUiResult<BoxStream<'static, AgUiResult<Event>>> {
        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();
        let drain = match session.drain_history().await {
            Ok(stream) => stream,
            Err(err) => {
                drop(prompt_guard);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code(
                        "ACP_HISTORY_DRAIN_ERROR",
                        format!("acp drain_history failed: {err}"),
                    )),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
        };
        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        let slow_consumer_timeout = self.state.inner.config.slow_consumer_timeout;
        let session = prompt_guard.entry.handle.clone();
        Ok(build_history_stream(
            thread_id,
            run_id,
            drain,
            translated_buffer,
            slow_consumer_timeout,
            session,
            prompt_guard,
            run_guard,
        ))
    }
}

#[async_trait]
impl RunHandler for BridgeHandler {
    async fn handle(
        &self,
        input: RunAgentInput,
    ) -> AgUiResult<BoxStream<'static, AgUiResult<Event>>> {
        let thread_id = input.thread_id.clone();
        let run_id = input.run_id.clone();

        let Some(run_guard) = self.state.try_claim_run(&thread_id, &run_id) else {
            return Ok(stream::iter([
                Ok(factory::run_started(thread_id.clone(), run_id.clone())),
                Ok(run_error_with_code(
                    "CONCURRENT_RUN",
                    "another AG-UI run is active for this thread",
                )),
            ])
            .boxed());
        };

        // Diagnostic: every AG-UI run that reaches the bridge. `msg_count`
        // and `tail` let operators see whether a click produced a bootstrap
        // (connect) run vs a prompt run, and on which thread.
        tracing::info!(
            thread_id = %thread_id,
            run_id = %run_id,
            msg_count = input.messages.len(),
            "AG-UI run received"
        );

        if input.resume.is_some() {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "AGUI_RESUME_UNSUPPORTED",
                    "AG-UI resume is unsupported; use the bridge's private approval flow",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        let requested_resume = match acp_resume_session_id(&input.forwarded_props) {
            Ok(session_id) => session_id,
            Err(message) => {
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code(
                        "ACP_RESUME_SESSION_ID_REQUIRED",
                        message,
                    )),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
        };

        // Extract before touching the frontend-tool registry so unsupported
        // multipart input is rejected without creating any session-side state.
        let trailing = extract_trailing_user_text(&input.messages);
        if matches!(&trailing, TrailingUser::NonText) {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "UNSUPPORTED_INPUT",
                    "ACP bridge accepts text-only user input; multipart content is unsupported",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        // A cached explicit-resume mismatch is a rejected run, not speculative
        // admission. Check it before replacing the live thread's frontend
        // tools so the cached SessionEntry and registry remain untouched.
        let cached_resume_mismatch = requested_resume.as_ref().is_some_and(|session_id| {
            self.state
                .inner
                .sessions
                .get(&thread_id)
                .is_some_and(|entry| {
                    !entry.handle.is_unusable() && entry.handle.session_id() != session_id
                })
        });
        if cached_resume_mismatch {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "ACP_RESUME_FAILED",
                    "requested ACP session does not match the cached thread mapping",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        // Push the per-run tools list into the frontend-tool registry so
        // the bridge's MCP endpoint serves the latest set when the agent
        // calls `tools/list`. Doing this BEFORE session_for ensures that a
        // first-run-on-thread session opens with mcp_servers visible AND
        // the registry already populated, so the agent's first tools/list
        // sees the intended tools.
        //
        // Caveat: most ACP agents call MCP `tools/list` once per session
        // and cache the result. If a later run on the same thread changes
        // the tool list, the agent may not pick the changes up. We detect
        // this and warn so operators can debug "why isn't my new tool
        // showing up?". `tools/listChanged` notifications could close
        // this gap; that's a future enhancement gated on agent support.
        let registry_entry = self.state.inner.frontend_tools.entry(&thread_id);
        let new_tools: Vec<FrontendToolDef> = input
            .tools
            .iter()
            .map(|t| FrontendToolDef {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();
        let previous_names: std::collections::BTreeSet<String> =
            registry_entry.tools().into_iter().map(|t| t.name).collect();
        let new_names: std::collections::BTreeSet<String> =
            new_tools.iter().map(|t| t.name.clone()).collect();
        if !previous_names.is_empty() && previous_names != new_names {
            tracing::warn!(
                thread_id = %thread_id,
                added = ?new_names.difference(&previous_names).cloned().collect::<Vec<_>>(),
                removed = ?previous_names.difference(&new_names).cloned().collect::<Vec<_>>(),
                "frontend tool set changed mid-thread; agents that cache MCP \
                 tools/list (e.g. opencode) may not see the change. Use a \
                 fresh thread_id to force re-discovery."
            );
        }
        registry_entry.set_tools(new_tools.clone());

        // The trailing-only result above follows the ACP protocol semantics:
        // only a `User` message at the **tail** of `messages[]` represents a
        // fresh turn. When the tail is an `assistant` /
        // `tool` / `activity` message, the AG-UI runtime is reposting
        // already-handled history — typically because CopilotKit-style
        // `agentic_chat` callers auto-fire a follow-up run after every
        // tool turn so the LLM sees the tool result. Re-prompting the
        // ACP agent on these follow-ups would replay the prior turn
        // against a session whose history already contains the reply,
        // producing the well-known "every run loops the previous turn"
        // pathology. We instead emit a clean noop run pair.

        // Bridge-private resume is opt-in only. The cache-aware admission
        // method below reuses a live entry, but performs a strict
        // session/load on a cache miss (including an entry that becomes
        // unusable before admission).
        let wants_resume = requested_resume.is_some();

        let entry_result = if wants_resume {
            self.state
                .session_for_resume(&thread_id, requested_resume)
                .await
        } else {
            self.state.session_for(&thread_id).await
        };
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(SessionAdmissionError::ResumeUnsupported(message)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_UNSUPPORTED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::ResumeFailed(message)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_FAILED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::ResumeMappingMismatch(message)) => {
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_FAILED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::Http(error)) => {
                // The registry entry is created before session admission so
                // the first MCP tools/list sees the requested tool set. A
                // failed admission must remove that speculative state.
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                return Err(error);
            }
            Err(SessionAdmissionError::Capacity(reason)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let _ = CAPACITY_REJECTED.try_with(|flag| flag.set(true));
                return Err(AgUiError::http(
                    503,
                    format!("ACP_SESSION_CAPACITY: {reason}"),
                ));
            }
        };

        // `session_for_resume` may evict an unusable cached session. That
        // eviction drops the registry entry while this run still holds its
        // old Arc, so reacquire the map entry after admission and restore the
        // current tool list before the replacement session can call MCP.
        let registry_entry = self.state.inner.frontend_tools.entry(&thread_id);
        registry_entry.set_tools(new_tools);
        let user_text = match trailing {
            TrailingUser::Text(text) => text,
            TrailingUser::NonUserTail | TrailingUser::Empty => {
                // A bootstrap/connect run with no fresh user turn. If we just
                // resumed (loaded) the session, stream the replayed history so
                // the client sees its prior conversation. Otherwise emit a
                // clean noop pair.
                if wants_resume {
                    tracing::debug!(
                        thread_id = %thread_id,
                        run_id = %run_id,
                        "resume bootstrap run; streaming loaded history"
                    );
                    return self
                        .stream_resume_history(thread_id, run_id, entry, run_guard)
                        .await;
                }
                tracing::debug!(
                    thread_id = %thread_id,
                    run_id = %run_id,
                    "RunAgentInput.messages tail is not a fresh user-text message; emitting noop run"
                );
                let evs = vec![
                    Ok(factory::run_started(thread_id.clone(), run_id.clone())),
                    Ok(factory::run_finished(thread_id, run_id)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            TrailingUser::NonText => unreachable!("multipart input was rejected above"),
        };

        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        let frontend_stream = install_frontend_sender(&registry_entry, translated_buffer);
        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();

        // Install the MCP route before submitting the ACP command. The agent
        // is allowed to issue tools/call as soon as it receives that command,
        // before prompt_with_turn() returns to this task.
        let prompt_result = session.prompt_with_turn(user_text).await;
        let stream = match prompt_result {
            Ok((prompt_stream, turn_id)) => build_event_stream(
                thread_id,
                run_id,
                prompt_stream,
                EventStreamContext {
                    session: session.clone(),
                    state: self.state.clone(),
                    registry_entry,
                    turn_id,
                    frontend_stream,
                },
                translated_buffer,
                prompt_guard,
                run_guard,
            ),
            Err(err) => {
                // Dropping the setup clears the sender and aborts any MCP
                // call that raced with prompt creation or request teardown.
                drop(frontend_stream);
                // Session is dead: evict it from the cache so the next
                // request on this thread_id rebuilds a fresh session
                // instead of replaying SessionClosed forever (until
                // idle_timeout).
                if matches!(err, agui_acp_bridge_core::BridgeError::SessionClosed)
                    || session.is_unusable()
                {
                    remove_session_if_same(&self.state.inner, &thread_id, &entry);
                }
                drop(prompt_guard);
                let error_event = match &err {
                    agui_acp_bridge_core::BridgeError::QueueCapacity { .. } => {
                        run_error_with_code("ACP_QUEUE_CAPACITY", err.to_string())
                    }
                    _ => acp_failure_run_error(&err),
                };
                let evs = vec![Ok(factory::run_started(thread_id, run_id)), Ok(error_event)];
                guarded_event_stream(evs, run_guard)
            }
        };
        Ok(stream)
    }
}

/// Outcome of inspecting the **trailing** message of an AG-UI
/// `RunAgentInput.messages`. The bridge's contract: a run carries a
/// fresh user prompt only when the tail is a `User` text message.
#[derive(Debug)]
enum TrailingUser {
    /// `messages[]` is empty.
    Empty,
    /// Tail is a non-user message (assistant / tool / activity / reasoning).
    /// This is what AG-UI runtimes (CopilotKit's `agentic_chat`, …) post
    /// when they auto-fire a follow-up run after a tool turn so the LLM
    /// sees the tool result. The agent has already handled the prior
    /// user turn; we MUST NOT re-prompt it.
    NonUserTail,
    /// Tail is a `User` message but the content is multi-part
    /// (images / files). We do not yet forward those to ACP `prompt()`,
    /// which is text-only in the bridge's current scope.
    NonText,
    /// Tail is a fresh `User` text message — forward to ACP.
    Text(String),
}

fn acp_resume_session_id(
    forwarded_props: &serde_json::Value,
) -> Result<Option<SessionId>, &'static str> {
    let Some(marker) = forwarded_props
        .as_object()
        .and_then(|props| props.get("acpResume"))
    else {
        return Ok(None);
    };

    match marker {
        serde_json::Value::Object(value) => {
            let Some(session_id) = value.get("sessionId").and_then(serde_json::Value::as_str)
            else {
                return Err("forwardedProps.acpResume.sessionId must be a non-empty string");
            };
            if session_id.trim().is_empty() {
                return Err("forwardedProps.acpResume.sessionId must be a non-empty string");
            }
            Ok(Some(SessionId::from(session_id.to_owned())))
        }
        _ => Err("forwardedProps.acpResume.sessionId must be a non-empty string"),
    }
}

/// Inspect the trailing message of `messages[]` per the bridge's
/// "trailing-user-only" contract. See [`TrailingUser`] for outcomes.
///
/// Why "trailing only" rather than "last user found via reverse search"?
/// AG-UI runtimes that drive `agentic_chat` (CopilotKit, …) re-fire
/// `runAgent` after every tool turn so their LLM-facing state machine
/// can see the tool result. Those follow-up runs carry the same
/// historical user message somewhere in the array but the **tail** is
/// always an `assistant`/`tool` message. A reverse-find extractor would
/// re-prompt the ACP agent with the historical user text on each
/// follow-up — and because the ACP session already contains the prior
/// reply in its history, the agent thinks the user is repeating the
/// same question and replies again, ad infinitum. Empirically this
/// shows up as "every run loops the previous turn".
///
/// The trailing-only rule matches the protocol intent: in ACP each
/// `prompt()` corresponds to one user-driven turn. AG-UI's `messages[]`
/// is the conversation transcript; the tail tells us what kind of turn
/// the runtime is asking for.
fn extract_trailing_user_text(messages: &[Message]) -> TrailingUser {
    let Some(last) = messages.last() else {
        return TrailingUser::Empty;
    };
    match last {
        Message::User(u) => match &u.content {
            UserMessageContent::Text(t) => TrailingUser::Text(t.clone()),
            UserMessageContent::Parts(_) => TrailingUser::NonText,
        },
        _ => TrailingUser::NonUserTail,
    }
}

struct EventStreamContext {
    session: Arc<AcpSessionHandle>,
    state: BridgeAppState,
    registry_entry: Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    turn_id: TurnId,
    /// The MCP channel and its conditional sender cleanup are installed
    /// before the ACP prompt command is submitted. This closes the first-tool
    /// window without changing the per-thread registry ownership rules.
    frontend_stream: FrontendStreamSetup,
}

struct FrontendStreamSetup {
    mcp_tool_rx: mpsc::Receiver<BridgeStreamItem>,
    clear_on_drop: ClearOnDrop,
}

fn install_frontend_sender(
    registry_entry: &Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    translated_buffer: usize,
) -> FrontendStreamSetup {
    // The receiver is moved into the SSE task after prompt creation; the
    // bounded buffer absorbs a call made in that short handoff window.
    let (mcp_tool_tx, mcp_tool_rx) = mpsc::channel::<BridgeStreamItem>(translated_buffer);
    registry_entry.set_active_sender(Some(mcp_tool_tx.clone()));
    FrontendStreamSetup {
        mcp_tool_rx,
        clear_on_drop: ClearOnDrop {
            entry: registry_entry.clone(),
            sender: mcp_tool_tx,
        },
    }
}

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

// ponytail: the split-out interval parameter exists only so tests can shrink
// it to milliseconds; threading it through a config struct for that alone
// would touch every call site for no production benefit.
#[allow(clippy::too_many_arguments)]
fn build_event_stream_with_keepalive(
    thread_id: String,
    run_id: String,
    prompt_stream: PromptStream,
    context: EventStreamContext,
    translated_buffer: usize,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
    keepalive_interval: std::time::Duration,
) -> BoxStream<'static, AgUiResult<Event>> {
    let EventStreamContext {
        session,
        state,
        registry_entry,
        turn_id,
        frontend_stream,
    } = context;
    let FrontendStreamSetup {
        mcp_tool_rx,
        clear_on_drop,
    } = frontend_stream;
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);
    let slow_consumer_timeout = state.inner.config.slow_consumer_timeout;

    tokio::spawn(async move {
        // Hold the guard for the duration of the prompt so the reaper
        // sees `active_prompts > 0` and refuses to drop the session.
        let _prompt_guard = prompt_guard;
        let _run_guard = run_guard;
        // Clear the registry's active sender on exit so MCP requests that
        // arrive after the prompt finishes are rejected promptly instead
        // of silently parking forever. We clear *conditionally* — only if
        // the slot still holds the sender THIS run installed — so an
        // overlapping newer run on the same thread_id (page refresh, a
        // CopilotKit follow-up run, a reconnect) keeps its own sender and
        // its in-flight tool calls don't get stranded into a timeout.
        let _clear_on_drop = clear_on_drop;
        // `None` once the MCP channel closes mid-run: that arm is then
        // disabled in the select below while the rest of the loop keeps
        // draining ACP events until Finished/RunError/disconnect.
        let mut mcp_tool_rx = Some(mcp_tool_rx);
        let session_for_stream = session;
        // Measure idle time between successful SSE sends, not incoming ACP
        // updates: suppressed or cached progress may produce no output.
        // Heartbeats also reset the deadline after delivery so a past
        // deadline cannot flood the client.
        let mut keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
        let mut retired = RetiredSseDrain::default();

        if send_prompt_sse(
            &tx,
            Ok(factory::run_started(thread_id.clone(), run_id.clone())),
            slow_consumer_timeout,
            &session_for_stream,
            &thread_id,
            &run_id,
            turn_id,
            &mut retired,
        )
        .await
        .is_err()
        {
            return;
        }

        let PromptStream {
            mut events,
            finished,
        } = prompt_stream;
        let mut translator = Translator::new();
        // Tell the translator to suppress agent-side ToolCall echoes for
        // any tool we just registered. Most agents prefix MCP-sourced
        // tool names with `<server-name>_` when surfacing them on
        // session/update; we cover both spellings.
        let suppressed_titles: Vec<String> = registry_entry
            .tools()
            .into_iter()
            .flat_map(|t| {
                [
                    t.name.clone(),
                    format!(
                        "{prefix}_{name}",
                        prefix = agui_acp_bridge_core::MCP_SERVER_NAME,
                        name = t.name,
                    ),
                ]
            })
            .collect();
        translator.set_suppressed_titles(suppressed_titles);
        let mut errored: Option<String> = None;

        // Helper: when an SSE send fails the client has disconnected.
        // Cancel the in-flight ACP turn so the agent stops doing work
        // nobody is reading. The session actor's run_prompt_with_cancel
        // also detects the events channel being dropped, so this is a
        // belt-and-suspenders approach.
        let cancel_on_disconnect = |session: Arc<AcpSessionHandle>, turn_id: TurnId| {
            if let Err(e) = session.cancel_turn(turn_id) {
                tracing::warn!(error = %e, "failed to cancel turn after client disconnect");
            }
        };

        loop {
            // Multiplex: we pull from both the ACP-side events channel
            // (session updates) and the MCP-side tool-call channel until
            // ACP signals Finished/RunError. Either source produces
            // BridgeStreamItem values that we translate uniformly.
            //
            // We deliberately do NOT use `biased` here. With chatty agents
            // (opencode emits dozens of `agent_thought_chunk` per second
            // during reasoning), a biased select would starve the MCP
            // channel — the agent's `tools/call` would queue indefinitely
            // and time out on its end. Fair scheduling is required for
            // correctness.
            let item = tokio::select! {
                acp = events.recv() => {
                    match acp {
                        Some(it) => it,
                        None => break,
                    }
                },
                // Only while the MCP channel is still open. Once every
                // sender clone is gone mid-run (e.g. a newer overlapping
                // run took over the registry slot), we drop this arm and
                // keep servicing `events` + `tx.closed()` until the turn
                // reaches Finished/RunError or the client disconnects.
                // Breaking out here would silently discard the remaining
                // ACP updates for the rest of the turn and lose the
                // disconnect-cancellation path below.
                mcp = async { mcp_tool_rx.as_mut().unwrap().recv().await }, if mcp_tool_rx.is_some() => {
                    match mcp {
                        Some(it) => it,
                        None => {
                            mcp_tool_rx = None;
                            continue;
                        }
                    }
                },
                // SSE keepalive: an idle turn (agent inside a long tool
                // call) otherwise emits zero bytes and proxy/ALB idle
                // timeouts (~60s) kill the connection, discarding the
                // turn's work. See `SSE_KEEPALIVE_INTERVAL`. Re-arm after
                // a successful send so slow sends cannot flood the client.
                _ = tokio::time::sleep_until(keepalive_deadline) => {
                    if send_prompt_sse(
                        &tx,
                        Ok(keepalive_event()),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    continue;
                }
                // Detect client disconnect even while idle. When the SSE
                // consumer drops, `tx` closes. Without this branch the loop
                // would park on `events.recv()` / `mcp_tool_rx.recv()` and
                // only notice the dead client on the *next* event — which
                // never comes while the agent is parked awaiting a frontend
                // tool result. That would pin `active_prompts > 0` (so the
                // reaper can't release the session) until `frontend_tool_timeout`
                // fires — the root cause of sessions piling up after refreshes.
                () = tx.closed() => {
                    cancel_on_disconnect(session_for_stream.clone(), turn_id);
                    return;
                }
            };

            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
                BridgeStreamItem::SessionInit {
                    modes,
                    models,
                    config_options,
                } => {
                    // Re-emitted by the session actor at the start of every
                    // prompt so reconnecting clients see the picker even on
                    // mid-thread runs. Always sent before any agent text.
                    let ev = session_init_event_with_config(
                        modes.as_ref(),
                        models.as_ref(),
                        config_options.as_deref(),
                    );
                    if send_prompt_sse(
                        &tx,
                        Ok(ev),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                }
                BridgeStreamItem::Finished { .. } => {
                    break;
                }
                BridgeStreamItem::RunError { message } => {
                    errored = Some(message);
                    break;
                }
                BridgeStreamItem::Interrupt { id, request } => {
                    // The session actor only emits `Interrupt` when the
                    // configured policy returned `Defer`. Emit a STATE_SNAPSHOT
                    // event so the frontend can render an approval dialog.
                    // The session actor is awaiting an external resolution
                    // via `BridgeAppState::resolve_permission` (typically
                    // surfaced over POST /approval). If the configured
                    // permission_timeout elapses, it falls back to deny.
                    let approval_state = serde_json::json!({
                        "approval": {
                            "pending": true,
                            "interruptId": id,
                            "toolName": request.tool_call.fields.title,
                            "options": request.options,
                        }
                    });
                    let event = Event::StateSnapshot(agui_rs_core::events::StateSnapshotEvent {
                        snapshot: approval_state,
                        base: agui_rs_core::events::BaseEventFields::default(),
                    });
                    if send_prompt_sse(
                        &tx,
                        Ok(event),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                }
                BridgeStreamItem::FrontendToolCall {
                    tool_call_id,
                    tool_name,
                    arguments,
                } => {
                    // Frontend tool dispatched by the agent through our
                    // MCP endpoint. Use the bypass-suppression path on
                    // the translator so this call surfaces even though
                    // the same tool name is in the suppression set
                    // (which exists to drop the *agent-side echo* of
                    // the same call). The matching TOOL_CALL_END is
                    // emitted by the MCP route once it has the result,
                    // via a `FrontendToolEnd` item below.
                    for ev in translator.translate_frontend_tool_call(
                        tool_call_id,
                        tool_name,
                        Some(&arguments),
                    ) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
                BridgeStreamItem::FrontendToolEnd { tool_call_id } => {
                    for ev in translator.translate_frontend_tool_end(&tool_call_id) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
            }
        }

        for ev in translator.flush() {
            if send_prompt_sse(
                &tx,
                Ok(ev),
                slow_consumer_timeout,
                &session_for_stream,
                &thread_id,
                &run_id,
                turn_id,
                &mut retired,
            )
            .await
            .is_err()
            {
                return;
            }
        }

        if session_for_stream.is_unusable() {
            remove_session_if_handle(&state.inner, &thread_id, &session_for_stream);
        }

        if let Some(msg) = errored {
            let _ = send_prompt_sse(
                &tx,
                Ok(factory::run_error(msg)),
                slow_consumer_timeout,
                &session_for_stream,
                &thread_id,
                &run_id,
                turn_id,
                &mut retired,
            )
            .await;
            return;
        }

        let finished_result = finished.await;
        if session_for_stream.is_unusable() {
            remove_session_if_handle(&state.inner, &thread_id, &session_for_stream);
        }

        let terminal = match finished_result {
            Ok(Ok(stop_reason)) => {
                stop_reason_terminal_event(thread_id.clone(), run_id.clone(), stop_reason)
            }
            Ok(Err(e)) => acp_failure_run_error(&e),
            Err(_) => factory::run_error("acp session dropped before finish"),
        };
        let _ = send_prompt_sse(
            &tx,
            Ok(terminal),
            slow_consumer_timeout,
            &session_for_stream,
            &thread_id,
            &run_id,
            turn_id,
            &mut retired,
        )
        .await;
    });

    ReceiverStream::new(rx).boxed()
}

/// Simplified event stream for a "resume bootstrap" run: emit `RUN_STARTED`,
/// translate the replayed history updates into AG-UI events, then emit the
/// terminal event reported by the history drain. No agent prompt is issued;
/// no frontend-tool routing is needed (history replay carries no live tool
/// calls). Both guards keep the session and thread admission alive until the
/// stream terminates.
#[allow(clippy::too_many_arguments)]
fn build_history_stream(
    thread_id: String,
    run_id: String,
    drain_stream: PromptStream,
    translated_buffer: usize,
    slow_consumer_timeout: std::time::Duration,
    session: Arc<AcpSessionHandle>,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    build_history_stream_with_keepalive(
        thread_id,
        run_id,
        drain_stream,
        translated_buffer,
        slow_consumer_timeout,
        session,
        prompt_guard,
        run_guard,
        SSE_KEEPALIVE_INTERVAL,
    )
}

// ponytail: see build_event_stream_with_keepalive for the parameter note.
#[allow(clippy::too_many_arguments)]
fn build_history_stream_with_keepalive(
    thread_id: String,
    run_id: String,
    drain_stream: PromptStream,
    translated_buffer: usize,
    slow_consumer_timeout: std::time::Duration,
    session: Arc<AcpSessionHandle>,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
    keepalive_interval: std::time::Duration,
) -> BoxStream<'static, AgUiResult<Event>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);

    tokio::spawn(async move {
        let _prompt_guard = prompt_guard;
        let _run_guard = run_guard;
        let mut retired = RetiredSseDrain::default();
        if send_history_sse(
            &tx,
            Ok(factory::run_started(thread_id.clone(), run_id.clone())),
            slow_consumer_timeout,
            &thread_id,
            &run_id,
            &session,
            &mut retired,
        )
        .await
        .is_err()
        {
            return;
        }

        let PromptStream {
            mut events,
            finished,
        } = drain_stream;
        let mut translator = Translator::new();
        // Keepalive starts one full interval after creation (so after the
        // RUN_STARTED send above): `interval`'s first `tick()` completes
        // immediately, which would emit a stray frame right after the run
        // header.
        let mut keepalive = tokio::time::interval_at(
            tokio::time::Instant::now() + keepalive_interval,
            keepalive_interval,
        );
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut drain_error: Option<String> = None;
        loop {
            let item = tokio::select! {
                item = events.recv() => item,
                // SSE keepalive for the history replay; see
                // `SSE_KEEPALIVE_INTERVAL`. Loops back without consuming a
                // stream item.
                _ = keepalive.tick() => {
                    if send_history_sse(&tx, Ok(keepalive_event()), slow_consumer_timeout, &thread_id, &run_id, &session, &mut retired)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                () = tx.closed() => return,
            };
            let Some(item) = item else {
                break;
            };
            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if send_history_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &thread_id,
                            &run_id,
                            &session,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                }
                BridgeStreamItem::SessionInit {
                    modes,
                    models,
                    config_options,
                } => {
                    let ev = session_init_event_with_config(
                        modes.as_ref(),
                        models.as_ref(),
                        config_options.as_deref(),
                    );
                    if send_history_sse(
                        &tx,
                        Ok(ev),
                        slow_consumer_timeout,
                        &thread_id,
                        &run_id,
                        &session,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                }
                BridgeStreamItem::Finished { .. } => break,
                BridgeStreamItem::RunError { message } => {
                    drain_error = Some(message);
                    break;
                }
                // History replay carries no interrupts or frontend tool
                // calls; ignore those variants defensively.
                _ => {}
            }
        }

        // Flush all open messages before the single history terminal event.
        for ev in translator.flush() {
            if send_history_sse(
                &tx,
                Ok(ev),
                slow_consumer_timeout,
                &thread_id,
                &run_id,
                &session,
                &mut retired,
            )
            .await
            .is_err()
            {
                return;
            }
        }

        let terminal = if let Some(message) = drain_error {
            run_error_with_code("ACP_HISTORY_DRAIN_ERROR", message)
        } else {
            match tokio::select! {
                result = finished => result,
                () = tx.closed() => return,
            } {
                Ok(Ok(stop_reason)) => {
                    stop_reason_terminal_event(thread_id.clone(), run_id.clone(), stop_reason)
                }
                Ok(Err(error)) => run_error_with_code(
                    "ACP_HISTORY_DRAIN_ERROR",
                    format!("acp history drain failed: {error}"),
                ),
                Err(_) => run_error_with_code(
                    "ACP_HISTORY_DRAIN_CLOSED",
                    "ACP history drain channel closed before completion",
                ),
            }
        };
        let _ = send_history_sse(
            &tx,
            Ok(terminal),
            slow_consumer_timeout,
            &thread_id,
            &run_id,
            &session,
            &mut retired,
        )
        .await;
    });

    ReceiverStream::new(rx).boxed()
}
///
/// The clear is **conditional** ([`ThreadEntry::clear_active_sender_if_same`]):
/// it only nulls the slot if it still holds the sender this run installed.
/// This prevents an older run's teardown from wiping a newer overlapping
/// run's sender on the same `thread_id`, which would otherwise strand the
/// newer run's in-flight frontend tool calls until they time out. The same
/// guard also covers cancellation or prompt-creation failure before the SSE
/// task takes ownership of the setup.
struct ClearOnDrop {
    entry: Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    sender: tokio::sync::mpsc::Sender<BridgeStreamItem>,
}

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        // Only act if the slot is still ours. If a newer overlapping run on
        // the same thread_id took over the sender, it now owns the pending
        // calls too, so we must not disturb them.
        if self.entry.clear_active_sender_if_same(&self.sender) {
            // We were the active run and we're going away (finished, errored,
            // or the client disconnected). Unblock any frontend-tool call
            // still parked on a oneshot so the agent's turn can unwind
            // instead of pinning the session until `frontend_tool_timeout`.
            self.entry
                .drain_pending("AG-UI run ended before tool resolved");
        }
    }
}

const DEFAULT_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// How often an idle SSE stream emits a keepalive frame.
///
/// The vendored `agui-rs-server` `sse_body` can only emit encoded AG-UI
/// `Event`s (`data: {json}\n\n`); a bare SSE `:comment` frame is not
/// expressible through it. The keepalive is therefore a protocol-legal
/// `CUSTOM` event (`agent:keepalive`) — the same mechanism the bridge already
/// uses for `agent:session_init`. It exists purely to defeat proxy/ALB idle
/// timeouts (~60s) while the agent sits inside a long tool call; consumers
/// should ignore its value.
const SSE_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Test hook: the keepalive interval for streams created inside tests. Tests
/// shrink it so cadence assertions run in milliseconds instead of seconds;
/// production callers go through [`build_event_stream`]/[`build_history_stream`],
/// which default to [`SSE_KEEPALIVE_INTERVAL`].
#[cfg(test)]
fn keepalive_interval_for_tests() -> std::time::Duration {
    std::time::Duration::from_millis(50)
}

fn keepalive_event() -> Event {
    Event::Custom(agui_rs_core::events::CustomEvent {
        name: "agent:keepalive".to_string(),
        value: serde_json::Value::Null,
        base: agui_rs_core::events::BaseEventFields::default(),
    })
}

fn invalid_agui_body(message: impl std::fmt::Display) -> Response {
    (
        StatusCode::BAD_REQUEST,
        format!("invalid request body: {message}"),
    )
        .into_response()
}

/// Read and validate the raw body before handing it to `agui-rs-server`.
///
/// `agui-rs-server` reads its route body with `to_bytes(..., usize::MAX)`, so
/// Axum's `DefaultBodyLimit` extractor layer does not constrain the direct
/// AG-UI route. This middleware is deliberately a body-reading boundary rather
/// than another extractor layer — it is the ONLY size enforcement on that
/// route. It ALWAYS parses and validates the
/// `RunAgentInput` (keeping invalid inputs out of session admission and
/// mapping them to HTTP 400) regardless of whether a body `limit` is
/// configured; only the size check itself is limit-gated.
///
/// It also enforces the JSON-in / SSE-out media-type boundary
/// (`docs/PROTOCOL_CONFORMANCE.md` §2 "Non-JSON/SSE AG-UI transport"): an
/// explicit protobuf `Accept` is rejected with HTTP 406 before any session
/// admission, and the outbound `Accept` is pinned to SSE so the upstream
/// encoder can never select protobuf for `*/*`.
async fn agui_input_boundary(limit: Option<usize>, request: Request<Body>, next: Next) -> Response {
    if request.method() != Method::POST || request.uri().path() != "/" {
        return next.run(request).await;
    }

    if let Some(reject) = reject_wrong_media_types(request.headers()) {
        return reject;
    }

    let content_length_over_limit = |request: &Request<Body>| {
        limit.is_some_and(|limit| {
            request
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > limit)
        })
    };
    if content_length_over_limit(&request) {
        let limit = limit.expect("checked above");
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("invalid request body: request body exceeds {limit} bytes"),
        )
            .into_response();
    }

    let (mut parts, body) = request.into_parts();
    // Pin the outbound Accept to SSE. `agui-rs-encoder` treats `*/*` (and an
    // absent Accept, which curl/browser defaults make common) as
    // protobuf-capable; this bridge only implements SSE, so downstream the
    // encoder must always see an explicit `text/event-stream`.
    parts.headers.insert(
        header::ACCEPT,
        HeaderValue::from_static(agui_rs_core::AGUI_MEDIA_TYPE_SSE),
    );
    let mut body_stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body_stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => return invalid_agui_body(error),
        };
        if let Some(limit) = limit
            && bytes.len().saturating_add(chunk.len()) > limit
        {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("invalid request body: request body exceeds {limit} bytes"),
            )
                .into_response();
        }
        bytes.extend_from_slice(&chunk);
    }

    let input = match serde_json::from_slice::<RunAgentInput>(&bytes) {
        Ok(input) => input,
        Err(error) => return invalid_agui_body(error),
    };
    if let Err(error) = input.validate() {
        return invalid_agui_body(error);
    }

    let (response, capacity_rejected) = CAPACITY_REJECTED
        .scope(Cell::new(false), async {
            let response = next
                .run(Request::from_parts(parts, Body::from(bytes)))
                .await;
            let rejected = CAPACITY_REJECTED.try_with(Cell::get).unwrap_or(false);
            (response, rejected)
        })
        .await;
    if capacity_rejected && response.status() == StatusCode::INTERNAL_SERVER_ERROR {
        let (mut parts, body) = response.into_parts();
        parts.status = StatusCode::SERVICE_UNAVAILABLE;
        Response::from_parts(parts, body)
    } else {
        response
    }
}

/// Enforce the bridge's JSON-in / SSE-out media-type boundary on the AG-UI
/// route (`docs/PROTOCOL_CONFORMANCE.md` §2 "Non-JSON/SSE AG-UI transport").
///
/// The upstream `agui-rs-server` route picks its encoder from `Accept`, and
/// `agui-rs-encoder` treats `*/*` as protobuf-capable — but this bridge only
/// implements the JSON/SSE path, so an explicit protobuf `Accept` is rejected
/// with HTTP 406 BEFORE any session admission or SSE stream starts. `*/*` and
/// a missing `Accept` are treated as SSE (the middleware later pins the
/// outbound `Accept` header to `text/event-stream`, so the upstream encoder
/// can never select protobuf for them). A non-JSON `Content-Type` is rejected
/// with 400; an absent `Content-Type` or one carrying charset/suffix
/// parameters (`application/json; charset=utf-8`,
/// `application/vnd.api+json`) is tolerated.
fn reject_wrong_media_types(headers: &HeaderMap) -> Option<Response> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    let accepts_proto = accept.is_some_and(|accept| {
        accept.split(',').any(|part| {
            part.split(';').next().is_some_and(|media| {
                media
                    .trim()
                    .eq_ignore_ascii_case(agui_rs_core::AGUI_MEDIA_TYPE_PROTOBUF)
            })
        })
    });
    if accepts_proto {
        return Some(
            (
                StatusCode::NOT_ACCEPTABLE,
                "this bridge only implements the AG-UI JSON/SSE transport; \
                 the AG-UI protobuf encoding is not supported",
            )
                .into_response(),
        );
    }

    let json_content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|media| {
            let media = media.trim();
            media.eq_ignore_ascii_case("application/json")
                || (media.ends_with("+json") && !media.is_empty())
        });
    if json_content_type {
        None
    } else {
        Some(invalid_agui_body("content-type must be application/json"))
    }
}

/// Build the AG-UI axum router for a given bridge state.
///
/// Mounts:
/// - `POST /` — AG-UI run endpoint (handled by [`BridgeHandler`])
/// - `GET /health` — anonymous liveness probe; returns `200 {"status":"ok"}`
/// - `POST /approval` — resolve a deferred permission request by interrupt id
///
/// A request body limit of 16 MiB is applied to every route. If your agents
/// receive significantly larger AG-UI inputs (e.g. very long conversation
/// histories), build the router yourself by composing
/// [`build_router_inner`] with your own `DefaultBodyLimit` layer.
pub fn build_router(state: BridgeAppState) -> axum::Router {
    build_router_inner_with_agui_body_limit(state, DEFAULT_BODY_LIMIT_BYTES).layer(
        axum::extract::DefaultBodyLimit::max(DEFAULT_BODY_LIMIT_BYTES),
    )
}

/// Same as [`build_router`] without the outer `DefaultBodyLimit` extractor
/// layer.
///
/// The direct AG-UI route is still size-bounded internally: its body-reading
/// boundary applies the same 16 MiB [`DEFAULT_BODY_LIMIT_BYTES`] cap, because
/// `agui-rs-server` reads that route with `to_bytes(..., usize::MAX)` and no
/// extractor layer can constrain it (see [`agui_input_boundary`]). There is
/// no public way to raise that AG-UI-route cap; a request over the limit is
/// rejected with HTTP 413.
pub fn build_router_inner(state: BridgeAppState) -> axum::Router {
    build_router_inner_with_agui_body_limit(state, DEFAULT_BODY_LIMIT_BYTES)
}

fn build_router_inner_with_agui_body_limit(
    state: BridgeAppState,
    agui_body_limit: usize,
) -> axum::Router {
    use axum::{Json, extract::State, routing::get, routing::post};

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ApprovalRequest {
        thread_id: String,
        interrupt_id: String,
        approved: bool,
        option_id: Option<String>,
    }

    async fn approval(
        State(state): State<BridgeAppState>,
        Json(body): Json<ApprovalRequest>,
    ) -> axum::http::StatusCode {
        use agent_client_protocol::schema::v1::PermissionOptionId;
        use agui_acp_bridge_core::PermissionDecision;
        let decision = if body.approved {
            // For an `approved` payload, the caller MUST supply the
            // `optionId` the user picked. We do not silently default to
            // "allow_once": that string is unlikely to be one of the
            // agent's advertised options, and validation would catch it
            // anyway — surfacing the 422 here gives a clearer error.
            let Some(option_id) = body.option_id else {
                return axum::http::StatusCode::BAD_REQUEST;
            };
            PermissionDecision::Allow {
                option_id: PermissionOptionId::new(option_id),
            }
        } else {
            PermissionDecision::Deny
        };
        match state.resolve_permission(&body.thread_id, &body.interrupt_id, decision) {
            ResolveOutcome::Resolved => axum::http::StatusCode::OK,
            ResolveOutcome::InvalidOption => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            ResolveOutcome::NotFound => axum::http::StatusCode::NOT_FOUND,
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ToolResponseBody {
        thread_id: String,
        tool_call_id: String,
        #[serde(default)]
        content: String,
        #[serde(default)]
        is_error: bool,
    }

    async fn tool_response(
        State(state): State<BridgeAppState>,
        Json(body): Json<ToolResponseBody>,
    ) -> axum::http::StatusCode {
        use agui_acp_bridge_core::FrontendToolResponse;
        let span = tracing::info_span!(
            "frontend_tool_response",
            tool_call_id = %body.tool_call_id,
            is_error = body.is_error,
        );
        let _enter = span.enter();
        let resp = if body.is_error {
            FrontendToolResponse::error(body.content)
        } else {
            FrontendToolResponse::ok(body.content)
        };
        if state.resolve_frontend_tool(&body.thread_id, &body.tool_call_id, resp) {
            tracing::info!("resolved");
            axum::http::StatusCode::OK
        } else {
            tracing::warn!("no pending tool call for id");
            axum::http::StatusCode::NOT_FOUND
        }
    }

    async fn health() -> Json<serde_json::Value> {
        Json(serde_json::json!({ "status": "ok" }))
    }

    /// `GET /sessions` — list the agent's persisted conversations via ACP
    /// `session/list`. The bridge holds no history of its own; this is a
    /// pass-through. A client supplies an entry's `sessionId` in the private
    /// resume marker while choosing its own AG-UI `threadId`.
    ///
    /// | Status | Meaning                                                  |
    /// |--------|----------------------------------------------------------|
    /// | 200    | `{ "sessions": [ { sessionId, cwd, title?, updatedAt? } ] }` |
    /// | 501    | the agent does not support `session/list`.               |
    /// | 502    | the agent errored or the listing connection failed.      |
    async fn sessions(State(state): State<BridgeAppState>) -> axum::response::Response {
        use axum::response::IntoResponse;
        match state.list_sessions().await {
            Ok(list) => Json(serde_json::json!({ "sessions": list })).into_response(),
            Err(BridgeError::Unsupported(what)) => (
                axum::http::StatusCode::NOT_IMPLEMENTED,
                Json(serde_json::json!({ "error": format!("agent does not support {what}") })),
            )
                .into_response(),
            Err(e) => {
                tracing::warn!(error = %e, "session/list failed");
                (
                    axum::http::StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response()
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SessionInitQuery {
        thread_id: String,
    }

    /// `GET /session/init?threadId=...` — synchronous discovery of the
    /// session's mode / model offering. Returns 404 when no session is
    /// open for that thread (the frontend should issue a normal AG-UI
    /// run first to create one).
    async fn session_init(
        State(state): State<BridgeAppState>,
        axum::extract::Query(q): axum::extract::Query<SessionInitQuery>,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        match state.session_init_state(&q.thread_id) {
            Some(init) => {
                let body = serde_json::json!({
                    "modes": init.modes,
                    "models": init.models,
                    "configOptions": init.config_options,
                });
                Json(body).into_response()
            }
            None => axum::http::StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SetSessionModeBody {
        thread_id: String,
        mode_id: String,
    }

    /// `POST /session/set-mode` — switch the session's mode. When config
    /// options are advertised this maps to the discovered `mode` option;
    /// otherwise it uses the old SDK-compatible `session/set_mode` fallback.
    ///
    /// | Status | Meaning                                                       |
    /// |--------|---------------------------------------------------------------|
    /// | 200    | mode accepted; the agent has confirmed the switch.            |
    /// | 404    | no session for `threadId`.                                    |
    /// | 409    | a lifecycle close/eviction currently owns the thread.          |
    /// | 408    | agent did not respond within `set_session_timeout`.           |
    /// | 422    | agent rejected (likely `modeId` not in `availableModes`).     |
    /// | 503    | session actor was closed mid-flight; retry creates a new one. |
    async fn set_mode(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionModeBody>,
    ) -> axum::http::StatusCode {
        match state.set_session_mode(&body.thread_id, body.mode_id).await {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_mode rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => {
                tracing::warn!("set_mode timed out waiting for agent");
                axum::http::StatusCode::REQUEST_TIMEOUT
            }
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/set-config-option` — set a discovered select/value-id
    /// config option. The agent response replaces the full cached option list.
    async fn set_config_option(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionConfigOptionBody>,
    ) -> axum::http::StatusCode {
        let value = typed_config_option_value(body.value);
        match state
            .set_session_config_option_value(&body.thread_id, body.config_id, value)
            .await
        {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_config_option rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => axum::http::StatusCode::REQUEST_TIMEOUT,
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/cancel` — request cancellation of the current turn for
    /// a cached session. The actor continues receiving final updates and
    /// drains all pending permissions before applying the grace timeout.
    async fn cancel_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<CancelSessionBody>,
    ) -> axum::http::StatusCode {
        match state.cancel_session(&body.thread_id) {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "session cancel failed");
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
            Err(SetSessionStatus::Timeout) => axum::http::StatusCode::REQUEST_TIMEOUT,
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/close` — explicitly close a cached ACP session when the
    /// agent advertises `sessionCapabilities.close`.
    async fn close_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<CloseSessionBody>,
    ) -> axum::http::StatusCode {
        match state.close_session(&body.thread_id).await {
            Ok(()) => axum::http::StatusCode::NO_CONTENT,
            Err(CloseSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(CloseSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(CloseSessionStatus::Unsupported) => axum::http::StatusCode::NOT_IMPLEMENTED,
            Err(CloseSessionStatus::Timeout) => axum::http::StatusCode::GATEWAY_TIMEOUT,
            Err(CloseSessionStatus::Acp(message)) => {
                tracing::warn!(error = %message, "session/close rejected by agent");
                axum::http::StatusCode::BAD_GATEWAY
            }
            Err(CloseSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/delete` — delete the persisted ACP session mapped to an
    /// exact bridge thread. Cache misses are local 404s.
    async fn delete_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<DeleteSessionBody>,
    ) -> axum::http::StatusCode {
        match state.delete_session(&body.thread_id).await {
            Ok(()) => axum::http::StatusCode::NO_CONTENT,
            Err(DeleteSessionStatus::InvalidInput) => axum::http::StatusCode::BAD_REQUEST,
            Err(DeleteSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(DeleteSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(DeleteSessionStatus::Unsupported) => axum::http::StatusCode::NOT_IMPLEMENTED,
            Err(DeleteSessionStatus::Timeout) => axum::http::StatusCode::GATEWAY_TIMEOUT,
            Err(DeleteSessionStatus::Acp(message)) => {
                tracing::warn!(error = %message, "session/delete rejected by agent");
                axum::http::StatusCode::BAD_GATEWAY
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SetSessionModelBody {
        thread_id: String,
        model_id: String,
    }

    /// `POST /session/set-model` — compatibility alias for the discovered
    /// model config option. It never sends ACP `session/set_model`.
    async fn set_model(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionModelBody>,
    ) -> axum::http::StatusCode {
        match state
            .set_session_model(&body.thread_id, body.model_id)
            .await
        {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_model rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => {
                tracing::warn!("set_model timed out waiting for agent");
                axum::http::StatusCode::REQUEST_TIMEOUT
            }
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    // The AG-UI router carries its own state (Arc<H>); our auxiliary routes
    // need `BridgeAppState`. Build them as a separate sub-router and `.merge()`.
    let aux: axum::Router = {
        let r = axum::Router::new()
            .route("/health", get(health))
            .route("/sessions", get(sessions))
            .route("/approval", post(approval))
            .route("/tool-response", post(tool_response))
            .route("/mcp/:thread", post(crate::mcp_endpoint::mcp_route))
            .route("/session/init", get(session_init))
            .route("/session/set-mode", post(set_mode))
            .route("/session/set-config-option", post(set_config_option))
            .route("/session/cancel", post(cancel_session))
            .route("/session/close", post(close_session))
            .route("/session/delete", post(delete_session))
            .route("/session/set-model", post(set_model));
        r.with_state(state.clone())
    };

    let allowed_origins = state.mcp_allowed_origins();
    let bearer_token = state.bearer_token();
    agui_rs_server::axum::agui_router(BridgeHandler::new(state))
        .layer(axum::middleware::from_fn(move |request, next| {
            agui_input_boundary(Some(agui_body_limit), request, next)
        }))
        .merge(aux)
        .layer(axum::middleware::from_fn(move |request, next| {
            let allowed_origins = allowed_origins.clone();
            let bearer_token = bearer_token.clone();
            async move {
                bridge_security_middleware(allowed_origins, bearer_token, request, next).await
            }
        }))
}

/// Builder for [`BridgeAppState`] when you need a non-default `BridgeConfig`,
/// a custom `PermissionPolicy` (e.g. `AutoDeny`, `Allowlist`), or want to
/// enable frontend-tool injection via [`BridgeAppStateBuilder::with_self_url`].
pub struct BridgeAppStateBuilder {
    client: Arc<dyn AcpClient>,
    cwd: PathBuf,
    config: BridgeConfig,
    policy: Arc<dyn PermissionPolicy>,
    self_url: Option<String>,
    bearer_token: Option<Arc<str>>,
    mcp_allowed_origins: HashSet<String>,
}

impl BridgeAppStateBuilder {
    /// Override the bridge configuration (timeouts, buffer sizes).
    #[must_use]
    pub fn with_config(mut self, config: BridgeConfig) -> Self {
        self.config = config;
        self
    }

    /// Override the permission policy applied to ACP `requestPermission`
    /// requests.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Require a bearer token on every route except exact `GET/HEAD /health`.
    ///
    /// Tokens are validated before they enter bridge state; they are never
    /// logged or included in the MCP URL.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Result<Self, String> {
        let token = token.into();
        validate_bearer_token(&token)?;
        self.bearer_token = Some(Arc::from(token));
        Ok(self)
    }

    /// Restrict present MCP `Origin` headers to this explicit allowlist.
    ///
    /// Origins are canonicalized to lowercase scheme/host plus effective
    /// port. Paths, trailing slashes, wildcards, `null`, and user information
    /// are rejected. Missing `Origin` remains allowed for non-browser agents.
    pub fn with_mcp_allowed_origins<I, S>(mut self, origins: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for origin in origins {
            let origin = origin.into();
            let canonical = crate::mcp_endpoint::canonicalize_origin(&origin)
                .map_err(|error| format!("invalid MCP allowed origin {origin:?}: {error}"))?;
            self.mcp_allowed_origins.insert(canonical);
        }
        Ok(self)
    }

    /// Enable frontend-tool injection (`useFrontendTool`-style tools) by
    /// telling the bridge what URL agents should use to reach its built-in
    /// MCP HTTP endpoint.
    ///
    /// In typical local-dev setups this is `http://127.0.0.1:<port>`. For
    /// reverse-proxy deployments, point at the public origin that routes
    /// `/mcp/...` back to the bridge. Trailing slash is tolerated.
    ///
    /// When this is set, every new session is opened with `mcp_servers =
    /// [{ url: <self_url>/mcp/<thread-token> }]`, gated on the agent's
    /// `mcpCapabilities.http`.
    #[must_use]
    pub fn with_self_url(mut self, url: impl Into<String>) -> Self {
        self.self_url = Some(url.into());
        self
    }

    /// Finalize the builder into a [`BridgeAppState`].
    #[must_use]
    pub fn build(self) -> BridgeAppState {
        let cwd = canonicalize_cwd(&self.cwd).unwrap_or_else(|err| {
            tracing::warn!(error = %err, cwd = %self.cwd.display(),
                "cwd canonicalize failed; using path as-is");
            self.cwd
        });
        BridgeAppState {
            inner: Arc::new(Inner {
                sessions: DashMap::new(),
                mcp_credentials: DashMap::new(),
                active_runs: DashMap::new(),
                active_settings: DashMap::new(),
                create_locks: DashMap::new(),
                capacity_gate: tokio::sync::Mutex::new(()),
                sessions_list_cache: tokio::sync::Mutex::new(None),
                sessions_list_gate: tokio::sync::Mutex::new(()),
                session_capacity: semaphore_for(self.config.max_sessions),
                client: self.client,
                cwd,
                config: self.config,
                policy: self.policy,
                frontend_tools: FrontendToolRegistry::new(),
                self_url: self.self_url.map(|u| u.trim_end_matches('/').to_string()),
                bearer_token: self.bearer_token,
                mcp_allowed_origins: self.mcp_allowed_origins,
                reaper: std::sync::Mutex::new(None),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agui_acp_bridge_core::{CustomAgentInProcessClient, InProcessAcpClient};
    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use serde_json::Value;
    use std::time::Duration;
    use tower::ServiceExt;

    const TEST_TOKEN: &str = "test-bearer-token-1234";

    fn authenticated_state() -> BridgeAppState {
        BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
            .with_bearer_token(TEST_TOKEN)
            .expect("test token is valid")
            .build()
    }

    fn auth_request(method: Method, uri: &str, authorization: Option<&str>) -> HttpRequest<Body> {
        let mut builder = HttpRequest::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(value) = authorization {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        builder.body(Body::from("{}")).unwrap()
    }

    #[test]
    fn prompt_stream_public_shape_remains_two_field_compatible() {
        let (_, events) = tokio::sync::mpsc::channel(1);
        let (_, finished) = tokio::sync::oneshot::channel::<
            Result<agent_client_protocol::schema::v1::StopReason, BridgeError>,
        >();
        let _stream = PromptStream { events, finished };
    }

    #[test]
    fn stop_reason_terminal_mapping_is_not_unconditionally_successful() {
        let cases = [
            (StopReason::EndTurn, "RUN_FINISHED", None),
            (StopReason::Cancelled, "RUN_ERROR", Some("ACP_CANCELLED")),
            (StopReason::MaxTokens, "RUN_ERROR", Some("ACP_MAX_TOKENS")),
            (
                StopReason::MaxTurnRequests,
                "RUN_ERROR",
                Some("ACP_MAX_TURN_REQUESTS"),
            ),
            (StopReason::Refusal, "RUN_ERROR", Some("ACP_REFUSAL")),
        ];

        for (reason, event_type, code) in cases {
            let value = serde_json::to_value(stop_reason_terminal_event(
                "thread".to_string(),
                "run".to_string(),
                reason,
            ))
            .expect("event serializes");
            assert_eq!(value["type"], event_type);
            assert_eq!(value.get("code").and_then(Value::as_str), code);
        }
    }

    #[test]
    fn default_state_policy_is_auto_deny() {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        assert_eq!(format!("{:?}", state.policy()), "AutoDeny");

        let builder_state =
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .build();
        assert_eq!(format!("{:?}", builder_state.policy()), "AutoDeny");
    }

    #[test]
    fn bearer_token_validation_rejects_unsafe_values() {
        for token in ["", "too-short", "token with spaces", "token\nwith-control"] {
            assert!(
                validate_bearer_token(token).is_err(),
                "token should be rejected: {token:?}"
            );
        }
        assert!(validate_bearer_token(TEST_TOKEN).is_ok());
    }

    #[test]
    fn mcp_origin_allowlist_canonicalizes_effective_ports() {
        assert_eq!(
            crate::mcp_endpoint::canonicalize_origin("HTTPS://Example.COM").unwrap(),
            "https://example.com:443"
        );
        assert_eq!(
            crate::mcp_endpoint::canonicalize_origin("http://[::1]:80").unwrap(),
            "http://[::1]:80"
        );
        assert!(crate::mcp_endpoint::canonicalize_origin("https://example.com/").is_err());
    }

    #[test]
    fn mcp_origin_allowlist_rejects_ambiguous_values() {
        for origin in [
            "null",
            "*",
            "https://*.example.com",
            "https://example.com/path",
            "https://example.com/",
            "https://example.com:bad",
            "https://user@example.com",
            "https://example.com?query=1",
        ] {
            assert!(
                BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                    .with_mcp_allowed_origins([origin])
                    .is_err(),
                "origin should be rejected: {origin}"
            );
        }
    }

    #[test]
    fn mcp_headers_never_carry_admin_token() {
        let state =
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .with_self_url("http://127.0.0.1:8080")
                .with_bearer_token(TEST_TOKEN)
                .expect("test token is valid")
                .build();
        let cfg = state.session_config_for("thread");
        assert!(
            cfg.mcp_headers.is_empty(),
            "transient config has no credential"
        );
        let scoped = McpCredential("scoped-token".into());
        let headers = state.mcp_headers(Some(&scoped));
        assert_eq!(headers[0].value, "Bearer scoped-token");
        assert!(!headers[0].value.contains(TEST_TOKEN));
        assert!(!format!("{cfg:?}").contains(TEST_TOKEN));
    }

    #[test]
    fn mcp_url_encodes_thread_token_as_one_path_segment() {
        let state =
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .with_self_url("http://127.0.0.1:8080")
                .build();
        let cfg = state.session_config_for("thread/slash?query#fragment");
        assert_eq!(
            cfg.mcp_url.as_deref(),
            Some("http://127.0.0.1:8080/mcp/thread%2Fslash%3Fquery%23fragment")
        );
    }

    #[tokio::test]
    async fn bearer_middleware_covers_every_protected_route() {
        let app = build_router(authenticated_state());
        let routes = [
            (Method::POST, "/"),
            (Method::POST, "/approval"),
            (Method::POST, "/tool-response"),
            (Method::GET, "/sessions"),
            (Method::GET, "/session/init?threadId=thread"),
            (Method::POST, "/session/set-mode"),
            (Method::POST, "/session/set-config-option"),
            (Method::POST, "/session/cancel"),
            (Method::POST, "/session/close"),
            (Method::POST, "/session/delete"),
            (Method::POST, "/session/set-model"),
        ];

        for (method, uri) in routes {
            for authorization in [
                None,
                Some("Bearer wrong-token-1234"),
                Some("Basic wrong-token-1234"),
                Some("Bearer"),
            ] {
                let response = app
                    .clone()
                    .oneshot(auth_request(method.clone(), uri, authorization))
                    .await
                    .expect("router response");
                assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri}"
                );
                assert_eq!(
                    response.headers().get(header::WWW_AUTHENTICATE),
                    Some(&HeaderValue::from_static("Bearer")),
                    "{method} {uri} must advertise bearer auth"
                );
            }

            let response = app
                .clone()
                .oneshot(auth_request(
                    method.clone(),
                    uri,
                    Some(&format!("Bearer {TEST_TOKEN}")),
                ))
                .await
                .expect("router response");
            assert_ne!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
        }
    }

    #[tokio::test]
    async fn only_exact_health_get_and_head_are_anonymous() {
        let app = build_router(authenticated_state());
        for method in [Method::GET, Method::HEAD] {
            let response = app
                .clone()
                .oneshot(auth_request(method, "/health", None))
                .await
                .expect("health response");
            assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
        }
        for (method, uri) in [
            (Method::GET, "/health?probe=1"),
            (Method::GET, "/health/"),
            (Method::POST, "/health"),
        ] {
            let response = app
                .clone()
                .oneshot(auth_request(method.clone(), uri, None))
                .await
                .expect("health response");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
        }
    }

    #[tokio::test]
    async fn tool_response_requires_thread_id() {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        let response = build_router(state)
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/tool-response")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"toolCallId":"call","content":"ok","isError":false}"#,
                    ))
                    .unwrap(),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn agui_input_validation_rejects_invalid_input_before_session_admission() {
        let mut duplicate_messages = RunAgentInput::new("thread", "run");
        for text in ["one", "two"] {
            duplicate_messages
                .messages
                .push(Message::User(agui_rs_core::types::UserMessage {
                    id: "duplicate-message-id".into(),
                    content: UserMessageContent::Text(text.into()),
                    name: None,
                    encrypted_value: None,
                }));
        }

        let inputs = [
            RunAgentInput::new("", "run"),
            RunAgentInput::new("thread", " "),
            duplicate_messages,
        ];
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));

        for input in inputs {
            let response = build_router(state.clone())
                .oneshot(
                    HttpRequest::post("/")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&input).unwrap()))
                        .unwrap(),
                )
                .await
                .expect("router response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("error body");
            assert!(body.starts_with(b"invalid request body: "));
        }

        assert_eq!(state.session_count(), 0);
    }

    #[tokio::test]
    async fn agui_body_limit_rejects_oversized_raw_body() {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        let response = build_router(state.clone())
            .oneshot(
                HttpRequest::post("/")
                    .header("content-type", "application/json")
                    .body(Body::from(vec![b'x'; DEFAULT_BODY_LIMIT_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body");
        assert_eq!(
            body,
            format!("invalid request body: request body exceeds {DEFAULT_BODY_LIMIT_BYTES} bytes")
                .as_bytes()
        );
        assert_eq!(state.session_count(), 0);
    }

    #[test]
    fn run_admission_releases_only_its_own_claim() {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        let first = state
            .try_claim_run("thread", "run-1")
            .expect("first run claims thread");
        assert!(state.try_claim_run("thread", "run-2").is_none());
        drop(first);
        assert!(state.try_claim_run("thread", "run-2").is_some());
    }

    struct SessionCreationRaceClient {
        opens: Arc<std::sync::atomic::AtomicUsize>,
        first_started: Arc<tokio::sync::Notify>,
        release_first: Arc<tokio::sync::Semaphore>,
        follower_started: Arc<tokio::sync::Notify>,
        third_started: Arc<tokio::sync::Notify>,
        release_followers: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl AcpClient for SessionCreationRaceClient {
        async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
            let call = self.opens.fetch_add(1, Ordering::SeqCst);
            match call {
                0 => {
                    self.first_started.notify_one();
                    self.release_first
                        .acquire()
                        .await
                        .expect("first release semaphore is live")
                        .forget();
                    Err(BridgeError::SessionClosed)
                }
                1 => {
                    self.follower_started.notify_one();
                    self.release_followers
                        .acquire()
                        .await
                        .expect("follower release semaphore is live")
                        .forget();
                    InProcessAcpClient::new().open_session(cfg).await
                }
                _ => {
                    self.third_started.notify_one();
                    self.release_followers
                        .acquire()
                        .await
                        .expect("follower release semaphore is live")
                        .forget();
                    InProcessAcpClient::new().open_session(cfg).await
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_creation_gate_survives_failure_with_queued_waiters() {
        let client = Arc::new(SessionCreationRaceClient {
            opens: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            first_started: Arc::new(tokio::sync::Notify::new()),
            release_first: Arc::new(tokio::sync::Semaphore::new(0)),
            follower_started: Arc::new(tokio::sync::Notify::new()),
            third_started: Arc::new(tokio::sync::Notify::new()),
            release_followers: Arc::new(tokio::sync::Semaphore::new(0)),
        });
        let state = BridgeAppState::new(client.clone(), PathBuf::from("/"));

        let first = {
            let state = state.clone();
            tokio::spawn(async move { state.session_for("creation-race").await })
        };
        tokio::time::timeout(Duration::from_secs(1), client.first_started.notified())
            .await
            .expect("first open_session must start");

        let queued = {
            let state = state.clone();
            tokio::spawn(async move { state.session_for("creation-race").await })
        };

        // The extra Arc proves the second caller retained the same gate before
        // the first creator is allowed to fail.
        let gate = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(gate) = state.inner.create_locks.get("creation-race") {
                    let gate = gate.clone();
                    if Arc::strong_count(&gate) >= 4 {
                        break gate;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued caller must retain the creation gate");
        drop(gate);

        client.release_first.add_permits(1);
        assert!(first.await.expect("first creator task").is_err());

        tokio::time::timeout(Duration::from_secs(1), client.follower_started.notified())
            .await
            .expect("queued caller must reach the retrying open_session");

        let contender = {
            let state = state.clone();
            tokio::spawn(async move { state.session_for("creation-race").await })
        };

        assert!(
            tokio::time::timeout(Duration::from_millis(250), client.third_started.notified())
                .await
                .is_err(),
            "a new caller must queue behind the old gate, not open a second session"
        );

        client.release_followers.add_permits(2);
        let queued_entry = tokio::time::timeout(Duration::from_secs(2), queued)
            .await
            .expect("queued creation must finish")
            .expect("queued creation task must not panic")
            .expect("queued creation must succeed");
        let contender_entry = tokio::time::timeout(Duration::from_secs(2), contender)
            .await
            .expect("contender creation must finish")
            .expect("contender creation task must not panic")
            .expect("contender creation must reuse the queued session");

        assert_eq!(client.opens.load(Ordering::SeqCst), 2);
        assert!(Arc::ptr_eq(&queued_entry, &contender_entry));
        let cached_entry = state
            .inner
            .sessions
            .get("creation-race")
            .expect("session is cached")
            .clone();
        assert!(Arc::ptr_eq(&cached_entry, &queued_entry));
        assert_eq!(state.session_count(), 1);
        assert!(state.inner.create_locks.is_empty());
    }

    async fn collect_history_terminal(
        finished_result: Option<Result<StopReason, BridgeError>>,
    ) -> Vec<Value> {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        let entry = state
            .session_for("history-test")
            .await
            .expect("history test session opens");
        let prompt_guard = entry.enter_prompt();
        let run_guard = state
            .try_claim_run("history-test", "history-run")
            .expect("history test claims thread");

        let (events_tx, events) = tokio::sync::mpsc::channel(1);
        drop(events_tx);
        let (finished_tx, finished) = tokio::sync::oneshot::channel();
        if let Some(result) = finished_result {
            finished_tx.send(result).expect("finished receiver is live");
        } else {
            drop(finished_tx);
        }

        let stream = build_history_stream(
            "history-test".into(),
            "history-run".into(),
            PromptStream { events, finished },
            1,
            Duration::from_secs(30),
            entry.handle.clone(),
            prompt_guard,
            run_guard,
        );
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|event| {
                serde_json::to_value(event.expect("event stream result")).expect("event serializes")
            })
            .collect()
    }

    #[tokio::test]
    async fn history_drain_error_and_closed_channel_never_finish_successfully() {
        for (finished_result, expected_code) in [
            (
                Some(Err(BridgeError::SessionClosed)),
                "ACP_HISTORY_DRAIN_ERROR",
            ),
            (None, "ACP_HISTORY_DRAIN_CLOSED"),
        ] {
            let events = collect_history_terminal(finished_result).await;
            assert!(!events.iter().any(|event| event["type"] == "RUN_FINISHED"));
            assert!(
                events.iter().any(|event| {
                    event["type"] == "RUN_ERROR" && event["code"] == expected_code
                })
            );
        }
    }

    #[tokio::test]
    async fn sse_send_helper_distinguishes_success_closed_and_timeout() {
        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(
            send_sse_with_timeout(&tx, 1_u8, Duration::from_millis(20)).await,
            Ok(())
        );
        assert_eq!(rx.recv().await, Some(1));

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1_u8).await.expect("fill bounded channel");
        assert_eq!(
            send_sse_with_timeout(&tx, 2_u8, Duration::from_millis(5)).await,
            Err(SseSendError::TimedOut)
        );
        assert_eq!(rx.recv().await, Some(1));

        let (tx, rx) = mpsc::channel::<u8>(1);
        drop(rx);
        assert_eq!(
            send_sse_with_timeout(&tx, 1_u8, Duration::from_secs(1)).await,
            Err(SseSendError::Closed)
        );

        let (tx, mut rx) = mpsc::channel(1);
        assert_eq!(
            send_sse_with_timeout(&tx, 3_u8, Duration::ZERO).await,
            Ok(())
        );
        assert_eq!(rx.recv().await, Some(3));
    }

    async fn disconnectable_test_session() -> (
        Arc<AcpSessionHandle>,
        Arc<tokio::sync::Notify>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let disconnect = Arc::new(tokio::sync::Notify::new());
        let disconnect_agent = disconnect.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ready_tx = Arc::new(std::sync::Mutex::new(Some(ready_tx)));
        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
            let disconnect = disconnect_agent.clone();
            let ready_tx = ready_tx.clone();
            async move {
                if let Some(tx) = ready_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                tokio::select! {
                    result = crate::test_agents::run_cancel_aware_slow_agent(stream) => result,
                    () = disconnect.notified() => Ok(()),
                }
            }
        }));
        let state = BridgeAppState::new(client, PathBuf::from("/"));
        let session = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("disconnectable"))
                .await
                .expect("in-process actor opens"),
        );
        (session, disconnect, ready_rx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_zero_send_unblocks_when_actor_retires() {
        for prompt_path in [true, false] {
            let (session, disconnect, agent_ready) = disconnectable_test_session().await;
            tokio::time::timeout(Duration::from_secs(1), agent_ready)
                .await
                .expect("agent runner starts")
                .expect("ready signal");
            let turn_id = if prompt_path {
                Some(
                    session
                        .prompt_with_turn("blocked send")
                        .await
                        .expect("turn starts")
                        .1,
                )
            } else {
                None
            };
            let (tx, _rx) = mpsc::channel(1);
            tx.send(Ok(keepalive_event()))
                .await
                .expect("fill SSE channel");
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let send_session = session.clone();
            let mut retired = RetiredSseDrain::default();
            let task = tokio::spawn(async move {
                let mut started_tx = Some(started_tx);
                std::future::poll_fn(|_cx| {
                    if let Some(signal) = started_tx.take() {
                        let _ = signal.send(());
                    }
                    std::task::Poll::Ready(())
                })
                .await;
                if let Some(turn_id) = turn_id {
                    send_prompt_sse(
                        &tx,
                        Ok(keepalive_event()),
                        Duration::ZERO,
                        &send_session,
                        "t",
                        "r",
                        turn_id,
                        &mut retired,
                    )
                    .await
                } else {
                    send_history_sse(
                        &tx,
                        Ok(keepalive_event()),
                        Duration::ZERO,
                        "t",
                        "r",
                        &send_session,
                        &mut retired,
                    )
                    .await
                }
            });
            tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .expect("send task reaches helper")
                .expect("signal");
            // Polling the helper establishes the full-channel send is pending before actor EOF.
            tokio::task::yield_now().await;
            disconnect.notify_one();
            tokio::time::timeout(Duration::from_secs(2), session.closed())
                .await
                .expect("real actor observes agent EOF");
            assert_eq!(
                tokio::time::timeout(RETIRED_SSE_DRAIN_GRACE + Duration::from_secs(1), task)
                    .await
                    .expect("blocked SSE send unblocks")
                    .expect("send task")
                    .unwrap_err(),
                SseSendError::ActorClosed,
            );
        }
    }

    #[tokio::test]
    async fn legacy_zero_send_keeps_waiting_for_live_actor() {
        let (session, _disconnect, agent_ready) = disconnectable_test_session().await;
        agent_ready.await.expect("agent runner starts");
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(Ok(keepalive_event()))
            .await
            .expect("fill SSE channel");
        let send_session = session.clone();
        let mut retired = RetiredSseDrain::default();
        let mut send = tokio::spawn(async move {
            send_history_sse(
                &tx,
                Ok(keepalive_event()),
                Duration::ZERO,
                "t",
                "r",
                &send_session,
                &mut retired,
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut send)
                .await
                .is_err(),
            "healthy actor must not trigger an arbitrary zero-timeout"
        );
        assert!(rx.recv().await.unwrap().is_ok());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), send)
                .await
                .expect("send completes after capacity frees")
                .expect("task"),
            Ok(())
        );
        assert!(rx.recv().await.unwrap().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_prompt_sse_send_cancels_turn_and_aborts_frontend_call() {
        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
            Box::pin(crate::test_agents::run_cancel_aware_slow_agent(stream))
        }));
        let state = BridgeAppState::builder(client, PathBuf::from("/"))
            .with_config(BridgeConfig {
                event_buffer: 1,
                slow_consumer_timeout: Duration::from_millis(10),
                ..BridgeConfig::default()
            })
            .build();
        let session = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("slow-consumer"))
                .await
                .expect("session opens"),
        );
        let (prompt_stream, turn_id) = session
            .prompt_with_turn("slow consumer test")
            .await
            .expect("prompt opens");
        let entry = Arc::new(SessionEntry::new(session.clone(), None));
        let registry_entry = state.inner.frontend_tools.entry("slow-consumer");
        let pending = registry_entry.register_pending("pending-tool".into());
        let frontend_stream = install_frontend_sender(&registry_entry, 1);
        let prompt_guard = entry.enter_prompt();
        let run_guard = state
            .try_claim_run("slow-consumer", "slow-run")
            .expect("run admission");

        // Do not poll the returned SSE stream. RUN_STARTED fills its internal
        // one-slot channel; the next SessionInit send must hit the timeout.
        let _stream = build_event_stream(
            "slow-consumer".into(),
            "slow-run".into(),
            prompt_stream,
            EventStreamContext {
                session,
                state: state.clone(),
                registry_entry,
                turn_id,
                frontend_stream,
            },
            1,
            prompt_guard,
            run_guard,
        );

        let response = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("slow consumer must tear down the stream")
            .expect("pending frontend call response");
        assert!(response.is_error);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while entry.active_prompts() != 0 && std::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            entry.active_prompts(),
            0,
            "timed out waiting for detached stream task to drop PromptGuard"
        );
        assert!(
            !entry.handle.is_unusable(),
            "slow consumer cleanup must cancel the turn, not unconditionally kill a healthy session"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retired_stalled_prompt_releases_claim_after_drain_grace() {
        let (session, disconnect, agent_ready) = disconnectable_test_session().await;
        tokio::time::timeout(Duration::from_secs(1), agent_ready)
            .await
            .expect("agent runner starts")
            .expect("ready signal");
        let (actual_prompt, turn_id) = session
            .prompt_with_turn("stalled retired stream")
            .await
            .expect("prompt starts");
        let PromptStream {
            events: actual_events,
            finished,
        } = actual_prompt;

        let state =
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .with_config(BridgeConfig {
                    event_buffer: 1,
                    slow_consumer_timeout: Duration::ZERO,
                    ..BridgeConfig::default()
                })
                .build();
        let entry = Arc::new(SessionEntry::new(session.clone(), None));
        let registry_entry = state.inner.frontend_tools.entry("retired-stall");
        let frontend_stream = install_frontend_sender(&registry_entry, 1);
        let prompt_guard = entry.enter_prompt();
        let run_guard = state
            .try_claim_run("retired-stall", "stalled-run")
            .expect("run claim acquired");
        assert_eq!(entry.active_prompts(), 1);
        assert!(state.try_claim_run("retired-stall", "overlap").is_none());

        let (events_tx, events) = mpsc::channel(1);
        let _stream = build_event_stream(
            "retired-stall".into(),
            "stalled-run".into(),
            PromptStream { events, finished },
            EventStreamContext {
                session: session.clone(),
                state: state.clone(),
                registry_entry,
                turn_id,
                frontend_stream,
            },
            1,
            prompt_guard,
            run_guard,
        );

        events_tx
            .send(BridgeStreamItem::SessionInit {
                modes: None,
                models: None,
                config_options: None,
            })
            .await
            .expect("queue first translated event");
        let queued = tokio::time::timeout(Duration::from_secs(1), events_tx.reserve())
            .await
            .expect("stream task consumes first source event")
            .expect("source event channel remains open");
        queued.send(BridgeStreamItem::SessionInit {
            modes: None,
            models: None,
            config_options: None,
        });
        assert!(matches!(
            events_tx.try_send(BridgeStreamItem::SessionInit {
                modes: None,
                models: None,
                config_options: None,
            }),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        // The internal output capacity is one: RUN_STARTED occupies it before
        // the source channel is consumed. The queued source event plus Full
        // result above prove the forwarding task is stalled behind that slot.
        tokio::task::yield_now().await;
        assert_eq!(entry.active_prompts(), 1);
        assert!(state.try_claim_run("retired-stall", "overlap").is_none());

        disconnect.notify_one();
        tokio::time::timeout(Duration::from_secs(2), session.closed())
            .await
            .expect("real ACP actor observes fixture EOF");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), async {
                loop {
                    if let Some(claim) = state.try_claim_run("retired-stall", "early") {
                        return claim;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "claim must remain held during retired-stream drain grace"
        );

        let claim = tokio::time::timeout(RETIRED_SSE_DRAIN_GRACE + Duration::from_secs(1), async {
            loop {
                if let Some(claim) = state.try_claim_run("retired-stall", "after-retirement") {
                    return claim;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired stalled stream releases same-thread run claim");
        assert_eq!(
            entry.active_prompts(),
            0,
            "PromptGuard released on actor retirement"
        );
        drop(claim);
        drop(_stream);
        drop(actual_events);
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_remove_replacement_session() {
        let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        let old_handle = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("race"))
                .await
                .expect("old session opens"),
        );
        let replacement_handle = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("race"))
                .await
                .expect("replacement session opens"),
        );
        let stale_entry = Arc::new(SessionEntry::new(old_handle, None));
        let replacement_entry = Arc::new(SessionEntry::new(replacement_handle, None));
        state
            .inner
            .sessions
            .insert("race".to_string(), replacement_entry.clone());

        assert!(!remove_session_if_same(&state.inner, "race", &stale_entry));
        let current = state
            .inner
            .sessions
            .get("race")
            .expect("replacement must remain cached")
            .clone();
        assert!(Arc::ptr_eq(&current, &replacement_entry));

        assert!(remove_session_if_same(
            &state.inner,
            "race",
            &replacement_entry
        ));
        assert!(!state.inner.sessions.contains_key("race"));
    }

    /// Agent whose `session/list` counts calls and returns a fixed summary.
    struct ListCountingClient {
        lists: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl AcpClient for ListCountingClient {
        async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
            InProcessAcpClient::new().open_session(cfg).await
        }

        async fn list_sessions(
            &self,
            _cfg: SessionConfig,
        ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, BridgeError> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(vec![agui_acp_bridge_core::SessionSummary {
                session_id: "listed-session".into(),
                cwd: "/".into(),
                title: None,
                updated_at: None,
            }])
        }
    }

    /// FIX 2 regression: N concurrent `GET /sessions` must collapse into ONE
    /// `session/list` spawn (each spawn forks an agent subprocess).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_session_listings_collapse_into_one_spawn() {
        let client = Arc::new(ListCountingClient {
            lists: std::sync::atomic::AtomicUsize::new(0),
        });
        let state = BridgeAppState::new(client.clone(), PathBuf::from("/"));

        let mut tasks = Vec::new();
        for _ in 0..25 {
            let state = state.clone();
            tasks.push(tokio::spawn(async move { state.list_sessions().await }));
        }
        for task in tasks {
            task.await.expect("list task").expect("listing succeeds");
        }
        assert_eq!(
            client.lists.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "25 concurrent listings must produce exactly one agent query"
        );

        // Errors are NOT cached: an Unsupported agent keeps failing (and the
        // gate still bounds concurrency), so callers see the real error.
        let unsupported =
            BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
        assert!(unsupported.list_sessions().await.is_err());
        assert!(unsupported.list_sessions().await.is_err());
    }

    /// Resume validation may list before capacity admission, but the listing
    /// remains globally single-flight and a rejected request cannot evict a
    /// busy session or open an actor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_validates_before_capacity_rejection_without_open_or_eviction() {
        // Client that counts every `session/list`: the resume path uses it.
        let client = Arc::new(ListCountingClient {
            lists: std::sync::atomic::AtomicUsize::new(0),
        });
        let state = BridgeAppState::builder(client.clone(), PathBuf::from("/"))
            .with_config(BridgeConfig {
                max_sessions: 1,
                ..BridgeConfig::default()
            })
            .build();

        // Fill the single capacity slot out-of-band.
        let handle = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("filler"))
                .await
                .expect("filler session opens"),
        );
        let permit = state
            .reserve_session_capacity()
            .await
            .expect("capacity is free")
            .expect("one permit available");
        let s1 = Arc::new(SessionEntry::new(handle, Some(permit)));
        state.inner.sessions.insert("filler".into(), s1.clone());
        // Mark the slot busy so LRU eviction cannot free it: only then does a
        // second resume hit the hard capacity wall instead of evicting.
        let _prompt_guard = s1.enter_prompt();

        // The valid marker is checked first; the full busy pool then rejects
        // admission without an open or eviction.
        let error = state
            .session_for_resume(
                "resume-thread",
                Some(SessionId::from("listed-session".to_string())),
            )
            .await
            .expect_err("capacity is exhausted");
        assert!(
            matches!(error, SessionAdmissionError::Capacity(_)),
            "expected capacity rejection, got: {error:?}"
        );
        assert_eq!(
            client.lists.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "resume validation should perform one bounded listing"
        );
        assert_eq!(state.session_count(), 1, "busy session must remain cached");
        assert!(state.inner.sessions.contains_key("filler"));
    }

    /// #4 regression: when a free permit exists, capacity reservation must
    /// NOT evict an idle session — the two-step fast path takes the permit
    /// before any LRU victim selection runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_fast_path_never_evicts_an_idle_session() {
        let state =
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .with_config(BridgeConfig {
                    max_sessions: 2,
                    ..BridgeConfig::default()
                })
                .build();

        // Park one idle (unclaimed) session.
        let handle = state
            .inner
            .client
            .open_session(state.session_config_for("idle-victim"))
            .await
            .expect("idle victim opens");
        state.inner.sessions.insert(
            "idle-victim".into(),
            Arc::new(SessionEntry::new(Arc::new(handle), None)),
        );

        // The pool has a free slot: acquiring it must leave the idle session
        // cached, not evict it to make room.
        let permit = state
            .reserve_session_capacity()
            .await
            .expect("a free permit exists")
            .expect("one permit available");
        drop(permit);
        assert_eq!(state.session_count(), 1, "fast path must not evict");
        assert!(
            state.inner.sessions.contains_key("idle-victim"),
            "an idle cached session must survive a free-permit acquisition"
        );
    }

    /// FIX 6 regression: `resolve_permission` only resolves within the
    /// supplied thread. Drives the real in-process permission agent so a
    /// genuine pending interrupt exists on one thread, then proves a
    /// resolution attempt from another thread leaves it untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn approval_resolution_is_thread_scoped() {
        use agui_acp_bridge_policy::InterruptViaAgUiEvent;

        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
            Box::pin(crate::test_agents::run_request_permission_agent(stream))
        }));
        let state = BridgeAppState::builder(client, PathBuf::from("/"))
            .with_policy(Arc::new(InterruptViaAgUiEvent))
            .build();

        // A real prompt defers a permission request into the live session.
        let session = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("owner-thread"))
                .await
                .expect("session opens"),
        );
        let entry = Arc::new(SessionEntry::new(session.clone(), None));
        state.inner.sessions.insert("owner-thread".into(), entry);
        let (_prompt_stream, _turn) = session
            .prompt_with_turn("request a permission")
            .await
            .expect("prompt opens");
        // The policy mints a uuid interrupt id; grab whatever is pending.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let pending_id = session
            .pending_permissions()
            .iter()
            .next()
            .map(|entry| entry.key().clone())
            .expect("agent must park a pending permission");

        // Wrong thread: must NOT consume the interrupt…
        let outcome = state.resolve_permission(
            "other-thread",
            &pending_id,
            agui_acp_bridge_core::PermissionDecision::Deny,
        );
        assert_eq!(outcome, ResolveOutcome::NotFound);
        assert!(
            session.pending_permissions().contains_key(&pending_id),
            "cross-thread approval must not consume another thread's pending permission"
        );

        // Right thread: resolves.
        let outcome = state.resolve_permission(
            "owner-thread",
            &pending_id,
            agui_acp_bridge_core::PermissionDecision::Deny,
        );
        assert_eq!(outcome, ResolveOutcome::Resolved);
        assert!(!session.pending_permissions().contains_key(&pending_id));
    }

    /// Keepalive regression test: an idle stream must emit keepalives at
    /// roughly the configured cadence AND must not flood — one frame per
    /// interval, re-armed by every tick, over a window of several intervals.
    /// Uses the ~50ms test interval so the whole run is well under a second.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_event_stream_emits_keepalives_at_cadence_without_flooding() {
        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
            Box::pin(crate::test_agents::run_long_running_agent(stream))
        }));
        let state = BridgeAppState::builder(client, PathBuf::from("/"))
            .with_config(BridgeConfig {
                slow_consumer_timeout: Duration::from_secs(10),
                ..BridgeConfig::default()
            })
            .build();
        let session = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("keepalive"))
                .await
                .expect("session opens"),
        );
        let (prompt_stream, turn_id) = session
            .prompt_with_turn("never finishes soon")
            .await
            .expect("prompt opens");
        let entry = Arc::new(SessionEntry::new(session.clone(), None));
        let registry_entry = state.inner.frontend_tools.entry("keepalive");
        let frontend_stream = install_frontend_sender(&registry_entry, 16);
        let prompt_guard = entry.enter_prompt();
        let run_guard = state.try_claim_run("keepalive", "run-ka").expect("claim");

        let interval = keepalive_interval_for_tests();
        let mut stream = build_event_stream_with_keepalive(
            "keepalive".into(),
            "run-ka".into(),
            prompt_stream,
            EventStreamContext {
                session,
                state: state.clone(),
                registry_entry,
                turn_id,
                frontend_stream,
            },
            16,
            prompt_guard,
            run_guard,
            interval,
        );

        // Observe keepalives over an idle window of ~4 intervals. Cadence:
        // an idle turn emits one frame per interval (± one tick of jitter).
        let window = interval * 4;
        let mut keepalive_count = 0usize;
        let _first_keepalive = tokio::time::timeout(window, async {
            loop {
                let event = tokio::time::timeout(interval * 3, stream.next())
                    .await
                    .expect("idle stream must produce events")
                    .expect("event decodes")
                    .expect("keepalive probe event");
                if let Event::Custom(custom) = event
                    && custom.name == "agent:keepalive"
                {
                    return custom;
                }
            }
        })
        .await
        .expect("first keepalive within the window");
        keepalive_count += 1;

        // Flood check: after the first tick, count keepalives for another
        // ~4 intervals. A re-armed deadline yields ≈1 per interval; the #2
        // bug (missing re-arm) yields an unbounded flood instead.
        let flood_window_end = tokio::time::Instant::now() + interval * 4;
        while tokio::time::Instant::now() < flood_window_end {
            let event = tokio::time::timeout(interval * 3, stream.next())
                .await
                .expect("idle stream must keep producing events")
                .expect("event decodes")
                .expect("flood-probe event");
            if let Event::Custom(custom) = event
                && custom.name == "agent:keepalive"
            {
                keepalive_count += 1;
            }
        }
        assert!(
            keepalive_count <= 8,
            "idle stream emitted {keepalive_count} keepalives over ~8 intervals; \
             >2 per interval means the deadline stopped re-arming (flood)"
        );
        assert!(
            keepalive_count >= 3,
            "idle stream emitted only {keepalive_count} keepalives over ~8 intervals; \
             cadence regressed below one per two intervals"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn silent_suppressed_tool_progress_does_not_postpone_keepalives() {
        let interval = keepalive_interval_for_tests();
        let update_interval = interval / 4;
        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
            Box::pin(run_silent_tool_progress_agent(stream, update_interval, 32))
        }));
        let state = BridgeAppState::builder(client, PathBuf::from("/"))
            .with_config(BridgeConfig {
                slow_consumer_timeout: Duration::from_secs(5),
                ..BridgeConfig::default()
            })
            .build();
        let session = Arc::new(
            state
                .inner
                .client
                .open_session(state.session_config_for("silent-progress"))
                .await
                .expect("session opens"),
        );
        let (prompt_stream, turn_id) = session
            .prompt_with_turn("silent progress")
            .await
            .expect("prompt opens");
        let entry = Arc::new(SessionEntry::new(session.clone(), None));
        let registry_entry = state.inner.frontend_tools.entry("silent-progress");
        registry_entry.set_tools(vec![FrontendToolDef {
            name: "silent_tool".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        let frontend_stream = install_frontend_sender(&registry_entry, 16);
        let prompt_guard = entry.enter_prompt();
        let run_guard = state
            .try_claim_run("silent-progress", "run-silent")
            .expect("claim run");
        let mut stream = build_event_stream_with_keepalive(
            "silent-progress".into(),
            "run-silent".into(),
            prompt_stream,
            EventStreamContext {
                session,
                state,
                registry_entry,
                turn_id,
                frontend_stream,
            },
            16,
            prompt_guard,
            run_guard,
            interval,
        );

        let mut keepalives = 0;
        tokio::time::timeout(Duration::from_secs(4), async {
            while let Some(event) = stream.next().await {
                let event = event.expect("event stream remains healthy");
                match event {
                    Event::Custom(custom) if custom.name == "agent:keepalive" => {
                        keepalives += 1;
                    }
                    Event::Custom(custom) if custom.name == "agent:session_init" => {}
                    Event::RunStarted(_) => {}
                    Event::RunFinished(_) => break,
                    other => panic!("suppressed progress unexpectedly emitted {other:?}"),
                }
            }
        })
        .await
        .expect("silent progress prompt completes on time");
        assert!(
            keepalives >= 3,
            "continuous suppressed updates postponed keepalives: saw {keepalives}"
        );
        assert!(
            keepalives <= 10,
            "keepalives flooded during suppressed updates: saw {keepalives}"
        );
    }

    async fn run_silent_tool_progress_agent(
        stream: tokio::io::DuplexStream,
        interval: Duration,
        updates: usize,
    ) -> Result<(), BridgeError> {
        use agent_client_protocol::schema::v1::{
            AgentCapabilities, InitializeRequest, InitializeResponse, NewSessionRequest,
            NewSessionResponse, PromptRequest, PromptResponse, SessionNotification, SessionUpdate,
            StopReason, ToolCall, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
        };
        use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

        let (read, write) = tokio::io::split(stream);
        let transport =
            agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
        agent_client_protocol::Agent
            .builder()
            .name("silent-suppressed-tool-progress-test")
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
                    responder.respond(NewSessionResponse::new(SessionId::from("silent-progress")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |req: PromptRequest,
                            responder,
                            cx: agent_client_protocol::ConnectionTo<
                    agent_client_protocol::Client,
                >| {
                    let session_id = req.session_id;
                    cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCall(ToolCall::new(
                            ToolCallId::new("silent-call"),
                            "agui-acp-bridge_silent_tool",
                        )),
                    ))?;
                    for _ in 0..updates {
                        cx.send_notification(SessionNotification::new(
                            session_id.clone(),
                            SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                                ToolCallId::new("silent-call"),
                                ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
                            )),
                        ))?;
                        tokio::time::sleep(interval).await;
                    }
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: agent_client_protocol::ConnectionTo<
                    agent_client_protocol::Client,
                >| {
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

    // ponytail: the wire shape stays pinned so a refactor cannot silently
    // emit an unparseable keepalive frame; cadence itself is covered by the
    // test above.
    #[test]
    fn keepalive_frame_is_a_null_value_custom_event() {
        let frame = agui_rs_encoder_ish();
        assert!(frame.contains("agent:keepalive"));
        assert!(frame.contains("\"value\":null"));
    }

    fn agui_rs_encoder_ish() -> String {
        let event = keepalive_event();
        serde_json::to_string(&event).expect("keepalive serializes")
    }

    /// `resume_cwd` must validate containment against the CANONICAL path but
    /// hand the agent back its OWN spelling.
    ///
    /// The symlinked prefix is built explicitly so this bites on Linux too:
    /// `/tmp` is not a symlink there, so the macOS bug this guards
    /// (`/var` -> `/private/var`, which every `std::env::temp_dir()` path
    /// traverses) was invisible to CI and only reproduced on developer
    /// machines — where it failed every resume under a temp cwd.
    #[test]
    #[cfg(unix)]
    fn resume_cwd_validates_canonically_but_returns_the_agent_spelling() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("agui-resume-cwd-{unique}"));
        let real = base.join("real");
        let nested = real.join("nested/project");
        std::fs::create_dir_all(&nested).expect("create nested cwd");
        let link = base.join("link");
        std::os::unix::fs::symlink(&real, &link).expect("symlinked prefix");

        let canonical_root = std::fs::canonicalize(&real).expect("canonical root");

        // The agent persisted its cwd spelled through the symlink.
        let reported = link.join("nested/project");
        let chosen = resume_cwd(&reported, &canonical_root).expect("nested cwd is allowed");
        assert_eq!(
            chosen, reported,
            "must hand back the spelling the agent persisted"
        );
        assert_ne!(
            chosen,
            std::fs::canonicalize(&reported).expect("canonical nested"),
            "returning the canonical spelling is the bug this test guards"
        );

        // Containment still resolves symlinks: a link inside the root that
        // points outside it must be rejected, not laundered.
        let escape = link.join("escape");
        std::os::unix::fs::symlink(std::env::temp_dir(), &escape).expect("escape symlink");
        assert!(
            resume_cwd(&escape, &canonical_root).is_err(),
            "a symlink escaping the bridge root must be rejected"
        );

        // Plain outside-root and relative paths stay rejected.
        assert!(resume_cwd(&nested, Path::new("/definitely/not/the/root")).is_err());
        assert!(resume_cwd(Path::new("relative/path"), &canonical_root).is_err());

        std::fs::remove_dir_all(&base).ok();
    }
}
