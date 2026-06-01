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
//! 5. Reap idle sessions: a background task drops sessions whose
//!    `last_used` timestamp is older than `BridgeConfig.idle_timeout`.
//!
//! Concurrency: distinct `thread_id`s are independent; concurrent runs on the
//! **same** `thread_id` will queue inside the underlying ACP session actor
//! (which processes prompts sequentially). The DashMap protects the
//! lazy-creation race.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use agui_rs_core::events::{Event, factory};
use agui_rs_core::types::{Message, RunAgentInput, UserMessageContent};
use agui_rs_server::error::{AgUiError, Result as AgUiResult};
use agui_rs_server::handler::RunHandler;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::{self, BoxStream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

use agui_acp_bridge_core::acp::{
    AcpClient, AcpSessionHandle, PromptStream, SessionConfig, SessionInitState,
};
use agui_acp_bridge_core::frontend_tools::{
    FrontendToolDef, FrontendToolRegistry, FrontendToolResponse,
};
use agui_acp_bridge_core::policy::PermissionPolicy;
use agui_acp_bridge_core::translation::{Translator, session_init_event};
use agui_acp_bridge_core::{BridgeConfig, BridgeError, BridgeStreamItem, canonicalize_cwd};
use agui_acp_bridge_policy::AutoAllow;

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

/// Outcome of [`BridgeAppState::set_session_mode`] /
/// [`BridgeAppState::set_session_model`]. The HTTP route maps these to the
/// status codes documented on the route itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetSessionStatus {
    /// No session for the supplied thread id (caller must create one first
    /// by issuing a normal AG-UI run).
    NotFound,
    /// Agent rejected the request. Typically because the supplied `mode_id`
    /// / `model_id` is not in the corresponding `available*` list.
    Acp(String),
    /// Agent did not respond within `BridgeConfig.set_session_timeout`.
    /// The session is left intact and the caller can retry.
    Timeout,
    /// Underlying session actor terminated; cache entry is evicted.
    SessionClosed,
}

/// Internal session record holding the handle plus a last-used timestamp.
#[derive(Debug)]
struct SessionEntry {
    handle: Arc<AcpSessionHandle>,
    last_used: parking_lot_like::Mutex<Instant>,
    /// Number of in-flight prompts on this session. The reaper refuses to
    /// drop entries with `active_prompts > 0` even if their `last_used` is
    /// stale: a long-running prompt would otherwise be killed mid-flight.
    active_prompts: std::sync::atomic::AtomicUsize,
}

impl SessionEntry {
    fn new(handle: Arc<AcpSessionHandle>) -> Self {
        Self {
            handle,
            last_used: parking_lot_like::Mutex::new(Instant::now()),
            active_prompts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn touch(&self) {
        *self.last_used.lock() = Instant::now();
    }

    fn last_used(&self) -> Instant {
        *self.last_used.lock()
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

/// Tiny wrapper module so we don't pull in `parking_lot`. `std::sync::Mutex`
/// is used; the wrapper just gives us `.lock()` returning the guard directly
/// (panicking on poison) so the call sites stay readable.
mod parking_lot_like {
    use std::sync::{Mutex as StdMutex, MutexGuard};

    #[derive(Debug)]
    pub struct Mutex<T>(StdMutex<T>);

    impl<T> Mutex<T> {
        pub fn new(t: T) -> Self {
            Self(StdMutex::new(t))
        }

        #[track_caller]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            self.0.lock().expect("session entry mutex poisoned")
        }
    }
}

/// Shared bridge state: ACP client factory + session map + cwd for new sessions
/// + bridge configuration + permission policy.
///
/// Constructed via [`BridgeAppState::new`] (uses [`AutoAllow`] +
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
    /// Per-thread async locks for the lazy session-creation critical section.
    /// Concurrent `session_for("x")` calls hold the same `Mutex<()>`, so the
    /// expensive `open_session` happens exactly once per thread id even
    /// under high request fan-in for the same thread.
    create_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
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
    /// Handle to the background reaper task; aborted when `Inner` drops so
    /// graceful shutdown doesn't leak a tokio worker.
    reaper: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(h) = self.reaper.lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
    }
}

impl BridgeAppState {
    /// Construct with default `BridgeConfig` and an `AutoAllow` policy.
    /// Suitable for development; production should use [`BridgeAppState::builder`].
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
        Self {
            inner: Arc::new(Inner {
                sessions: DashMap::new(),
                create_locks: DashMap::new(),
                client,
                cwd,
                config: BridgeConfig::default(),
                policy: Arc::new(AutoAllow),
                frontend_tools: FrontendToolRegistry::new(),
                self_url: None,
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
            policy: Arc::new(AutoAllow),
            self_url: None,
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

    /// List persisted sessions via ACP `session/list`.
    ///
    /// Stateless pass-through: opens a short-lived ACP connection, queries
    /// the agent, and returns its summaries. Returns
    /// [`BridgeError::Unsupported`] when the agent does not advertise the
    /// `session/list` capability. The HTTP layer maps that to `501`.
    pub async fn list_sessions(
        &self,
    ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, BridgeError> {
        // Use a synthetic thread token for the transient connection's
        // (unused) MCP URL slot — listing issues no prompts, so no MCP
        // endpoint is needed.
        let cfg = self.session_config_for("__list__");
        tokio::time::timeout(
            self.inner.config.open_session_timeout,
            self.inner.client.list_sessions(cfg),
        )
        .await
        .map_err(|_| BridgeError::Timeout(self.inner.config.open_session_timeout))?
    }

    /// Resolve a deferred permission request for any cached session.
    ///
    /// Looks up the pending interrupt id across every live session and, if
    /// found, delivers the decision after validating it against the
    /// agent-advertised option set (see [`AcpSessionHandle::resolve_permission`]
    /// for details). Returns:
    /// - `ResolveOutcome::Resolved` — the decision was accepted and delivered.
    /// - `ResolveOutcome::InvalidOption` — an `Allow` decision named an
    ///   `option_id` the agent did not offer; the entry remains pending.
    /// - `ResolveOutcome::NotFound` — no session has a pending permission
    ///   with that id (already resolved, timed out, or never existed).
    #[must_use]
    pub fn resolve_permission(
        &self,
        interrupt_id: &str,
        decision: agui_acp_bridge_core::PermissionDecision,
    ) -> ResolveOutcome {
        // Quick check: which session (if any) has the entry, and is the
        // option valid? We do this without consuming the entry first, so an
        // `InvalidOption` outcome leaves the request retryable.
        for entry in self.inner.sessions.iter() {
            let pending = entry.value().handle.pending_permissions();
            let Some(record) = pending.get(interrupt_id) else {
                continue;
            };
            if let agui_acp_bridge_core::PermissionDecision::Allow { ref option_id } = decision {
                if !record.allows_option(option_id.0.as_ref()) {
                    return ResolveOutcome::InvalidOption;
                }
            }
            // Drop the read-guard before calling resolve (which takes a
            // write-guard via DashMap::remove) to avoid deadlock.
            drop(record);
            if entry
                .value()
                .handle
                .resolve_permission(interrupt_id, decision)
            {
                return ResolveOutcome::Resolved;
            }
            // Lost a race against another resolver — fall through to keep
            // scanning, though in practice the entry is now gone.
            return ResolveOutcome::NotFound;
        }
        ResolveOutcome::NotFound
    }

    /// Snapshot the cached `SessionInitState` for an existing thread, or
    /// `None` if no session has been opened for it yet. Powers the
    /// `GET /session/init` discovery endpoint.
    #[must_use]
    pub fn session_init_state(&self, thread_id: &str) -> Option<SessionInitState> {
        self.inner
            .sessions
            .get(thread_id)
            .map(|e| e.handle.init_state())
    }

    /// Send `session/set_mode` to the session bound to `thread_id`.
    ///
    /// Returns:
    /// - `Ok(())` — agent accepted the new mode.
    /// - `Err(SetSessionStatus::NotFound)` — no session exists for that thread.
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
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        match tokio::time::timeout(timeout, entry.handle.set_mode(mode_id)).await {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                self.inner.sessions.remove(thread_id);
                self.inner.frontend_tools.drop_thread(thread_id);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Send `session/set_model` to the session bound to `thread_id`.
    ///
    /// See [`BridgeAppState::set_session_mode`] for the error semantics.
    /// Only available with the `unstable_session_model` feature (default-on).
    #[cfg(feature = "unstable_session_model")]
    pub async fn set_session_model(
        &self,
        thread_id: &str,
        model_id: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        match tokio::time::timeout(timeout, entry.handle.set_model(model_id)).await {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                self.inner.sessions.remove(thread_id);
                self.inner.frontend_tools.drop_thread(thread_id);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Spawn the background idle-session reaper.
    ///
    /// The reaper wakes every `idle_timeout / 4` (capped between 1s and 30s)
    /// and removes any session whose `last_used` instant is older than
    /// `idle_timeout`. Dropping the entry drops the `Arc<AcpSessionHandle>`,
    /// which terminates the actor task and (for subprocess clients) kills
    /// the child.
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
                let mut to_drop = Vec::new();
                for entry in inner.sessions.iter() {
                    if entry.value().active_prompts() > 0 {
                        // A prompt is in flight; never reap it. Long-running
                        // turns must survive idle-timeout windows shorter
                        // than the turn duration.
                        continue;
                    }
                    let last = entry.value().last_used();
                    if now.saturating_duration_since(last) >= idle {
                        to_drop.push(entry.key().clone());
                    }
                }
                for key in to_drop {
                    // remove_if avoids the iter→remove race: another caller
                    // may have touched the entry between our scan and now,
                    // in which case we leave it alone for the next tick.
                    let removed = inner.sessions.remove_if(&key, |_, v| {
                        v.active_prompts() == 0
                            && now.saturating_duration_since(v.last_used()) >= idle
                    });
                    if removed.is_some() {
                        // Also drop the matching frontend-tools registry
                        // entry so its tool list, pending oneshots, and
                        // reverse-index rows don't accumulate for the life
                        // of the process every time a thread is reaped.
                        inner.frontend_tools.drop_thread(&key);
                        tracing::info!(thread_id = %key, "reaping idle ACP session");
                    }
                }
                drop(inner);
            }
        });
        if let Ok(mut slot) = self.inner.reaper.lock() {
            if let Some(prev) = slot.replace(handle) {
                prev.abort();
            }
        }
    }

    /// Frontend-tool registry. Used by the MCP HTTP endpoint to read the
    /// per-thread tool list, register pending calls, and route results
    /// back into the live SSE stream.
    pub fn frontend_tools(&self) -> &FrontendToolRegistry {
        &self.inner.frontend_tools
    }

    /// Resolve a frontend tool call posted back from the browser.
    /// Returns `true` if a pending entry existed and was consumed.
    pub fn resolve_frontend_tool(
        &self,
        tool_call_id: &str,
        response: FrontendToolResponse,
    ) -> bool {
        self.inner
            .frontend_tools
            .resolve_anywhere(tool_call_id, response)
    }

    fn session_config_for(&self, thread_token: &str) -> SessionConfig {
        self.session_config_for_with(thread_token, None)
    }

    fn session_config_for_with(
        &self,
        thread_token: &str,
        load_session_id: Option<String>,
    ) -> SessionConfig {
        let mcp_url = self
            .inner
            .self_url
            .as_ref()
            .map(|base| format!("{base}/mcp/{thread_token}"));
        SessionConfig {
            cwd: self.inner.cwd.clone(),
            policy: self.inner.policy.clone(),
            config: self.inner.config.clone(),
            mcp_url,
            load_session_id,
        }
    }

    /// Evict the least-recently-used **idle** session(s) so that inserting
    /// one more entry stays within `config.max_sessions`. A `max_sessions`
    /// of `0` disables the cap entirely.
    ///
    /// "Idle" means `active_prompts == 0`: a session with an in-flight prompt
    /// is never evicted, so live work is never killed. If the cap is reached
    /// but every cached session is busy, we evict nothing and let the new
    /// session push the map one over the cap — preferable to dropping a
    /// session mid-turn. The next call (once something goes idle) brings the
    /// map back under the cap.
    ///
    /// Dropping the `SessionEntry` drops its `Arc<AcpSessionHandle>`, which
    /// terminates the actor (and kills the subprocess for process clients).
    /// The matching frontend-tools registry entry is dropped too.
    fn evict_for_capacity(&self) {
        let cap = self.inner.config.max_sessions;
        if cap == 0 {
            return;
        }
        // We are about to insert one entry, so make room until len < cap.
        while self.inner.sessions.len() >= cap {
            // Find the LRU idle entry.
            let mut victim: Option<(String, Instant)> = None;
            for e in self.inner.sessions.iter() {
                if e.value().active_prompts() > 0 {
                    continue;
                }
                let last = e.value().last_used();
                match &victim {
                    Some((_, best)) if *best <= last => {}
                    _ => victim = Some((e.key().clone(), last)),
                }
            }
            let Some((key, last)) = victim else {
                // Every cached session is busy; don't kill live work.
                tracing::warn!(
                    max_sessions = cap,
                    cached = self.inner.sessions.len(),
                    "session cap reached but all sessions are busy; allowing overflow"
                );
                return;
            };
            // remove_if guards against a race where the victim became busy
            // or was touched between selection and removal.
            let removed = self.inner.sessions.remove_if(&key, |_, v| {
                v.active_prompts() == 0 && v.last_used() == last
            });
            if removed.is_some() {
                self.inner.frontend_tools.drop_thread(&key);
                tracing::info!(thread_id = %key, "evicting LRU idle session to honour max_sessions");
            } else {
                // Lost the race; re-scan on the next loop iteration. Guard
                // against a spin if the map didn't shrink.
                if self.inner.sessions.len() >= cap {
                    return;
                }
            }
        }
    }

    async fn session_for(&self, thread_id: &str) -> Result<Arc<SessionEntry>, AgUiError> {
        self.session_for_resume(thread_id, None).await
    }

    /// Like [`session_for`] but, on a cache miss, opens the session by
    /// **loading** the existing ACP session named by `resume` (replaying its
    /// history) instead of creating a fresh one. When `resume` is `None`,
    /// or the agent doesn't support `loadSession`, a new session is created.
    async fn session_for_resume(
        &self,
        thread_id: &str,
        resume: Option<String>,
    ) -> Result<Arc<SessionEntry>, AgUiError> {
        // Fast path: already cached.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            existing.touch();
            return Ok(existing.clone());
        }

        // Slow path: serialise concurrent first-time creators on the same
        // thread id behind a per-key async mutex. The mutex is allocated
        // lazily (one Arc per active id). All waiters get the same Arc;
        // a third caller arriving while the second still holds the guard
        // queues behind it because the entry is still in `create_locks`.
        let lock = self
            .inner
            .create_locks
            .entry(thread_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        // Re-check inside the critical section: another waiter may have
        // already populated the entry while we were queued for the lock.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            existing.touch();
            return Ok(existing.clone());
        }

        let handle_result = tokio::time::timeout(
            self.inner.config.open_session_timeout,
            self.inner
                .client
                .open_session(self.session_config_for_with(thread_id, resume)),
        )
        .await;

        // Whether creation succeeded or failed, free the lock entry. We
        // explicitly drop the guard first so the lock entry's strong count
        // is just the one we hold, and remove the entry; any concurrent
        // waiter that already cloned the Arc still has a valid Mutex to
        // wait on (just no longer reachable through `create_locks`).
        let handle = match handle_result {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                drop(_guard);
                self.inner.create_locks.remove(thread_id);
                return Err(AgUiError::other(format!("acp open_session failed: {e}")));
            }
            Err(_) => {
                drop(_guard);
                self.inner.create_locks.remove(thread_id);
                return Err(AgUiError::other(format!(
                    "acp open_session timed out after {:?}",
                    self.inner.config.open_session_timeout
                )));
            }
        };
        let entry = Arc::new(SessionEntry::new(Arc::new(handle)));
        // Enforce the optional session cap by evicting the
        // least-recently-used *idle* session before inserting. This keeps
        // the number of live agent subprocesses bounded when clients churn
        // through many distinct thread_ids (e.g. a browser that mints a
        // fresh thread on every refresh). We never evict a session with an
        // in-flight prompt; if every cached session is busy we let the new
        // one through rather than killing live work.
        self.evict_for_capacity();
        self.inner
            .sessions
            .insert(thread_id.to_string(), entry.clone());
        drop(_guard);
        self.inner.create_locks.remove(thread_id);
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
pub struct BridgeHandler {
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
    ) -> AgUiResult<BoxStream<'static, AgUiResult<Event>>> {
        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();
        let drain = match session.drain_history().await {
            Ok(stream) => stream,
            Err(err) => {
                drop(prompt_guard);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(factory::run_error(format!(
                        "acp drain_history failed: {err}"
                    ))),
                ];
                return Ok(stream::iter(evs).boxed());
            }
        };
        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        Ok(build_history_stream(thread_id, run_id, drain, translated_buffer, prompt_guard).boxed())
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

        // Diagnostic: every AG-UI run that reaches the bridge. `msg_count`
        // and `tail` let operators see whether a click produced a bootstrap
        // (connect) run vs a prompt run, and on which thread.
        tracing::info!(
            thread_id = %thread_id,
            run_id = %run_id,
            msg_count = input.messages.len(),
            "AG-UI run received"
        );

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
        registry_entry.set_tools(new_tools);

        // Extract the prompt to forward (if any). The ACP protocol
        // semantics: only a `User` message at the **tail** of `messages[]`
        // represents a fresh turn. When the tail is an `assistant` /
        // `tool` / `activity` message, the AG-UI runtime is reposting
        // already-handled history — typically because CopilotKit-style
        // `agentic_chat` callers auto-fire a follow-up run after every
        // tool turn so the LLM sees the tool result. Re-prompting the
        // ACP agent on these follow-ups would replay the prior turn
        // against a session whose history already contains the reply,
        // producing the well-known "every run loops the previous turn"
        // pathology. We instead emit a clean noop run pair.
        let trailing = extract_trailing_user_text(&input.messages);

        // Decide whether to *resume* an existing ACP session (replaying its
        // history) rather than create a fresh one. Resume is detected purely
        // from the protocol shape — we do NOT rely on a client-set flag,
        // because CopilotKit's `connectAgent` (bootstrap) path does not
        // forward provider `properties` into `forwardedProps`.
        //
        // The signal: a **bootstrap run** (no fresh trailing user message)
        // for a `thread_id` the bridge has **no live session** for. That's
        // exactly what the frontend issues when it opens a past conversation:
        // it sets the agent's `threadId` to the conversation's ACP SessionId
        // and connects without prompting. We then `session/load` that id.
        //
        // A genuinely new conversation also bootstraps with no live session,
        // but its `threadId` won't exist in the agent — `session/load` fails
        // and the session layer falls back to `session/new` transparently, so
        // a stray load attempt is harmless. We additionally require the
        // thread_id to *look* like an agent SessionId (non-empty and not the
        // obvious fresh-uuid the client mints) only implicitly: the load
        // fallback makes a wrong guess a no-op.
        let no_live_session = !self.state.inner.sessions.contains_key(&thread_id);
        let is_bootstrap = !matches!(trailing, TrailingUser::Text(_));
        let wants_resume = no_live_session && is_bootstrap;

        let entry = if wants_resume {
            self.state
                .session_for_resume(&thread_id, Some(thread_id.clone()))
                .await?
        } else {
            self.state.session_for(&thread_id).await?
        };
        let user_text = match trailing {
            TrailingUser::Text(text) => text,
            TrailingUser::NonUserTail | TrailingUser::Empty | TrailingUser::NonText => {
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
                    return self.stream_resume_history(thread_id, run_id, entry).await;
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
                return Ok(stream::iter(evs).boxed());
            }
        };

        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();

        let prompt_result = session.prompt(user_text).await;
        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        let stream = match prompt_result {
            Ok(prompt_stream) => build_event_stream(
                thread_id,
                run_id,
                prompt_stream,
                session,
                translated_buffer,
                prompt_guard,
                registry_entry,
            )
            .boxed(),
            Err(err) => {
                // Session is dead: evict it from the cache so the next
                // request on this thread_id rebuilds a fresh session
                // instead of replaying SessionClosed forever (until
                // idle_timeout).
                if matches!(err, agui_acp_bridge_core::BridgeError::SessionClosed) {
                    self.state.inner.sessions.remove(&thread_id);
                    self.state.inner.frontend_tools.drop_thread(&thread_id);
                }
                drop(prompt_guard);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(factory::run_error(format!("acp prompt failed: {err}"))),
                ];
                stream::iter(evs).boxed()
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

fn build_event_stream(
    thread_id: String,
    run_id: String,
    prompt_stream: PromptStream,
    session: Arc<AcpSessionHandle>,
    translated_buffer: usize,
    prompt_guard: PromptGuard,
    registry_entry: Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
) -> BoxStream<'static, AgUiResult<Event>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);

    // The MCP endpoint needs a live `Sender<BridgeStreamItem>` to dispatch
    // tool-call events into the active prompt's actor channel. We bridge
    // the MCP endpoint and this stream by giving the registry an mpsc
    // sender; we forward FrontendToolCall items into translator output
    // here (in the same loop that handles ACP-side updates).
    let (mcp_tool_tx, mut mcp_tool_rx) =
        tokio::sync::mpsc::channel::<BridgeStreamItem>(translated_buffer);
    registry_entry.set_active_sender(Some(mcp_tool_tx.clone()));

    tokio::spawn(async move {
        // Hold the guard for the duration of the prompt so the reaper
        // sees `active_prompts > 0` and refuses to drop the session.
        let _prompt_guard = prompt_guard;
        // Clear the registry's active sender on exit so MCP requests that
        // arrive after the prompt finishes are rejected promptly instead
        // of silently parking forever. We clear *conditionally* — only if
        // the slot still holds the sender THIS run installed — so an
        // overlapping newer run on the same thread_id (page refresh, a
        // CopilotKit follow-up run, a reconnect) keeps its own sender and
        // its in-flight tool calls don't get stranded into a timeout.
        let _clear_on_drop = ClearOnDrop {
            entry: registry_entry.clone(),
            sender: mcp_tool_tx,
        };

        let _ = tx
            .send(Ok(factory::run_started(thread_id.clone(), run_id.clone())))
            .await;

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

        let session_for_stream = session;

        // Helper: when an SSE send fails the client has disconnected.
        // Cancel the in-flight ACP turn so the agent stops doing work
        // nobody is reading. The session actor's run_prompt_with_cancel
        // also detects the events channel being dropped, so this is a
        // belt-and-suspenders approach.
        let cancel_on_disconnect = |session: Arc<AcpSessionHandle>| {
            if let Err(e) = session.cancel() {
                tracing::warn!(error = %e, "failed to cancel session after client disconnect");
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
                acp = events.recv() => match acp {
                    Some(it) => it,
                    None => break,
                },
                mcp = mcp_tool_rx.recv() => match mcp {
                    Some(it) => it,
                    // mcp channel closing is fine — the registry entry
                    // will close it when the session is reaped.
                    None => continue,
                },
                // Detect client disconnect even while idle. When the SSE
                // consumer drops, `tx` closes. Without this branch the loop
                // would park on `events.recv()` / `mcp_tool_rx.recv()` and
                // only notice the dead client on the *next* event — which
                // never comes while the agent is parked awaiting a frontend
                // tool result. That would pin `active_prompts > 0` (so the
                // reaper can't release the session) until `frontend_tool_timeout`
                // fires — the root cause of sessions piling up after refreshes.
                () = tx.closed() => {
                    cancel_on_disconnect(session_for_stream.clone());
                    return;
                }
            };

            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if tx.send(Ok(ev)).await.is_err() {
                            cancel_on_disconnect(session_for_stream.clone());
                            return;
                        }
                    }
                }
                BridgeStreamItem::SessionInit { modes, models } => {
                    // Re-emitted by the session actor at the start of every
                    // prompt so reconnecting clients see the picker even on
                    // mid-thread runs. Always sent before any agent text.
                    let ev = session_init_event(modes.as_ref(), models.as_ref());
                    if tx.send(Ok(ev)).await.is_err() {
                        cancel_on_disconnect(session_for_stream.clone());
                        return;
                    }
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
                    if tx.send(Ok(event)).await.is_err() {
                        cancel_on_disconnect(session_for_stream.clone());
                        return;
                    }
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
                        if tx.send(Ok(ev)).await.is_err() {
                            cancel_on_disconnect(session_for_stream.clone());
                            return;
                        }
                    }
                }
                BridgeStreamItem::FrontendToolEnd { tool_call_id } => {
                    for ev in translator.translate_frontend_tool_end(&tool_call_id) {
                        if tx.send(Ok(ev)).await.is_err() {
                            cancel_on_disconnect(session_for_stream.clone());
                            return;
                        }
                    }
                }
            }
        }

        for ev in translator.flush() {
            if tx.send(Ok(ev)).await.is_err() {
                cancel_on_disconnect(session_for_stream.clone());
                return;
            }
        }

        if let Some(msg) = errored {
            let _ = tx.send(Ok(factory::run_error(msg))).await;
            return;
        }

        match finished.await {
            Ok(Ok(_stop_reason)) => {
                let _ = tx.send(Ok(factory::run_finished(thread_id, run_id))).await;
            }
            Ok(Err(e)) => {
                let _ = tx
                    .send(Ok(factory::run_error(format!("acp prompt errored: {e}"))))
                    .await;
            }
            Err(_) => {
                let _ = tx
                    .send(Ok(factory::run_error("acp session dropped before finish")))
                    .await;
            }
        }
    });

    ReceiverStream::new(rx).boxed()
}

/// Simplified event stream for a "resume bootstrap" run: emit `RUN_STARTED`,
/// translate the replayed history updates into AG-UI events, then
/// `RUN_FINISHED`. No agent prompt is issued; no frontend-tool routing is
/// needed (history replay carries no live tool calls). The `prompt_guard`
/// keeps the session un-reapable for the brief duration of the replay.
fn build_history_stream(
    thread_id: String,
    run_id: String,
    drain_stream: PromptStream,
    translated_buffer: usize,
    prompt_guard: PromptGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);

    tokio::spawn(async move {
        let _prompt_guard = prompt_guard;
        let _ = tx
            .send(Ok(factory::run_started(thread_id.clone(), run_id.clone())))
            .await;

        let PromptStream {
            mut events,
            finished,
        } = drain_stream;
        let mut translator = Translator::new();

        while let Some(item) = events.recv().await {
            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if tx.send(Ok(ev)).await.is_err() {
                            return;
                        }
                    }
                }
                BridgeStreamItem::SessionInit { modes, models } => {
                    let ev = session_init_event(modes.as_ref(), models.as_ref());
                    if tx.send(Ok(ev)).await.is_err() {
                        return;
                    }
                }
                BridgeStreamItem::Finished { .. } => break,
                BridgeStreamItem::RunError { message } => {
                    for ev in translator.flush() {
                        let _ = tx.send(Ok(ev)).await;
                    }
                    let _ = tx.send(Ok(factory::run_error(message))).await;
                    return;
                }
                // History replay carries no interrupts or frontend tool
                // calls; ignore those variants defensively.
                _ => {}
            }
        }

        for ev in translator.flush() {
            if tx.send(Ok(ev)).await.is_err() {
                return;
            }
        }

        // Await the drain's terminal signal, then close the run.
        let _ = finished.await;
        let _ = tx.send(Ok(factory::run_finished(thread_id, run_id))).await;
    });

    ReceiverStream::new(rx).boxed()
}
///
/// The clear is **conditional** ([`ThreadEntry::clear_active_sender_if_same`]):
/// it only nulls the slot if it still holds the sender this run installed.
/// This prevents an older run's teardown from wiping a newer overlapping
/// run's sender on the same `thread_id`, which would otherwise strand the
/// newer run's in-flight frontend tool calls until they time out.
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
                .abort_pending_calls("AG-UI run ended before tool resolved");
        }
    }
}

/// Build the AG-UI axum router for a given bridge state.
///
/// Mounts:
/// - `POST /` — AG-UI run endpoint (handled by [`BridgeHandler`])
/// - `GET /health` — liveness probe; returns `200 {"status":"ok","sessions":N}`
/// - `POST /approval` — resolve a deferred permission request by interrupt id
///
/// A request body limit of 16 MiB is applied to every route. If your agents
/// receive significantly larger AG-UI inputs (e.g. very long conversation
/// histories), build the router yourself by composing
/// [`build_router_inner`] with your own `DefaultBodyLimit` layer.
pub fn build_router(state: BridgeAppState) -> axum::Router {
    const DEFAULT_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;
    build_router_inner(state).layer(axum::extract::DefaultBodyLimit::max(
        DEFAULT_BODY_LIMIT_BYTES,
    ))
}

/// Same as [`build_router`] without the request body limit. Compose your
/// own [`axum::extract::DefaultBodyLimit`] when 16 MiB is wrong for your
/// deployment.
pub fn build_router_inner(state: BridgeAppState) -> axum::Router {
    use axum::{Json, extract::State, routing::get, routing::post};

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ApprovalRequest {
        interrupt_id: String,
        approved: bool,
        option_id: Option<String>,
    }

    async fn approval(
        State(state): State<BridgeAppState>,
        Json(body): Json<ApprovalRequest>,
    ) -> axum::http::StatusCode {
        use agent_client_protocol::schema::PermissionOptionId;
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
        match state.resolve_permission(&body.interrupt_id, decision) {
            ResolveOutcome::Resolved => axum::http::StatusCode::OK,
            ResolveOutcome::InvalidOption => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            ResolveOutcome::NotFound => axum::http::StatusCode::NOT_FOUND,
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ToolResponseBody {
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
        if state.resolve_frontend_tool(&body.tool_call_id, resp) {
            tracing::info!("resolved");
            axum::http::StatusCode::OK
        } else {
            tracing::warn!("no pending tool call for id");
            axum::http::StatusCode::NOT_FOUND
        }
    }

    async fn health(State(state): State<BridgeAppState>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "status": "ok",
            "sessions": state.session_count(),
        }))
    }

    /// `GET /sessions` — list the agent's persisted conversations via ACP
    /// `session/list`. The bridge holds no history of its own; this is a
    /// pass-through. Each entry's `sessionId` doubles as the AG-UI
    /// `threadId` a client uses to resume the conversation.
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

    /// `POST /session/set-mode` — switch the session's ACP mode.
    ///
    /// | Status | Meaning                                                       |
    /// |--------|---------------------------------------------------------------|
    /// | 200    | mode accepted; the agent has confirmed the switch.            |
    /// | 404    | no session for `threadId`.                                    |
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

    #[cfg(feature = "unstable_session_model")]
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SetSessionModelBody {
        thread_id: String,
        model_id: String,
    }

    /// `POST /session/set-model` — switch the session's ACP model.
    /// Same status-code semantics as `/session/set-mode`. Only mounted
    /// when the `unstable_session_model` feature is enabled.
    #[cfg(feature = "unstable_session_model")]
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
            .route("/session/set-mode", post(set_mode));
        #[cfg(feature = "unstable_session_model")]
        let r = r.route("/session/set-model", post(set_model));
        r.with_state(state.clone())
    };

    agui_rs_server::axum::agui_router(BridgeHandler::new(state)).merge(aux)
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
                create_locks: DashMap::new(),
                client: self.client,
                cwd,
                config: self.config,
                policy: self.policy,
                frontend_tools: FrontendToolRegistry::new(),
                self_url: self.self_url.map(|u| u.trim_end_matches('/').to_string()),
                reaper: std::sync::Mutex::new(None),
            }),
        }
    }
}
