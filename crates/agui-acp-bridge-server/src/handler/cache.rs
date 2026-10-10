use super::*;

/// Internal session record holding the handle plus a last-used timestamp.
#[derive(Debug)]
pub(super) struct SessionEntry {
    pub(super) handle: Arc<AcpSessionHandle>,
    /// Counts this actor against `max_sessions` until the entry and every
    /// external `Arc<SessionEntry>` holding it are dropped.
    pub(super) _capacity_permit: Option<OwnedSemaphorePermit>,
    pub(super) last_used: std::sync::Mutex<Instant>,
    /// Number of in-flight prompts on this session. The reaper refuses to
    /// drop entries with `active_prompts > 0` even if their `last_used` is
    /// stale: a long-running prompt would otherwise be killed mid-flight.
    pub(super) active_prompts: std::sync::atomic::AtomicUsize,
    pub(super) mcp_credential: Option<Arc<McpCredential>>,
}

pub(super) struct McpCredential(pub(super) String);

impl std::fmt::Debug for McpCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("McpCredential([redacted])")
    }
}

/// One cached `GET /sessions` snapshot. Bounded by construction: a single
/// slot holding at most one snapshot. Only successes are cached — errors
/// return uncached (they still hit the gate, so concurrency stays bounded).
pub(super) struct SessionsListCacheEntry {
    summaries: Vec<agui_acp_bridge_core::SessionSummary>,
    fetched_at: Instant,
}

/// How long a `GET /sessions` snapshot may be reused before the next request
/// re-queries the agent (which forks a subprocess).
pub(super) const SESSIONS_LIST_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

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
    pub(super) async fn sessions_list(
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
    pub(super) async fn invalidate_sessions_list_cache(&self) {
        let _gate = self.sessions_list_gate.lock().await;
        *self.sessions_list_cache.lock().await = None;
    }
}

impl SessionEntry {
    #[cfg(test)]
    pub(super) fn new(
        handle: Arc<AcpSessionHandle>,
        capacity_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self::new_scoped(handle, capacity_permit, None)
    }

    pub(super) fn new_scoped(
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

    pub(super) fn touch(&self) {
        *self.last_used.lock().expect("session entry mutex poisoned") = Instant::now();
    }

    pub(super) fn last_used(&self) -> Instant {
        *self.last_used.lock().expect("session entry mutex poisoned")
    }

    pub(super) fn active_prompts(&self) -> usize {
        self.active_prompts
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn enter_prompt(self: &Arc<Self>) -> PromptGuard {
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
    pub(super) entry: Arc<SessionEntry>,
}

impl Drop for PromptGuard {
    fn drop(&mut self) {
        self.entry
            .active_prompts
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.entry.touch();
    }
}

/// Remove a cached session only if the caller still owns the same entry that
/// it observed. A replacement session may already be installed under the
/// same thread id; an unconditional `remove(thread_id)` would delete that
/// newer session.
pub(super) fn remove_session_if_same(
    inner: &Inner,
    thread_id: &str,
    expected: &Arc<SessionEntry>,
) -> bool {
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
pub(super) fn remove_session_if_handle(
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

pub(super) fn revoke_mcp_credential(inner: &Inner, thread_id: &str, entry: &SessionEntry) {
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
pub(super) async fn graceful_close_removed(
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

impl BridgeAppState {
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
}
impl BridgeAppState {
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

    pub(super) fn evict_unusable(&self, thread_id: &str, expected: &Arc<SessionEntry>) {
        let removed = self.inner.sessions.remove_if(thread_id, |_, entry| {
            Arc::ptr_eq(entry, expected) && entry.handle.is_unusable()
        });
        if removed.is_some() {
            revoke_mcp_credential(&self.inner, thread_id, expected);
            self.inner.frontend_tools.drop_thread(thread_id);
        }
    }
}
