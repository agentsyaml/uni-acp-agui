use super::*;

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(h) = self.reaper.lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
    }
}

impl BridgeAppState {
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
}
