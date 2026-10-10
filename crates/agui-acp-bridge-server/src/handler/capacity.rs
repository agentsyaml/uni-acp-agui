use super::*;

impl BridgeAppState {
    /// Try to reserve one real live-session slot. The short gate protects
    /// idle-victim selection and permit acquisition; it is released before
    /// the ACP handshake begins. An evicted entry may still hold its permit
    /// through an external `Arc`, so this method never assumes map removal
    /// means the actor is gone.
    pub(super) async fn reserve_session_capacity(
        &self,
    ) -> Result<Option<OwnedSemaphorePermit>, &'static str> {
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
    pub(super) async fn reserve_session_capacity_with_eviction(
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
}
