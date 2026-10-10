use super::*;

/// Per-thread gate for lazy session creation.
///
/// `users` counts callers that have retained this gate, including callers
/// queued on `lock`. A gate is only removed after its last user releases it;
/// otherwise a failed creator could remove the map entry while an older
/// waiter still held an Arc to the old mutex, allowing a new caller to create
/// a second gate for the same thread.
pub(super) struct SessionCreateGate {
    pub(super) lock: tokio::sync::Mutex<()>,
    pub(super) users: AtomicUsize,
    pub(super) closing: AtomicBool,
}

impl SessionCreateGate {
    pub(super) fn new() -> Self {
        Self {
            lock: tokio::sync::Mutex::new(()),
            users: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
        }
    }

    /// Retain the gate unless its last user has started removing it.
    pub(super) fn try_retain(&self) -> bool {
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
pub(super) struct SessionCreateGuard {
    pub(super) inner: Arc<Inner>,
    pub(super) thread_id: String,
    pub(super) gate: Arc<SessionCreateGate>,
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
/// The mutually-exclusive claims that protect one thread's cached ACP entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ThreadClaim {
    Run(String),
    Lifecycle(&'static str),
}

/// RAII ownership of one run or lifecycle admission claim.
///
/// The conditional removal matters if a stale teardown races a replacement
/// claim: an old operation must never release a newer operation's slot.
pub(super) struct ThreadClaimGuard {
    pub(super) inner: Arc<Inner>,
    pub(super) thread_id: String,
    pub(super) claim: ThreadClaim,
}

pub(super) type RunAdmissionGuard = ThreadClaimGuard;
pub(super) type LifecycleGuard = ThreadClaimGuard;

impl ThreadClaimGuard {
    pub(super) fn claim(&self) -> &ThreadClaim {
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
    pub(super) async fn acquire_session_create_gate(
        self: &Arc<Self>,
        thread_id: &str,
    ) -> SessionCreateGuard {
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

    pub(super) fn remove_session_create_gate(
        &self,
        thread_id: &str,
        expected: &Arc<SessionCreateGate>,
    ) {
        if !expected.closing.load(Ordering::Acquire) || expected.users.load(Ordering::Acquire) != 0
        {
            return;
        }
        let _ = self
            .create_locks
            .remove_if(thread_id, |_, current| Arc::ptr_eq(current, expected));
    }

    pub(super) fn try_claim(
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

    pub(super) fn try_claim_run(
        self: &Arc<Self>,
        thread_id: &str,
        run_id: &str,
    ) -> Option<RunAdmissionGuard> {
        self.try_claim(thread_id, ThreadClaim::Run(run_id.to_string()))
    }

    pub(super) fn try_claim_lifecycle(
        self: &Arc<Self>,
        thread_id: &str,
        reason: &'static str,
    ) -> Option<LifecycleGuard> {
        self.try_claim(thread_id, ThreadClaim::Lifecycle(reason))
    }

    pub(super) fn has_lifecycle_claim(&self, thread_id: &str) -> bool {
        self.active_runs
            .get(thread_id)
            .is_some_and(|claim| matches!(claim.value(), ThreadClaim::Lifecycle(_)))
    }

    pub(super) fn owns_claim(&self, thread_id: &str, claim: &ThreadClaim) -> bool {
        self.active_runs
            .get(thread_id)
            .is_some_and(|current| current.value() == claim)
    }
}

/// RAII marker for an in-flight session setting RPC. Settings are serialized by
/// the actor but still count as busy for close/reaper/LRU admission.
pub(super) struct SettingGuard {
    pub(super) inner: Arc<Inner>,
    pub(super) thread_id: String,
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
impl BridgeAppState {
    pub(super) fn try_claim_run(&self, thread_id: &str, run_id: &str) -> Option<RunAdmissionGuard> {
        self.inner.try_claim_run(thread_id, run_id)
    }

    pub(super) fn try_claim_lifecycle(
        &self,
        thread_id: &str,
        reason: &'static str,
    ) -> Option<LifecycleGuard> {
        self.inner.try_claim_lifecycle(thread_id, reason)
    }
}
impl BridgeAppState {
    pub(super) fn enter_setting(&self, thread_id: &str) -> Result<SettingGuard, SetSessionStatus> {
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
}
