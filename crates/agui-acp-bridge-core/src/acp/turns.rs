use crate::policy::PermissionDecision;
use dashmap::DashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::oneshot;

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
    pub(crate) _budget: PermissionReservation,
}

impl PendingPermission {
    pub(crate) fn new(
        resolver: oneshot::Sender<PermissionDecision>,
        valid_option_ids: HashSet<String>,
        turn: Arc<TurnState>,
        budget: PermissionReservation,
    ) -> Self {
        Self {
            resolver,
            valid_option_ids,
            turn,
            _budget: budget,
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
pub(super) const MAX_PENDING_PERMISSIONS_PER_TURN: usize = 128;
pub(crate) const MAX_PENDING_PERMISSION_BYTES_PER_REQUEST: usize = 64 * 1024;
pub(super) const MAX_PENDING_PERMISSION_BYTES_PER_TURN: usize = 16 * 1024 * 1024;

#[derive(Debug, Default)]
pub(super) struct PermissionBudget {
    count: usize,
    bytes: usize,
}

#[derive(Debug)]
pub(crate) struct PermissionReservation {
    budget: Arc<StdMutex<PermissionBudget>>,
    bytes: usize,
}

impl Drop for PermissionReservation {
    fn drop(&mut self) {
        let mut budget = self.budget.lock().expect("permission budget poisoned");
        budget.count -= 1;
        budget.bytes -= self.bytes;
    }
}

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
    pub(super) pending_ids: StdMutex<HashSet<String>>,
    permission_budget: Arc<StdMutex<PermissionBudget>>,
    failed: AtomicBool,
    pub(crate) failure_notify: tokio::sync::Notify,
    pub(crate) cancel_notify: tokio::sync::Notify,
}

impl TurnState {
    pub(crate) fn new() -> Self {
        Self {
            id: TurnId(NEXT_TURN_ID.fetch_add(1, Ordering::Relaxed)),
            cancelled: AtomicBool::new(false),
            pending_ids: StdMutex::new(HashSet::new()),
            permission_budget: Arc::new(StdMutex::new(PermissionBudget::default())),
            failed: AtomicBool::new(false),
            failure_notify: tokio::sync::Notify::new(),
            cancel_notify: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn id(&self) -> TurnId {
        self.id
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn reserve_permission(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Option<PermissionReservation> {
        let mut budget = self
            .permission_budget
            .lock()
            .expect("permission budget poisoned");
        if budget.count >= MAX_PENDING_PERMISSIONS_PER_TURN
            || bytes > MAX_PENDING_PERMISSION_BYTES_PER_REQUEST
            || bytes > MAX_PENDING_PERMISSION_BYTES_PER_TURN
            || budget.bytes > MAX_PENDING_PERMISSION_BYTES_PER_TURN - bytes
            || self.is_cancelled()
        {
            return None;
        }
        budget.count += 1;
        budget.bytes += bytes;
        Some(PermissionReservation {
            budget: self.permission_budget.clone(),
            bytes,
        })
    }

    pub(crate) fn fail(&self) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            self.failure_notify.notify_waiters();
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Register a pending permission under `interrupt_id`.
    ///
    /// Returns `false` if the turn is cancelled **or** if `interrupt_id` is
    /// already registered. A duplicate must never silently clobber a live
    /// entry: `DashMap::insert` would replace another turn's oneshot
    /// resolver while the first turn's `pending_ids` still holds the id, so
    /// a later `cancel_and_drain` on that turn would resolve a permission
    /// the UI surfaced for a different turn. The trait contract says the
    /// interrupt id is bridge-assigned, so the bridge owns the uniqueness
    /// invariant here.
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
        // The DashMap entry API makes the occupancy check + insert atomic
        // per shard, closing the cross-turn race the separate `contains_key`
        // call would leave open.
        match pending_permissions.entry(interrupt_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => false,
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(pending);
                ids.insert(interrupt_id);
                true
            }
        }
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
    pub(super) fn len(&self) -> usize {
        self.turns.lock().expect("turn queue poisoned").len()
    }
}

impl Default for SessionTurnQueue {
    fn default() -> Self {
        Self::new(0)
    }
}
