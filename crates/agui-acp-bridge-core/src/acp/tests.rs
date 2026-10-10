use super::*;
use crate::policy::PermissionDecision;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::{Mutex as StdMutex, atomic::AtomicBool};

#[test]
fn turn_queue_rejects_at_capacity_without_disturbing_existing_turn() {
    let queue = SessionTurnQueue::new(1);
    let active = queue.try_enqueue().expect("first turn fits");
    assert!(matches!(queue.try_enqueue(), Err(1)));
    assert!(queue.find(active.id()).is_some());

    queue.remove(&active);
    assert!(queue.try_enqueue().is_ok());
}

#[test]
fn zero_turn_queue_limit_is_explicitly_unlimited() {
    let queue = SessionTurnQueue::new(0);
    for _ in 0..128 {
        assert!(queue.try_enqueue().is_ok());
    }
}

#[test]
fn clear_releases_all_turn_slots() {
    let queue = SessionTurnQueue::new(2);
    let _first = queue.try_enqueue().expect("first turn fits");
    let _second = queue.try_enqueue().expect("second turn fits");
    assert_eq!(queue.len(), 2);

    queue.clear();

    assert_eq!(queue.len(), 0);
    assert!(queue.try_enqueue().is_ok());
}

#[test]
fn pending_permission_admission_is_bounded_and_released() {
    let turn = Arc::new(TurnState::new());
    let mut leases: Vec<_> = (0..MAX_PENDING_PERMISSIONS_PER_TURN)
        .map(|_| turn.reserve_permission(32).expect("within pending cap"))
        .collect();
    assert!(turn.reserve_permission(1).is_none());
    drop(leases.pop());
    assert!(turn.reserve_permission(1).is_some());
    assert!(
        turn.reserve_permission(MAX_PENDING_PERMISSION_BYTES_PER_REQUEST + 1)
            .is_none()
    );
}

#[test]
fn duplicate_interrupt_id_must_not_clobber_another_turns_pending_permission() {
    let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
    let turn_a = Arc::new(TurnState::new());
    let turn_b = Arc::new(TurnState::new());

    let (tx_a, mut rx_a) = oneshot::channel();
    let (_tx_b_rejected, _rx_b_rejected) = oneshot::channel();
    let (tx_b_own, mut rx_b_own) = oneshot::channel();

    // Both turns request the *same* bridge-assigned interrupt id (a
    // misbehaving policy reusing an id). Only the first registration
    // may succeed; the second must be rejected without dropping
    // turn A's live resolver.
    assert!(turn_a.register_pending(
        &pending_permissions,
        "shared-id".to_string(),
        PendingPermission::new(
            tx_a,
            HashSet::new(),
            turn_a.clone(),
            turn_a.reserve_permission(0).unwrap(),
        ),
    ));
    assert!(
        !turn_b.register_pending(
            &pending_permissions,
            "shared-id".to_string(),
            PendingPermission::new(
                _tx_b_rejected,
                HashSet::new(),
                turn_b.clone(),
                turn_b.reserve_permission(0).unwrap(),
            ),
        ),
        "duplicate interrupt id must be rejected"
    );
    // Turn B also holds a legitimate pending permission under its own
    // id, as would happen mid-turn alongside the colliding request.
    assert!(turn_b.register_pending(
        &pending_permissions,
        "turn-b-own".to_string(),
        PendingPermission::new(
            tx_b_own,
            HashSet::new(),
            turn_b.clone(),
            turn_b.reserve_permission(0).unwrap(),
        ),
    ));

    // Turn A cancels: it drains exactly its own single entry.
    turn_a.cancel_and_drain(&pending_permissions);
    assert!(matches!(rx_a.try_recv(), Ok(PermissionDecision::Deny)));

    // Turn B's legitimate permission was untouched by turn A's drain:
    // still pending, resolvable with a real decision.
    match rx_b_own.try_recv() {
        Err(oneshot::error::TryRecvError::Empty) => {}
        other => panic!("turn B's permission must stay pending, got {other:?}"),
    }
}

#[tokio::test]
async fn cancelled_prompt_send_releases_turn_slot() {
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let queue = Arc::new(SessionTurnQueue::new(1));
    let handle = Arc::new(AcpSessionHandle::new(
        cmd_tx.clone(),
        Arc::new(DashMap::new()),
        queue.clone(),
        Arc::new(AtomicBool::new(false)),
        SessionId::from("test-session"),
        false,
        Arc::new(StdMutex::new(SessionInitState::default())),
        1,
    ));
    let (ack_tx, _ack_rx) = oneshot::channel();
    cmd_tx
        .send(SessionCommand::SetMode {
            mode_id: "blocked".into(),
            ack: ack_tx,
        })
        .await
        .expect("fill command channel");

    let prompt = tokio::spawn({
        let handle = handle.clone();
        async move { handle.prompt_with_turn("blocked prompt").await }
    });
    for _ in 0..16 {
        if queue.len() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        queue.len(),
        1,
        "prompt must enqueue before send backpressure"
    );

    prompt.abort();
    let _ = prompt.await;
    assert_eq!(queue.len(), 0, "cancelled enqueue must release its slot");
}
