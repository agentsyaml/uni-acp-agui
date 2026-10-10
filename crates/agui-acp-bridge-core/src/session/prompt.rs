use super::*;

pub(super) struct PromptState<'a> {
    pub(super) cx: &'a ConnectionTo<Agent>,
    pub(super) session_id: &'a SessionId,
    pub(super) event_slot: &'a EventSlot,
    pub(super) error_slot: &'a EventSlot,
    pub(super) init_state: &'a Mutex<SessionInitState>,
    pub(super) load_buffer: &'a LoadBuffer,
    pub(super) spill: &'a SpillBuffer,
    pub(super) pending_permissions: &'a PendingPermissions,
    pub(super) event_mailbox: &'a EventMailboxTx,
    pub(super) turn_queue: &'a Arc<SessionTurnQueue>,
    pub(super) unusable: &'a AtomicBool,
    pub(super) cancel_grace_timeout: Duration,
}

pub(super) async fn run_prompt(
    prompt: Vec<ContentBlock>,
    events_tx: mpsc::Sender<BridgeStreamItem>,
    finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
    turn: Arc<TurnState>,
    state: PromptState<'_>,
) -> bool {
    let route = Arc::new(EventRoute {
        events_tx: events_tx.clone(),
        turn: turn.clone(),
        state: Mutex::new(RouteState::default()),
    });
    *state.error_slot.lock().expect("error route slot poisoned") = Some(route.clone());
    let snapshot = state
        .init_state
        .lock()
        .expect("init_state poisoned")
        .clone();
    let _ = events_tx
        .send(BridgeStreamItem::SessionInit {
            modes: snapshot.modes,
            models: snapshot.models,
            config_options: snapshot.config_options,
        })
        .await;

    let replay = state
        .load_buffer
        .lock()
        .expect("load buffer poisoned")
        .take();
    if let Some(history) = replay {
        for update in history.updates {
            if events_tx
                .send(BridgeStreamItem::Update(update))
                .await
                .is_err()
            {
                break;
            }
        }
    }
    let spilled: Vec<_> = std::mem::take(&mut *state.spill.lock().expect("spill buffer poisoned"));
    for update in spilled {
        if events_tx
            .send(BridgeStreamItem::Update(update))
            .await
            .is_err()
        {
            break;
        }
    }
    *state.event_slot.lock().expect("event slot poisoned") = Some(route.clone());

    let result = run_prompt_with_cancel(
        state.cx,
        state.session_id,
        prompt,
        &events_tx,
        turn.clone(),
        (*state.pending_permissions).clone(),
        state.cancel_grace_timeout,
    )
    .await;
    let grace_expired =
        matches!(&result, Err(BridgeError::CancelGraceExpired(_))) && turn.is_cancelled();
    let peer_closed = matches!(&result, Err(BridgeError::SessionClosed));
    let failure_limit = route.state.lock().expect("event route poisoned").limit;
    let mut result = if turn.is_failed() {
        Err(mailbox_limit_error(
            failure_limit.unwrap_or(EventLimit::Count),
        ))
    } else {
        result
    };
    turn.cancel_and_drain(state.pending_permissions);

    let terminal = match &result {
        Ok(stop_reason) => BridgeStreamItem::Finished {
            stop_reason: *stop_reason,
        },
        Err(error) => BridgeStreamItem::RunError {
            message: if turn.is_failed() {
                format!(
                    "session event mailbox {} limit exceeded",
                    failure_limit.unwrap_or(EventLimit::Count).name()
                )
            } else {
                error.to_string()
            },
        },
    };
    let terminal_ack = state.event_mailbox.enqueue_terminal(&route, terminal);
    if turn.is_failed() && result.is_ok() {
        let limit = route
            .state
            .lock()
            .expect("event route poisoned")
            .limit
            .unwrap_or(EventLimit::Count);
        result = Err(mailbox_limit_error(limit));
    }
    *state.event_slot.lock().expect("event slot poisoned") = None;
    if grace_expired || peer_closed {
        state.unusable.store(true, Ordering::Release);
    }
    let _ = finished_tx.send(result);
    drop(events_tx);
    state.turn_queue.remove(&turn);
    if !matches!(terminal_ack.await, Ok(Ok(()))) {
        state.unusable.store(true, Ordering::Release);
        let _ = state
            .error_slot
            .lock()
            .expect("error route slot poisoned")
            .take();
        return false;
    }
    let _ = state
        .error_slot
        .lock()
        .expect("error route slot poisoned")
        .take();
    if grace_expired {
        state.unusable.store(true, Ordering::Release);
        return false;
    }
    true
}
