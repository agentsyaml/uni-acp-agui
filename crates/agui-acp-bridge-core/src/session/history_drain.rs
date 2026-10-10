use super::*;

pub(super) async fn drain_history(
    events_tx: mpsc::Sender<BridgeStreamItem>,
    finished_tx: oneshot::Sender<Result<StopReason, BridgeError>>,
    error_slot: &EventSlot,
    init_state: &Mutex<SessionInitState>,
    load_buffer: &LoadBuffer,
    spill: &SpillBuffer,
) {
    let route = Arc::new(EventRoute {
        events_tx: events_tx.clone(),
        turn: Arc::new(TurnState::new()),
        state: Mutex::new(RouteState::default()),
    });
    *error_slot.lock().expect("error route slot poisoned") = Some(route);
    let snapshot = init_state.lock().expect("init_state poisoned").clone();
    let _ = events_tx
        .send(BridgeStreamItem::SessionInit {
            modes: snapshot.modes,
            models: snapshot.models,
            config_options: snapshot.config_options,
        })
        .await;
    let replay = load_buffer.lock().expect("load buffer poisoned").take();
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
    let spilled: Vec<_> = std::mem::take(&mut *spill.lock().expect("spill buffer poisoned"));
    for update in spilled {
        if events_tx
            .send(BridgeStreamItem::Update(update))
            .await
            .is_err()
        {
            break;
        }
    }
    let _ = finished_tx.send(Ok(StopReason::EndTurn));
    drop(events_tx);
    let _ = error_slot.lock().expect("error route slot poisoned").take();
}
