use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn session_notification(
    notification: SessionNotification,
    init_state: Arc<Mutex<SessionInitState>>,
    load_buffer: LoadBuffer,
    event_slot: EventSlot,
    mailbox: EventMailboxTx,
    spill_buffer: SpillBuffer,
    pending_permissions: PendingPermissions,
) -> Result<(), agent_client_protocol::Error> {
    let load_state = load_buffer
        .lock()
        .expect("load buffer poisoned")
        .as_ref()
        .map(|history| history.exceeded);
    if load_state == Some(true) {
        return Err(load_history_limit_error());
    }
    let load_bytes = if load_state == Some(false) {
        Some(
            serde_json::to_vec(&notification)
                .map_err(agent_client_protocol::Error::into_internal_error)?
                .len(),
        )
    } else {
        None
    };
    if let agent_client_protocol::schema::v1::SessionUpdate::CurrentModeUpdate(ref mode) =
        notification.update
    {
        let new_id = mode.current_mode_id.0.to_string();
        let mut guard = init_state.lock().expect("init_state poisoned");
        if let Some(modes) = guard.modes.as_mut() {
            modes.current_mode_id = new_id;
        }
    } else if let agent_client_protocol::schema::v1::SessionUpdate::ConfigOptionUpdate(ref update) =
        notification.update
    {
        let options = update.config_options.clone();
        let mut guard = init_state.lock().expect("init_state poisoned");
        guard.config_options = Some(options.clone());
        sync_legacy_picker_state(&mut guard, &options);
    }
    {
        let mut buffer = load_buffer.lock().expect("load buffer poisoned");
        if let Some(history) = buffer.as_mut()
            && let Some(bytes) = load_bytes
        {
            if history.append(notification.update, bytes).is_err() {
                return Err(load_history_limit_error());
            }
            return Ok(());
        }
    }
    let route = event_slot.lock().expect("event slot poisoned").clone();
    if let Some(route) = route {
        if let Err(limit) =
            mailbox.enqueue_data(&route, BridgeStreamItem::Update(notification.update))
            && route.turn.is_failed()
        {
            route.turn.cancel_and_drain(&pending_permissions);
            tracing::warn!(?limit, "session event mailbox limit exceeded");
        }
    } else {
        let mut spill = spill_buffer.lock().expect("spill buffer poisoned");
        if spill.len() >= SPILL_CAPACITY {
            tracing::warn!(
                capacity = SPILL_CAPACITY,
                "out-of-turn session/update spill overflow; dropping update"
            );
        } else {
            spill.push(notification.update);
        }
    }
    Ok(())
}
