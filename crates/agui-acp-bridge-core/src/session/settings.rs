use super::*;

/// Deliver a setting result or make the session unusable when the caller has
/// gone away between the closed check and `send`.
pub(super) fn send_setting_ack(
    ack: oneshot::Sender<Result<(), BridgeError>>,
    result: Result<(), BridgeError>,
    unusable: &AtomicBool,
) -> bool {
    if ack.send(result).is_err() {
        unusable.store(true, Ordering::Release);
        false
    } else {
        true
    }
}

/// Shared body of `SetMode`/`SetConfigOption`: run one actor-serial setting
/// RPC and reconcile every caller-gone path.
///
/// Returns `false` only when the actor must shut down (the caller vanished at
/// any point). All three poison cases — the responder closed while the ACP
/// request raced it, the request itself timed out, and the acknowledgement
/// failing to deliver — mark the session unusable before exiting, because a
/// half-applied setting leaves the cached snapshot untrustworthy for reuse.
pub(super) async fn run_setting_command(
    mut ack: oneshot::Sender<Result<(), BridgeError>>,
    timeout: Duration,
    unusable: &AtomicBool,
    request: impl Future<Output = Result<(), BridgeError>>,
) -> bool {
    // Settings are deliberately actor-serial. A prompt owns the ACP turn
    // until it finishes, so a queued setting cannot race a later setting or
    // overwrite its newer cached snapshot. The HTTP caller may time out while
    // this command waits behind a prompt; do not apply a command whose
    // responder has already gone away.
    if ack.is_closed() {
        return true;
    }

    let res = tokio::select! {
        _ = ack.closed() => {
            // The caller disconnected while the ACP request was in flight.
            // Its eventual response cannot safely update a session that may
            // be reused, so close this actor.
            unusable.store(true, Ordering::Release);
            return false;
        }
        result = tokio::time::timeout(timeout, request) => match result {
            Ok(result) => result,
        Err(_) => {
            // A timed-out setting leaves ACP state uncertain: report the
            // timeout to the caller if it can still receive it, then always
            // shut the actor down.
            unusable.store(true, Ordering::Release);
            let _ = send_setting_ack(ack, Err(BridgeError::Timeout(timeout)), unusable);
            return false;
        }
        },
    };
    if ack.is_closed() && res.is_ok() {
        unusable.store(true, Ordering::Release);
        return false;
    }
    send_setting_ack(ack, res, unusable)
}
/// Send `session/set_mode` and, on success, update the cached init state's
/// `current_mode_id`. Returns the agent's error if the request was rejected
/// (e.g. unknown `mode_id`).
pub(super) async fn send_set_mode(
    cx: &ConnectionTo<Agent>,
    session_id: &SessionId,
    mode_id: &str,
    init_state: Arc<Mutex<SessionInitState>>,
) -> Result<(), BridgeError> {
    let mode_id_arc: agent_client_protocol::schema::v1::SessionModeId = mode_id.to_string().into();
    cx.send_request(SetSessionModeRequest::new(
        session_id.clone(),
        mode_id_arc.clone(),
    ))
    .block_task()
    .await
    .map_err(BridgeError::Acp)?;
    let mut guard = init_state.lock().expect("init_state poisoned");
    if let Some(modes) = guard.modes.as_mut() {
        modes.current_mode_id = mode_id.to_string();
    }
    Ok(())
}

pub(super) fn sync_legacy_picker_state(
    init_state: &mut SessionInitState,
    config_options: &[SessionConfigOption],
) {
    let current_value = |category: SessionConfigOptionCategory| {
        config_options
            .iter()
            .find(|option| option.category.as_ref() == Some(&category))
            .and_then(|option| match &option.kind {
                SessionConfigKind::Select(select) => Some(select.current_value.0.to_string()),
                _ => None,
            })
    };

    if let Some(current_mode_id) = current_value(SessionConfigOptionCategory::Mode)
        && let Some(modes) = init_state.modes.as_mut()
    {
        modes.current_mode_id = current_mode_id;
    }
    #[cfg(feature = "unstable_session_model")]
    if let Some(current_model_id) = current_value(SessionConfigOptionCategory::Model)
        && let Some(models) = init_state.models.as_mut()
    {
        models.current_model_id = current_model_id;
    }
}

/// Send the stable ACP `session/set_config_option` request and replace the
/// cached config-option snapshot with the response's complete list.
pub(super) async fn send_set_config_option(
    cx: &ConnectionTo<Agent>,
    session_id: &SessionId,
    config_id: &str,
    value: &SessionConfigOptionValue,
    init_state: Arc<Mutex<SessionInitState>>,
) -> Result<(), BridgeError> {
    let response: SetSessionConfigOptionResponse = cx
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            config_id.to_string(),
            value.clone(),
        ))
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;
    let mut guard = init_state.lock().expect("init_state poisoned");
    guard.config_options = Some(response.config_options.clone());
    sync_legacy_picker_state(&mut guard, &response.config_options);
    Ok(())
}
