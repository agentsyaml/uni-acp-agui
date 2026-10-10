use super::*;

/// Resolve every pending permission with `Deny` and remove its map entry.
///
/// Called when the session is shutting down (either via a normal
/// `cmd_rx` close or after a connection error) so the spawn tasks
/// awaiting `resolve_rx` exit immediately rather than after
/// `permission_timeout`. Safe to call from any sync context — the
/// oneshot send is non-blocking.
pub(super) fn drain_pending_permissions(pending: &PendingPermissions) {
    let keys: Vec<String> = pending.iter().map(|e| e.key().clone()).collect();
    for key in keys {
        if let Some((_, p)) = pending.remove(&key) {
            // The receiver may already be gone (timeout fired first).
            // We don't care about the send result.
            let _ = p.resolver.send(PermissionDecision::Deny);
        }
    }
}

/// Consult the configured policy and respond to a `requestPermission` request.
///
/// Decision flow:
/// - `Allow { option_id }` → respond inline with `Selected(option_id)`
/// - `Deny` → respond inline with `Cancelled`
/// - `Defer { interrupt_id }` → emit `BridgeStreamItem::Interrupt`, then
///   **spawn a task** to await an external resolution via
///   [`AcpSessionHandle::resolve_permission`] with the configured timeout
///   and respond from there. The handler returns immediately so the SDK
///   dispatch loop is **not** blocked while waiting for user approval —
///   a critical correctness property since the loop also delivers
///   `session/update` notifications and other concurrent work for the
///   same connection. Timeout / no-active-prompt fall back to `Cancelled`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_permission_request(
    req: RequestPermissionRequest,
    responder: agent_client_protocol::Responder<RequestPermissionResponse>,
    policy: Arc<dyn PermissionPolicy>,
    event_slot: EventSlot,
    event_mailbox: EventMailboxTx,
    pending_permissions: PendingPermissions,
    permission_timeout: Duration,
) -> Result<(), agent_client_protocol::Error> {
    let route = event_slot.lock().expect("event slot poisoned").clone();
    let Some(route) = route else {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    };
    let turn = route.turn.clone();
    let request_bytes =
        json_size_bounded(&req, crate::acp::MAX_PENDING_PERMISSION_BYTES_PER_REQUEST);
    let Some(permission_budget) = turn.reserve_permission(request_bytes) else {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    };
    let decision = policy.decide(&req).await;

    // A policy may have been awaiting its own async work when the turn was
    // cancelled. Do not let that late decision resurrect a cancelled ACP
    // permission request.
    if turn.is_cancelled() || turn.is_failed() {
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    }

    match decision {
        PermissionDecision::Allow { option_id } => {
            responder.respond(RequestPermissionResponse::new(
                RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option_id)),
            ))
        }
        PermissionDecision::Deny => responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        )),
        PermissionDecision::Defer { interrupt_id } => {
            // Register the pending permission BEFORE sending the Interrupt,
            // so a resolve() call that arrives before our await on rx still
            // hits the map.
            let (resolve_tx, resolve_rx) = oneshot::channel::<PermissionDecision>();
            let valid_option_ids = req
                .options
                .iter()
                .map(|o| o.option_id.0.to_string())
                .collect::<std::collections::HashSet<_>>();
            let registered = turn.register_pending(
                &pending_permissions,
                interrupt_id.clone(),
                crate::acp::PendingPermission::new(
                    resolve_tx,
                    valid_option_ids,
                    turn.clone(),
                    permission_budget,
                ),
            );
            if !registered {
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            // Same dispatch-loop constraint as the notification handler: a
            // blocking send here would stall `session/prompt`'s response and
            // deadlock the turn when the channel is full. Enqueue off-loop.
            if event_mailbox
                .enqueue_data(
                    &route,
                    BridgeStreamItem::Interrupt {
                        id: interrupt_id.clone(),
                        request: req.clone(),
                    },
                )
                .is_err()
            {
                if turn.is_failed() {
                    turn.cancel_and_drain(&pending_permissions);
                }
                turn.remove_pending(&pending_permissions, &interrupt_id);
                return responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Cancelled,
                ));
            }

            // KEY CORRECTNESS POINT: spawn the wait off the dispatch loop.
            // Awaiting `resolve_rx` here would block every subsequent
            // notification/request the agent sends until the user
            // approves/denies (or we time out — minutes by default).
            let pending = pending_permissions.clone();
            let turn_for_wait = turn.clone();
            tokio::spawn(async move {
                let resolution = tokio::time::timeout(permission_timeout, resolve_rx).await;
                // Always remove from the pending map (even on timeout).
                turn_for_wait.remove_pending(&pending, &interrupt_id);
                let response = match resolution {
                    Ok(Ok(PermissionDecision::Allow { option_id })) => {
                        RequestPermissionResponse::new(RequestPermissionOutcome::Selected(
                            SelectedPermissionOutcome::new(option_id),
                        ))
                    }
                    Ok(Ok(PermissionDecision::Deny)) | Ok(Ok(PermissionDecision::Defer { .. })) => {
                        // A `Defer` arriving as a *resolution* makes no sense
                        // (it's the policy's initial verdict, not an answer).
                        // Treat as deny so we never leave the agent waiting.
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                    Ok(Err(_)) => {
                        // Sender dropped without resolving — treat as deny.
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                    Err(_) => {
                        tracing::warn!(
                            interrupt_id = %interrupt_id,
                            timeout_secs = permission_timeout.as_secs(),
                            "permission request timed out; denying"
                        );
                        RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
                    }
                };
                if let Err(e) = responder.respond(response) {
                    tracing::warn!(error = %e, "failed to send deferred permission response");
                }
            });
            Ok(())
        }
    }
}
