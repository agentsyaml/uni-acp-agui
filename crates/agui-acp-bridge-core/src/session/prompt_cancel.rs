use super::*;

/// Run a prompt while concurrently watching for a cancel notification and
/// the prompt-event receiver being dropped.
///
/// On cancel **or** disconnect we send the ACP `session/cancel` notification
/// to the agent and **continue awaiting** the prompt response: the agent is
/// expected to wind down and respond with `StopReason::Cancelled`. Awaiting
/// the response is what lets the `Cancelled` envelope reach the caller's
/// `finished` oneshot.
///
/// Subsequent cancel signals while we're already awaiting are absorbed
/// silently — the first cancel did the work.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_prompt_with_cancel(
    cx: &ConnectionTo<Agent>,
    session_id: &agent_client_protocol::schema::v1::SessionId,
    prompt: Vec<ContentBlock>,
    events_tx: &mpsc::Sender<BridgeStreamItem>,
    turn: Arc<TurnState>,
    pending_permissions: PendingPermissions,
    cancel_grace_timeout: Duration,
) -> Result<StopReason, BridgeError> {
    // Register the waiter before checking the flag. `enable()` closes the
    // check/register gap: a notify_one that races this setup remains queued
    // for this future instead of being lost.
    let cancelled = turn.cancel_notify.notified();
    tokio::pin!(cancelled);
    cancelled.as_mut().enable();

    // A queued turn can be cancelled before the actor starts it. Do not send
    // an ACP prompt for a caller that has already disconnected.
    if turn.is_cancelled() {
        turn.cancel_and_drain(&pending_permissions);
        return Ok(StopReason::Cancelled);
    }

    let mut prompt_fut = std::pin::pin!(async {
        cx.send_request(PromptRequest::new(session_id.clone(), prompt))
            .block_task()
            .await
            .map_err(BridgeError::Acp)
    });

    let mut cancel_deadline = Box::pin(tokio::time::sleep(Duration::from_secs(
        100 * 365 * 24 * 60 * 60,
    )));
    let mut already_cancelled = false;
    let incoming_closed = cx.incoming_closed();
    tokio::pin!(incoming_closed);
    let response = loop {
        tokio::select! {
            biased;

            res = &mut prompt_fut => break res,

            // Prefer a response already queued by the dispatch loop over EOF,
            // while still terminating an outstanding RPC when the peer closes.
            () = &mut incoming_closed => break Err(BridgeError::SessionClosed),

            () = &mut cancelled, if !already_cancelled => {
                already_cancelled = true;
                turn.cancel_and_drain(&pending_permissions);
                let _ = cx.send_notification(
                    agent_client_protocol::schema::v1::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
                cancel_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + cancel_grace_timeout);
            }

            // If the SSE consumer drops, eagerly cancel so the agent
            // stops doing work nobody will read.
            () = events_tx.closed(), if !already_cancelled => {
                already_cancelled = true;
                turn.cancel_and_drain(&pending_permissions);
                let _ = cx.send_notification(
                    agent_client_protocol::schema::v1::CancelNotification::new(
                        session_id.clone(),
                    ),
                );
                cancel_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + cancel_grace_timeout);
            }

            () = &mut cancel_deadline, if already_cancelled => {
                tracing::warn!(
                    ?cancel_grace_timeout,
                    "agent did not acknowledge session/cancel within grace window"
                );
                break Err(BridgeError::CancelGraceExpired(cancel_grace_timeout));
            }
        }
    };

    let response = response?;
    Ok(response.stop_reason)
}
