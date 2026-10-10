use std::sync::Arc;

use super::{
    AcpSessionHandle, AgUiResult, BridgeStreamItem, Event, PromptGuard, PromptStream,
    RetiredSseDrain, RunAdmissionGuard, SSE_KEEPALIVE_INTERVAL, Translator, factory,
    keepalive_event, run_error_with_code, send_history_sse, session_init_event_with_config,
    stop_reason_terminal_event,
};
use futures::stream::{BoxStream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

/// Simplified event stream for a "resume bootstrap" run: emit `RUN_STARTED`,
/// translate the replayed history updates into AG-UI events, then emit the
/// terminal event reported by the history drain. No agent prompt is issued;
/// no frontend-tool routing is needed (history replay carries no live tool
/// calls). Both guards keep the session and thread admission alive until the
/// stream terminates.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_history_stream(
    thread_id: String,
    run_id: String,
    drain_stream: PromptStream,
    translated_buffer: usize,
    slow_consumer_timeout: std::time::Duration,
    session: Arc<AcpSessionHandle>,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    build_history_stream_with_keepalive(
        thread_id,
        run_id,
        drain_stream,
        translated_buffer,
        slow_consumer_timeout,
        session,
        prompt_guard,
        run_guard,
        SSE_KEEPALIVE_INTERVAL,
    )
}

// ponytail: see build_event_stream_with_keepalive for the parameter note.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_history_stream_with_keepalive(
    thread_id: String,
    run_id: String,
    drain_stream: PromptStream,
    translated_buffer: usize,
    slow_consumer_timeout: std::time::Duration,
    session: Arc<AcpSessionHandle>,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
    keepalive_interval: std::time::Duration,
) -> BoxStream<'static, AgUiResult<Event>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);

    tokio::spawn(async move {
        let _prompt_guard = prompt_guard;
        let _run_guard = run_guard;
        let mut retired = RetiredSseDrain::default();
        if send_history_sse(
            &tx,
            Ok(factory::run_started(thread_id.clone(), run_id.clone())),
            slow_consumer_timeout,
            &thread_id,
            &run_id,
            &session,
            &mut retired,
        )
        .await
        .is_err()
        {
            return;
        }

        let PromptStream {
            mut events,
            finished,
        } = drain_stream;
        let mut translator = Translator::new();
        // Keepalive starts one full interval after creation (so after the
        // RUN_STARTED send above): `interval`'s first `tick()` completes
        // immediately, which would emit a stray frame right after the run
        // header.
        let mut keepalive = tokio::time::interval_at(
            tokio::time::Instant::now() + keepalive_interval,
            keepalive_interval,
        );
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut drain_error: Option<String> = None;
        loop {
            let item = tokio::select! {
                item = events.recv() => item,
                // SSE keepalive for the history replay; see
                // `SSE_KEEPALIVE_INTERVAL`. Loops back without consuming a
                // stream item.
                _ = keepalive.tick() => {
                    if send_history_sse(&tx, Ok(keepalive_event()), slow_consumer_timeout, &thread_id, &run_id, &session, &mut retired)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                () = tx.closed() => return,
            };
            let Some(item) = item else {
                break;
            };
            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if send_history_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &thread_id,
                            &run_id,
                            &session,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                }
                BridgeStreamItem::SessionInit {
                    modes,
                    models,
                    config_options,
                } => {
                    let ev = session_init_event_with_config(
                        modes.as_ref(),
                        models.as_ref(),
                        config_options.as_deref(),
                    );
                    if send_history_sse(
                        &tx,
                        Ok(ev),
                        slow_consumer_timeout,
                        &thread_id,
                        &run_id,
                        &session,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                }
                BridgeStreamItem::Finished { .. } => break,
                BridgeStreamItem::RunError { message } => {
                    drain_error = Some(message);
                    break;
                }
                // History replay carries no interrupts or frontend tool
                // calls; ignore those variants defensively.
                _ => {}
            }
        }

        // Flush all open messages before the single history terminal event.
        for ev in translator.flush() {
            if send_history_sse(
                &tx,
                Ok(ev),
                slow_consumer_timeout,
                &thread_id,
                &run_id,
                &session,
                &mut retired,
            )
            .await
            .is_err()
            {
                return;
            }
        }

        let terminal = if let Some(message) = drain_error {
            run_error_with_code("ACP_HISTORY_DRAIN_ERROR", message)
        } else {
            match tokio::select! {
                result = finished => result,
                () = tx.closed() => return,
            } {
                Ok(Ok(stop_reason)) => {
                    stop_reason_terminal_event(thread_id.clone(), run_id.clone(), stop_reason)
                }
                Ok(Err(error)) => run_error_with_code(
                    "ACP_HISTORY_DRAIN_ERROR",
                    format!("acp history drain failed: {error}"),
                ),
                Err(_) => run_error_with_code(
                    "ACP_HISTORY_DRAIN_CLOSED",
                    "ACP history drain channel closed before completion",
                ),
            }
        };
        let _ = send_history_sse(
            &tx,
            Ok(terminal),
            slow_consumer_timeout,
            &thread_id,
            &run_id,
            &session,
            &mut retired,
        )
        .await;
    });

    ReceiverStream::new(rx).boxed()
}
