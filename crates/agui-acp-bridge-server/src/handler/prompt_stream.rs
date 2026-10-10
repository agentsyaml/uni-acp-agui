use std::sync::Arc;

use super::{
    AcpSessionHandle, AgUiResult, BridgeAppState, BridgeStreamItem, ClearOnDrop, Event,
    PromptGuard, PromptStream, RetiredSseDrain, RunAdmissionGuard, Translator, TurnId,
    acp_failure_run_error, factory, keepalive_event, remove_session_if_handle, send_prompt_sse,
    session_init_event_with_config, stop_reason_terminal_event,
};
use futures::stream::{BoxStream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub(super) struct EventStreamContext {
    pub(super) session: Arc<AcpSessionHandle>,
    pub(super) state: BridgeAppState,
    pub(super) registry_entry: Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    pub(super) turn_id: TurnId,
    /// The MCP channel and its conditional sender cleanup are installed
    /// before the ACP prompt command is submitted. This closes the first-tool
    /// window without changing the per-thread registry ownership rules.
    pub(super) frontend_stream: FrontendStreamSetup,
}

pub(super) struct FrontendStreamSetup {
    mcp_tool_rx: mpsc::Receiver<BridgeStreamItem>,
    clear_on_drop: ClearOnDrop,
}

pub(super) fn install_frontend_sender(
    registry_entry: &Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    translated_buffer: usize,
) -> FrontendStreamSetup {
    // The receiver is moved into the SSE task after prompt creation; the
    // bounded buffer absorbs a call made in that short handoff window.
    let (mcp_tool_tx, mcp_tool_rx) = mpsc::channel::<BridgeStreamItem>(translated_buffer);
    registry_entry.set_active_sender(Some(mcp_tool_tx.clone()));
    FrontendStreamSetup {
        mcp_tool_rx,
        clear_on_drop: ClearOnDrop {
            entry: registry_entry.clone(),
            sender: mcp_tool_tx,
        },
    }
}

// ponytail: the split-out interval parameter exists only so tests can shrink
// it to milliseconds; threading it through a config struct for that alone
// would touch every call site for no production benefit.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_event_stream_with_keepalive(
    thread_id: String,
    run_id: String,
    prompt_stream: PromptStream,
    context: EventStreamContext,
    translated_buffer: usize,
    prompt_guard: PromptGuard,
    run_guard: RunAdmissionGuard,
    keepalive_interval: std::time::Duration,
) -> BoxStream<'static, AgUiResult<Event>> {
    let EventStreamContext {
        session,
        state,
        registry_entry,
        turn_id,
        frontend_stream,
    } = context;
    let FrontendStreamSetup {
        mcp_tool_rx,
        clear_on_drop,
    } = frontend_stream;
    let (tx, rx) = tokio::sync::mpsc::channel::<AgUiResult<Event>>(translated_buffer);
    let slow_consumer_timeout = state.inner.config.slow_consumer_timeout;

    tokio::spawn(async move {
        // Hold the guard for the duration of the prompt so the reaper
        // sees `active_prompts > 0` and refuses to drop the session.
        let _prompt_guard = prompt_guard;
        let _run_guard = run_guard;
        // Clear the registry's active sender on exit so MCP requests that
        // arrive after the prompt finishes are rejected promptly instead
        // of silently parking forever. We clear *conditionally* — only if
        // the slot still holds the sender THIS run installed — so an
        // overlapping newer run on the same thread_id (page refresh, a
        // CopilotKit follow-up run, a reconnect) keeps its own sender and
        // its in-flight tool calls don't get stranded into a timeout.
        let _clear_on_drop = clear_on_drop;
        // `None` once the MCP channel closes mid-run: that arm is then
        // disabled in the select below while the rest of the loop keeps
        // draining ACP events until Finished/RunError/disconnect.
        let mut mcp_tool_rx = Some(mcp_tool_rx);
        let session_for_stream = session;
        // Measure idle time between successful SSE sends, not incoming ACP
        // updates: suppressed or cached progress may produce no output.
        // Heartbeats also reset the deadline after delivery so a past
        // deadline cannot flood the client.
        let mut keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
        let mut retired = RetiredSseDrain::default();

        if send_prompt_sse(
            &tx,
            Ok(factory::run_started(thread_id.clone(), run_id.clone())),
            slow_consumer_timeout,
            &session_for_stream,
            &thread_id,
            &run_id,
            turn_id,
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
        } = prompt_stream;
        let mut translator = Translator::new();
        // Tell the translator to suppress agent-side ToolCall echoes for
        // any tool we just registered. Most agents prefix MCP-sourced
        // tool names with `<server-name>_` when surfacing them on
        // session/update; we cover both spellings.
        let suppressed_titles: Vec<String> = registry_entry
            .tools()
            .into_iter()
            .flat_map(|t| {
                [
                    t.name.clone(),
                    format!(
                        "{prefix}_{name}",
                        prefix = agui_acp_bridge_core::MCP_SERVER_NAME,
                        name = t.name,
                    ),
                ]
            })
            .collect();
        translator.set_suppressed_titles(suppressed_titles);
        let mut errored: Option<String> = None;

        // Helper: when an SSE send fails the client has disconnected.
        // Cancel the in-flight ACP turn so the agent stops doing work
        // nobody is reading. The session actor's run_prompt_with_cancel
        // also detects the events channel being dropped, so this is a
        // belt-and-suspenders approach.
        let cancel_on_disconnect = |session: Arc<AcpSessionHandle>, turn_id: TurnId| {
            if let Err(e) = session.cancel_turn(turn_id) {
                tracing::warn!(error = %e, "failed to cancel turn after client disconnect");
            }
        };

        loop {
            // Multiplex: we pull from both the ACP-side events channel
            // (session updates) and the MCP-side tool-call channel until
            // ACP signals Finished/RunError. Either source produces
            // BridgeStreamItem values that we translate uniformly.
            //
            // We deliberately do NOT use `biased` here. With chatty agents
            // (opencode emits dozens of `agent_thought_chunk` per second
            // during reasoning), a biased select would starve the MCP
            // channel — the agent's `tools/call` would queue indefinitely
            // and time out on its end. Fair scheduling is required for
            // correctness.
            let item = tokio::select! {
                acp = events.recv() => {
                    match acp {
                        Some(it) => it,
                        None => break,
                    }
                },
                // Only while the MCP channel is still open. Once every
                // sender clone is gone mid-run (e.g. a newer overlapping
                // run took over the registry slot), we drop this arm and
                // keep servicing `events` + `tx.closed()` until the turn
                // reaches Finished/RunError or the client disconnects.
                // Breaking out here would silently discard the remaining
                // ACP updates for the rest of the turn and lose the
                // disconnect-cancellation path below.
                mcp = async { mcp_tool_rx.as_mut().unwrap().recv().await }, if mcp_tool_rx.is_some() => {
                    match mcp {
                        Some(it) => it,
                        None => {
                            mcp_tool_rx = None;
                            continue;
                        }
                    }
                },
                // SSE keepalive: an idle turn (agent inside a long tool
                // call) otherwise emits zero bytes and proxy/ALB idle
                // timeouts (~60s) kill the connection, discarding the
                // turn's work. See `SSE_KEEPALIVE_INTERVAL`. Re-arm after
                // a successful send so slow sends cannot flood the client.
                _ = tokio::time::sleep_until(keepalive_deadline) => {
                    if send_prompt_sse(
                        &tx,
                        Ok(keepalive_event()),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    continue;
                }
                // Detect client disconnect even while idle. When the SSE
                // consumer drops, `tx` closes. Without this branch the loop
                // would park on `events.recv()` / `mcp_tool_rx.recv()` and
                // only notice the dead client on the *next* event — which
                // never comes while the agent is parked awaiting a frontend
                // tool result. That would pin `active_prompts > 0` (so the
                // reaper can't release the session) until `frontend_tool_timeout`
                // fires — the root cause of sessions piling up after refreshes.
                () = tx.closed() => {
                    cancel_on_disconnect(session_for_stream.clone(), turn_id);
                    return;
                }
            };

            match item {
                BridgeStreamItem::Update(update) => {
                    for ev in translator.translate(update) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
                BridgeStreamItem::SessionInit {
                    modes,
                    models,
                    config_options,
                } => {
                    // Re-emitted by the session actor at the start of every
                    // prompt so reconnecting clients see the picker even on
                    // mid-thread runs. Always sent before any agent text.
                    let ev = session_init_event_with_config(
                        modes.as_ref(),
                        models.as_ref(),
                        config_options.as_deref(),
                    );
                    if send_prompt_sse(
                        &tx,
                        Ok(ev),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                }
                BridgeStreamItem::Finished { .. } => {
                    break;
                }
                BridgeStreamItem::RunError { message } => {
                    errored = Some(message);
                    break;
                }
                BridgeStreamItem::Interrupt { id, request } => {
                    // The session actor only emits `Interrupt` when the
                    // configured policy returned `Defer`. Emit a STATE_SNAPSHOT
                    // event so the frontend can render an approval dialog.
                    // The session actor is awaiting an external resolution
                    // via `BridgeAppState::resolve_permission` (typically
                    // surfaced over POST /approval). If the configured
                    // permission_timeout elapses, it falls back to deny.
                    let approval_state = serde_json::json!({
                        "approval": {
                            "pending": true,
                            "interruptId": id,
                            "toolName": request.tool_call.fields.title,
                            "options": request.options,
                        }
                    });
                    let event = Event::StateSnapshot(agui_rs_core::events::StateSnapshotEvent {
                        snapshot: approval_state,
                        base: agui_rs_core::events::BaseEventFields::default(),
                    });
                    if send_prompt_sse(
                        &tx,
                        Ok(event),
                        slow_consumer_timeout,
                        &session_for_stream,
                        &thread_id,
                        &run_id,
                        turn_id,
                        &mut retired,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                }
                BridgeStreamItem::FrontendToolCall {
                    tool_call_id,
                    tool_name,
                    arguments,
                } => {
                    // Frontend tool dispatched by the agent through our
                    // MCP endpoint. Use the bypass-suppression path on
                    // the translator so this call surfaces even though
                    // the same tool name is in the suppression set
                    // (which exists to drop the *agent-side echo* of
                    // the same call). Translation returns START, optional
                    // complete ARGS, and END together. SSE sends these as
                    // sequential frames, not atomically; a failed send
                    // returns immediately so a partial envelope is never
                    // flushed or re-ended.
                    for ev in translator.translate_frontend_tool_call(
                        tool_call_id,
                        tool_name,
                        Some(&arguments),
                    ) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
                BridgeStreamItem::FrontendToolEnd { tool_call_id } => {
                    // Compatibility path for legacy producers. Canonical
                    // FrontendToolCall items already closed their envelope,
                    // so translation returns no events for their ids.
                    for ev in translator.translate_frontend_tool_end(&tool_call_id) {
                        if send_prompt_sse(
                            &tx,
                            Ok(ev),
                            slow_consumer_timeout,
                            &session_for_stream,
                            &thread_id,
                            &run_id,
                            turn_id,
                            &mut retired,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                        keepalive_deadline = tokio::time::Instant::now() + keepalive_interval;
                    }
                }
            }
        }

        for ev in translator.flush() {
            if send_prompt_sse(
                &tx,
                Ok(ev),
                slow_consumer_timeout,
                &session_for_stream,
                &thread_id,
                &run_id,
                turn_id,
                &mut retired,
            )
            .await
            .is_err()
            {
                return;
            }
        }

        if session_for_stream.is_unusable() {
            remove_session_if_handle(&state.inner, &thread_id, &session_for_stream);
        }

        if let Some(msg) = errored {
            let _ = send_prompt_sse(
                &tx,
                Ok(factory::run_error(msg)),
                slow_consumer_timeout,
                &session_for_stream,
                &thread_id,
                &run_id,
                turn_id,
                &mut retired,
            )
            .await;
            return;
        }

        let finished_result = finished.await;
        if session_for_stream.is_unusable() {
            remove_session_if_handle(&state.inner, &thread_id, &session_for_stream);
        }

        let terminal = match finished_result {
            Ok(Ok(stop_reason)) => {
                stop_reason_terminal_event(thread_id.clone(), run_id.clone(), stop_reason)
            }
            Ok(Err(e)) => acp_failure_run_error(&e),
            Err(_) => factory::run_error("acp session dropped before finish"),
        };
        let _ = send_prompt_sse(
            &tx,
            Ok(terminal),
            slow_consumer_timeout,
            &session_for_stream,
            &thread_id,
            &run_id,
            turn_id,
            &mut retired,
        )
        .await;
    });

    ReceiverStream::new(rx).boxed()
}
