use super::*;

/// `RunHandler` that translates each AG-UI POST into one ACP prompt turn.
#[derive(Clone, Debug)]
pub(crate) struct BridgeHandler {
    state: BridgeAppState,
}

impl BridgeHandler {
    #[must_use]
    pub fn new(state: BridgeAppState) -> Self {
        Self { state }
    }

    /// Build an SSE stream that replays a resumed session's loaded history
    /// (via [`AcpSessionHandle::drain_history`]) and then finishes, without
    /// prompting the agent. Used for "resume bootstrap" runs.
    pub(super) async fn stream_resume_history(
        &self,
        thread_id: String,
        run_id: String,
        entry: Arc<SessionEntry>,
        run_guard: RunAdmissionGuard,
    ) -> AgUiResult<BoxStream<'static, AgUiResult<Event>>> {
        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();
        let drain = match session.drain_history().await {
            Ok(stream) => stream,
            Err(err) => {
                drop(prompt_guard);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code(
                        "ACP_HISTORY_DRAIN_ERROR",
                        format!("acp drain_history failed: {err}"),
                    )),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
        };
        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        let slow_consumer_timeout = self.state.inner.config.slow_consumer_timeout;
        let session = prompt_guard.entry.handle.clone();
        Ok(build_history_stream(
            thread_id,
            run_id,
            drain,
            translated_buffer,
            slow_consumer_timeout,
            session,
            prompt_guard,
            run_guard,
        ))
    }
}

#[async_trait]
impl RunHandler for BridgeHandler {
    async fn handle(
        &self,
        input: RunAgentInput,
    ) -> AgUiResult<BoxStream<'static, AgUiResult<Event>>> {
        let thread_id = input.thread_id.clone();
        let run_id = input.run_id.clone();

        let Some(run_guard) = self.state.try_claim_run(&thread_id, &run_id) else {
            return Ok(stream::iter([
                Ok(factory::run_started(thread_id.clone(), run_id.clone())),
                Ok(run_error_with_code(
                    "CONCURRENT_RUN",
                    "another AG-UI run is active for this thread",
                )),
            ])
            .boxed());
        };

        // Diagnostic: every AG-UI run that reaches the bridge. `msg_count`
        // and `tail` let operators see whether a click produced a bootstrap
        // (connect) run vs a prompt run, and on which thread.
        tracing::info!(
            thread_id = %thread_id,
            run_id = %run_id,
            msg_count = input.messages.len(),
            "AG-UI run received"
        );

        if input.resume.is_some() {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "AGUI_RESUME_UNSUPPORTED",
                    "AG-UI resume is unsupported; use the bridge's private approval flow",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        let requested_resume = match acp_resume_session_id(&input.forwarded_props) {
            Ok(session_id) => session_id,
            Err(message) => {
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code(
                        "ACP_RESUME_SESSION_ID_REQUIRED",
                        message,
                    )),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
        };

        // Extract before touching the frontend-tool registry so unsupported
        // multipart input is rejected without creating any session-side state.
        let trailing = extract_trailing_user_text(&input.messages);
        if matches!(&trailing, TrailingUser::NonText) {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "UNSUPPORTED_INPUT",
                    "ACP bridge accepts text-only user input; multipart content is unsupported",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        // A cached explicit-resume mismatch is a rejected run, not speculative
        // admission. Check it before replacing the live thread's frontend
        // tools so the cached SessionEntry and registry remain untouched.
        let cached_resume_mismatch = requested_resume.as_ref().is_some_and(|session_id| {
            self.state
                .inner
                .sessions
                .get(&thread_id)
                .is_some_and(|entry| {
                    !entry.handle.is_unusable() && entry.handle.session_id() != session_id
                })
        });
        if cached_resume_mismatch {
            let evs = vec![
                Ok(factory::run_started(thread_id, run_id)),
                Ok(run_error_with_code(
                    "ACP_RESUME_FAILED",
                    "requested ACP session does not match the cached thread mapping",
                )),
            ];
            return Ok(guarded_event_stream(evs, run_guard));
        }

        // Push the per-run tools list into the frontend-tool registry so
        // the bridge's MCP endpoint serves the latest set when the agent
        // calls `tools/list`. Doing this BEFORE session_for ensures that a
        // first-run-on-thread session opens with mcp_servers visible AND
        // the registry already populated, so the agent's first tools/list
        // sees the intended tools.
        //
        // Caveat: most ACP agents call MCP `tools/list` once per session
        // and cache the result. If a later run on the same thread changes
        // the tool list, the agent may not pick the changes up. We detect
        // this and warn so operators can debug "why isn't my new tool
        // showing up?". `tools/listChanged` notifications could close
        // this gap; that's a future enhancement gated on agent support.
        let registry_entry = self.state.inner.frontend_tools.entry(&thread_id);
        let new_tools: Vec<FrontendToolDef> = input
            .tools
            .iter()
            .map(|t| FrontendToolDef {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.parameters.clone(),
            })
            .collect();
        let previous_names: std::collections::BTreeSet<String> =
            registry_entry.tools().into_iter().map(|t| t.name).collect();
        let new_names: std::collections::BTreeSet<String> =
            new_tools.iter().map(|t| t.name.clone()).collect();
        if !previous_names.is_empty() && previous_names != new_names {
            tracing::warn!(
                thread_id = %thread_id,
                added = ?new_names.difference(&previous_names).cloned().collect::<Vec<_>>(),
                removed = ?previous_names.difference(&new_names).cloned().collect::<Vec<_>>(),
                "frontend tool set changed mid-thread; agents that cache MCP \
                 tools/list (e.g. opencode) may not see the change. Use a \
                 fresh thread_id to force re-discovery."
            );
        }
        registry_entry.set_tools(new_tools.clone());

        // The trailing-only result above follows the ACP protocol semantics:
        // only a `User` message at the **tail** of `messages[]` represents a
        // fresh turn. When the tail is an `assistant` /
        // `tool` / `activity` message, the AG-UI runtime is reposting
        // already-handled history — typically because CopilotKit-style
        // `agentic_chat` callers auto-fire a follow-up run after every
        // tool turn so the LLM sees the tool result. Re-prompting the
        // ACP agent on these follow-ups would replay the prior turn
        // against a session whose history already contains the reply,
        // producing the well-known "every run loops the previous turn"
        // pathology. We instead emit a clean noop run pair.

        // Bridge-private resume is opt-in only. The cache-aware admission
        // method below reuses a live entry, but performs a strict
        // session/load on a cache miss (including an entry that becomes
        // unusable before admission).
        let wants_resume = requested_resume.is_some();

        let entry_result = if wants_resume {
            self.state
                .session_for_resume(&thread_id, requested_resume)
                .await
        } else {
            self.state.session_for(&thread_id).await
        };
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(SessionAdmissionError::ResumeUnsupported(message)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_UNSUPPORTED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::ResumeFailed(message)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_FAILED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::ResumeMappingMismatch(message)) => {
                let evs = vec![
                    Ok(factory::run_started(thread_id, run_id)),
                    Ok(run_error_with_code("ACP_RESUME_FAILED", message)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            Err(SessionAdmissionError::Http(error)) => {
                // The registry entry is created before session admission so
                // the first MCP tools/list sees the requested tool set. A
                // failed admission must remove that speculative state.
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                return Err(error);
            }
            Err(SessionAdmissionError::Capacity(reason)) => {
                self.state.inner.frontend_tools.drop_thread(&thread_id);
                let _ = CAPACITY_REJECTED.try_with(|flag| flag.set(true));
                return Err(AgUiError::http(
                    503,
                    format!("ACP_SESSION_CAPACITY: {reason}"),
                ));
            }
        };

        // `session_for_resume` may evict an unusable cached session. That
        // eviction drops the registry entry while this run still holds its
        // old Arc, so reacquire the map entry after admission and restore the
        // current tool list before the replacement session can call MCP.
        let registry_entry = self.state.inner.frontend_tools.entry(&thread_id);
        registry_entry.set_tools(new_tools);
        let user_text = match trailing {
            TrailingUser::Text(text) => text,
            TrailingUser::NonUserTail | TrailingUser::Empty => {
                // A bootstrap/connect run with no fresh user turn. If we just
                // resumed (loaded) the session, stream the replayed history so
                // the client sees its prior conversation. Otherwise emit a
                // clean noop pair.
                if wants_resume {
                    tracing::debug!(
                        thread_id = %thread_id,
                        run_id = %run_id,
                        "resume bootstrap run; streaming loaded history"
                    );
                    return self
                        .stream_resume_history(thread_id, run_id, entry, run_guard)
                        .await;
                }
                tracing::debug!(
                    thread_id = %thread_id,
                    run_id = %run_id,
                    "RunAgentInput.messages tail is not a fresh user-text message; emitting noop run"
                );
                let evs = vec![
                    Ok(factory::run_started(thread_id.clone(), run_id.clone())),
                    Ok(factory::run_finished(thread_id, run_id)),
                ];
                return Ok(guarded_event_stream(evs, run_guard));
            }
            TrailingUser::NonText => unreachable!("multipart input was rejected above"),
        };

        let translated_buffer = self.state.inner.config.event_buffer.max(1);
        let frontend_stream = install_frontend_sender(&registry_entry, translated_buffer);
        let prompt_guard = entry.enter_prompt();
        let session = entry.handle.clone();

        // Install the MCP route before submitting the ACP command. The agent
        // is allowed to issue tools/call as soon as it receives that command,
        // before prompt_with_turn() returns to this task.
        let prompt_result = session.prompt_with_turn(user_text).await;
        let stream = match prompt_result {
            Ok((prompt_stream, turn_id)) => build_event_stream(
                thread_id,
                run_id,
                prompt_stream,
                EventStreamContext {
                    session: session.clone(),
                    state: self.state.clone(),
                    registry_entry,
                    turn_id,
                    frontend_stream,
                },
                translated_buffer,
                prompt_guard,
                run_guard,
            ),
            Err(err) => {
                // Dropping the setup clears the sender and aborts any MCP
                // call that raced with prompt creation or request teardown.
                drop(frontend_stream);
                // Session is dead: evict it from the cache so the next
                // request on this thread_id rebuilds a fresh session
                // instead of replaying SessionClosed forever (until
                // idle_timeout).
                if matches!(err, agui_acp_bridge_core::BridgeError::SessionClosed)
                    || session.is_unusable()
                {
                    remove_session_if_same(&self.state.inner, &thread_id, &entry);
                }
                drop(prompt_guard);
                let error_event = match &err {
                    agui_acp_bridge_core::BridgeError::QueueCapacity { .. } => {
                        run_error_with_code("ACP_QUEUE_CAPACITY", err.to_string())
                    }
                    _ => acp_failure_run_error(&err),
                };
                let evs = vec![Ok(factory::run_started(thread_id, run_id)), Ok(error_event)];
                guarded_event_stream(evs, run_guard)
            }
        };
        Ok(stream)
    }
}
