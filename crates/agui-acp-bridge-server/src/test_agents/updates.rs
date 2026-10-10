use super::*;

/// Agent that increments a per-session turn counter and echoes it back as
/// `"turn N: <prompt>"`. Used to verify that same `thread_id` reuses the same
/// `SessionId` across runs (per `BridgeAppState::session_for` cache).
pub async fn run_stateful_session_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let counter: std::sync::Arc<Mutex<std::collections::HashMap<String, u32>>> =
        std::sync::Arc::new(Mutex::new(std::collections::HashMap::new()));
    let counter_for_prompt = counter.clone();

    Agent
        .builder()
        .name("agui-bridge-stateful-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let prompt_text = req
                    .prompt
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text(t) => Some(t.text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let session_key: String = req.session_id.clone().0.to_string();
                let turn = {
                    let mut guard = counter_for_prompt.lock().expect("counter poisoned");
                    let entry = guard.entry(session_key).or_insert(0);
                    *entry += 1;
                    *entry
                };

                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(format!("turn {turn}: {prompt_text}")),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that streams a heterogeneous set of `SessionUpdate` variants in a
/// single turn:
///
///   1. AgentThoughtChunk (text)
///   2. Plan (with two entries)
///   3. AgentMessageChunk (text)        ← only this should produce TEXT_MESSAGE_*
///   4. UserMessageChunk (text)         ← still goes through translator
///   5. AgentMessageChunk (image)       ← non-text → RawEvent
///
/// Verifies the translator's handling of the full SessionUpdate enum surface
/// per `translation.rs`.
pub async fn run_mixed_updates_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-mixed-updates-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("thinking..."),
                    ))),
                ))?;

                let plan = Plan::new(vec![
                    PlanEntry::new(
                        "step one",
                        PlanEntryPriority::High,
                        PlanEntryStatus::Pending,
                    ),
                    PlanEntry::new(
                        "step two",
                        PlanEntryPriority::Medium,
                        PlanEntryStatus::Pending,
                    ),
                ]);
                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::Plan(plan),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("hello "),
                    ))),
                ))?;
                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("world"),
                    ))),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::UserMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("(echoed user)"),
                    ))),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Image(
                        ImageContent::new("aGVsbG8=", "image/png"),
                    ))),
                ))?;

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
