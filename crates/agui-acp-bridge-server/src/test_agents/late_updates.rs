use super::*;

/// Agent that emits a chunk, responds to prompt, **then** sends a late
/// notification AFTER the PromptResponse has been delivered.
///
/// Late notifications are spilled into the bridge's bounded spill buffer
/// (`session.rs` `SpillBuffer`) and rebroadcast at the start of the NEXT run
/// on the same thread, ahead of that run's own events. They never appear
/// retroactively in the run that already terminated (no events after the
/// terminal event). This agent emits its in-band chunk on every prompt, so
/// the draining run distinguishes the spilled update from turn-two's own
/// text.
pub async fn run_late_notification_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-late-notif-test")
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
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("in-band"),
                    ))),
                ))?;

                // 100ms delay ensures bridge clears its event slot (session.rs:165) before late notif arrives.
                let cx_clone = cx.clone();
                let sid_for_late = sid.clone();
                let _ = cx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = cx_clone.send_notification(SessionNotification::new(
                        sid_for_late,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("LATE-AFTER-FINISH"),
                        ))),
                    ));
                    Ok(())
                });

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

/// Agent that spills past the bridge's capacity: every prompt gets one
/// in-band chunk plus `count` LATE-AFTER-FINISH-<i> chunks after the prompt
/// response (matching `SpillBuffer`'s 32-entry cap when `count > 32`). Used
/// to prove overflow drops with a warning while keeping the session usable.
pub async fn run_late_notification_flood_agent(
    stream: DuplexStream,
    count: usize,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-late-notif-flood-test")
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
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("in-band"),
                    ))),
                ))?;

                let cx_clone = cx.clone();
                let sid_for_late = sid.clone();
                let _ = cx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    for index in 0..count {
                        let _ = cx_clone.send_notification(SessionNotification::new(
                            sid_for_late.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!(
                                    "LATE-AFTER-FINISH-{index}"
                                ))),
                            )),
                        ));
                    }
                    Ok(())
                });

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
