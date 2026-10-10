use super::*;

/// Agent that emits up to 1000 text chunks at 50ms intervals (~50s total),
/// then responds with `EndTurn`. Exits the loop early if `send_notification`
/// fails (i.e. the consumer dropped). Used to test client-disconnect handling:
/// when the HTTP/SSE consumer drops, the bridge should ideally cancel the
/// session — see audit finding §6.
///
/// Tests should wrap calls in `tokio::time::timeout` with a budget shorter
/// than 50 seconds.
pub async fn run_long_running_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-long-running-test")
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
                for i in 0..1000u32 {
                    let chunk_res = cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!("chunk {i} ")),
                        ))),
                    ));
                    if chunk_res.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
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

/// Agent that emits `count` text chunks, each carrying its own ordinal
/// (`"<i>;"`), back-to-back with no inter-chunk delay, then returns
/// `EndTurn`. Unlike [`run_long_running_agent`] (which paces at 50ms and is
/// meant for disconnect tests), this fixture maximises throughput so tests
/// can assert the bridge preserves chunk ordering and loses none of them
/// under load.
///
/// The ordinal-with-separator format (`"0;1;2;..."` once concatenated) lets
/// a test reconstruct the delivered sequence from the SSE body and assert it
/// is exactly `0..count` in order.
pub async fn run_counting_agent(stream: DuplexStream, count: u32) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-counting-test")
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
                for i in 0..count {
                    cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!("{i};")),
                        ))),
                    ))?;
                }
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
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
