use super::*;

/// Agent fixture whose slow prompt observes `session/cancel` and returns
/// `Cancelled`. It distinguishes cancelling the active turn from cancelling a
/// queued turn in the bridge's turn-identity tests.
pub async fn run_cancel_aware_slow_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::CancelNotification;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_for_notification = cancelled.clone();
    let cancelled_for_prompt = cancelled.clone();

    Agent
        .builder()
        .name("agui-bridge-cancel-aware-slow-test")
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
        .on_receive_notification(
            async move |_req: CancelNotification, _cx| {
                cancelled_for_notification.store(true, Ordering::Release);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                for _ in 0..100 {
                    if cancelled_for_prompt.load(Ordering::Acquire) {
                        return responder.respond(PromptResponse::new(StopReason::Cancelled));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("active turn completed"),
                    ))),
                ))?;
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

/// Agent fixture that never returns the prompt response. It is used to verify
/// the bridge's cancel grace timeout and session eviction path.
pub async fn run_unresponsive_cancel_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-unresponsive-cancel-test")
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
            async move |_req: PromptRequest,
                        _responder,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
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
