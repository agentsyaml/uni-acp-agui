use super::*;

fn permission_request(session_id: SessionId, tool_call_id: &str) -> RequestPermissionRequest {
    let fields = ToolCallUpdateFields::new().title(tool_call_id.to_string());
    RequestPermissionRequest::new(
        session_id,
        ToolCallUpdate::new(tool_call_id.to_string(), fields),
        vec![
            PermissionOption::new(
                PermissionOptionId::new("allow"),
                "Allow".to_string(),
                PermissionOptionKind::AllowOnce,
            ),
            PermissionOption::new(
                PermissionOptionId::new("deny"),
                "Deny".to_string(),
                PermissionOptionKind::RejectOnce,
            ),
        ],
    )
}

/// Agent that issues a `requestPermission` request to the client, then
/// completes the prompt based on the outcome. Exercises the bridge's
/// permission handler (notification → policy → response) end-to-end.
///
/// Uses `SentRequest::on_receiving_ok_result` (the SDK-recommended pattern
/// for chaining requests inside a handler) instead of `block_task`, which
/// would deadlock the dispatch loop.
pub async fn run_request_permission_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-request-permission-test")
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
                let tc_update = ToolCallUpdate::new(
                    "tc-1",
                    ToolCallUpdateFields::new().title("Read file".to_string()),
                );
                let options = vec![
                    PermissionOption::new(
                        PermissionOptionId::new("allow"),
                        "Allow".to_string(),
                        PermissionOptionKind::AllowOnce,
                    ),
                    PermissionOption::new(
                        PermissionOptionId::new("deny"),
                        "Deny".to_string(),
                        PermissionOptionKind::RejectOnce,
                    ),
                ];
                let perm_req =
                    RequestPermissionRequest::new(req.session_id.clone(), tc_update, options);

                // SDK-recommended pattern: schedule a task that runs when the
                // bridge's response arrives, *without* blocking the dispatch
                // loop (which would deadlock — the response can't be received
                // while the handler is awaiting it).
                cx.send_request(perm_req)
                    .on_receiving_result(async move |outcome| match outcome {
                        Ok(_resp) => responder.respond(PromptResponse::new(StopReason::EndTurn)),
                        Err(e) => responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "permission request failed: {e}"
                            )),
                        ),
                    })
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

/// Agent fixture that keeps two permission requests pending at once and only
/// completes the prompt after both responses arrive.
pub async fn run_multiple_pending_permission_agent(
    stream: DuplexStream,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-multiple-permissions-test")
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
                let session_id = req.session_id;
                let first = cx.send_request(permission_request(session_id.clone(), "tc-1"));
                let second = cx.send_request(permission_request(session_id.clone(), "tc-2"));
                let cx_for_finish = cx.clone();
                cx.spawn(async move {
                    let _ = first.block_task().await;
                    let _ = second.block_task().await;
                    cx_for_finish.send_notification(SessionNotification::new(
                        session_id,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("cancel tail"),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::Cancelled))
                })
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

/// Agent fixture that sends a second permission request only after the first
/// one has been answered. Tests cancel after the first interrupt; the second
/// request therefore races directly with post-cancel dispatch and must be
/// answered cancelled without entering the pending map.
pub async fn run_permission_after_cancel_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-permission-after-cancel-test")
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
                let session_id = req.session_id;
                let second = permission_request(session_id.clone(), "tc-after-cancel");
                let cx_after_first = cx.clone();
                cx.send_request(permission_request(session_id, "tc-before-cancel"))
                    .on_receiving_result(async move |_first| {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        cx_after_first.send_request(second).on_receiving_result(
                            async move |_second| {
                                responder.respond(PromptResponse::new(StopReason::Cancelled))
                            },
                        )
                    })
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
