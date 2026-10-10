use super::*;

/// Agent fixture for proving discovered config validation happens before ACP.
/// Any setting request is a test failure; invalid HTTP values must be rejected
/// from the cached snapshot without entering this handler.
pub async fn run_rejecting_config_agent(
    stream: DuplexStream,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest,
    };
    use std::sync::atomic::Ordering;

    fn options() -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "ask",
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "model-a",
                    vec![
                        SessionConfigSelectOption::new("model-a", "Model A"),
                        SessionConfigSelectOption::new("model-b", "Model B"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-rejecting-config-test")
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
                responder.respond(
                    NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                        .config_options(options()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let calls = calls.clone();
                async move |_req: SetSessionConfigOptionRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("invalid config value reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("config validation ready"),
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

/// Agent fixture with no config snapshot or legacy mode capability. Any
/// setting request is a test failure; the bridge must reject it locally.
pub async fn run_rejecting_undiscovered_settings_agent(
    stream: DuplexStream,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{SetSessionConfigOptionRequest, SetSessionModeRequest};
    use std::sync::atomic::Ordering;

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-rejecting-undiscovered-settings-test")
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
            {
                let calls = calls.clone();
                async move |_req: SetSessionConfigOptionRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("undiscovered config option reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let calls = calls.clone();
                async move |_req: SetSessionModeRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("undiscovered legacy mode reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("undiscovered settings ready"),
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
