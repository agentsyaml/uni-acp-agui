use super::*;

/// Probe shared with the boolean config-option integration tests.
#[derive(Debug, Default, Clone)]
pub struct BooleanConfigProbe {
    initialize_boolean_capability: Arc<Mutex<Option<bool>>>,
    requests: Arc<Mutex<Vec<(String, SessionConfigOptionValue)>>>,
}

impl BooleanConfigProbe {
    #[must_use]
    pub fn initialize_boolean_capability(&self) -> bool {
        self.initialize_boolean_capability
            .lock()
            .expect("boolean capability probe poisoned")
            .unwrap_or(false)
    }

    #[must_use]
    pub fn requests(&self) -> Vec<(String, SessionConfigOptionValue)> {
        self.requests
            .lock()
            .expect("boolean config probe poisoned")
            .clone()
    }
}

/// Agent fixture for the stable typed session configuration values.
pub async fn run_boolean_config_agent(
    stream: DuplexStream,
    probe: Arc<BooleanConfigProbe>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigOption, SessionConfigSelectOption, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse,
    };

    fn options(enabled: bool, mode: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::boolean("enabled", "Enabled", enabled),
            SessionConfigOption::select(
                "mode",
                "Mode",
                mode.to_string(),
                vec![
                    SessionConfigSelectOption::new("ask", "Ask"),
                    SessionConfigSelectOption::new("code", "Code"),
                ],
            ),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let enabled = Arc::new(Mutex::new(false));
    let mode = Arc::new(Mutex::new("ask".to_string()));

    Agent
        .builder()
        .name("agui-bridge-boolean-config-test")
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: InitializeRequest, responder, _cx| {
                    let advertised = req
                        .client_capabilities
                        .session
                        .as_ref()
                        .and_then(|session| session.config_options.as_ref())
                        .and_then(|options| options.boolean.as_ref())
                        .is_some();
                    *probe
                        .initialize_boolean_capability
                        .lock()
                        .expect("boolean capability probe poisoned") = Some(advertised);
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let enabled = enabled.clone();
                let mode = mode.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .config_options(options(
                                *enabled.lock().expect("enabled value poisoned"),
                                &mode.lock().expect("mode value poisoned"),
                            )),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let probe = probe.clone();
                let enabled = enabled.clone();
                let mode = mode.clone();
                async move |req: SetSessionConfigOptionRequest,
                            responder,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    let config_id = req.config_id.0.to_string();
                    probe
                        .requests
                        .lock()
                        .expect("boolean config probe poisoned")
                        .push((config_id.clone(), req.value.clone()));
                    match (config_id.as_str(), req.value) {
                        ("enabled", SessionConfigOptionValue::Boolean { value }) => {
                            *enabled.lock().expect("enabled value poisoned") = value;
                        }
                        ("mode", SessionConfigOptionValue::ValueId { value }) => {
                            *mode.lock().expect("mode value poisoned") = value.0.to_string();
                        }
                        _ => {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "unexpected config option value",
                                ),
                            );
                        }
                    }
                    responder.respond(SetSessionConfigOptionResponse::new(options(
                        *enabled.lock().expect("enabled value poisoned"),
                        &mode.lock().expect("mode value poisoned"),
                    )))
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
                        TextContent::new("boolean config ready"),
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

/// Agent that emits a complete replacement `ConfigOptionUpdate` during the
/// prompt. Used to prove the bridge does not merge stale option snapshots.
pub async fn run_config_update_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ConfigOptionUpdate, SessionConfigKind, SessionConfigOption, SessionConfigSelect,
        SessionConfigSelectOption,
    };

    fn options(id: &str, value: &str) -> Vec<SessionConfigOption> {
        vec![SessionConfigOption::new(
            id.to_string(),
            "Config",
            SessionConfigKind::Select(SessionConfigSelect::new(
                value.to_string(),
                vec![SessionConfigSelectOption::new(value.to_string(), "Value")],
            )),
        )]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    Agent
        .builder()
        .name("agui-bridge-config-update-test")
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
                        .config_options(options("initial", "before")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(options(
                        "replacement",
                        "after",
                    ))),
                ))?;
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("config updated"),
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

/// Agent fixture whose setting RPC never responds. The bridge must bound the
/// actor-side wait and evict the now-uncertain session instead of reusing it.
pub async fn run_unresponsive_setting_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    };

    fn mode_options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-unresponsive-setting-test")
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
                        .config_options(mode_options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: SetSessionConfigOptionRequest, _responder, _cx| {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // The actor must close the connection before this response is
                // reached; keeping the branch typed makes the fixture's
                // behavior explicit without introducing a never type.
                let _ = SetSessionConfigOptionResponse::new(mode_options("code"));
                Ok(())
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
                        TextContent::new("setting fixture ready"),
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
