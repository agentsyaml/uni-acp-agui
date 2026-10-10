use super::*;

/// Agent that advertises a `SessionModeState` and select/value-id mode/model
/// config options in `NewSessionResponse`. It accepts the stable
/// `session/set_config_option` request and emits update notifications.
///
/// Used to exercise the bridge's mode/model discovery + switch surface
/// (`SessionInit` event, `/session/set-mode`, `/session/set-config-option`,
/// the compatibility `/session/set-model` alias, and `/session/init`).
///
/// The agent records the most recent set request so tests can assert it
/// flowed through. Concurrency: the inner `Mutex`es are tiny and only
/// touched on the dispatch loop, so contention is irrelevant.
pub async fn run_modes_models_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ConfigOptionUpdate, CurrentModeUpdate, SessionConfigKind, SessionConfigOption,
        SessionConfigOptionCategory, SessionConfigSelect, SessionConfigSelectOption, SessionMode,
        SessionModeState, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
        SetSessionModeRequest, SetSessionModeResponse,
    };
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let current_mode = std::sync::Arc::new(Mutex::new("ask".to_string()));
    let current_model = std::sync::Arc::new(Mutex::new("gpt-4o-mini".to_string()));

    let cm_for_new = current_mode.clone();
    let cmod_for_new = current_model.clone();
    let cm_for_set_mode = current_mode.clone();
    let cm_for_set_config = current_mode.clone();
    let cmod_for_set_config = current_model.clone();

    fn config_options_for(mode: &str, model: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    mode.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("architect", "Architect"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    model.to_string(),
                    vec![
                        SessionConfigSelectOption::new("gpt-4o-mini", "GPT-4o mini"),
                        SessionConfigSelectOption::new("gpt-4o", "GPT-4o"),
                        SessionConfigSelectOption::new("claude-sonnet", "Claude Sonnet"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    Agent
        .builder()
        .name("agui-bridge-modes-models-test")
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
            {
                let cm = cm_for_new.clone();
                let cmod = cmod_for_new.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    let modes = SessionModeState::new(
                        cm.lock().expect("mode poisoned").clone(),
                        vec![
                            SessionMode::new("ask", "Ask")
                                .description("Read-only conversational mode".to_string()),
                            SessionMode::new("architect", "Architect")
                                .description("Plan-and-design mode".to_string()),
                            SessionMode::new("code", "Code")
                                .description("Edit-the-codebase mode".to_string()),
                        ],
                    );
                    let mode = cm.lock().expect("mode poisoned").clone();
                    let model = cmod.lock().expect("model poisoned").clone();
                    let response =
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .modes(modes)
                            .config_options(config_options_for(&mode, &model));
                    responder.respond(response)
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cm = cm_for_set_config.clone();
                let cmod = cmod_for_set_config.clone();
                async move |req: SetSessionConfigOptionRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let config_id = req.config_id.0.to_string();
                    let value = req
                        .value
                        .as_value_id()
                        .map(|id| id.0.to_string())
                        .unwrap_or_default();
                    match config_id.as_str() {
                        "mode" if ["ask", "architect", "code"].contains(&value.as_str()) => {
                            *cm.lock().expect("mode poisoned") = value;
                        }
                        "model"
                            if ["gpt-4o-mini", "gpt-4o", "claude-sonnet"]
                                .contains(&value.as_str()) =>
                        {
                            *cmod.lock().expect("model poisoned") = value;
                        }
                        _ => {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "unknown config option value",
                                ),
                            );
                        }
                    }
                    let mode = cm.lock().expect("mode poisoned").clone();
                    let model = cmod.lock().expect("model poisoned").clone();
                    let options = config_options_for(&mode, &model);
                    let _ = cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(options.clone())),
                    ));
                    responder.respond(SetSessionConfigOptionResponse::new(options))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cm = cm_for_set_mode.clone();
                async move |req: SetSessionModeRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let mode_id = req.mode_id.0.to_string();
                    // Reject unknown modes so the bridge can surface a 422.
                    if !["ask", "architect", "code"].contains(&mode_id.as_str()) {
                        return responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "unknown mode_id: {mode_id}"
                            )),
                        );
                    }
                    *cm.lock().expect("mode poisoned") = mode_id.clone();
                    // Notify the bridge so its `init_state` cache and the
                    // SessionInit emitted on the next prompt reflect the
                    // change. This also gives translator coverage of the
                    // `CurrentModeUpdate → agent:mode_update` path under a
                    // real `set_mode` flow.
                    let _ = cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(mode_id)),
                    ));
                    responder.respond(SetSessionModeResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                // Keep a prompt in flight long enough for setting tests to
                // exercise the actor's serial command queue.
                tokio::time::sleep(Duration::from_millis(250)).await;
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("ok"),
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

/// Agent that advertises legacy modes alongside a non-mode config snapshot.
/// The bridge must use `session/set_mode` instead of assuming every non-empty
/// `config_options` list contains a mode option.
pub async fn run_mixed_mode_capabilities_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CurrentModeUpdate, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelect, SessionConfigSelectOption, SessionMode, SessionModeState,
        SetSessionModeRequest, SetSessionModeResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let current_mode = std::sync::Arc::new(Mutex::new("ask".to_string()));
    let current_mode_for_new = current_mode.clone();
    let current_mode_for_set = current_mode.clone();

    Agent
        .builder()
        .name("agui-bridge-mixed-mode-capabilities-test")
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
            {
                let current_mode = current_mode_for_new.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    let mode = current_mode.lock().expect("mode poisoned").clone();
                    let modes = SessionModeState::new(
                        mode,
                        vec![
                            SessionMode::new("ask", "Ask"),
                            SessionMode::new("code", "Code"),
                        ],
                    );
                    let model = SessionConfigOption::new(
                        "model",
                        "Model",
                        SessionConfigKind::Select(SessionConfigSelect::new(
                            "gpt-4o-mini",
                            vec![SessionConfigSelectOption::new("gpt-4o-mini", "GPT-4o mini")],
                        )),
                    )
                    .category(SessionConfigOptionCategory::Model);
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .modes(modes)
                            .config_options(vec![model]),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let current_mode = current_mode_for_set.clone();
                async move |req: SetSessionModeRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let mode = req.mode_id.0.to_string();
                    if !["ask", "code"].contains(&mode.as_str()) {
                        return responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "unknown mode_id: {mode}"
                            )),
                        );
                    }
                    *current_mode.lock().expect("mode poisoned") = mode.clone();
                    cx.send_notification(SessionNotification::new(
                        req.session_id,
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(mode)),
                    ))?;
                    responder.respond(SetSessionModeResponse::new())
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
                        TextContent::new("mixed mode ready"),
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
