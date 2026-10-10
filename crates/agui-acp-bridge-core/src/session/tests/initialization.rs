use super::*;

#[derive(Debug)]
pub(super) struct DenyPolicy;

#[async_trait::async_trait]
impl crate::policy::PermissionPolicy for DenyPolicy {
    async fn decide(
        &self,
        _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
    ) -> PermissionDecision {
        PermissionDecision::Deny
    }
}

pub(super) async fn run_unsupported_client_methods_agent(
    stream: tokio::io::DuplexStream,
    unsupported: Arc<AtomicUsize>,
    capabilities_ok: Arc<AtomicBool>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, CreateTerminalRequest, InitializeResponse, KillTerminalRequest,
        NewSessionRequest, NewSessionResponse, PromptResponse, ReadTextFileRequest,
        ReleaseTerminalRequest, TerminalId, TerminalOutputRequest, WaitForTerminalExitRequest,
        WriteTextFileRequest,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-unsupported-client-methods-test")
        .on_receive_request(
            {
                let capabilities_ok = capabilities_ok.clone();
                async move |req: InitializeRequest, responder, _cx| {
                    capabilities_ok.store(
                        !req.client_capabilities.fs.read_text_file
                            && !req.client_capabilities.fs.write_text_file
                            && !req.client_capabilities.terminal,
                        Ordering::SeqCst,
                    );
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from("test-session")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let unsupported_for_handler = unsupported.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let session_id = req.session_id;
                    let terminal_id = TerminalId::new("unsupported-test-terminal");
                    let unsupported = unsupported_for_handler.clone();
                    let cx_for_requests = cx.clone();
                    cx.spawn(async move {
                        let read = cx_for_requests
                            .send_request(ReadTextFileRequest::new(
                                session_id.clone(),
                                "/tmp/unsupported.txt",
                            ))
                            .block_task()
                            .await;
                        if matches!(
                            read,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let write = cx_for_requests
                            .send_request(WriteTextFileRequest::new(
                                session_id.clone(),
                                "/tmp/unsupported.txt",
                                "probe",
                            ))
                            .block_task()
                            .await;
                        if matches!(
                            write,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let create = cx_for_requests
                            .send_request(CreateTerminalRequest::new(session_id.clone(), "true"))
                            .block_task()
                            .await;
                        if matches!(
                            create,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let output = cx_for_requests
                            .send_request(TerminalOutputRequest::new(
                                session_id.clone(),
                                terminal_id.clone(),
                            ))
                            .block_task()
                            .await;
                        if matches!(
                            output,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let wait = cx_for_requests
                            .send_request(WaitForTerminalExitRequest::new(
                                session_id.clone(),
                                terminal_id.clone(),
                            ))
                            .block_task()
                            .await;
                        if matches!(
                            wait,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let kill = cx_for_requests
                            .send_request(KillTerminalRequest::new(
                                session_id.clone(),
                                terminal_id.clone(),
                            ))
                            .block_task()
                            .await;
                        if matches!(
                            kill,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        let release = cx_for_requests
                            .send_request(ReleaseTerminalRequest::new(session_id, terminal_id))
                            .block_task()
                            .await;
                        if matches!(
                            release,
                            Err(error)
                                if matches!(
                                    error.code,
                                    agent_client_protocol::ErrorCode::MethodNotFound
                                )
                        ) {
                            unsupported.fetch_add(1, Ordering::SeqCst);
                        }

                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    })
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::util::internal_error(
                            "unhandled request",
                        )),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
#[test]
fn successful_setting_ack_send_failure_marks_session_unusable() {
    let unusable = AtomicBool::new(false);
    let (ack, receiver) = oneshot::channel();
    drop(receiver);

    // `Ok(())` models a completed ACP setting RPC whose caller vanished
    // before the actor could deliver the result.
    assert!(!send_setting_ack(ack, Ok(()), &unusable));
    assert!(unusable.load(Ordering::Acquire));
}

#[test]
fn load_init_state_merges_replayed_capabilities_with_partial_response() {
    use agent_client_protocol::schema::v1::{
        LoadSessionResponse, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelect, SessionConfigSelectOption, SessionMode,
    };

    let replayed_options = vec![
        SessionConfigOption::new(
            "mode",
            "Mode",
            SessionConfigKind::Select(SessionConfigSelect::new(
                "replayed-mode",
                vec![SessionConfigSelectOption::new(
                    "replayed-mode",
                    "Replayed mode",
                )],
            )),
        )
        .category(SessionConfigOptionCategory::Mode),
        SessionConfigOption::new(
            "model",
            "Model",
            SessionConfigKind::Select(SessionConfigSelect::new(
                "replayed-model",
                vec![SessionConfigSelectOption::new(
                    "replayed-model",
                    "Replayed model",
                )],
            )),
        )
        .category(SessionConfigOptionCategory::Model),
    ];
    let replayed_mode = SessionModeState::new(
        "replayed-mode",
        vec![SessionMode::new("replayed-mode", "Replayed mode")],
    );
    let replayed = SessionInitState {
        modes: Some(modes_from_state(&replayed_mode)),
        #[cfg(feature = "unstable_session_model")]
        models: models_from_config_options(&replayed_options),
        #[cfg(not(feature = "unstable_session_model"))]
        models: None,
        config_options: Some(replayed_options.clone()),
    };

    let response_mode = SessionModeState::new(
        "response-mode",
        vec![SessionMode::new("response-mode", "Response mode")],
    );
    let merged = init_state_from_load(&LoadSessionResponse::new().modes(response_mode), &replayed);
    assert_eq!(
        merged
            .modes
            .as_ref()
            .map(|modes| modes.current_mode_id.as_str()),
        Some("response-mode")
    );
    assert_eq!(merged.config_options, Some(replayed_options.clone()));
    #[cfg(feature = "unstable_session_model")]
    assert_eq!(
        merged
            .models
            .as_ref()
            .map(|models| models.current_model_id.as_str()),
        Some("replayed-model")
    );

    let response_options = vec![
        SessionConfigOption::new(
            "model",
            "Model",
            SessionConfigKind::Select(SessionConfigSelect::new(
                "response-model",
                vec![SessionConfigSelectOption::new(
                    "response-model",
                    "Response model",
                )],
            )),
        )
        .category(SessionConfigOptionCategory::Model),
    ];
    let merged = init_state_from_load(
        &LoadSessionResponse::new().config_options(response_options.clone()),
        &replayed,
    );
    assert_eq!(
        merged
            .modes
            .as_ref()
            .map(|modes| modes.current_mode_id.as_str()),
        Some("replayed-mode")
    );
    assert_eq!(merged.config_options, Some(response_options));
    #[cfg(feature = "unstable_session_model")]
    assert_eq!(
        merged
            .models
            .as_ref()
            .map(|models| models.current_model_id.as_str()),
        Some("response-model")
    );
}

#[test]
fn mcp_http_server_propagates_authorization_header() {
    let server = mcp_http_server(
        "http://127.0.0.1:8080/mcp/thread".into(),
        vec![HttpHeader::new("Authorization", "Bearer test-token")],
    );
    let McpServer::Http(server) = server else {
        unreachable!("helper must build an HTTP MCP server");
    };
    assert_eq!(server.headers.len(), 1);
    assert_eq!(server.headers[0].name, "Authorization");
    assert_eq!(server.headers[0].value, "Bearer test-token");
}
