use super::*;
/// Keepalive regression test: an idle stream must emit keepalives at
/// roughly the configured cadence AND must not flood — one frame per
/// interval, re-armed by every tick, over a window of several intervals.
/// Uses virtual time so cadence and flood checks are deterministic.
#[tokio::test(start_paused = true)]
async fn idle_event_stream_emits_keepalives_at_cadence_without_flooding() {
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
        Box::pin(crate::test_agents::run_unresponsive_cancel_agent(stream))
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            slow_consumer_timeout: Duration::from_secs(10),
            ..BridgeConfig::default()
        })
        .build();
    let session = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("keepalive"))
            .await
            .expect("session opens"),
    );
    let (prompt_stream, turn_id) = session
        .prompt_with_turn("never finishes soon")
        .await
        .expect("prompt opens");
    let entry = Arc::new(SessionEntry::new(session.clone(), None));
    let registry_entry = state.inner.frontend_tools.entry("keepalive");
    let frontend_stream = install_frontend_sender(&registry_entry, 16);
    let prompt_guard = entry.enter_prompt();
    let run_guard = state.try_claim_run("keepalive", "run-ka").expect("claim");

    let interval = keepalive_interval_for_tests();
    let mut stream = build_event_stream_with_keepalive(
        "keepalive".into(),
        "run-ka".into(),
        prompt_stream,
        EventStreamContext {
            session,
            state: state.clone(),
            registry_entry,
            turn_id,
            frontend_stream,
        },
        16,
        prompt_guard,
        run_guard,
        interval,
    );

    let started = stream.next().await.expect("run started").expect("event");
    assert!(matches!(started, Event::RunStarted(_)));
    let session_init = stream
        .next()
        .await
        .expect("session init event")
        .expect("event");
    assert!(matches!(
        session_init,
        Event::Custom(ref custom) if custom.name == "agent:session_init"
    ));
    let mut keepalive_count = 0;
    for _ in 0..8 {
        tokio::time::advance(interval).await;
        tokio::task::yield_now().await;
        let event = stream
            .next()
            .await
            .expect("stream remains open")
            .expect("event");
        assert!(matches!(event, Event::Custom(ref custom) if custom.name == "agent:keepalive"));
        keepalive_count += 1;
        assert!(
            futures::FutureExt::now_or_never(stream.next()).is_none(),
            "duplicate keepalive was immediately available"
        );
    }
    assert_eq!(keepalive_count, 8, "one keepalive per interval");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_suppressed_tool_progress_does_not_postpone_keepalives() {
    let interval = keepalive_interval_for_tests();
    let update_interval = interval / 4;
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        Box::pin(run_silent_tool_progress_agent(stream, update_interval, 32))
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            slow_consumer_timeout: Duration::from_secs(5),
            ..BridgeConfig::default()
        })
        .build();
    let session = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("silent-progress"))
            .await
            .expect("session opens"),
    );
    let (prompt_stream, turn_id) = session
        .prompt_with_turn("silent progress")
        .await
        .expect("prompt opens");
    let entry = Arc::new(SessionEntry::new(session.clone(), None));
    let registry_entry = state.inner.frontend_tools.entry("silent-progress");
    registry_entry.set_tools(vec![FrontendToolDef {
        name: "silent_tool".into(),
        description: String::new(),
        parameters: serde_json::json!({"type":"object"}),
    }]);
    let frontend_stream = install_frontend_sender(&registry_entry, 16);
    let prompt_guard = entry.enter_prompt();
    let run_guard = state
        .try_claim_run("silent-progress", "run-silent")
        .expect("claim run");
    let mut stream = build_event_stream_with_keepalive(
        "silent-progress".into(),
        "run-silent".into(),
        prompt_stream,
        EventStreamContext {
            session,
            state,
            registry_entry,
            turn_id,
            frontend_stream,
        },
        16,
        prompt_guard,
        run_guard,
        interval,
    );

    let mut keepalives = 0;
    tokio::time::timeout(Duration::from_secs(4), async {
        while let Some(event) = stream.next().await {
            let event = event.expect("event stream remains healthy");
            match event {
                Event::Custom(custom) if custom.name == "agent:keepalive" => {
                    keepalives += 1;
                }
                Event::Custom(custom) if custom.name == "agent:session_init" => {}
                Event::RunStarted(_) => {}
                Event::RunFinished(_) => break,
                other => panic!("suppressed progress unexpectedly emitted {other:?}"),
            }
        }
    })
    .await
    .expect("silent progress prompt completes on time");
    assert!(
        keepalives >= 3,
        "continuous suppressed updates postponed keepalives: saw {keepalives}"
    );
    assert!(
        keepalives <= 10,
        "keepalives flooded during suppressed updates: saw {keepalives}"
    );
}

async fn run_silent_tool_progress_agent(
    stream: tokio::io::DuplexStream,
    interval: Duration,
    updates: usize,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeRequest, InitializeResponse, NewSessionRequest,
        NewSessionResponse, PromptRequest, PromptResponse, SessionNotification, SessionUpdate,
        StopReason, ToolCall, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
    };
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
    agent_client_protocol::Agent
            .builder()
            .name("silent-suppressed-tool-progress-test")
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
                    responder.respond(NewSessionResponse::new(SessionId::from("silent-progress")))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |req: PromptRequest,
                            responder,
                            cx: agent_client_protocol::ConnectionTo<
                    agent_client_protocol::Client,
                >| {
                    let session_id = req.session_id;
                    cx.send_notification(SessionNotification::new(
                        session_id.clone(),
                        SessionUpdate::ToolCall(ToolCall::new(
                            ToolCallId::new("silent-call"),
                            "agui-acp-bridge_silent_tool",
                        )),
                    ))?;
                    for _ in 0..updates {
                        cx.send_notification(SessionNotification::new(
                            session_id.clone(),
                            SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                                ToolCallId::new("silent-call"),
                                ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
                            )),
                        ))?;
                        tokio::time::sleep(interval).await;
                    }
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_dispatch(
                async move |message: agent_client_protocol::Dispatch,
                            _cx: agent_client_protocol::ConnectionTo<
                    agent_client_protocol::Client,
                >| {
                    match message {
                        agent_client_protocol::Dispatch::Response(result, router) => {
                            router.route_with_result(result)
                        }
                        agent_client_protocol::Dispatch::Request(_, responder) => responder
                            .respond_with_error(agent_client_protocol::Error::method_not_found()),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }
                },
                agent_client_protocol::on_receive_dispatch!(),
            )
            .connect_to(transport)
            .await
            .map_err(BridgeError::Acp)
}

// ponytail: the wire shape stays pinned so a refactor cannot silently
// emit an unparseable keepalive frame; cadence itself is covered by the
// test above.
#[test]
fn keepalive_frame_is_a_null_value_custom_event() {
    let frame = agui_rs_encoder_ish();
    assert!(frame.contains("agent:keepalive"));
    assert!(frame.contains("\"value\":null"));
}

fn agui_rs_encoder_ish() -> String {
    let event = keepalive_event();
    serde_json::to_string(&event).expect("keepalive serializes")
}
