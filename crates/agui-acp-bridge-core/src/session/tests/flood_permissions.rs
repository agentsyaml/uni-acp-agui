use super::*;

/// Policy that defers every permission request to the AG-UI client.
#[derive(Debug)]
struct DeferPolicy;

#[async_trait::async_trait]
impl crate::policy::PermissionPolicy for DeferPolicy {
    async fn decide(
        &self,
        _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
    ) -> PermissionDecision {
        PermissionDecision::Defer {
            interrupt_id: "test-interrupt".into(),
        }
    }
}

/// Regression agent for the dispatch-loop stall (BUG 1): on prompt it
/// floods more `session/update` notifications than the per-prompt event
/// channel holds and then issues a `requestPermission` request. Before
/// the fix, the notification handler's blocking send filled the channel
/// and hung the SDK's single dispatch loop, so neither the
/// `session/prompt` response nor the permission request was ever routed.
async fn run_flooding_permission_agent(
    stream: tokio::io::DuplexStream,
    update_count: usize,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, ContentChunk, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PermissionOption, PermissionOptionId, PermissionOptionKind, PromptResponse, SessionUpdate,
        StopReason, TextContent, ToolCallId, ToolCallUpdate, ToolCallUpdateFields,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-flooding-permission-test")
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
                responder.respond(NewSessionResponse::new(SessionId::from("flood-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder: agent_client_protocol::Responder<
                agent_client_protocol::schema::v1::PromptResponse,
            >,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let session_id = req.session_id;
                // Spawn: awaiting the permission response inline here
                // would block the agent's own dispatch loop (which must
                // stay free to read that very response).
                let cx_for_task = cx.clone();
                let _ = cx.spawn(async move {
                    for i in 0..update_count {
                        cx_for_task.send_notification(SessionNotification::new(
                            session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!("chunk-{i} "))),
                            )),
                        ))?;
                    }
                    // Then ask for permission — this must be readable by
                    // the client even while the event channel is full.
                    cx_for_task
                        .send_request(RequestPermissionRequest::new(
                            session_id.clone(),
                            ToolCallUpdate::new(
                                ToolCallId::new("flood-tc"),
                                ToolCallUpdateFields::new().title("Flood permission"),
                            ),
                            vec![PermissionOption::new(
                                PermissionOptionId::new("allow-once"),
                                "Allow once",
                                PermissionOptionKind::AllowOnce,
                            )],
                        ))
                        .block_task()
                        .await?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                });
                Ok(())
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

/// Regression test for the dispatch-loop stall (BUG 1): the agent floods
/// 4x the per-prompt event channel's capacity with `session/update`s and
/// then issues a `requestPermission` mid-turn. Before the fix, the
/// notification handler's blocking send filled the channel and hung the
/// SDK's single sequential dispatch loop — the `session/prompt` response
/// and the permission request were never routed, so the turn never
/// completed. Now the mailbox delivers everything and the turn finishes.
#[tokio::test]
async fn flooding_updates_do_not_stall_dispatch_loop_and_permission_is_answered() {
    use agent_client_protocol::schema::v1::PermissionOptionId;

    // Default event_buffer is 64; flood well past it.
    const UPDATE_COUNT: usize = 256;

    let handle = spawn_in_process_session_with(
        SessionConfig {
            cwd: PathBuf::from("/"),
            policy: Arc::new(DeferPolicy),
            config: crate::config::BridgeConfig::default(),
            mcp_url: None,
            mcp_headers: Vec::new(),
            load_session_id: None,
        },
        move |stream| Box::pin(run_flooding_permission_agent(stream, UPDATE_COUNT)),
    )
    .await
    .expect("flooding session opens");

    let mut prompt = handle.prompt("flood").await.expect("prompt opens");

    // Consume like a slow-but-connected SSE consumer: drain events and,
    // when the deferred permission interrupt surfaces, answer it from the
    // "frontend". Before the fix the first recv() already timed out — the
    // stalled dispatch loop could not even route the prompt response.
    let mut updates = 0usize;
    let mut interrupts = 0usize;
    loop {
        let item = tokio::time::timeout(std::time::Duration::from_secs(10), prompt.events.recv())
            .await
            .expect("event arrives before timeout (dispatch loop must not stall)");
        let Some(item) = item else {
            break;
        };
        match item {
            BridgeStreamItem::Update(_) => updates += 1,
            BridgeStreamItem::Interrupt { .. } => {
                interrupts += 1;
                assert!(
                    handle.resolve_permission(
                        "test-interrupt",
                        PermissionDecision::Allow {
                            option_id: PermissionOptionId::new("allow-once"),
                        },
                    ),
                    "interrupt resolution must be accepted mid-turn"
                );
            }
            BridgeStreamItem::Finished { .. } => break,
            _ => {}
        }
    }
    assert!(
        interrupts >= 1,
        "the agent's requestPermission must surface as an Interrupt"
    );
    assert_eq!(updates, UPDATE_COUNT);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(10), prompt.finished)
            .await
            .expect("finished arrives before timeout")
            .expect("finished sender remains")
            .expect("prompt succeeds"),
        StopReason::EndTurn
    );
}

#[tokio::test]
async fn completed_turn_drains_deferred_permissions_before_next_turn() {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PermissionOption, PermissionOptionId, PermissionOptionKind, PromptResponse, ToolCallId,
        ToolCallUpdate, ToolCallUpdateFields,
    };

    // The current fixture uses a constant interrupt ID; unique request IDs
    // are needed here because all 80 callbacks remain pending together.
    #[derive(Debug)]
    struct UniqueDeferPolicy;
    #[async_trait::async_trait]
    impl crate::policy::PermissionPolicy for UniqueDeferPolicy {
        async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
            PermissionDecision::Defer {
                interrupt_id: request.tool_call.fields.title.clone().unwrap(),
            }
        }
    }
    let (start_tx, mut start_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();
    let (cancelled_tx, mut cancelled_rx) = mpsc::unbounded_channel::<(usize, usize, bool)>();
    let cfg = SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(UniqueDeferPolicy),
        config: crate::config::BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    };
    let handle = spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(async move {
                let (read, write) = tokio::io::split(stream);
                let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
                Agent.builder().name("completed-turn-permissions-test")
                    .on_receive_request(async move |req: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(req.protocol_version).agent_capabilities(AgentCapabilities::new()))
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_request(async move |_req: NewSessionRequest, responder, _cx| {
                        responder.respond(NewSessionResponse::new(SessionId::from("drain-test")))
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_request({
                        let start_tx = start_tx.clone();
                        let cancelled_tx = cancelled_tx.clone();
                        async move |req: PromptRequest, responder, cx: ConnectionTo<agent_client_protocol::Client>| {
                            let start_tx = start_tx.clone();
                            let cancelled_tx = cancelled_tx.clone();
                            let round = req.prompt.iter().filter_map(|b| match b { ContentBlock::Text(t) => t.text.parse::<usize>().ok(), _ => None }).next().unwrap();
                            let (go_tx, go_rx) = oneshot::channel(); start_tx.send(go_tx).unwrap();
                            let cx = cx.clone();
                            let spawn_cx = cx.clone();
                            spawn_cx.spawn(async move {
                                for i in 0..80 {
                                    let req = RequestPermissionRequest::new(req.session_id.clone(), ToolCallUpdate::new(ToolCallId::new(format!("{round}-{i}")), ToolCallUpdateFields::new().title(format!("{round}-{i}"))), vec![PermissionOption::new(PermissionOptionId::new("allow"), "Allow", PermissionOptionKind::AllowOnce)]);
                                    let request_cx = cx.clone(); let request_spawn = request_cx.clone();
                                    let cancelled_tx = cancelled_tx.clone();
                                    let _ = request_cx.spawn(async move {
                                        let result = request_spawn.send_request(req).block_task().await;
                                        let cancelled = matches!(result, Ok(response) if response.outcome == RequestPermissionOutcome::Cancelled);
                                        let _ = cancelled_tx.send((round, i, cancelled));
                                        Ok(())
                                    });
                                }
                                let _ = go_rx.await;
                                responder.respond(PromptResponse::new(StopReason::EndTurn))
                            }).expect("agent prompt task spawned"); Ok(())
                        }
                    }, agent_client_protocol::on_receive_request!())
                    .on_receive_dispatch(async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| match message {
                        agent_client_protocol::Dispatch::Response(result, router) => router.route_with_result(result),
                        agent_client_protocol::Dispatch::Request(_, responder) => responder.respond_with_error(agent_client_protocol::Error::method_not_found()),
                        agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                    }, agent_client_protocol::on_receive_dispatch!())
                    .connect_to(transport).await.map_err(BridgeError::Acp)
            })
        }).await.expect("session opens");
    for round in 0..2 {
        let mut prompt = handle.prompt(round.to_string()).await.unwrap();
        let release = start_rx.recv().await.unwrap();
        let mut ids = Vec::new();
        while ids.len() < 80 {
            if let BridgeStreamItem::Interrupt { id, .. } = prompt.events.recv().await.unwrap() {
                ids.push(id);
            }
        }
        release.send(()).unwrap();
        let mut finished = false;
        while let Some(item) = prompt.events.recv().await {
            if matches!(item, BridgeStreamItem::Finished { .. }) {
                finished = true;
                break;
            }
            assert!(!matches!(item, BridgeStreamItem::RunError { .. }));
        }
        assert!(finished);
        assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
        assert!(handle.pending_permissions().is_empty());
        assert!(ids.iter().all(|id| !handle.resolve_permission(
            id,
            PermissionDecision::Allow {
                option_id: PermissionOptionId::new("allow")
            }
        )));
        let mut cancelled_count = 0;
        for _ in 0..80 {
            let (actual_round, index, cancelled) =
                tokio::time::timeout(std::time::Duration::from_secs(5), cancelled_rx.recv())
                    .await
                    .expect("permission response arrives")
                    .expect("agent response channel remains connected");
            assert_eq!(actual_round, round);
            assert!(index < 80);
            assert!(
                cancelled,
                "request permission response must be Cancelled, not approval or another error"
            );
            cancelled_count += 1;
        }
        assert_eq!(cancelled_count, 80);
    }
    let mut prompt = handle.prompt("2").await.unwrap();
    let release = start_rx.recv().await.unwrap();
    let mut count = 0;
    while count < 80 {
        if matches!(
            prompt.events.recv().await.unwrap(),
            BridgeStreamItem::Interrupt { .. }
        ) {
            count += 1;
        }
    }
    release.send(()).unwrap();
    while let Some(item) = prompt.events.recv().await {
        if matches!(item, BridgeStreamItem::Finished { .. }) {
            break;
        }
    }
    assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
    assert_eq!(count, 80);
    assert!(handle.pending_permissions().is_empty());
}
