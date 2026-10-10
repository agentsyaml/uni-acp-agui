use super::*;

#[tokio::test]
async fn validates_cancel_drains_multiple_pending_permissions() {
    let mut cfg = session_config();
    cfg.policy = Arc::new(InterruptViaAgUiEvent);
    cfg.config.cancel_grace_timeout = Duration::from_secs(1);
    let handle = client_for(test_agents::run_multiple_pending_permission_agent)
        .open_session(cfg)
        .await
        .expect("session opens");
    let mut prompt = handle.prompt("cancel two").await.expect("prompt opens");

    let mut interrupt_count = 0;
    loop {
        let item = tokio::time::timeout(Duration::from_secs(2), prompt.events.recv())
            .await
            .expect("permission events must arrive")
            .expect("prompt event stream must remain open");
        match item {
            agui_acp_bridge_server::BridgeStreamItem::Interrupt { .. } => {
                interrupt_count += 1;
                if interrupt_count == 2 {
                    break;
                }
            }
            agui_acp_bridge_server::BridgeStreamItem::SessionInit { .. }
            | agui_acp_bridge_server::BridgeStreamItem::Update(_) => {}
            other => panic!("unexpected item before permissions: {other:?}"),
        }
    }
    assert_eq!(handle.pending_permissions().len(), 2);

    handle.cancel().expect("cancel must be accepted");
    assert_eq!(
        handle.pending_permissions().len(),
        0,
        "cancel must drain every pending permission"
    );

    let mut saw_cancel_tail = false;
    let mut finished_count = 0;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), prompt.events.recv())
        .await
        .expect("cancelled prompt must finish")
    {
        if matches!(
            &item,
            agui_acp_bridge_server::BridgeStreamItem::Update(
                agent_client_protocol::schema::v1::SessionUpdate::AgentMessageChunk(_)
            )
        ) {
            saw_cancel_tail = true;
        }
        if matches!(
            &item,
            agui_acp_bridge_server::BridgeStreamItem::Finished { .. }
        ) {
            finished_count += 1;
        }
    }
    assert_eq!(
        finished_count, 1,
        "cancelled turn must emit Finished exactly once"
    );
    assert!(
        saw_cancel_tail,
        "cancel must continue receiving final updates"
    );
    let result = tokio::time::timeout(Duration::from_secs(2), prompt.finished)
        .await
        .expect("finished oneshot must resolve")
        .expect("finished sender must remain")
        .expect("prompt should return a stop reason");
    assert_eq!(
        result,
        agent_client_protocol::schema::v1::StopReason::Cancelled
    );
}

#[tokio::test]
async fn validates_immediate_cancel_wakes_prompt_start_path() {
    let mut cfg = session_config();
    cfg.config.cancel_grace_timeout = Duration::from_secs(1);
    let handle = client_for(test_agents::run_cancel_aware_slow_agent)
        .open_session(cfg)
        .await
        .expect("session opens");
    let prompt = handle
        .prompt("cancel immediately")
        .await
        .expect("prompt opens");

    // Cancel before yielding to the actor. This pins the prompt-start path
    // where the cancellation waiter is registered and must not lose the wake.
    handle.cancel().expect("cancel must be accepted");
    let result = tokio::time::timeout(Duration::from_secs(2), prompt.finished)
        .await
        .expect("immediate cancel must finish")
        .expect("finished sender must remain")
        .expect("prompt should return a stop reason");
    assert_eq!(
        result,
        agent_client_protocol::schema::v1::StopReason::Cancelled
    );
}

#[tokio::test]
async fn validates_queued_turn_disconnect_cannot_cancel_active_turn() {
    let mut cfg = session_config();
    cfg.config.cancel_grace_timeout = Duration::from_secs(1);
    let handle = client_for(test_agents::run_cancel_aware_slow_agent)
        .open_session(cfg)
        .await
        .expect("session opens");

    let (first, first_turn) = handle
        .prompt_with_turn("active")
        .await
        .expect("first prompt opens");
    let (second, second_turn) = handle
        .prompt_with_turn("queued")
        .await
        .expect("second prompt opens");
    assert_ne!(
        first_turn, second_turn,
        "queued prompt turns must have distinct identities"
    );

    // This is the SSE-disconnect cleanup equivalent for the queued stream.
    // Dropping/cancelling B must not send session/cancel for active A.
    handle
        .cancel_turn(second_turn)
        .expect("queued turn cancel must be accepted");
    drop(second);

    let result = tokio::time::timeout(Duration::from_secs(2), first.finished)
        .await
        .expect("active prompt must finish")
        .expect("finished sender must remain")
        .expect("prompt should return a stop reason");
    assert_eq!(
        result,
        agent_client_protocol::schema::v1::StopReason::EndTurn,
        "cancelling queued B must leave active A running"
    );
}

#[tokio::test]
async fn validates_prompt_queue_capacity_rejects_without_disturbing_active_turn() {
    let mut cfg = session_config();
    cfg.config.max_queued_turns = 1;
    let handle = client_for(test_agents::run_cancel_aware_slow_agent)
        .open_session(cfg)
        .await
        .expect("session opens");

    let (active, _) = handle
        .prompt_with_turn("active")
        .await
        .expect("active prompt opens");
    let rejected = handle.prompt_with_turn("over capacity").await;
    assert!(matches!(
        rejected,
        Err(agui_acp_bridge_server::BridgeError::QueueCapacity {
            max_queued_turns: 1
        })
    ));

    let result = tokio::time::timeout(Duration::from_secs(2), active.finished)
        .await
        .expect("active prompt must finish")
        .expect("finished sender must remain")
        .expect("prompt should return a stop reason");
    assert_eq!(
        result,
        agent_client_protocol::schema::v1::StopReason::EndTurn
    );
}

#[tokio::test]
async fn validates_permission_registration_after_cancel_is_cancelled() {
    let mut cfg = session_config();
    cfg.policy = Arc::new(InterruptViaAgUiEvent);
    cfg.config.cancel_grace_timeout = Duration::from_secs(1);
    let handle = client_for(test_agents::run_permission_after_cancel_agent)
        .open_session(cfg)
        .await
        .expect("session opens");
    let mut prompt = handle.prompt("cancel race").await.expect("prompt opens");

    loop {
        let item = tokio::time::timeout(Duration::from_secs(2), prompt.events.recv())
            .await
            .expect("first permission must arrive")
            .expect("prompt event stream must remain open");
        if matches!(
            item,
            agui_acp_bridge_server::BridgeStreamItem::Interrupt { .. }
        ) {
            break;
        }
    }
    handle.cancel().expect("cancel must be accepted");
    assert_eq!(handle.pending_permissions().len(), 0);

    let mut late_interrupts = 0;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), prompt.events.recv())
        .await
        .expect("cancel race must finish")
    {
        if matches!(
            item,
            agui_acp_bridge_server::BridgeStreamItem::Interrupt { .. }
        ) {
            late_interrupts += 1;
        }
        if matches!(
            item,
            agui_acp_bridge_server::BridgeStreamItem::Finished { .. }
        ) {
            break;
        }
    }
    assert_eq!(
        late_interrupts, 0,
        "late permission must not be re-registered"
    );
    assert_eq!(handle.pending_permissions().len(), 0);
    let result = tokio::time::timeout(Duration::from_secs(2), prompt.finished)
        .await
        .expect("finished oneshot must resolve")
        .expect("finished sender must remain")
        .expect("prompt should return a stop reason");
    assert_eq!(
        result,
        agent_client_protocol::schema::v1::StopReason::Cancelled
    );
}

#[tokio::test]
async fn validates_cancel_grace_timeout_evicts_session_before_reuse() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    let opens = Arc::new(AtomicUsize::new(0));
    let opens_for_client = opens.clone();
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        let first = opens_for_client.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                test_agents::run_unresponsive_cancel_agent(stream).await
            } else {
                test_agents::run_single_chunk_agent(stream).await
            }
        })
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            cancel_grace_timeout: Duration::from_millis(100),
            ..BridgeConfig::default()
        })
        .build();
    let app = agui_acp_bridge_server::build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body =
        serde_json::to_vec(&user_input("thread-unknown-after-cancel", "run-1", "hang")).unwrap();
    let mut first_stream = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let mut started = vec![0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_secs(2), first_stream.read(&mut started))
        .await
        .expect("first run must start");

    let cancel_body =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-unknown-after-cancel"}))
            .unwrap();
    let mut cancel_response = raw_post(addr, "/session/cancel", cancel_body, "").await;
    assert_eq!(read_status_code(&mut cancel_response).await, StatusCode::OK);
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        drain_to_end_local(&mut first_stream),
    )
    .await
    .expect("cancelled unresponsive run must terminate");

    assert_eq!(state.session_count(), 0, "unknown session must be evicted");

    let (status, second_body) = collect_sse_body(
        state,
        user_input("thread-unknown-after-cancel", "run-2", "fresh"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "new session must be usable");
    assert!(
        second_body.contains("only chunk") && second_body.contains("RUN_FINISHED"),
        "same thread must create a fresh ACP session after timeout:\n{second_body}"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 2);

    server.abort();
}

#[tokio::test]
async fn validates_idle_reaper_drops_unused_sessions() {
    // Configure a 200ms idle timeout, prompt once to materialize a session,
    // then wait for the reaper to drop it. The reaper interval is
    // `min(idle/4, 30s)`, capped to a 1s minimum, so the timing budget is:
    //  - prompt (~0ms) → session count = 1
    //  - sleep 1.5s → reaper has run at least once with idle elapsed
    //  - assert session count = 0
    use agui_acp_bridge_core::BridgeConfig;
    use std::path::PathBuf;
    use std::time::Duration;

    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let (_, _) = collect_sse_body(state.clone(), user_input("thread-reap", "run-reap", "hi")).await;
    assert_eq!(
        state.session_count(),
        1,
        "session must be cached after prompt"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(
        state.session_count(),
        0,
        "reaper must drop session whose last_used is older than idle_timeout"
    );
}
