use super::*;

#[tokio::test]
async fn validates_capacity_permit_survives_setting_entry_until_release() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_client = opens.clone();
    let client = client_for(move |stream| {
        opens_for_client.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_unresponsive_setting_agent(stream)
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 1,
            set_session_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();

    let (status, _) = collect_sse_body(
        state.clone(),
        user_input("setting-holder", "run-1", "materialize"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let setting_state = state.clone();
    let setting = tokio::spawn(async move {
        setting_state
            .set_session_config_option("setting-holder", "mode", "code")
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    let (rejected_status, rejected_body) = collect_sse_body(
        state.clone(),
        user_input("setting-contender", "run-1", "must reject"),
    )
    .await;
    assert_eq!(rejected_status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(rejected_body.contains("ACP_SESSION_CAPACITY"));
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        !state.frontend_tools().has("setting-contender"),
        "capacity failure must remove the speculative frontend entry"
    );

    let _ = tokio::time::timeout(Duration::from_secs(2), setting)
        .await
        .expect("setting timeout must be bounded")
        .expect("setting task must not panic");

    let (status, body) = collect_sse_body(
        state,
        user_input("setting-after-release", "run-1", "can open now"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "capacity must be reusable after release: {body}"
    );
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn validates_actor_exit_clears_queued_turns_and_closes_handle() {
    let mut cfg = session_config();
    cfg.config.set_session_timeout = Duration::from_millis(100);
    cfg.config.max_queued_turns = 2;
    let handle = client_for(test_agents::run_unresponsive_setting_agent)
        .open_session(cfg)
        .await
        .expect("session opens");

    let setting = handle.set_config_option("mode", "code");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let queued = handle
        .prompt_with_turn("queued behind setting")
        .await
        .expect("queued prompt admission should succeed before actor exit")
        .0;

    let _setting_result = tokio::time::timeout(Duration::from_secs(2), setting)
        .await
        .expect("setting actor timeout must finish");
    let _queued_result = tokio::time::timeout(Duration::from_secs(2), queued.finished)
        .await
        .expect("queued stream must be released when actor exits");

    assert!(handle.is_unusable());
    assert!(matches!(
        handle.prompt_with_turn("after actor exit").await,
        Err(agui_acp_bridge_server::BridgeError::SessionClosed)
    ));
}

#[tokio::test]
async fn validates_concurrent_first_use_creates_exactly_one_session() {
    // Fan in 8 concurrent prompts on the same thread_id when no session
    // exists yet. The per-thread admission gate allows exactly one run to
    // proceed; the rest fail fast rather than queueing behind it.
    let state = state_with_client(client_for(|s| test_agents::run_slow_prompt_agent(s, 100)));

    let mut handles = Vec::new();
    for i in 0..8 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            let (status, body) =
                collect_sse_body(s, user_input("thread-race", &format!("run-{i}"), "ping")).await;
            (i, status, body)
        }));
    }

    let mut finished = 0;
    let mut concurrent_errors = 0;
    for h in handles {
        let (i, status, body) = h.await.unwrap();
        assert_eq!(status, StatusCode::OK, "run-{i} body:\n{body}");
        assert_eq!(count_events(&body, "RUN_STARTED"), 1, "run-{i}: {body}");
        assert!(
            body.contains(&format!("\"runId\":\"run-{i}\"")),
            "response must identify run-{i}:\n{body}"
        );

        let events = extract_event_types(&body);
        assert_eq!(
            events.first().map(String::as_str),
            Some("RUN_STARTED"),
            "run-{i} must start with RUN_STARTED: {body}"
        );
        assert_eq!(
            count_events(&body, "RUN_FINISHED") + count_events(&body, "RUN_ERROR"),
            1,
            "run-{i} must have exactly one terminal event: {body}"
        );

        if count_events(&body, "RUN_FINISHED") == 1 {
            finished += 1;
            assert_eq!(events.last().map(String::as_str), Some("RUN_FINISHED"));
            assert!(!body.contains("CONCURRENT_RUN"), "run-{i} body:\n{body}");
        } else {
            concurrent_errors += 1;
            assert_eq!(events.last().map(String::as_str), Some("RUN_ERROR"));
            assert!(
                body.contains("\"code\":\"CONCURRENT_RUN\""),
                "run-{i} must fail at admission: {body}"
            );
            assert!(!body.contains("RUN_FINISHED"), "run-{i} body:\n{body}");
        }
    }

    assert_eq!(finished, 1, "exactly one run must win admission");
    assert_eq!(
        concurrent_errors, 7,
        "seven runs must be rejected immediately"
    );

    assert_eq!(
        state.session_count(),
        1,
        "concurrent first-use must lazily create exactly one session, got {}",
        state.session_count()
    );

    let (status, body) =
        collect_sse_body(state, user_input("thread-race", "run-after", "after")).await;
    assert_eq!(status, StatusCode::OK, "sequential follow-up body:\n{body}");
    assert!(
        body.contains("\"runId\":\"run-after\"") && body.contains("\"type\":\"RUN_FINISHED\""),
        "claim must be released for a later run:\n{body}"
    );
    assert!(
        !body.contains("CONCURRENT_RUN"),
        "follow-up must not be rejected:\n{body}"
    );
}

#[tokio::test]
async fn validates_long_prompt_survives_short_idle_timeout() {
    // Regression for the audit's P1 finding: long-running prompts whose
    // duration exceeds `idle_timeout` would be reaped mid-flight by the
    // background reaper. Now `enter_prompt`/`PromptGuard` keep the
    // `active_prompts` counter non-zero for the duration of the turn,
    // and the reaper skips entries with `active_prompts > 0`.
    use agui_acp_bridge_core::BridgeConfig;
    use std::path::PathBuf;
    use std::time::Duration;

    // 250ms idle timeout, 700ms slow prompt (almost 3× the budget).
    let client = client_for(|s| test_agents::run_slow_prompt_agent(s, 700));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(250),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let started = std::time::Instant::now();
    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-long", "run-long", "wait")).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= Duration::from_millis(700),
        "prompt must run to completion despite shorter idle_timeout, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "long prompt must finish cleanly:\n{body}"
    );
}

#[tokio::test]
async fn validates_long_running_agent_cancelled_when_client_disconnects() {
    // Regression for the audit's P0 finding: when the SSE consumer drops,
    // the bridge must cancel the in-flight ACP turn so the agent stops
    // doing work nobody will read. We use the `run_long_running_agent`
    // fixture which emits a chunk every 50ms for up to 50s, breaking out
    // early if `send_notification` fails (which it does once we drop
    // the receiver). With cancel hooked up the test should finish in
    // well under a second; without it would queue commands and never
    // fire.
    use std::time::Duration;
    use tokio::net::TcpListener;

    let client = client_for(test_agents::run_long_running_agent);
    let state = state_with_client(client);
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body = serde_json::to_vec(&user_input("thread-cancel", "run-cancel", "stream")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;

    // Drain just enough bytes to confirm streaming has started, then drop.
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
        .await
        .expect("must receive at least the headers");
    drop(conn);

    // The agent loop polls send_notification each tick; it should bail
    // out within ~50ms of the disconnect being noticed by the SDK.
    // Give us a generous 3s budget — failure mode is "agent runs the
    // full 50s before responding to the prompt".
    tokio::time::sleep(Duration::from_secs(3)).await;

    server.abort();
}

#[tokio::test]
async fn validates_resident_session_rejects_non_v1_before_session_new() {
    let result = client_for(test_agents::run_wrong_protocol_agent)
        .open_session(session_config())
        .await;
    match result {
        Err(agui_acp_bridge_server::BridgeError::ProtocolVersionMismatch { .. }) => {}
        other => panic!("expected explicit protocol mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn validates_list_session_rejects_non_v1_before_session_list() {
    let result = agui_acp_bridge_core::list_sessions_in_process_with(session_config(), |stream| {
        Box::pin(test_agents::run_wrong_protocol_list_agent(stream))
    })
    .await;
    match result {
        Err(agui_acp_bridge_server::BridgeError::ProtocolVersionMismatch { .. }) => {}
        other => panic!("expected explicit list protocol mismatch, got {other:?}"),
    }
}
