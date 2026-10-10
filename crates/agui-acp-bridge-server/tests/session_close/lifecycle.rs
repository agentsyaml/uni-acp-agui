use super::*;

#[tokio::test]
async fn validates_idle_reaper_sends_close_before_dropping_session() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let client = client_for({
        let closed_ids = closed_ids.clone();
        move |stream| {
            test_agents::run_close_agent(
                stream,
                closed_ids.clone(),
                test_agents::CloseBehavior::Success,
            )
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-reap-close", "run-1", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(state.session_count(), 0);
    assert_eq!(
        closed_ids.lock().expect("close ids poisoned").as_slice(),
        ["real-close-session-id"]
    );
}

#[tokio::test]
async fn validates_lru_eviction_closes_the_idle_victim_before_admission() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let client = client_for({
        let closed_ids = closed_ids.clone();
        move |stream| {
            test_agents::run_close_agent(
                stream,
                closed_ids.clone(),
                test_agents::CloseBehavior::Success,
            )
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 1,
            ..BridgeConfig::default()
        })
        .build();

    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-lru-a", "run-a", "first")).await;
    assert_eq!(status, StatusCode::OK, "first prompt failed: {body}");
    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-lru-b", "run-b", "second")).await;
    assert_eq!(status, StatusCode::OK, "second prompt failed: {body}");

    assert_eq!(state.session_count(), 1);
    assert_eq!(
        closed_ids.lock().expect("close ids poisoned").as_slice(),
        ["real-close-session-id"]
    );
}

#[tokio::test]
async fn validates_reaper_claim_blocks_replacement_run_and_preserves_registry() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let client = client_for({
        let control = control.clone();
        move |stream| {
            test_agents::run_close_setting_agent(stream, closed_ids.clone(), control.clone())
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            set_session_timeout: Duration::from_secs(1),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-reaper-claim", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    tokio::time::timeout(Duration::from_secs(3), control.wait_close_started())
        .await
        .expect("reaper must enter the gated close");
    let replacement_registry = state.frontend_tools().entry("thread-reaper-claim");

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-reaper-claim", "run-replacement", "must reject"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("CONCURRENT_RUN"),
        "replacement run must be rejected while lifecycle close owns the thread: {body}"
    );
    assert_eq!(state.session_count(), 0);

    control.release_close();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(Arc::ptr_eq(
        &replacement_registry,
        &state.frontend_tools().entry("thread-reaper-claim")
    ));

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-reaper-claim", "run-after-close", "fresh"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "claim release must admit a new run: {body}"
    );
}

#[tokio::test]
async fn validates_lru_claim_blocks_replacement_run_and_preserves_registry() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let client = client_for({
        let control = control.clone();
        move |stream| {
            test_agents::run_close_setting_agent(stream, closed_ids.clone(), control.clone())
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 1,
            set_session_timeout: Duration::from_secs(1),
            ..BridgeConfig::default()
        })
        .build();

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-lru-claim", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let contender_state = state.clone();
    let contender = tokio::spawn(async move {
        collect_sse_body(
            contender_state,
            user_input("thread-lru-contender", "run-1", "open after eviction"),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), control.wait_close_started())
        .await
        .expect("LRU eviction must enter the gated close");
    let replacement_registry = state.frontend_tools().entry("thread-lru-claim");

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-lru-claim", "run-replacement", "must reject"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("CONCURRENT_RUN"),
        "replacement run must be rejected during LRU close: {body}"
    );

    control.release_close();
    let (status, body) = contender.await.expect("contender task joins");
    assert_eq!(
        status,
        StatusCode::OK,
        "contender must open after close: {body}"
    );
    assert!(Arc::ptr_eq(
        &replacement_registry,
        &state.frontend_tools().entry("thread-lru-claim")
    ));
}
