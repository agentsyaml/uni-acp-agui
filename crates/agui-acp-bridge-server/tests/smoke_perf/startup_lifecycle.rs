use super::*;

#[tokio::test]
async fn startup_in_process_open_session_is_fast() {
    // The in-process handshake (initialize + session/new over an in-memory
    // duplex) involves no process spawn, so it should complete in low
    // milliseconds. We assert a generous 2s ceiling: the point is to catch a
    // regression that turns the handshake into seconds (e.g. an accidental
    // blocking call or a lost wakeup), not to benchmark.
    let client = InProcessAcpClient::new();

    let started = Instant::now();
    let handle = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_session(test_session_config()),
    )
    .await
    .expect("open_session deadlocked")
    .expect("open_session failed");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "in-process open_session should be fast, took {elapsed:?}"
    );

    // The handle must be immediately usable: a prompt right after open
    // should stream and finish well within a couple of seconds.
    let prompt_started = Instant::now();
    let mut stream = tokio::time::timeout(Duration::from_secs(5), handle.prompt("ping"))
        .await
        .expect("prompt deadlocked")
        .expect("prompt failed");
    // Drain to completion.
    while let Ok(Some(_item)) =
        tokio::time::timeout(Duration::from_secs(2), stream.events.recv()).await
    {}
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.finished).await;
    assert!(
        prompt_started.elapsed() < Duration::from_secs(3),
        "first prompt after open should complete promptly, took {:?}",
        prompt_started.elapsed()
    );
}

#[tokio::test]
async fn startup_repeated_open_sessions_have_stable_latency() {
    // Open and tear down many independent sessions back to back. This guards
    // against a per-open resource leak that would make later opens slower (or
    // fail outright). We assert every open stays under a generous ceiling.
    let client = InProcessAcpClient::new();

    for i in 0..32 {
        let started = Instant::now();
        let handle = tokio::time::timeout(
            Duration::from_secs(5),
            client.open_session(test_session_config()),
        )
        .await
        .unwrap_or_else(|_| panic!("open_session #{i} deadlocked"))
        .unwrap_or_else(|e| panic!("open_session #{i} failed: {e}"));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "open_session #{i} regressed to {elapsed:?}"
        );
        // Drop the handle immediately; the embedded agent task must wind down
        // on its own without blocking the next open.
        drop(handle);
    }
}

#[tokio::test]
async fn startup_timeout_bounds_a_slow_handshake() {
    // A backend that stalls on `session/new` must not hang the bridge: the
    // configured `open_session_timeout` has to abort the handshake and surface
    // an error to the HTTP layer. We give the agent a 2s handshake delay and a
    // 200ms timeout, then assert the request returns quickly (well under the
    // delay) rather than blocking for the full 2s.
    let agent_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dropped_for_agent = agent_dropped.clone();
    let client = client_for(move |s| {
        let dropped = dropped_for_agent.clone();
        async move {
            let _drop_flag = DropFlag(dropped);
            test_agents::run_slow_handshake_agent(s, 2_000).await
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            open_session_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();

    let app = build_router(state.clone());
    let body = serde_json::to_vec(&user_input("thread-slow-hs", "run-1", "hi")).unwrap();

    let started = Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        tower::ServiceExt::oneshot(
            app,
            axum::http::Request::post("/")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        ),
    )
    .await
    .expect("router must not hang past the open_session_timeout")
    .expect("router error");
    let elapsed = started.elapsed();

    // handler.rs returns Err(AgUiError) from `session_for` on timeout, which
    // the agui-rs server maps to HTTP 500.
    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a timed-out handshake must surface as an error status, not a hung 200"
    );
    assert!(
        elapsed < Duration::from_millis(1_500),
        "must fail fast on the configured timeout (200ms), took {elapsed:?}"
    );
    // No session should have been cached for the failed open.
    assert_eq!(
        state.session_count(),
        0,
        "a failed open_session must not leave a cached entry"
    );
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "a failed session admission must not leave frontend registry state"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while agent_dropped.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed-out handshake actor must be aborted, not detached");
}

#[tokio::test]
async fn capacity_gate_does_not_cover_concurrent_handshakes() {
    let state = BridgeAppState::builder(
        client_for(|s| test_agents::run_slow_handshake_agent(s, 300)),
        PathBuf::from("/"),
    )
    .with_config(BridgeConfig {
        max_sessions: 2,
        open_session_timeout: Duration::from_secs(2),
        ..BridgeConfig::default()
    })
    .build();

    let started = Instant::now();
    let first = {
        let state = state.clone();
        tokio::spawn(async move {
            collect_sse_body(state, user_input("parallel-hs-a", "run", "hi")).await
        })
    };
    let second = {
        let state = state.clone();
        tokio::spawn(async move {
            collect_sse_body(state, user_input("parallel-hs-b", "run", "hi")).await
        })
    };
    let (first_status, _) = first.await.expect("first handshake task");
    let (second_status, _) = second.await.expect("second handshake task");

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "capacity selection must not serialize 300ms handshakes: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn lifecycle_distinct_threads_create_distinct_sessions() {
    // Each unique thread_id must map to its own session; the count must equal
    // the number of distinct ids regardless of run order.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    for i in 0..10 {
        let thread = format!("thread-{i}");
        let (status, body) =
            collect_sse_body(state.clone(), user_input(&thread, "run-1", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    }

    assert_eq!(
        state.session_count(),
        10,
        "ten distinct thread_ids must yield ten cached sessions"
    );
}

#[tokio::test]
async fn lifecycle_reuse_keeps_session_count_flat() {
    // Many runs on a single thread_id must never grow the session map beyond
    // one entry — the core reuse contract that keeps memory bounded for a
    // long-lived conversation.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    for i in 0..25 {
        let (status, body) = collect_sse_body(
            state.clone(),
            user_input("thread-reuse", &format!("run-{i}"), "ping"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert_eq!(
            state.session_count(),
            1,
            "session count must stay at 1 across reuse (run {i})"
        );
    }

    // The stateful agent counts turns per SessionId; 25 reuses must have all
    // landed on the same session, so the final turn number is 25.
    let (_, last) = collect_sse_body(
        state.clone(),
        user_input("thread-reuse", "run-final", "last"),
    )
    .await;
    assert!(
        last.contains("turn 26: last"),
        "all reuses must share one session (expected turn 26), body:\n{last}"
    );
}

#[tokio::test]
async fn lifecycle_idle_reaper_drains_many_sessions_to_zero() {
    // Materialize many distinct sessions, then let the reaper drain them all.
    // Guards the reaper's ability to keep the map bounded after a burst of
    // distinct conversations that then go idle.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    for i in 0..12 {
        let thread = format!("idle-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(state.session_count(), 12, "all sessions cached after burst");

    // Reaper interval is min(idle/4, 30s) floored to 1s; 1.5s guarantees at
    // least one sweep with the idle window elapsed.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert_eq!(
        state.session_count(),
        0,
        "idle reaper must drain every idle session back to zero"
    );
}

#[tokio::test]
async fn lifecycle_failing_prompt_does_not_poison_other_threads() {
    // Error isolation: a thread whose agent errors on prompt must surface a
    // RUN_ERROR for that thread only. A different, healthy thread sharing the
    // same bridge must be entirely unaffected and complete cleanly. This
    // guards against one bad conversation taking down the whole gateway.
    let failing = state_with_client(client_for(test_agents::run_failing_prompt_agent));
    let (fstatus, fbody) = collect_sse_body(
        failing.clone(),
        user_input("thread-bad", "run-1", "explode"),
    )
    .await;
    assert_eq!(fstatus, StatusCode::OK);
    assert!(
        fbody.contains("\"type\":\"RUN_ERROR\""),
        "the failing thread must surface RUN_ERROR, body:\n{fbody}"
    );

    // Re-running the same failing thread must keep failing cleanly (no panic,
    // no hang) — the bridge stays responsive.
    let (fstatus2, fbody2) =
        collect_sse_body(failing.clone(), user_input("thread-bad", "run-2", "again")).await;
    assert_eq!(fstatus2, StatusCode::OK);
    assert!(
        fbody2.contains("\"type\":\"RUN_ERROR\""),
        "a repeat run on the failing thread must still error cleanly, body:\n{fbody2}"
    );

    // A separate bridge with a healthy agent is unaffected — error handling on
    // one conversation does not leak global state.
    let healthy = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (hstatus, hbody) =
        collect_sse_body(healthy, user_input("thread-good", "run-1", "hi")).await;
    assert_eq!(hstatus, StatusCode::OK);
    assert!(
        hbody.contains("\"type\":\"RUN_FINISHED\""),
        "a healthy thread must finish cleanly regardless of other failures, body:\n{hbody}"
    );
}
