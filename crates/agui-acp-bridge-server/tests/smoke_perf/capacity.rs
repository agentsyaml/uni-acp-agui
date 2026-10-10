use super::*;

#[tokio::test]
async fn reaper_also_drops_frontend_tools_registry_entry() {
    // Drive a run that registers the thread in the frontend-tools registry,
    // then let the idle reaper drop the session. The registry entry must be
    // dropped too — otherwise it leaks.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let (status, _) =
        collect_sse_body(state.clone(), user_input("thread-reg", "run-1", "hi")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.session_count(), 1, "session cached after run");
    assert_eq!(
        state.frontend_tools().thread_count(),
        1,
        "registry entry created for the thread"
    );

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert_eq!(state.session_count(), 0, "reaper drops the session");
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "reaper must also drop the frontend-tools registry entry (no leak)"
    );
}

#[tokio::test]
async fn max_sessions_evicts_lru_idle_session() {
    // With max_sessions = 3, running 6 distinct idle threads in sequence must
    // never let the cached count exceed 3: each new session past the cap
    // evicts the least-recently-used idle one. This models a browser that
    // mints a fresh threadId on every refresh and proves sessions can't grow
    // without bound.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 3,
            ..BridgeConfig::default()
        })
        .build();

    for i in 0..6 {
        let thread = format!("cap-{i}");
        let (status, body) =
            collect_sse_body(state.clone(), user_input(&thread, "run-1", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert!(
            state.session_count() <= 3,
            "cached session count {} must never exceed the cap of 3 (after thread {i})",
            state.session_count()
        );
    }

    assert_eq!(
        state.session_count(),
        3,
        "exactly the cap's worth of sessions should remain"
    );
    // The registry must track the cap too — evicted threads' entries are
    // dropped, so the registry never exceeds the cap either.
    assert!(
        state.frontend_tools().thread_count() <= 3,
        "registry entries must be dropped alongside evicted sessions, got {}",
        state.frontend_tools().thread_count()
    );
}

#[tokio::test]
async fn max_sessions_zero_means_unlimited() {
    // Zero is an explicit development override: no eviction.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 0,
            ..BridgeConfig::default()
        })
        .build();

    for i in 0..8 {
        let thread = format!("unl-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
    }

    assert_eq!(
        state.session_count(),
        8,
        "with max_sessions=0 all distinct threads stay cached"
    );
}

#[tokio::test]
async fn max_sessions_does_not_evict_busy_sessions() {
    // A session with an in-flight (slow) prompt must NOT be evicted even when
    // the cap is reached — live work is never killed. We set cap = 1, start a
    // slow run on thread A, and while it is in flight start a run on thread B.
    // B must be rejected before a second ACP actor is opened.
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_agent = opens.clone();
    let client = client_for(move |s| {
        opens_for_agent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_slow_prompt_agent(s, 400)
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 1,
            ..BridgeConfig::default()
        })
        .build();

    let s1 = state.clone();
    let a =
        tokio::spawn(
            async move { collect_sse_body(s1, user_input("busy-A", "run-A", "wait")).await },
        );
    // Give A time to enter its prompt (active_prompts > 0).
    tokio::time::sleep(Duration::from_millis(100)).await;

    let s2 = state.clone();
    let b =
        tokio::spawn(
            async move { collect_sse_body(s2, user_input("busy-B", "run-B", "wait")).await },
        );

    let (sa, ba) = a.await.unwrap();
    let (sb, bb) = b.await.unwrap();
    assert_eq!(sa, StatusCode::OK);
    assert_eq!(sb, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        ba.contains("\"type\":\"RUN_FINISHED\""),
        "busy session A must complete cleanly, not be evicted mid-flight:\n{ba}"
    );
    assert!(
        bb.contains("session capacity reached"),
        "session B must explain the capacity rejection:\n{bb}"
    );
    assert!(
        bb.contains("ACP_SESSION_CAPACITY"),
        "session B must expose a non-success capacity error code:\n{bb}"
    );
    assert_eq!(
        opens.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "capacity rejection must not open a second ACP actor"
    );
    assert!(
        !state.frontend_tools().has("busy-B"),
        "capacity rejection must not leave a speculative frontend registry entry"
    );
}

#[tokio::test]
async fn max_sessions_hard_cap_bounds_concurrent_first_use() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_agent = opens.clone();
    let client = client_for(move |s| {
        opens_for_agent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_slow_prompt_agent(s, 400)
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 2,
            ..BridgeConfig::default()
        })
        .build();

    let mut tasks = Vec::new();
    for i in 0..8 {
        let state = state.clone();
        tasks.push(tokio::spawn(async move {
            collect_sse_body(
                state,
                user_input(&format!("cap-concurrent-{i}"), "run", "wait"),
            )
            .await
        }));
    }

    let mut rejected = 0;
    for task in tasks {
        let (status, _) = task.await.expect("concurrent request task");
        if status == StatusCode::SERVICE_UNAVAILABLE {
            rejected += 1;
        }
    }
    assert!(
        rejected > 0,
        "the hard cap must reject excess busy sessions"
    );
    assert!(state.session_count() <= 2);
    assert_eq!(
        opens.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "concurrent first-use must open at most the configured cap"
    );
}

#[tokio::test]
async fn capacity_rejection_is_503_but_open_failure_stays_500() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = opens.clone();
    let state = BridgeAppState::builder(
        client_for(move |s| {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            test_agents::run_slow_prompt_agent(s, 500)
        }),
        PathBuf::from("/"),
    )
    .with_config(BridgeConfig {
        max_sessions: 1,
        ..BridgeConfig::default()
    })
    .build();
    let holder = {
        let state = state.clone();
        tokio::spawn(
            async move { collect_sse_body(state, user_input("holder", "r1", "wait")).await },
        )
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, body) =
        collect_sse_body(state.clone(), user_input("contender", "r1", "reject")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 1);
    let _ = holder.await.expect("holder");

    struct FailingOpen;
    #[async_trait::async_trait]
    impl AcpClient for FailingOpen {
        async fn open_session(
            &self,
            _: SessionConfig,
        ) -> Result<agui_acp_bridge_server::AcpSessionHandle, agui_acp_bridge_server::BridgeError>
        {
            Err(agui_acp_bridge_server::BridgeError::Unsupported(
                "ACP_SESSION_CAPACITY ordinary open failure".into(),
            ))
        }
        async fn list_sessions(
            &self,
            _: SessionConfig,
        ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, agui_acp_bridge_server::BridgeError>
        {
            unreachable!()
        }
    }
    let app = build_router(BridgeAppState::new(
        Arc::new(FailingOpen),
        PathBuf::from("/"),
    ));
    let body = serde_json::to_vec(&user_input("failure", "r1", "fail")).unwrap();
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn stable_thread_id_across_many_runs_stays_one_session() {
    // Models the *fixed* frontend behaviour: the browser pins one threadId
    // and reuses it across every reload/run. The bridge must keep exactly one
    // cached session no matter how many runs arrive — this is the property the
    // demo's persisted-threadId change relies on.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    for i in 0..40 {
        let (status, body) = collect_sse_body(
            state.clone(),
            user_input("pinned-thread", &format!("run-{i}"), "hi"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert_eq!(
            state.session_count(),
            1,
            "a pinned threadId must never create more than one session (run {i})"
        );
    }
}

#[tokio::test]
async fn churned_thread_ids_are_bounded_by_cap_and_reaper() {
    // Models the *unfixed* / worst-case frontend behaviour: a fresh threadId
    // on every run (e.g. a client that doesn't pin one). Even then the bridge
    // must not grow without bound — the cap holds the count, and once runs go
    // idle the reaper drains them. This is the server-side safety net behind
    // the client fix.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 8,
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    // 40 distinct threads (like 40 refreshes), each a quick completed run.
    for i in 0..40 {
        let thread = format!("churn-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            state.session_count() <= 8,
            "cap must bound churned sessions, saw {} after thread {i}",
            state.session_count()
        );
    }

    // After everything goes idle, the reaper drains the survivors to zero.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        state.session_count(),
        0,
        "reaper must drain all idle churned sessions"
    );
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "registry must be drained alongside the sessions"
    );
}
