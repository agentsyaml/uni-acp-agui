use super::*;

#[tokio::test]
async fn validates_settings_wait_for_prompt_and_expired_call_is_not_applied() {
    // Settings are actor-serial: a command sent while a prompt is active
    // waits behind that prompt. If the HTTP caller times out first, the
    // queued command must be discarded rather than applied later.
    use std::time::Duration;
    use tokio::net::TcpListener;

    let state = BridgeAppState::builder(
        client_for(test_agents::run_modes_models_agent),
        PathBuf::from("/"),
    )
    .with_config(BridgeConfig {
        set_session_timeout: Duration::from_millis(50),
        ..BridgeConfig::default()
    })
    .build();
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Open the session with a small first run so the cache has an entry.
    let body = serde_json::to_vec(&user_input("thread-conc", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let _ = tokio::time::timeout(Duration::from_secs(3), drain_to_end_local(&mut conn)).await;
    drop(conn);

    // Open a second SSE connection and read just enough to know the prompt
    // is active. The fixture holds it for 250ms.
    let body2 = serde_json::to_vec(&user_input("thread-conc", "run-2", "stay")).unwrap();
    let mut blocked = raw_post(addr, "/", body2, "Accept: text/event-stream\r\n").await;
    use tokio::io::AsyncReadExt;
    let mut hdr = vec![0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_secs(2), blocked.read(&mut hdr)).await;

    // The setting request times out while the prompt is still running.
    let payload =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-conc", "modeId": "code"}))
            .unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", payload, "").await;
    let code = read_status_code(&mut resp).await;
    assert_eq!(
        code,
        StatusCode::REQUEST_TIMEOUT,
        "set-mode must wait for the actor and then time out"
    );

    // Let the prompt finish and give the actor a chance to observe that the
    // timed-out setting responder was dropped. It must not apply "code".
    let trailer = tokio::time::timeout(Duration::from_secs(3), drain_to_end_local(&mut blocked))
        .await
        .expect("prompt must finish after the setting timeout");
    assert!(trailer.contains("\"type\":\"RUN_FINISHED\""));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let init = state
        .session_init_state("thread-conc")
        .expect("session must exist");
    assert_eq!(
        init.modes.as_ref().map(|m| m.current_mode_id.as_str()),
        Some("ask"),
        "a timed-out queued setting must not apply after the prompt"
    );

    // A fresh setting after the prompt still works and updates the cache.
    let payload =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-conc", "modeId": "code"}))
            .unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", payload, "").await;
    assert_eq!(read_status_code(&mut resp).await, StatusCode::OK);
    assert_eq!(
        state
            .session_init_state("thread-conc")
            .and_then(|init| init.modes.map(|m| m.current_mode_id)),
        Some("code".to_string())
    );

    server.abort();
}

#[tokio::test]
async fn validates_unresponsive_setting_rpc_is_bounded_and_evicts_session() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    let opens = Arc::new(AtomicUsize::new(0));
    let opens_for_client = opens.clone();
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        let first = opens_for_client.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                test_agents::run_unresponsive_setting_agent(stream).await
            } else {
                test_agents::run_single_chunk_agent(stream).await
            }
        })
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: Duration::from_millis(100),
            ..BridgeConfig::default()
        })
        .build();

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-setting-timeout", "run-1", "boot"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initial prompt must succeed: {body}"
    );

    let app = agui_acp_bridge_server::build_router(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let payload = serde_json::to_vec(
        &serde_json::json!({"threadId": "thread-setting-timeout", "modeId": "code"}),
    )
    .unwrap();
    let started = std::time::Instant::now();
    let mut response = raw_post(addr, "/session/set-mode", payload, "").await;
    let code = read_status_code(&mut response).await;
    let elapsed = started.elapsed();
    assert_eq!(code, StatusCode::REQUEST_TIMEOUT);
    assert!(
        elapsed < Duration::from_secs(2),
        "unresponsive setting RPC must have a bounded return: {elapsed:?}"
    );

    // The actor-side timeout marks the session unusable; the next cache read
    // evicts it instead of exposing a state that may have changed late.
    assert!(
        state.session_init_state("thread-setting-timeout").is_none(),
        "timed-out setting session must be evicted"
    );
    assert_eq!(state.session_count(), 0);

    let (status, fresh_body) = collect_sse_body(
        state,
        user_input("thread-setting-timeout", "run-2", "fresh"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(fresh_body.contains("only chunk"));
    assert_eq!(opens.load(Ordering::SeqCst), 2);

    server.abort();
}
