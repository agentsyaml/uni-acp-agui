use super::*;

#[tokio::test]
async fn validates_defer_policy_emits_state_snapshot_and_resolves_via_approval() {
    // End-to-end deferred approval flow:
    // 1. Configure `InterruptViaAgUiEvent` so the bridge defers every request.
    // 2. The session handler emits `BridgeStreamItem::Interrupt` to the
    //    SSE handler, which surfaces it as a `STATE_SNAPSHOT` event.
    // 3. We pluck the `interruptId` out of the snapshot and POST `/approval`
    //    to resolve it.
    // 4. The agent receives `Allow`, the prompt completes, RUN_FINISHED ships.
    //
    // Because `oneshot` buffers the entire body, we drive both halves
    // concurrently with a real bound HTTP server.
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, std::path::PathBuf::from("/"))
        .with_policy(policy)
        .build();
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Send the prompt and stream the response while concurrently watching
    // for the STATE_SNAPSHOT event.
    let prompt_body =
        serde_json::to_vec(&user_input("thread-defer", "run-defer", "do read")).unwrap();
    let mut response = raw_post(addr, "/", prompt_body, "Accept: text/event-stream\r\n").await;

    // Read until we see STATE_SNAPSHOT, extract its interruptId, POST approval.
    let (snapshot_payload, snapshot_prefix) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_snapshot(&mut response),
    )
    .await
    .expect("STATE_SNAPSHOT must arrive within 5s")
    .expect("STATE_SNAPSHOT must contain an interruptId");

    let interrupt_id = snapshot_payload
        .pointer("/snapshot/approval/interruptId")
        .and_then(serde_json::Value::as_str)
        .expect("interruptId must be a string")
        .to_string();

    // Resolve via /approval.
    let approval_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-defer",
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut approval_resp = raw_post(addr, "/approval", approval_body, "").await;
    assert_eq!(
        read_status_code(&mut approval_resp).await,
        StatusCode::OK,
        "POST /approval must accept the interruptId"
    );

    // Drain the rest of the SSE stream and assert RUN_FINISHED.
    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end(response))
        .await
        .expect("stream must complete after approval");

    let full_body = format!("{snapshot_prefix}{trailer}");
    assert_eq!(
        count_events(&full_body, "STATE_SNAPSHOT"),
        1,
        "approval must emit exactly one private STATE_SNAPSHOT, body:\n{full_body}"
    );
    assert_eq!(
        count_events(&full_body, "RUN_FINISHED"),
        1,
        "approval run must emit exactly one RUN_FINISHED, body:\n{full_body}"
    );
    assert_eq!(count_events(&full_body, "RUN_ERROR"), 0);
    assert_eq!(
        extract_event_types(&full_body).last().map(String::as_str),
        Some("RUN_FINISHED"),
        "terminal event must be last after approval, body:\n{full_body}"
    );
    assert!(
        trailer.contains("\"type\":\"RUN_FINISHED\""),
        "RUN_FINISHED must arrive after approval, body:\n{trailer}"
    );

    server.abort();
}

#[tokio::test]
async fn validates_defer_with_permission_timeout_falls_back_to_cancelled() {
    // When a Defer'd permission goes unanswered, the bridge must time out
    // (per BridgeConfig.permission_timeout) and respond Cancelled instead of
    // hanging forever. We use 200ms so the test runs fast.
    use agui_acp_bridge_core::BridgeConfig;
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::path::PathBuf;
    use std::time::Duration;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy)
        .with_config(BridgeConfig {
            permission_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();

    let started = std::time::Instant::now();
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(5),
        collect_sse_body(state, user_input("thread-to", "run-to", "do read")),
    )
    .await
    .expect("must not hang past timeout × 25");
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= Duration::from_millis(200),
        "must not return before the configured permission_timeout, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "after timeout the agent receives Cancelled and finishes the turn, body:\n{body}"
    );
}
