use super::*;

#[tokio::test]
async fn validates_health_endpoint_returns_minimal_body() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    // Drive one prompt to materialize a session.
    let (_, _) = collect_sse_body(state.clone(), user_input("thread-h", "run-h", "warmup")).await;

    let app = agui_acp_bridge_server::build_router(state);
    let response = app
        .oneshot(HttpRequest::get("/health").body(Body::empty()).unwrap())
        .await
        .expect("health request must succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("collect health body")
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap(),
        serde_json::json!({"status": "ok"})
    );
}

#[tokio::test]
async fn validates_approval_endpoint_returns_404_for_unknown_thread() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let app = agui_acp_bridge_server::build_router(state);

    // `/approval` is thread-scoped: the body must name the thread whose
    // live session owns the pending interrupt. Here the thread does not
    // exist, so the THREAD lookup rejects before the interrupt id is ever
    // consulted. (The interrupt-id branch on a live thread is covered by
    // `validates_approval_endpoint_returns_404_for_unknown_interrupt_on_live_thread`.)
    let body = serde_json::to_vec(&serde_json::json!({
        "threadId": "no-such-thread",
        "interruptId": "does-not-exist",
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let response = app
        .oneshot(
            HttpRequest::post("/approval")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("router error");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn validates_approval_endpoint_returns_400_when_approved_without_option_id() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let app = agui_acp_bridge_server::build_router(state);

    let body = serde_json::to_vec(&serde_json::json!({
        // Missing `optionId` is rejected (400) before the thread-scoped
        // interrupt lookup.
        "threadId": "approval-validation-thread",
        "interruptId": "anything",
        "approved": true,
    }))
    .unwrap();
    let response = app
        .oneshot(
            HttpRequest::post("/approval")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("router error");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn validates_approval_endpoint_returns_422_for_unknown_option_id() {
    // Drive the deferred-approval flow with a bogus optionId so we can
    // observe the new InvalidOption → 422 path. We POST an Allow with
    // optionId="never-offered" and assert the bridge rejects it (and the
    // pending request stays alive — a follow-up valid POST resolves it
    // and the run finishes).
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy)
        .build();
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let prompt_body = serde_json::to_vec(&user_input("thread-422", "run-422", "do read")).unwrap();
    let mut response = raw_post(addr, "/", prompt_body, "Accept: text/event-stream\r\n").await;

    let (snapshot, _snapshot_prefix) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_snapshot(&mut response),
    )
    .await
    .expect("STATE_SNAPSHOT")
    .expect("interruptId");
    let interrupt_id = snapshot
        .pointer("/snapshot/approval/interruptId")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .to_string();

    // First attempt: bogus optionId → 422.
    let bad_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-422",
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "never-offered",
    }))
    .unwrap();
    let mut bad = raw_post(addr, "/approval", bad_body, "").await;
    assert_eq!(
        read_status_code(&mut bad).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    // Second attempt: valid optionId → 200 → run completes.
    let good_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-422",
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut good = raw_post(addr, "/approval", good_body, "").await;
    assert_eq!(read_status_code(&mut good).await, StatusCode::OK);

    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end(response))
        .await
        .expect("drain");
    assert!(trailer.contains("\"type\":\"RUN_FINISHED\""));

    server.abort();
}

#[tokio::test]
async fn validates_approval_endpoint_returns_404_for_unknown_interrupt_on_live_thread() {
    // Half of the 404 contract the unknown-thread test cannot reach: the
    // thread exists with a LIVE deferred interrupt, but the posted
    // interruptId matches no pending permission (already answered, timed
    // out, or never existed) → 404, and the live interrupt stays resolvable.
    //
    // We drive the real defer flow (InterruptViaAgUiEvent → STATE_SNAPSHOT)
    // so the thread genuinely owns a pending interrupt, then POST a bogus
    // interrupt id against it. This fails if the interrupt-id branch of
    // `resolve_permission` were deleted or always returned Resolved.
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::path::PathBuf;
    use tokio::net::TcpListener;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy)
        .build();
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let prompt_body =
        serde_json::to_vec(&user_input("thread-404-live", "run-404", "do read")).unwrap();
    let mut response = raw_post(addr, "/", prompt_body, "Accept: text/event-stream\r\n").await;

    let (snapshot, _prefix) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_snapshot(&mut response),
    )
    .await
    .expect("STATE_SNAPSHOT")
    .expect("interruptId");
    assert!(
        snapshot
            .pointer("/snapshot/approval/interruptId")
            .and_then(serde_json::Value::as_str)
            .is_some(),
        "a live interrupt must be pending before we probe with a bogus id"
    );

    // Bogus interruptId against the LIVE thread → 404.
    let bogus_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-404-live",
        "interruptId": "no-such-interrupt",
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut bogus = raw_post(addr, "/approval", bogus_body, "").await;
    assert_eq!(
        read_status_code(&mut bogus).await,
        StatusCode::NOT_FOUND,
        "unknown interruptId on a live thread must 404"
    );

    // The pending permission must survive the failed probe: a valid POST
    // still resolves it and the run finishes.
    let good_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-404-live",
        "interruptId": snapshot
            .pointer("/snapshot/approval/interruptId")
            .and_then(serde_json::Value::as_str)
            .unwrap(),
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut good = raw_post(addr, "/approval", good_body, "").await;
    assert_eq!(read_status_code(&mut good).await, StatusCode::OK);

    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end(response))
        .await
        .expect("drain");
    assert!(trailer.contains("\"type\":\"RUN_FINISHED\""));

    server.abort();
}

#[tokio::test]
async fn validates_approval_endpoint_rejects_body_without_thread_id() {
    // `ApprovalRequest.thread_id` is required: a body without `threadId`
    // never reaches `resolve_permission` at all. This is the only new
    // required field of the thread-scoping change — pin its rejection.
    // Missing `optionId` on an approved body is pinned by
    // `validates_approval_endpoint_returns_400_when_approved_without_option_id`.
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let app = agui_acp_bridge_server::build_router(state);

    let body = serde_json::to_vec(&serde_json::json!({
        "interruptId": "anything",
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let response = app
        .oneshot(
            HttpRequest::post("/approval")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("router error");

    // axum's Json extractor rejects a serde deserialization failure with
    // 422 (the handler itself returns 400 only for missing `optionId`).
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "missing threadId must be rejected by input validation"
    );
}
