use super::*;

#[tokio::test]
async fn mcp_origin_allowlist_requires_exact_present_origins() {
    use tower::ServiceExt;

    let state = BridgeAppState::builder(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    )
    .with_mcp_allowed_origins(["HTTPS://Allowed.Example"])
    .unwrap()
    .build();
    let app = crate::handler::build_router(state);

    let allowed = with_origin(
        modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        ),
        "https://allowed.example:443",
    );
    assert_eq!(
        app.clone().oneshot(allowed).await.unwrap().status(),
        StatusCode::OK
    );

    let missing = modern_request(
        "server/discover",
        Some(2),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    assert_eq!(
        app.clone().oneshot(missing).await.unwrap().status(),
        StatusCode::OK
    );

    for origin in [
        "https://evil.example",
        "null",
        "*",
        "https://*.allowed.example",
        "https://allowed.example/",
        "https://allowed.example/path",
        "https://allowed.example?query=1",
        "https://allowed.example:bad",
    ] {
        let request = with_origin(
            modern_request(
                "server/discover",
                Some(3),
                json!({}),
                Some("application/json, text/event-stream"),
            ),
            origin,
        );
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN,
            "origin should be rejected: {origin}"
        );
    }
}

#[tokio::test]
async fn present_origin_is_rejected_without_configuration_before_json_dispatch() {
    use axum::body::Body;
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let app = crate::handler::build_router(state);
    let request = axum::http::Request::post("/mcp/thread")
        .header("origin", "https://any.example")
        .body(Body::from("not-json"))
        .unwrap();
    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn invalid_origin_never_dispatches_a_frontend_tool_call() {
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let entry = state.frontend_tools().entry("thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "alert".into(),
        description: "alert".into(),
        parameters: json!({"type": "object"}),
    }]);
    let (active_tx, mut events) = tokio::sync::mpsc::channel(1);
    entry.set_active_sender(Some(active_tx));
    let app = crate::handler::build_router(state.clone());
    let request = axum::http::Request::post("/mcp/thread")
        .header("content-type", "application/json")
        .header("origin", "https://evil.example")
        .body(axum::body::Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "alert", "arguments": {}}
            })
            .to_string(),
        ))
        .unwrap();

    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(state.frontend_tools().pending_len("thread"), 0);
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn mcp_origin_check_does_not_replace_bearer_authentication() {
    use tower::ServiceExt;

    let state = BridgeAppState::builder(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    )
    .with_mcp_allowed_origins(["https://allowed.example"])
    .unwrap()
    .with_bearer_token("origin-test-bearer-token")
    .unwrap()
    .build();
    let app = crate::handler::build_router(state);

    let missing_bearer = with_origin(
        modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        ),
        "https://allowed.example",
    );
    assert_eq!(
        app.clone().oneshot(missing_bearer).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    let invalid_without_bearer = with_origin(
        modern_request(
            "server/discover",
            Some(2),
            json!({}),
            Some("application/json, text/event-stream"),
        ),
        "https://evil.example",
    );
    assert_eq!(
        app.clone()
            .oneshot(invalid_without_bearer)
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );

    let mut authenticated_missing_origin = modern_request(
        "server/discover",
        Some(3),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    authenticated_missing_origin.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        "Bearer origin-test-bearer-token".parse().unwrap(),
    );
    assert_eq!(
        app.clone()
            .oneshot(authenticated_missing_origin)
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let mut invalid_origin = with_origin(
        modern_request(
            "server/discover",
            Some(4),
            json!({}),
            Some("application/json, text/event-stream"),
        ),
        "https://evil.example",
    );
    invalid_origin.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        "Bearer origin-test-bearer-token".parse().unwrap(),
    );
    assert_eq!(
        app.oneshot(invalid_origin).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}
