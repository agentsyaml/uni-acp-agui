use super::*;

#[tokio::test]
async fn modern_tools_use_the_bound_thread_registry() {
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let entry = state.frontend_tools().entry("thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "missing-tool".into(),
        description: "modern".into(),
        parameters: json!({"type": "object"}),
    }]);
    let app = crate::handler::build_router(state);

    let list = modern_request(
        "tools/list",
        Some(10),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.clone().oneshot(list).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["resultType"], "complete");
    assert_eq!(body["result"]["cacheScope"], MODERN_TOOLS_CACHE_SCOPE);
    assert_eq!(body["result"]["ttlMs"], MODERN_CACHE_TTL_MS);
    assert_eq!(
        body["result"]["_meta"][MODERN_SERVER_INFO_METADATA_KEY]["name"],
        "agui-acp-bridge"
    );
    assert_eq!(body["result"]["tools"][0]["name"], "missing-tool");

    let call = modern_request(
        "tools/call",
        Some(11),
        json!({"name": "missing-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.oneshot(call).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("error").is_none());
    assert_eq!(body["result"]["resultType"], "complete");
    assert_eq!(body["result"]["isError"], true);
}

#[tokio::test]
async fn modern_method_errors_and_notifications_use_http_semantics() {
    use axum::body::Body;
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let app = crate::handler::build_router(state);

    let unknown = modern_request(
        "does/not-exist",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.clone().oneshot(unknown).await.unwrap()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], -32601);

    let unknown_notification = modern_request(
        "does/not-exist",
        None,
        json!({}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) =
        response_json(app.clone().oneshot(unknown_notification).await.unwrap()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], -32601);
    assert_eq!(body["id"], Value::Null);

    let mut method_mismatch = modern_request(
        "server/discover",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    method_mismatch
        .headers_mut()
        .insert(MCP_METHOD_HEADER, "tools/list".parse().unwrap());
    let (status, body) = response_json(app.clone().oneshot(method_mismatch).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut name_on_list = modern_request(
        "tools/list",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    name_on_list
        .headers_mut()
        .insert(MCP_NAME_HEADER, "unexpected".parse().unwrap());
    let (status, body) = response_json(app.clone().oneshot(name_on_list).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let name_mismatch = modern_request(
        "tools/call",
        Some(8),
        json!({"name": "other-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.clone().oneshot(name_mismatch).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut encoded_name = modern_request(
        "tools/call",
        Some(8),
        json!({"name": "é", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    encoded_name.headers_mut().insert(
        MCP_NAME_HEADER,
        axum::http::HeaderValue::from_static("=?base64?w6k=?="),
    );
    let (status, body) = response_json(app.clone().oneshot(encoded_name).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32602);

    for plain_name in ["=?foo", "foo?="] {
        let mut plain_name_request = modern_request(
            "tools/call",
            Some(8),
            json!({"name": plain_name, "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        plain_name_request.headers_mut().insert(
            MCP_NAME_HEADER,
            axum::http::HeaderValue::from_static(plain_name),
        );
        let (status, body) =
            response_json(app.clone().oneshot(plain_name_request).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], -32602);
    }

    let mut malformed_name = modern_request(
        "tools/call",
        Some(8),
        json!({"name": "missing-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    malformed_name.headers_mut().insert(
        MCP_NAME_HEADER,
        axum::http::HeaderValue::from_static("=?base64?%%%?="),
    );
    let (status, body) = response_json(app.clone().oneshot(malformed_name).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let notification_name_mismatch = modern_request(
        "tools/call",
        None,
        json!({"name": "other-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(
        app.clone()
            .oneshot(notification_name_mismatch)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);
    assert_eq!(body["id"], Value::Null);

    let unknown_tool_notification = modern_request(
        "tools/call",
        None,
        json!({"name": "missing-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(
        app.clone()
            .oneshot(unknown_tool_notification)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32602);
    assert_eq!(body["id"], Value::Null);

    let mut replay = modern_request(
        "server/discover",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    replay
        .headers_mut()
        .insert(LAST_EVENT_ID_HEADER, "event-1".parse().unwrap());
    let (status, body) = response_json(app.clone().oneshot(replay).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["resultType"], "complete");

    let mut session_id = modern_request(
        "server/discover",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    session_id
        .headers_mut()
        .insert(MCP_SESSION_HEADER, "session-1".parse().unwrap());
    let (status, body) = response_json(app.clone().oneshot(session_id).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["resultType"], "complete");

    let mut batch = modern_request(
        "server/discover",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    *batch.body_mut() =
        Body::from("[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server/discover\"}]");
    let (status, body) = response_json(app.clone().oneshot(batch).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32600);

    let invalid = modern_request(
        "tools/call",
        Some(9),
        json!({"name": "missing-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.clone().oneshot(invalid).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32602);

    let mut null_id = modern_request(
        "server/discover",
        Some(8),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    *null_id.body_mut() = Body::from(
        json!({
            "jsonrpc": "2.0",
            "id": null,
            "method": "server/discover",
            "params": {"_meta": {
                "io.modelcontextprotocol/protocolVersion": MODERN_PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {}
            }}
        })
        .to_string(),
    );
    let (status, body) = response_json(app.clone().oneshot(null_id).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32600);
    assert_eq!(body["id"], Value::Null);

    let notification = modern_request(
        "notifications/initialized",
        None,
        json!({}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.clone().oneshot(notification).await.unwrap()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, Value::Null);

    for method in [axum::http::Method::GET, axum::http::Method::DELETE] {
        let request = axum::http::Request::builder()
            .method(method)
            .uri("/mcp/thread")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}

#[tokio::test]
async fn modern_tools_call_arguments_must_be_objects_before_dispatch() {
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let entry = state.frontend_tools().entry("thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "missing-tool".into(),
        description: "modern".into(),
        parameters: json!({"type": "object"}),
    }]);
    let (active_tx, mut events) = tokio::sync::mpsc::channel(8);
    entry.set_active_sender(Some(active_tx));
    let app = crate::handler::build_router(state.clone());

    for (label, arguments) in [
        ("array", json!([])),
        ("string", json!("text")),
        ("number", json!(42)),
        ("boolean", json!(true)),
        ("null", Value::Null),
    ] {
        let call = modern_request(
            "tools/call",
            Some(10),
            json!({"name": "missing-tool", "arguments": arguments}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(call).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK, "invalid {label} arguments");
        assert_eq!(body["error"]["code"], -32602, "invalid {label} arguments");
        assert_eq!(
            body["error"]["message"], "invalid params: arguments must be an object",
            "invalid {label} arguments"
        );
        assert_eq!(state.frontend_tools().pending_len("thread"), 0);
        assert!(events.try_recv().is_err(), "invalid {label} dispatched");
    }

    let parsed =
        parse_tools_call_params(json!({"name": "missing-tool"}), ProtocolMode::Modern).unwrap();
    assert_eq!(parsed.arguments, json!({}));

    let invalid_notification = modern_request(
        "tools/call",
        None,
        json!({"name": "missing-tool", "arguments": []}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) =
        response_json(app.clone().oneshot(invalid_notification).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32602);
    assert_eq!(
        body["error"]["message"],
        "invalid params: arguments must be an object"
    );
    assert_eq!(body["id"], Value::Null);

    let notification = modern_request(
        "tools/call",
        None,
        json!({"name": "missing-tool"}),
        Some("application/json, text/event-stream"),
    );
    let (status, body) = response_json(app.oneshot(notification).await.unwrap()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, Value::Null);
    assert_eq!(state.frontend_tools().pending_len("thread"), 0);
    assert!(events.try_recv().is_err(), "valid notification dispatched");
}
