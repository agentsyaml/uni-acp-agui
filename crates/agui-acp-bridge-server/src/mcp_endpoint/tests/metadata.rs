use super::*;

#[tokio::test]
async fn modern_headers_and_metadata_are_enforced() {
    use axum::body::Body;
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let app = crate::handler::build_router(state);

    let mut missing_content_type = modern_request("server/discover", Some(1), json!({}), None);
    missing_content_type.headers_mut().remove("content-type");
    let (status, _) = response_json(app.clone().oneshot(missing_content_type).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let missing_accept = modern_request("server/discover", Some(1), json!({}), None);
    let (status, body) = response_json(app.clone().oneshot(missing_accept).await.unwrap()).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
    assert_eq!(body["error"]["code"], -32600);

    let mut missing_metadata = modern_request("server/discover", Some(1), json!({}), None);
    *missing_metadata.body_mut() = Body::from(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {}
        })
        .to_string(),
    );
    missing_metadata.headers_mut().insert(
        "accept",
        "application/json, text/event-stream".parse().unwrap(),
    );
    let (status, body) = response_json(app.clone().oneshot(missing_metadata).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32602);

    let mut top_level_metadata = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    *top_level_metadata.body_mut() = Body::from(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {},
            "_meta": {"io.modelcontextprotocol/protocolVersion": MODERN_PROTOCOL_VERSION}
        })
        .to_string(),
    );
    let (status, body) =
        response_json(app.clone().oneshot(top_level_metadata).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32602);

    let mut dot_metadata = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    *dot_metadata.body_mut() = Body::from(
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {"_meta": {
                "io.modelcontextprotocol.protocolVersion": MODERN_PROTOCOL_VERSION,
                "io.modelcontextprotocol.clientCapabilities": {}
            }}
        })
        .to_string(),
    );
    let (status, body) = response_json(app.clone().oneshot(dot_metadata).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32602);

    let mut missing_method = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    missing_method.headers_mut().remove(MCP_METHOD_HEADER);
    let (status, body) = response_json(app.clone().oneshot(missing_method).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut missing_name = modern_request(
        "tools/call",
        Some(1),
        json!({"name": "missing-tool", "arguments": {}}),
        Some("application/json, text/event-stream"),
    );
    missing_name.headers_mut().remove(MCP_NAME_HEADER);
    let (status, body) = response_json(app.clone().oneshot(missing_name).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut missing_version = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    missing_version.headers_mut().remove("MCP-Protocol-Version");
    let (status, body) = response_json(app.clone().oneshot(missing_version).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut malformed_version = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    malformed_version
        .headers_mut()
        .insert("MCP-Protocol-Version", "not-a-version".parse().unwrap());
    let (status, body) = response_json(app.clone().oneshot(malformed_version).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32020);

    let mut unsupported_version = modern_request(
        "server/discover",
        Some(1),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    unsupported_version
        .headers_mut()
        .insert("MCP-Protocol-Version", "2025-06-18".parse().unwrap());
    let (status, body) = response_json(app.oneshot(unsupported_version).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32022);
    assert_eq!(body["error"]["data"]["requested"], "2025-06-18");
    assert_eq!(
        body["error"]["data"]["supported"][0],
        MODERN_PROTOCOL_VERSION
    );
}

#[tokio::test]
async fn modern_discover_is_stateless_and_returns_capabilities() {
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let app = crate::handler::build_router(state);
    let request = modern_request(
        "server/discover",
        Some(7),
        json!({}),
        Some("application/json, text/event-stream"),
    );
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(MCP_SESSION_HEADER).is_none());
    let (_, body) = response_json(response).await;
    assert_eq!(body["result"]["resultType"], "complete");
    assert_eq!(
        body["result"]["supportedVersions"][0],
        MODERN_PROTOCOL_VERSION
    );
    assert_eq!(
        body["result"]["_meta"][MODERN_SERVER_INFO_METADATA_KEY]["name"],
        "agui-acp-bridge"
    );
    assert_eq!(body["result"]["cacheScope"], MODERN_DISCOVER_CACHE_SCOPE);
    assert_eq!(
        body["result"]["capabilities"]["tools"]["listChanged"],
        false
    );
}
