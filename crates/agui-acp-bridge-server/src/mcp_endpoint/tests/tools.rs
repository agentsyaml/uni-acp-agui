use super::*;

#[test]
fn tool_def_to_mcp_tool_uses_input_schema_key() {
    let def = FrontendToolDef {
        name: "alert".into(),
        description: "show alert".into(),
        parameters: json!({"type":"object","properties":{"text":{"type":"string"}}}),
    };
    let tool = tool_def_to_mcp_tool(def);
    assert_eq!(tool["name"], "alert");
    assert_eq!(tool["description"], "show alert");
    assert!(tool["inputSchema"]["properties"]["text"].is_object());
}

#[test]
fn mcp_text_content_shape_matches_spec() {
    let v = mcp_text_content("hi");
    assert_eq!(v["isError"], false);
    assert_eq!(v["content"][0]["type"], "text");
    assert_eq!(v["content"][0]["text"], "hi");
}

#[test]
fn mcp_tool_failure_is_result_error_not_jsonrpc_error() {
    let result = mcp_error_content("frontend failed");
    let response = serde_json::to_value(JsonRpcResponse::ok(json!(1), result)).unwrap();

    assert!(response.get("error").is_none());
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(response["result"]["content"][0]["type"], "text");
    assert_eq!(response["result"]["content"][0]["text"], "frontend failed");
}

#[tokio::test]
async fn legacy_initialize_and_tools_list_remain_request_response_compatible() {
    use tower::ServiceExt;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let entry = state.frontend_tools().entry("thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "legacy_tool".into(),
        description: "legacy".into(),
        parameters: json!({"type": "object"}),
    }]);
    let app = crate::handler::build_router(state);

    let legacy_session = axum::http::Request::post("/mcp/thread")
        .header("content-type", "application/json")
        .header(MCP_SESSION_HEADER, "legacy-session")
        .body(axum::body::Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
            })
            .to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(legacy_session).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    let legacy_replay = axum::http::Request::post("/mcp/thread")
        .header("content-type", "application/json")
        .header(LAST_EVENT_ID_HEADER, "legacy-event")
        .body(axum::body::Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
            })
            .to_string(),
        ))
        .unwrap();
    let (status, body) = response_json(app.clone().oneshot(legacy_replay).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["protocolVersion"], LEGACY_PROTOCOL_VERSION);

    let initialize = axum::http::Request::post("/mcp/thread")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
            })
            .to_string(),
        ))
        .unwrap();
    let (status, body) = response_json(app.clone().oneshot(initialize).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["protocolVersion"], LEGACY_PROTOCOL_VERSION);

    let list = axum::http::Request::post("/mcp/thread")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}).to_string(),
        ))
        .unwrap();
    let (status, body) = response_json(app.oneshot(list).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["tools"][0]["name"], "legacy_tool");
}

#[tokio::test]
async fn cancelled_tools_call_before_channel_acceptance_emits_no_lifecycle_events() {
    use std::time::Duration;

    let state = BridgeAppState::new(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    );
    let entry = state.frontend_tools().entry("cancel-thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "alert".into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }]);
    let (active_tx, mut events) = tokio::sync::mpsc::channel(1);
    entry.set_active_sender(Some(active_tx.clone()));
    active_tx
        .try_send(BridgeStreamItem::FrontendToolCall {
            tool_call_id: "fills-channel".into(),
            tool_name: "filler".into(),
            arguments: json!({}),
        })
        .expect("fill bounded channel");

    let call_state = state.clone();
    let task = tokio::spawn(async move {
        handle_tools_call(
            &call_state,
            "cancel-thread",
            json!(1),
            json!({"name":"alert","arguments":{}}),
            ProtocolMode::Legacy,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while entry.pending_len() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("MCP call registers before waiting for channel capacity");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(entry.pending_len(), 1);

    task.abort();
    assert!(task.await.expect_err("task was aborted").is_cancelled());
    assert_eq!(entry.pending_len(), 0);
    assert!(matches!(
        events.recv().await,
        Some(BridgeStreamItem::FrontendToolCall { tool_call_id, .. })
            if tool_call_id == "fills-channel"
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), events.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn accepted_tool_call_publishes_complete_envelope_before_result() {
    use agui_acp_bridge_core::frontend_tools::FrontendToolResponse;
    use std::time::Duration;

    for (thread, is_error, content) in [
        ("success", false, "structured result"),
        ("failure", true, "structured error"),
    ] {
        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry(thread);
        entry.set_tools(vec![FrontendToolDef {
            name: "alert".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }]);
        let (sender, mut events) = tokio::sync::mpsc::channel(4);
        entry.set_active_sender(Some(sender));
        let call_state = state.clone();
        let thread_id = thread.to_string();
        let task = tokio::spawn(async move {
            handle_tools_call(
                &call_state,
                &thread_id,
                json!(1),
                json!({"name":"alert","arguments":{}}),
                ProtocolMode::Legacy,
            )
            .await
        });
        let call = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("call event")
            .expect("frontend call");
        let BridgeStreamItem::FrontendToolCall { tool_call_id, .. } = call else {
            panic!("expected frontend call, got {call:?}");
        };
        assert_eq!(
            entry.pending_len(),
            1,
            "result remains pending after envelope publication"
        );
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let response = if is_error {
            FrontendToolResponse::error(content)
        } else {
            FrontendToolResponse::ok(content)
        };
        assert!(entry.resolve_pending(&tool_call_id, response));
        let (status, body) = response_json(task.await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["isError"], is_error);
        assert_eq!(body["result"]["content"][0]["text"], content);
        assert_eq!(entry.pending_len(), 0);
        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn accepted_tool_call_timeout_does_not_emit_a_second_end() {
    use agui_acp_bridge_core::BridgeConfig;
    use std::time::Duration;

    let state = BridgeAppState::builder(
        std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
        std::path::PathBuf::from("/"),
    )
    .with_config(BridgeConfig {
        frontend_tool_timeout: Duration::from_millis(100),
        ..BridgeConfig::default()
    })
    .build();
    let entry = state.frontend_tools().entry("timeout-thread");
    entry.set_tools(vec![FrontendToolDef {
        name: "alert".into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    }]);
    let (sender, mut events) = tokio::sync::mpsc::channel(4);
    entry.set_active_sender(Some(sender));
    let call_state = state.clone();
    let task = tokio::spawn(async move {
        handle_tools_call(
            &call_state,
            "timeout-thread",
            json!(1),
            json!({"name":"alert","arguments":{}}),
            ProtocolMode::Legacy,
        )
        .await
    });
    let call = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("call event")
        .expect("frontend call");
    let BridgeStreamItem::FrontendToolCall { tool_call_id, .. } = call else {
        panic!("expected frontend call, got {call:?}");
    };
    assert_eq!(entry.pending_len(), 1);
    let (status, body) = response_json(task.await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["isError"], true);
    assert_eq!(
        body["result"]["content"][0]["text"],
        "frontend tool timed out"
    );
    assert_eq!(entry.pending_len(), 0);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(!tool_call_id.is_empty());
}
