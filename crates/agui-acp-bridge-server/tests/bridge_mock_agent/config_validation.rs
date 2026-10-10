use super::*;

#[tokio::test]
async fn validates_boolean_config_route_uses_typed_values_and_rejects_mismatches() {
    use agent_client_protocol::schema::v1::SessionConfigOptionValue;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let probe = Arc::new(test_agents::BooleanConfigProbe::default());
    let probe_for_client = probe.clone();
    let client = client_for(move |stream| {
        test_agents::run_boolean_config_agent(stream, probe_for_client.clone())
    });
    let state = state_with_client(client);
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body = serde_json::to_vec(&user_input("thread-boolean", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let _ = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn))
        .await
        .expect("first run must finish");
    drop(conn);

    assert!(
        probe.initialize_boolean_capability(),
        "live initialize must advertise boolean config options"
    );
    let option_ids = state
        .session_init_state("thread-boolean")
        .and_then(|init| init.config_options)
        .expect("boolean config options must be cached")
        .into_iter()
        .map(|option| option.id.0.to_string())
        .collect::<Vec<_>>();
    assert_eq!(option_ids, ["enabled", "mode"]);

    let boolean_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-boolean",
        "configId": "enabled",
        "value": true,
    }))
    .unwrap();
    let mut boolean_response = raw_post(addr, "/session/set-config-option", boolean_body, "").await;
    assert_eq!(
        read_status_code(&mut boolean_response).await,
        StatusCode::OK
    );

    let string_body = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-boolean",
        "configId": "mode",
        "value": "code",
    }))
    .unwrap();
    let mut string_response = raw_post(addr, "/session/set-config-option", string_body, "").await;
    assert_eq!(read_status_code(&mut string_response).await, StatusCode::OK);

    let requests = probe.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        &requests[0].1,
        SessionConfigOptionValue::Boolean { value: true }
    ));
    assert!(matches!(
        &requests[1].1,
        SessionConfigOptionValue::ValueId { value } if value.0.as_ref() == "code"
    ));

    let string_to_boolean = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-boolean",
        "configId": "enabled",
        "value": "true",
    }))
    .unwrap();
    let mut mismatch = raw_post(addr, "/session/set-config-option", string_to_boolean, "").await;
    assert_eq!(
        read_status_code(&mut mismatch).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let boolean_to_select = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-boolean",
        "configId": "mode",
        "value": false,
    }))
    .unwrap();
    let mut mismatch = raw_post(addr, "/session/set-config-option", boolean_to_select, "").await;
    assert_eq!(
        read_status_code(&mut mismatch).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(probe.requests().len(), 2, "mismatches must not reach ACP");

    let unknown = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-boolean",
        "configId": "missing",
        "value": true,
    }))
    .unwrap();
    let mut unknown_response = raw_post(addr, "/session/set-config-option", unknown, "").await;
    assert_eq!(
        read_status_code(&mut unknown_response).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unknown advertised config IDs must be rejected before ACP"
    );
    assert_eq!(
        probe.requests().len(),
        2,
        "unknown config IDs must not reach ACP"
    );

    server.abort();
}

#[tokio::test]
async fn validates_config_domains_before_generic_mode_or_model_agent_calls() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_client = calls.clone();
    let client = client_for(move |stream| {
        test_agents::run_rejecting_config_agent(stream, calls_for_client.clone())
    });
    let state = state_with_client(client);
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-config-validation", "run-1", "boot"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initial run must finish: {body}");

    let app = agui_acp_bridge_server::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let requests = [
        (
            "/session/set-config-option",
            serde_json::json!({
                "threadId": "thread-config-validation",
                "configId": "mode",
                "value": "not-advertised",
            }),
        ),
        (
            "/session/set-config-option",
            serde_json::json!({
                "threadId": "thread-config-validation",
                "configId": "missing",
                "value": "ask",
            }),
        ),
        (
            "/session/set-mode",
            serde_json::json!({
                "threadId": "thread-config-validation",
                "modeId": "not-advertised",
            }),
        ),
        (
            "/session/set-model",
            serde_json::json!({
                "threadId": "thread-config-validation",
                "modelId": "not-advertised",
            }),
        ),
    ];
    for (path, body) in requests {
        let mut response = raw_post(addr, path, serde_json::to_vec(&body).unwrap(), "").await;
        assert_eq!(
            read_status_code(&mut response).await,
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid config request must be rejected before ACP: {path}"
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "invalid config requests must not call the rejecting ACP mock"
    );

    server.abort();
}

#[tokio::test]
async fn validates_undiscovered_settings_before_acp_calls() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_client = calls.clone();
    let client = client_for(move |stream| {
        test_agents::run_rejecting_undiscovered_settings_agent(stream, calls_for_client.clone())
    });
    let state = state_with_client(client);
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-undiscovered-settings", "run-1", "boot"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initial run must finish: {body}");
    let init = state
        .session_init_state("thread-undiscovered-settings")
        .expect("session must exist");
    assert!(init.config_options.is_none());
    assert!(init.modes.is_none());

    let app = agui_acp_bridge_server::build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let generic = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-undiscovered-settings",
        "configId": "anything",
        "value": true,
    }))
    .unwrap();
    let mut response = raw_post(addr, "/session/set-config-option", generic, "").await;
    assert_eq!(
        read_status_code(&mut response).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "generic config setting without a snapshot must be rejected"
    );

    let legacy_mode = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-undiscovered-settings",
        "modeId": "ask",
    }))
    .unwrap();
    let mut response = raw_post(addr, "/session/set-mode", legacy_mode, "").await;
    assert_eq!(
        read_status_code(&mut response).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "legacy mode setting without a capability must be rejected"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "undiscovered settings must not call the rejecting ACP mock"
    );

    server.abort();
}
