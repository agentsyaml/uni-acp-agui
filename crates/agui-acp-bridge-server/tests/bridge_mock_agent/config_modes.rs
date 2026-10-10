use super::*;

#[cfg(feature = "unstable_session_model")]
#[tokio::test]
async fn validates_session_init_event_advertises_modes_and_models() {
    // The mode-and-model agent advertises three modes + three models in
    // `NewSessionResponse`. The bridge surfaces them as a CUSTOM
    // `agent:session_init` event emitted ahead of any agent text — so a
    // frontend can render its picker before the first chunk lands.
    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let (status, body) = collect_sse_body(state, user_input("thread-mm", "run-mm", "ping")).await;
    assert_eq!(status, StatusCode::OK, "expected 200, body:\n{body}");

    // The session_init CUSTOM event must come BEFORE TEXT_MESSAGE_START.
    let types = extract_event_types(&body);
    let init_idx = types
        .iter()
        .position(|t| t == "CUSTOM")
        .expect("expected a CUSTOM event somewhere, got: {types:?}");
    let text_idx = types
        .iter()
        .position(|t| t == "TEXT_MESSAGE_START")
        .expect("expected TEXT_MESSAGE_START, got: {types:?}");
    assert!(
        init_idx < text_idx,
        "agent:session_init must precede text, got order: {types:?}"
    );

    // Spot-check the payload JSON contains both pickers.
    assert!(
        body.contains("\"agent:session_init\""),
        "expected agent:session_init custom event name, body:\n{body}"
    );
    assert!(
        body.contains("\"availableModes\"") && body.contains("\"architect\""),
        "expected availableModes with architect entry, body:\n{body}"
    );
    assert!(
        body.contains("\"availableModels\"") && body.contains("\"claude-sonnet\""),
        "expected availableModels with claude-sonnet entry, body:\n{body}"
    );
    assert!(
        body.contains("\"configOptions\""),
        "expected configOptions in the stable session-init event, body:\n{body}"
    );
    assert!(
        body.contains("\"id\":\"mode\"") && body.contains("\"id\":\"model\""),
        "expected discovered mode/model config options, body:\n{body}"
    );
}

#[tokio::test]
async fn validates_config_option_update_replaces_cached_snapshot() {
    let state = state_with_client(client_for(test_agents::run_config_update_agent));
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-config-update", "run-config-update", "update"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("\"acp.session_update\"")
            && body.contains("\"sessionUpdate\":\"config_option_update\"")
            && body.contains("\"id\":\"replacement\"")
            && body.contains("\"currentValue\":\"after\""),
        "live config update must keep the complete CUSTOM payload, body:\n{body}"
    );
    let options = state
        .session_init_state("thread-config-update")
        .and_then(|init| init.config_options)
        .expect("config options must remain cached");
    assert_eq!(options.len(), 1, "update must replace, not merge, options");
    assert_eq!(options[0].id.0.as_ref(), "replacement");
    assert!(body.contains("config updated"));
}

#[tokio::test]
async fn validates_set_mode_endpoint_round_trips_through_agent() {
    use std::time::Duration;
    use tokio::net::TcpListener;

    // 1. Open a session via a normal AG-UI run (so the bridge's session
    //    cache has an entry keyed by thread_id).
    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Drive a tiny run to create the session.
    let body = serde_json::to_vec(&user_input("thread-set-mode", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn))
        .await
        .expect("first run must finish");
    assert!(
        trailer.contains("\"type\":\"RUN_FINISHED\""),
        "first run must reach RUN_FINISHED, body:\n{trailer}"
    );
    drop(conn);

    // 2. POST /session/set-mode { threadId, modeId: "code" } → 200
    let payload =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-set-mode", "modeId": "code"}))
            .unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", payload, "").await;
    assert_eq!(read_status_code(&mut resp).await, StatusCode::OK);

    // 3. Cached init state should reflect the new current mode.
    let init = state
        .session_init_state("thread-set-mode")
        .expect("session must exist after first run");
    assert_eq!(
        init.modes.as_ref().map(|m| m.current_mode_id.as_str()),
        Some("code"),
        "current_mode_id must move to 'code' after set-mode"
    );

    // The explicit server-facing route uses the same stable wire method.
    let config_payload = serde_json::to_vec(&serde_json::json!({
        "threadId": "thread-set-mode",
        "configId": "model",
        "value": "gpt-4o",
    }))
    .unwrap();
    let mut config_resp = raw_post(addr, "/session/set-config-option", config_payload, "").await;
    assert_eq!(read_status_code(&mut config_resp).await, StatusCode::OK);
    let init = state
        .session_init_state("thread-set-mode")
        .expect("session must remain cached");
    assert!(
        init.config_options.as_ref().is_some_and(|options| {
            options.iter().any(|option| {
                option.id.0.as_ref() == "model"
                    && matches!(
                        &option.kind,
                        agent_client_protocol::schema::v1::SessionConfigKind::Select(select)
                            if select.current_value.0.as_ref() == "gpt-4o"
                    )
            })
        }),
        "set-config-option must replace the cached full list"
    );

    // 4. Unknown mode → 422 (agent rejects)
    let bad =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-set-mode", "modeId": "wat"}))
            .unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", bad, "").await;
    assert_eq!(
        read_status_code(&mut resp).await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "unknown mode_id must yield 422"
    );

    // 5. Unknown thread → 404
    let nope =
        serde_json::to_vec(&serde_json::json!({"threadId": "no-such", "modeId": "ask"})).unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", nope, "").await;
    assert_eq!(
        read_status_code(&mut resp).await,
        StatusCode::NOT_FOUND,
        "unknown thread_id must yield 404"
    );

    server.abort();
}

#[tokio::test]
async fn validates_mixed_mode_capabilities_fall_back_to_legacy_set_mode() {
    let state = state_with_client(client_for(test_agents::run_mixed_mode_capabilities_agent));
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-mixed-mode", "run-1", "boot"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");

    state
        .set_session_mode("thread-mixed-mode", "code")
        .await
        .expect("legacy session/set_mode fallback must succeed");
    assert_eq!(
        state
            .session_init_state("thread-mixed-mode")
            .and_then(|init| init.modes.map(|m| m.current_mode_id)),
        Some("code".to_string())
    );
}

#[tokio::test]
async fn validates_set_model_alias_uses_config_option_wire_method() {
    use std::time::Duration;
    use tokio::net::TcpListener;

    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body = serde_json::to_vec(&user_input("thread-set-model", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let _ = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn))
        .await
        .expect("first run must finish");
    drop(conn);

    let payload = serde_json::to_vec(
        &serde_json::json!({"threadId": "thread-set-model", "modelId": "claude-sonnet"}),
    )
    .unwrap();
    let mut resp = raw_post(addr, "/session/set-model", payload, "").await;
    assert_eq!(read_status_code(&mut resp).await, StatusCode::OK);

    let init = state
        .session_init_state("thread-set-model")
        .expect("session must exist");
    #[cfg(feature = "unstable_session_model")]
    assert_eq!(
        init.models.as_ref().map(|m| m.current_model_id.as_str()),
        Some("claude-sonnet"),
        "current_model_id must move after set-model"
    );
    assert!(
        init.config_options.as_ref().is_some_and(|options| {
            options.iter().any(|option| {
                option.id.0.as_ref() == "model"
                    && matches!(
                        &option.kind,
                        agent_client_protocol::schema::v1::SessionConfigKind::Select(select)
                            if select.current_value.0.as_ref() == "claude-sonnet"
                    )
            })
        }),
        "model alias must replace the complete config-option cache"
    );

    let bad = serde_json::to_vec(
        &serde_json::json!({"threadId": "thread-set-model", "modelId": "fake-model"}),
    )
    .unwrap();
    let mut resp = raw_post(addr, "/session/set-model", bad, "").await;
    assert_eq!(
        read_status_code(&mut resp).await,
        StatusCode::UNPROCESSABLE_ENTITY,
    );

    server.abort();
}

#[cfg(feature = "unstable_session_model")]
#[tokio::test]
async fn validates_session_init_endpoint_returns_modes_and_models() {
    use std::time::Duration;
    use tokio::net::TcpListener;

    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Before any run: session does not exist → 404.
    let mut resp = raw_get(addr, "/session/init?threadId=missing").await;
    assert_eq!(read_status_code(&mut resp).await, StatusCode::NOT_FOUND);

    // After a run: session opens and discovery returns the picker payload.
    let body = serde_json::to_vec(&user_input("thread-init", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let _ = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn))
        .await
        .expect("first run must finish");
    drop(conn);

    let mut resp = raw_get(addr, "/session/init?threadId=thread-init").await;
    let (code, body) = read_status_and_body(&mut resp).await;
    assert_eq!(code, StatusCode::OK);
    assert!(
        body.contains("\"availableModes\"") && body.contains("\"architect\""),
        "GET /session/init body must include modes, body:\n{body}"
    );
    assert!(
        body.contains("\"availableModels\"") && body.contains("\"gpt-4o\""),
        "GET /session/init body must include models, body:\n{body}"
    );
    assert!(
        body.contains("\"configOptions\"") && body.contains("\"id\":\"mode\""),
        "GET /session/init body must include config options, body:\n{body}"
    );

    server.abort();
}

#[tokio::test]
async fn validates_set_mode_then_next_prompt_session_init_reflects_change() {
    // End-to-end cache-coherence guarantee: after a successful
    // /session/set-mode, the *next* prompt on the same thread must emit
    // a SessionInit whose currentModeId is the new value. Same-prompt
    // mid-flight changes are not in scope (ACP doesn't promise that
    // either) — we only assert the cache is updated for the next turn.
    use std::time::Duration;
    use tokio::net::TcpListener;

    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // First run: SessionInit should advertise currentMode=ask (mock default).
    let body = serde_json::to_vec(&user_input("thread-cache", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn))
        .await
        .expect("first run finishes");
    drop(conn);
    assert!(
        trailer.contains("\"currentModeId\":\"ask\""),
        "first run SessionInit must report currentModeId=ask, got:\n{trailer}"
    );

    // Switch to "code".
    let payload =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-cache", "modeId": "code"}))
            .unwrap();
    let mut resp = raw_post(addr, "/session/set-mode", payload, "").await;
    assert_eq!(read_status_code(&mut resp).await, StatusCode::OK);

    // Second run on the SAME thread: SessionInit must now show "code".
    let body2 = serde_json::to_vec(&user_input("thread-cache", "run-2", "again")).unwrap();
    let mut conn2 = raw_post(addr, "/", body2, "Accept: text/event-stream\r\n").await;
    let trailer2 = tokio::time::timeout(Duration::from_secs(5), drain_to_end_local(&mut conn2))
        .await
        .expect("second run finishes");
    drop(conn2);
    assert!(
        trailer2.contains("\"currentModeId\":\"code\""),
        "second run SessionInit must reflect set_mode result; got:\n{trailer2}"
    );

    server.abort();
}

#[tokio::test]
async fn validates_session_init_event_uses_null_for_missing_modes_and_models() {
    // Schema-stability guarantee: even when the agent advertises neither
    // modes nor models, the SessionInit CUSTOM event payload still has
    // both keys present with `null` values, matching `GET /session/init`.
    // Frontends can branch on `payload.modes === null` once for both
    // transports.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (status, body) = collect_sse_body(state, user_input("thread-null", "run-1", "x")).await;
    assert_eq!(status, StatusCode::OK);

    // Find the agent:session_init data line.
    let init_line = body
        .lines()
        .filter(|l| l.starts_with("data:"))
        .find(|l| l.contains("\"name\":\"agent:session_init\""))
        .expect("must emit agent:session_init even when no modes/models");
    assert!(
        init_line.contains("\"modes\":null") && init_line.contains("\"models\":null"),
        "SessionInit must use explicit null for missing fields, got:\n{init_line}"
    );
}
