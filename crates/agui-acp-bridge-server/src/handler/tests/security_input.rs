use super::*;
fn authenticated_state() -> BridgeAppState {
    BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
        .with_bearer_token(TEST_TOKEN)
        .expect("test token is valid")
        .build()
}

fn auth_request(method: Method, uri: &str, authorization: Option<&str>) -> HttpRequest<Body> {
    let mut builder = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(value) = authorization {
        builder = builder.header(header::AUTHORIZATION, value);
    }
    builder.body(Body::from("{}")).unwrap()
}

#[test]
fn prompt_stream_public_shape_remains_two_field_compatible() {
    let (_, events) = tokio::sync::mpsc::channel(1);
    let (_, finished) = tokio::sync::oneshot::channel::<
        Result<agent_client_protocol::schema::v1::StopReason, BridgeError>,
    >();
    let _stream = PromptStream { events, finished };
}

#[test]
fn stop_reason_terminal_mapping_is_not_unconditionally_successful() {
    let cases = [
        (StopReason::EndTurn, "RUN_FINISHED", None),
        (StopReason::Cancelled, "RUN_ERROR", Some("ACP_CANCELLED")),
        (StopReason::MaxTokens, "RUN_ERROR", Some("ACP_MAX_TOKENS")),
        (
            StopReason::MaxTurnRequests,
            "RUN_ERROR",
            Some("ACP_MAX_TURN_REQUESTS"),
        ),
        (StopReason::Refusal, "RUN_ERROR", Some("ACP_REFUSAL")),
    ];

    for (reason, event_type, code) in cases {
        let value = serde_json::to_value(stop_reason_terminal_event(
            "thread".to_string(),
            "run".to_string(),
            reason,
        ))
        .expect("event serializes");
        assert_eq!(value["type"], event_type);
        assert_eq!(value.get("code").and_then(Value::as_str), code);
    }
}

#[test]
fn default_state_policy_is_auto_deny() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    assert_eq!(format!("{:?}", state.policy()), "AutoDeny");

    let builder_state =
        BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/")).build();
    assert_eq!(format!("{:?}", builder_state.policy()), "AutoDeny");
}

#[test]
fn bearer_token_validation_rejects_unsafe_values() {
    for token in ["", "too-short", "token with spaces", "token\nwith-control"] {
        assert!(
            validate_bearer_token(token).is_err(),
            "token should be rejected: {token:?}"
        );
    }
    assert!(validate_bearer_token(TEST_TOKEN).is_ok());
}

#[test]
fn mcp_origin_allowlist_canonicalizes_effective_ports() {
    assert_eq!(
        crate::mcp_endpoint::canonicalize_origin("HTTPS://Example.COM").unwrap(),
        "https://example.com:443"
    );
    assert_eq!(
        crate::mcp_endpoint::canonicalize_origin("http://[::1]:80").unwrap(),
        "http://[::1]:80"
    );
    assert!(crate::mcp_endpoint::canonicalize_origin("https://example.com/").is_err());
}

#[test]
fn mcp_origin_allowlist_rejects_ambiguous_values() {
    for origin in [
        "null",
        "*",
        "https://*.example.com",
        "https://example.com/path",
        "https://example.com/",
        "https://example.com:bad",
        "https://user@example.com",
        "https://example.com?query=1",
    ] {
        assert!(
            BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
                .with_mcp_allowed_origins([origin])
                .is_err(),
            "origin should be rejected: {origin}"
        );
    }
}

#[test]
fn mcp_headers_never_carry_admin_token() {
    let state = BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
        .with_self_url("http://127.0.0.1:8080")
        .with_bearer_token(TEST_TOKEN)
        .expect("test token is valid")
        .build();
    let cfg = state.session_config_for("thread");
    assert!(
        cfg.mcp_headers.is_empty(),
        "transient config has no credential"
    );
    let scoped = McpCredential("scoped-token".into());
    let headers = state.mcp_headers(Some(&scoped));
    assert_eq!(headers[0].value, "Bearer scoped-token");
    assert!(!headers[0].value.contains(TEST_TOKEN));
    assert!(!format!("{cfg:?}").contains(TEST_TOKEN));
}

#[test]
fn mcp_url_encodes_thread_token_as_one_path_segment() {
    let state = BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
        .with_self_url("http://127.0.0.1:8080")
        .build();
    let cfg = state.session_config_for("thread/slash?query#fragment");
    assert_eq!(
        cfg.mcp_url.as_deref(),
        Some("http://127.0.0.1:8080/mcp/thread%2Fslash%3Fquery%23fragment")
    );
}

#[tokio::test]
async fn bearer_middleware_covers_every_protected_route() {
    let app = build_router(authenticated_state());
    let routes = [
        (Method::POST, "/"),
        (Method::POST, "/approval"),
        (Method::POST, "/tool-response"),
        (Method::GET, "/sessions"),
        (Method::GET, "/session/init?threadId=thread"),
        (Method::POST, "/session/set-mode"),
        (Method::POST, "/session/set-config-option"),
        (Method::POST, "/session/cancel"),
        (Method::POST, "/session/close"),
        (Method::POST, "/session/delete"),
        (Method::POST, "/session/set-model"),
    ];

    for (method, uri) in routes {
        for authorization in [
            None,
            Some("Bearer wrong-token-1234"),
            Some("Basic wrong-token-1234"),
            Some("Bearer"),
        ] {
            let response = app
                .clone()
                .oneshot(auth_request(method.clone(), uri, authorization))
                .await
                .expect("router response");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
            assert_eq!(
                response.headers().get(header::WWW_AUTHENTICATE),
                Some(&HeaderValue::from_static("Bearer")),
                "{method} {uri} must advertise bearer auth"
            );
        }

        let response = app
            .clone()
            .oneshot(auth_request(
                method.clone(),
                uri,
                Some(&format!("Bearer {TEST_TOKEN}")),
            ))
            .await
            .expect("router response");
        assert_ne!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }
}

#[tokio::test]
async fn only_exact_health_get_and_head_are_anonymous() {
    let app = build_router(authenticated_state());
    for method in [Method::GET, Method::HEAD] {
        let response = app
            .clone()
            .oneshot(auth_request(method, "/health", None))
            .await
            .expect("health response");
        assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
    }
    for (method, uri) in [
        (Method::GET, "/health?probe=1"),
        (Method::GET, "/health/"),
        (Method::POST, "/health"),
    ] {
        let response = app
            .clone()
            .oneshot(auth_request(method.clone(), uri, None))
            .await
            .expect("health response");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }
}

#[tokio::test]
async fn tool_response_requires_thread_id() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let response = build_router(state)
        .oneshot(
            HttpRequest::builder()
                .method(Method::POST)
                .uri("/tool-response")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"toolCallId":"call","content":"ok","isError":false}"#,
                ))
                .unwrap(),
        )
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn agui_input_validation_rejects_invalid_input_before_session_admission() {
    let mut duplicate_messages = RunAgentInput::new("thread", "run");
    for text in ["one", "two"] {
        duplicate_messages
            .messages
            .push(Message::User(agui_rs_core::types::UserMessage {
                id: "duplicate-message-id".into(),
                content: UserMessageContent::Text(text.into()),
                name: None,
                encrypted_value: None,
            }));
    }

    let inputs = [
        RunAgentInput::new("", "run"),
        RunAgentInput::new("thread", " "),
        duplicate_messages,
    ];
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));

    for input in inputs {
        let response = build_router(state.clone())
            .oneshot(
                HttpRequest::post("/")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&input).unwrap()))
                    .unwrap(),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body");
        assert!(body.starts_with(b"invalid request body: "));
    }

    assert_eq!(state.session_count(), 0);
}

#[tokio::test]
async fn agui_body_limit_rejects_oversized_raw_body() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let response = build_router(state.clone())
        .oneshot(
            HttpRequest::post("/")
                .header("content-type", "application/json")
                .body(Body::from(vec![b'x'; DEFAULT_BODY_LIMIT_BYTES + 1]))
                .unwrap(),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("error body");
    assert_eq!(
        body,
        format!("invalid request body: request body exceeds {DEFAULT_BODY_LIMIT_BYTES} bytes")
            .as_bytes()
    );
    assert_eq!(state.session_count(), 0);
}

/// FIX 6 regression: `resolve_permission` only resolves within the
/// supplied thread. A real pending interrupt on one thread must remain
/// untouched when a different thread attempts to resolve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approval_resolution_is_thread_scoped() {
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;

    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
        Box::pin(crate::test_agents::run_request_permission_agent(stream))
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(Arc::new(InterruptViaAgUiEvent))
        .build();

    // A real prompt defers a permission request into the live session.
    let session = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("owner-thread"))
            .await
            .expect("session opens"),
    );
    let entry = Arc::new(SessionEntry::new(session.clone(), None));
    state.inner.sessions.insert("owner-thread".into(), entry);
    let (_prompt_stream, _turn) = session
        .prompt_with_turn("request a permission")
        .await
        .expect("prompt opens");
    // The policy mints a uuid interrupt id; grab whatever is pending.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let pending_id = session
        .pending_permissions()
        .iter()
        .next()
        .map(|entry| entry.key().clone())
        .expect("agent must park a pending permission");

    // Wrong thread: must NOT consume the interrupt…
    let outcome = state.resolve_permission(
        "other-thread",
        &pending_id,
        agui_acp_bridge_core::PermissionDecision::Deny,
    );
    assert_eq!(outcome, ResolveOutcome::NotFound);
    assert!(
        session.pending_permissions().contains_key(&pending_id),
        "cross-thread approval must not consume another thread's pending permission"
    );

    // Right thread: resolves.
    let outcome = state.resolve_permission(
        "owner-thread",
        &pending_id,
        agui_acp_bridge_core::PermissionDecision::Deny,
    );
    assert_eq!(outcome, ResolveOutcome::Resolved);
    assert!(!session.pending_permissions().contains_key(&pending_id));
}
