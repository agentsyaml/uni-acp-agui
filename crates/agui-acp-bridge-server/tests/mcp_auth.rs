use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use agui_acp_bridge_core::{BridgeConfig, SessionConfig, SessionSummary, acp::AcpSessionHandle};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, BridgeError, CustomAgentInProcessClient, build_router, test_agents,
};
use async_trait::async_trait;
use axum::{
    body::{Body, Bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

struct CapturingClient {
    inner: Arc<dyn AcpClient>,
    configs: Arc<Mutex<Vec<SessionConfig>>>,
}

#[async_trait]
impl AcpClient for CapturingClient {
    async fn open_session(&self, config: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        self.configs.lock().unwrap().push(config.clone());
        self.inner.open_session(config).await
    }
    async fn list_sessions(
        &self,
        config: SessionConfig,
    ) -> Result<Vec<SessionSummary>, BridgeError> {
        self.inner.list_sessions(config).await
    }
}

fn mcp_request(path: &str, token: &str, origin: Option<&str>) -> Request<Body> {
    let mut builder = Request::post(path)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));
    if let Some(origin) = origin {
        builder = builder.header("origin", origin);
    }
    builder
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
        ))
        .unwrap()
}

async fn start(state: &BridgeAppState, thread: &str) {
    let app = build_router(state.clone());
    let body =
        serde_json::to_string(&agui_rs_core::types::RunAgentInput::new(thread, "r1")).unwrap();
    let response = app
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .header("authorization", "Bearer admin-token-1234567890")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap();
}

async fn assert_rejected_without_body_poll(
    state: &BridgeAppState,
    path: &str,
    token: &str,
    origin: Option<&str>,
    expected: StatusCode,
) {
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let body = Body::from_stream(futures::stream::poll_fn(move |_| {
        observed.store(true, Ordering::SeqCst);
        std::task::Poll::<Option<Result<Bytes, std::io::Error>>>::Pending
    }));
    let mut request = Request::post(path)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));
    if let Some(origin) = origin {
        request = request.header("origin", origin);
    }
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        build_router(state.clone()).oneshot(request.body(body).unwrap()),
    )
    .await
    .expect("authentication must reject without waiting for a request body")
    .unwrap();
    assert_eq!(response.status(), expected);
    assert!(!polled.load(Ordering::SeqCst), "request body was polled");
}

#[tokio::test]
async fn mcp_rejects_before_polling_request_body() {
    let configs = Arc::new(Mutex::new(Vec::new()));
    let inner: Arc<dyn AcpClient> =
        Arc::new(CustomAgentInProcessClient::new(|stream| async move {
            test_agents::run_close_agent(
                stream,
                Arc::new(Mutex::new(Vec::new())),
                test_agents::CloseBehavior::Success,
            )
            .await
        }));
    let state = BridgeAppState::builder(
        Arc::new(CapturingClient {
            inner,
            configs: configs.clone(),
        }),
        PathBuf::from("/"),
    )
    .with_self_url("http://127.0.0.1:8080")
    .with_bearer_token("admin-token-1234567890")
    .unwrap()
    .with_mcp_allowed_origins(["https://allowed.example"])
    .unwrap()
    .build();
    start(&state, "owned-thread").await;
    let token = configs.lock().unwrap()[0].mcp_headers[0]
        .value
        .strip_prefix("Bearer ")
        .unwrap()
        .to_owned();

    assert_rejected_without_body_poll(
        &state,
        "/mcp/unknown-thread",
        &token,
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_rejected_without_body_poll(
        &state,
        "/mcp/owned-thread",
        "wrong-token",
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_rejected_without_body_poll(
        &state,
        "/mcp/owned-thread",
        "admin-token-1234567890",
        None,
        StatusCode::UNAUTHORIZED,
    )
    .await;
    assert_rejected_without_body_poll(
        &state,
        "/mcp/owned-thread",
        &token,
        Some("https://evil.example"),
        StatusCode::FORBIDDEN,
    )
    .await;
}

#[tokio::test]
async fn scoped_mcp_credential_is_thread_limited_and_revoked_on_removal() {
    let configs = Arc::new(Mutex::new(Vec::new()));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let closed_for_agent = closed.clone();
    let inner: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        let closed = closed_for_agent.clone();
        async move {
            test_agents::run_close_agent(stream, closed, test_agents::CloseBehavior::Success).await
        }
    }));
    let client = Arc::new(CapturingClient {
        inner,
        configs: configs.clone(),
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_self_url("http://127.0.0.1:8080")
        .with_bearer_token("admin-token-1234567890")
        .unwrap()
        .with_mcp_allowed_origins(["https://allowed.example"])
        .unwrap()
        .with_config(BridgeConfig {
            idle_timeout: std::time::Duration::from_secs(3600),
            ..Default::default()
        })
        .build();
    start(&state, "owned-thread").await;
    let first = configs.lock().unwrap()[0].clone();
    let token = first
        .mcp_headers
        .iter()
        .find(|h| h.name == "Authorization")
        .unwrap()
        .value
        .strip_prefix("Bearer ")
        .unwrap()
        .to_string();
    assert_ne!(token, "admin-token-1234567890");
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request("/mcp/owned-thread", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let tools_list = Request::post("/mcp/owned-thread")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
        ))
        .unwrap();
    assert_eq!(
        build_router(state.clone())
            .oneshot(tools_list)
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request("/mcp/other-thread", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        build_router(state.clone())
            .oneshot(
                Request::post("/approval")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from("{}"))
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request(
                "/mcp/owned-thread",
                &token,
                Some("https://evil.example")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );

    state
        .close_session("owned-thread")
        .await
        .expect("close removes session");
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request("/mcp/owned-thread", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    start(&state, "owned-thread").await;
    let replacement = {
        let configs = configs.lock().unwrap();
        configs[1].mcp_headers[0]
            .value
            .strip_prefix("Bearer ")
            .unwrap()
            .to_string()
    };
    assert_ne!(replacement, token);
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request("/mcp/owned-thread", &replacement, None))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        build_router(state)
            .oneshot(mcp_request("/mcp/owned-thread", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn scoped_credential_uses_decoded_encoded_thread_segment() {
    let configs = Arc::new(Mutex::new(Vec::new()));
    let inner: Arc<dyn AcpClient> =
        Arc::new(CustomAgentInProcessClient::new(|stream| async move {
            test_agents::run_close_agent(
                stream,
                Arc::new(Mutex::new(Vec::new())),
                test_agents::CloseBehavior::Success,
            )
            .await
        }));
    let state = BridgeAppState::builder(
        Arc::new(CapturingClient {
            inner,
            configs: configs.clone(),
        }),
        PathBuf::from("/"),
    )
    .with_self_url("http://127.0.0.1:8080")
    .with_bearer_token("admin-token-1234567890")
    .unwrap()
    .build();
    start(&state, "odd/thread").await;
    let token = configs.lock().unwrap()[0].mcp_headers[0]
        .value
        .strip_prefix("Bearer ")
        .unwrap()
        .to_owned();
    assert_eq!(
        build_router(state)
            .oneshot(mcp_request("/mcp/odd%2Fthread", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn failed_open_revokes_credential_registered_for_initialization() {
    struct FailedOpen(Arc<Mutex<Option<SessionConfig>>>);
    #[async_trait]
    impl AcpClient for FailedOpen {
        async fn open_session(
            &self,
            config: SessionConfig,
        ) -> Result<AcpSessionHandle, BridgeError> {
            *self.0.lock().unwrap() = Some(config);
            Err(BridgeError::Unsupported("initialization failed".into()))
        }
        async fn list_sessions(
            &self,
            _: SessionConfig,
        ) -> Result<Vec<SessionSummary>, BridgeError> {
            unreachable!()
        }
    }
    let captured = Arc::new(Mutex::new(None));
    let state = BridgeAppState::builder(Arc::new(FailedOpen(captured.clone())), PathBuf::from("/"))
        .with_self_url("http://127.0.0.1:8080")
        .with_bearer_token("admin-token-1234567890")
        .unwrap()
        .build();
    let app = build_router(state.clone());
    let input = agui_rs_core::types::RunAgentInput::new("failed-open", "r1");
    let response = app
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .header("authorization", "Bearer admin-token-1234567890")
                .body(Body::from(serde_json::to_vec(&input).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let config = captured.lock().unwrap().take().unwrap();
    let token = config.mcp_headers[0]
        .value
        .strip_prefix("Bearer ")
        .unwrap()
        .to_owned();
    assert_eq!(
        build_router(state)
            .oneshot(mcp_request("/mcp/failed-open", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn cancelled_open_revokes_credential_registered_before_handshake() {
    struct PendingOpen(Arc<Mutex<Option<SessionConfig>>>, Arc<tokio::sync::Notify>);
    #[async_trait]
    impl AcpClient for PendingOpen {
        async fn open_session(
            &self,
            config: SessionConfig,
        ) -> Result<AcpSessionHandle, BridgeError> {
            *self.0.lock().unwrap() = Some(config);
            self.1.notify_one();
            std::future::pending().await
        }
        async fn list_sessions(
            &self,
            _: SessionConfig,
        ) -> Result<Vec<SessionSummary>, BridgeError> {
            unreachable!()
        }
    }
    let captured = Arc::new(Mutex::new(None));
    let ready = Arc::new(tokio::sync::Notify::new());
    let state = BridgeAppState::builder(
        Arc::new(PendingOpen(captured.clone(), ready.clone())),
        PathBuf::from("/"),
    )
    .with_self_url("http://127.0.0.1:8080")
    .with_bearer_token("admin-token-1234567890")
    .unwrap()
    .build();
    let app = build_router(state.clone());
    let input = agui_rs_core::types::RunAgentInput::new("cancelled-open", "r1");
    let opening = tokio::spawn(
        app.oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .header("authorization", "Bearer admin-token-1234567890")
                .body(Body::from(serde_json::to_vec(&input).unwrap()))
                .unwrap(),
        ),
    );
    ready.notified().await;
    let token = captured.lock().unwrap().as_ref().unwrap().mcp_headers[0]
        .value
        .strip_prefix("Bearer ")
        .unwrap()
        .to_owned();
    assert_eq!(
        build_router(state.clone())
            .oneshot(mcp_request("/mcp/cancelled-open", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    opening.abort();
    let _ = opening.await;
    assert_eq!(
        build_router(state)
            .oneshot(mcp_request("/mcp/cancelled-open", &token, None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}
