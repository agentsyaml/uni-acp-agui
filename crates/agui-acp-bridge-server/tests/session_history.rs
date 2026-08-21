//! Integration tests for the ACP-backed conversation-history surface:
//!
//! - `GET /sessions` → ACP `session/list` pass-through.
//! - Resume an existing conversation via `session/load` (history replay)
//!   when a run carries `forwardedProps.acpResume = true`.
//!
//! The bridge holds no history of its own; these verify it faithfully
//! surfaces what the agent persists. A shared session store backs the mock
//! agent so its persisted sessions survive across the separate ACP
//! connections the bridge opens for `open_session` / `list_sessions`
//! (modelling a real agent that persists to disk).

mod support;

use std::path::PathBuf;
use std::sync::Arc;

use agui_acp_bridge_core::SessionSummary;
use agui_acp_bridge_core::acp::{AcpClient, AcpSessionHandle, SessionConfig};
use agui_acp_bridge_server::test_agents::SharedSessionStore;
use agui_acp_bridge_server::{
    BridgeAppState, BridgeError, InProcessAcpClient, build_router, test_agents,
};
use agui_rs_core::types::RunAgentInput;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use support::{collect_sse_body, user_input};

/// An `AcpClient` whose every connection shares one in-memory session store,
/// so sessions created on one connection are visible to later `session/list`
/// and `session/load` connections — exactly how a real persisting agent
/// behaves.
struct SharedHistoryClient {
    store: SharedSessionStore,
}

impl SharedHistoryClient {
    fn new() -> Self {
        Self {
            store: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl AcpClient for SharedHistoryClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let store = self.store.clone();
        agui_acp_bridge_core::spawn_in_process_session_with(cfg, move |s| {
            let store = store.clone();
            Box::pin(test_agents::run_session_history_agent_with(s, store))
        })
        .await
    }

    async fn list_sessions(&self, cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        // Exercise the REAL listing code path (initialize → capability gate →
        // session/list over a duplex), against a fresh agent instance that
        // shares this client's session store. This is the same path a
        // subprocess client (opencode) drives.
        let store = self.store.clone();
        agui_acp_bridge_core::list_sessions_in_process_with(cfg, move |s| {
            let store = store.clone();
            Box::pin(test_agents::run_session_history_agent_with(s, store))
        })
        .await
    }
}

async fn get_sessions_json(state: &BridgeAppState) -> (StatusCode, Value) {
    let app = build_router(state.clone());
    let resp = app
        .oneshot(HttpRequest::get("/sessions").body(Body::empty()).unwrap())
        .await
        .expect("router error");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

#[tokio::test]
async fn sessions_endpoint_returns_501_when_unsupported() {
    // The echo agent does not support session/list.
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let (status, _) = get_sessions_json(&state).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn sessions_endpoint_lists_created_conversations() {
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    // Initially empty.
    let (s0, b0) = get_sessions_json(&state).await;
    assert_eq!(s0, StatusCode::OK, "body: {b0}");
    assert_eq!(b0["sessions"].as_array().unwrap().len(), 0);

    // Drive two conversations on distinct threads.
    let (sa, _) = collect_sse_body(state.clone(), user_input("t-a", "r1", "hello A")).await;
    assert_eq!(sa, StatusCode::OK);
    let (sb, _) = collect_sse_body(state.clone(), user_input("t-b", "r1", "hello B")).await;
    assert_eq!(sb, StatusCode::OK);

    // GET /sessions now reports both, with titles from the first prompt.
    let (s1, b1) = get_sessions_json(&state).await;
    assert_eq!(s1, StatusCode::OK, "body: {b1}");
    let sessions = b1["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 2, "expected two sessions, got: {b1}");
    let titles: Vec<&str> = sessions
        .iter()
        .filter_map(|s| s["title"].as_str())
        .collect();
    assert!(titles.contains(&"hello A"), "titles: {titles:?}");
    assert!(titles.contains(&"hello B"), "titles: {titles:?}");
    // Every entry carries a sessionId usable as a resume threadId.
    assert!(sessions.iter().all(|s| s["sessionId"].as_str().is_some()));
}

#[tokio::test]
async fn resume_bootstrap_run_replays_loaded_history() {
    // 1. Create a conversation and record two turns under thread "conv".
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (_, _) = collect_sse_body(state.clone(), user_input("conv", "r1", "alpha")).await;
    let (_, _) = collect_sse_body(state.clone(), user_input("conv", "r2", "beta")).await;

    // The bridge's live session for "conv" used an agent-assigned SessionId.
    // To resume by loading, the frontend would use that SessionId (from GET
    // /sessions) as the threadId. Discover it.
    let summaries = state.list_sessions().await.expect("list");
    assert!(!summaries.is_empty(), "agent must have recorded a session");
    let resume_id = summaries[0].session_id.clone();

    // 2. Issue an explicit bridge-private resume bootstrap run. The marker is
    //    required; without it the same input is an ordinary new session/no-op
    //    run.
    let input = RunAgentInput::new(&resume_id, "r-resume");
    let mut input = input;
    input.forwarded_props = serde_json::json!({"acpResume": true});
    let (status, body) = collect_sse_body(state.clone(), input).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "resume run must finish cleanly, body:\n{body}"
    );
    // The loaded history (HISTORY:user:alpha / assistant / beta …) must be
    // replayed as text into the stream.
    assert!(
        body.contains("HISTORY:user:alpha") && body.contains("HISTORY:user:beta"),
        "resume must replay the loaded conversation history, body:\n{body}"
    );
    assert!(
        body.contains("\"configOptions\"") && body.contains("\"currentValue\":\"loaded\""),
        "session/load config options must be cached and emitted, body:\n{body}"
    );
    assert!(
        state
            .session_init_state(&resume_id)
            .and_then(|init| init.config_options)
            .is_some_and(|options| {
                options.iter().any(|option| {
                    option.id.0.as_ref() == "history-mode"
                        && matches!(
                            &option.kind,
                            agent_client_protocol::schema::v1::SessionConfigKind::Select(select)
                                if select.current_value.0.as_ref() == "loaded"
                        )
                })
            }),
        "cached session/load state must contain the full config option list"
    );
}

#[tokio::test]
async fn explicit_resume_on_live_session_drains_without_new_session() {
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (_, _) = collect_sse_body(state.clone(), user_input("live", "r1", "alpha")).await;

    let mut input = RunAgentInput::new("live", "r-resume-live");
    input.forwarded_props = serde_json::json!({"acpResume": true});
    let (status, body) = collect_sse_body(state.clone(), input).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("RUN_FINISHED"), "body:\n{body}");
    assert!(
        body.contains("configOptions"),
        "explicit resume must drain the live session through the resume path:\n{body}"
    );
    assert_eq!(state.list_sessions().await.expect("list").len(), 1);
}

#[tokio::test]
async fn bootstrap_without_marker_stays_on_normal_path() {
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (_, _) = collect_sse_body(state.clone(), user_input("original", "r1", "alpha")).await;
    let resume_id = state.list_sessions().await.expect("list")[0]
        .session_id
        .clone();

    let input = RunAgentInput::new(&resume_id, "r-bootstrap");
    let (status, body) = collect_sse_body(state.clone(), input).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("RUN_FINISHED"));
    assert!(
        !body.contains("HISTORY:"),
        "markerless bootstrap must not load:\n{body}"
    );

    let sessions = state.list_sessions().await.expect("list after bootstrap");
    assert_eq!(
        sessions.len(),
        2,
        "markerless bootstrap must create a new ACP session"
    );
}

#[tokio::test]
async fn explicit_resume_with_trailing_user_loads_before_prompt() {
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (_, _) = collect_sse_body(state.clone(), user_input("original", "r1", "alpha")).await;
    let resume_id = state.list_sessions().await.expect("list")[0]
        .session_id
        .clone();
    let mut input = user_input(&resume_id, "r-resume-prompt", "gamma");
    input.forwarded_props = serde_json::json!({"acpResume": true});

    let (status, body) = collect_sse_body(state, input).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("HISTORY:user:alpha"),
        "load history missing:\n{body}"
    );
    assert!(body.contains("echo: gamma"), "new prompt missing:\n{body}");
}

#[tokio::test]
async fn explicit_resume_without_load_capability_is_a_run_error() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let mut input = RunAgentInput::new("resume-unsupported", "r-unsupported");
    input.forwarded_props = serde_json::json!({"acpResume": true});

    let (status, body) = collect_sse_body(state.clone(), input).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "resume failure must be an AG-UI stream:\n{body}"
    );
    assert!(body.contains("ACP_RESUME_UNSUPPORTED"), "body:\n{body}");
    assert!(
        !body.contains("RUN_FINISHED"),
        "resume failure must not succeed:\n{body}"
    );
    assert_eq!(state.session_count(), 0);
    assert_eq!(state.frontend_tools().thread_count(), 0);
}

#[tokio::test]
async fn explicit_resume_load_failure_is_a_run_error_without_new_session() {
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("/"));
    let mut input = RunAgentInput::new("unknown-session-id", "r-failed");
    input.forwarded_props = serde_json::json!({"acpResume": true});

    let (status, body) = collect_sse_body(state.clone(), input).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "resume failure must be an AG-UI stream:\n{body}"
    );
    assert!(body.contains("ACP_RESUME_FAILED"), "body:\n{body}");
    assert!(
        !body.contains("RUN_FINISHED"),
        "resume failure must not succeed:\n{body}"
    );
    assert_eq!(state.session_count(), 0);
    assert_eq!(state.frontend_tools().thread_count(), 0);
    assert!(state.list_sessions().await.expect("list").is_empty());
}
