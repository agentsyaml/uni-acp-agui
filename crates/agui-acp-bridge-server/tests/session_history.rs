//! Integration tests for the ACP-backed conversation-history surface:
//!
//! - `GET /sessions` → ACP `session/list` pass-through.
//! - Resume an existing conversation via `session/load` (history replay)
//!   when a run carries `forwardedProps.acpResume = true` for a thread the
//!   bridge has no live session for.
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

    // 2. Issue a resume bootstrap run: a run with NO trailing user message
    //    on a thread the bridge has no live session for. The bridge detects
    //    this shape as a resume and issues `session/load`, replaying the
    //    stored history (shared store) back into the stream. No client flag
    //    is needed — detection is purely from the protocol shape (this is
    //    what CopilotKit's `connectAgent` bootstrap looks like).
    let input = RunAgentInput::new(&resume_id, "r-resume");
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
}
