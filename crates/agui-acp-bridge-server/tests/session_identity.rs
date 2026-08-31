//! Explicit AG-UI thread and ACP SessionId identity coverage.

mod support;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agui_acp_bridge_server::test_agents::SharedSessionStore;
use agui_acp_bridge_server::{
    AcpClient, AcpSessionHandle, BridgeAppState, BridgeError, CustomAgentInProcessClient,
    SessionConfig, SessionSummary, test_agents,
};
use async_trait::async_trait;
use axum::http::StatusCode;

use support::{collect_sse_body, user_input};

struct IdentityClient {
    store: SharedSessionStore,
}

impl IdentityClient {
    fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl AcpClient for IdentityClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let store = self.store.clone();
        agui_acp_bridge_core::spawn_in_process_session_with(cfg, move |stream| {
            Box::pin(test_agents::run_session_history_agent_with(stream, store))
        })
        .await
    }

    async fn list_sessions(&self, cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        let store = self.store.clone();
        agui_acp_bridge_core::list_sessions_in_process_with(cfg, move |stream| {
            Box::pin(test_agents::run_session_history_agent_with(stream, store))
        })
        .await
    }
}

fn resume_input(
    thread_id: &str,
    run_id: &str,
    session_id: &str,
) -> agui_rs_core::types::RunAgentInput {
    let mut input = agui_rs_core::types::RunAgentInput::new(thread_id, run_id);
    input.forwarded_props = serde_json::json!({
        "acpResume": {"sessionId": session_id}
    });
    input
}

#[tokio::test]
async fn normal_thread_id_equal_to_acp_id_still_creates_a_new_session() {
    let state = BridgeAppState::new(Arc::new(IdentityClient::new()), PathBuf::from("/"));
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("logical-thread", "run-1", "first"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");

    let original_id = state.list_sessions().await.unwrap()[0].session_id.clone();
    let (status, body) =
        collect_sse_body(state.clone(), user_input(&original_id, "run-2", "second")).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("echo: second"),
        "normal run did not prompt:\n{body}"
    );
    assert!(
        !body.contains("HISTORY:"),
        "normal run unexpectedly loaded:\n{body}"
    );
    assert_eq!(state.list_sessions().await.unwrap().len(), 2);
}

#[tokio::test]
async fn explicit_resume_loads_the_supplied_id_not_the_thread_id() {
    let state = BridgeAppState::new(Arc::new(IdentityClient::new()), PathBuf::from("/"));
    let _ = collect_sse_body(state.clone(), user_input("source", "run-1", "persisted")).await;
    let session_id = state.list_sessions().await.unwrap()[0].session_id.clone();

    let (status, body) = collect_sse_body(state.clone(), {
        let mut input = resume_input("resume-thread", "run-resume", &session_id);
        input.messages = user_input("resume-thread", "message", "continued").messages;
        input
    })
    .await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("HISTORY:user:persisted"),
        "history missing:\n{body}"
    );
    assert!(
        body.contains("echo: continued"),
        "resumed prompt missing:\n{body}"
    );
    assert_eq!(state.list_sessions().await.unwrap().len(), 1);
}

#[tokio::test]
async fn explicit_resume_cache_hit_requires_the_mapped_acp_id() {
    let state = BridgeAppState::new(Arc::new(IdentityClient::new()), PathBuf::from("/"));
    let mut initial = user_input("mapped-thread", "run-1", "one");
    initial.tools.push(agui_rs_core::types::Tool {
        name: "kept-tool".into(),
        description: "must survive a rejected resume".into(),
        parameters: serde_json::json!({"type": "object"}),
        metadata: None,
    });
    let _ = collect_sse_body(state.clone(), initial).await;
    let mapped_tools = state.frontend_tools().entry("mapped-thread");
    assert_eq!(mapped_tools.tools()[0].name, "kept-tool");
    let first_id = state.list_sessions().await.unwrap()[0].session_id.clone();
    let _ = collect_sse_body(state.clone(), user_input("other-thread", "run-2", "two")).await;
    let second_id = state
        .list_sessions()
        .await
        .unwrap()
        .into_iter()
        .find(|session| session.session_id != first_id)
        .unwrap()
        .session_id;

    let (status, body) = collect_sse_body(
        state.clone(),
        resume_input("mapped-thread", "run-mismatch", &second_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("ACP_RESUME_FAILED"), "body:\n{body}");
    assert_eq!(state.session_count(), 2);
    assert!(Arc::ptr_eq(
        &mapped_tools,
        &state.frontend_tools().entry("mapped-thread")
    ));
    assert_eq!(mapped_tools.tools()[0].name, "kept-tool");
    assert_eq!(state.list_sessions().await.unwrap().len(), 2);
}

#[tokio::test]
async fn distinct_agui_threads_keep_distinct_live_sessions() {
    let client = Arc::new(CustomAgentInProcessClient::new(
        test_agents::run_stateful_session_agent,
    ));
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (_, first_a) = collect_sse_body(state.clone(), user_input("thread-a", "a-1", "one")).await;
    let (_, first_b) = collect_sse_body(state.clone(), user_input("thread-b", "b-1", "one")).await;
    let (_, second_a) = collect_sse_body(state.clone(), user_input("thread-a", "a-2", "two")).await;

    assert!(first_a.contains("turn 1: one"), "thread A:\n{first_a}");
    assert!(first_b.contains("turn 1: one"), "thread B:\n{first_b}");
    assert!(
        second_a.contains("turn 2: two"),
        "thread A reuse:\n{second_a}"
    );
    assert_eq!(state.session_count(), 2);
}
