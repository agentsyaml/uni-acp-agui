//! M0.5 HTTP/SSE roundtrip tests.
//!
//! Drive `agui-acp-bridge-server::build_router(state)` via `tower::ServiceExt::oneshot`
//! using the in-process echo agent. Asserts the AG-UI run lifecycle:
//! `RUN_STARTED` → `TEXT_MESSAGE_START` → `TEXT_MESSAGE_CONTENT`+ →
//! `TEXT_MESSAGE_END` → `RUN_FINISHED` arrives in the SSE response body.

mod support;

use std::path::PathBuf;
use std::sync::Arc;

use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, InProcessAcpClient, acp::CustomAgentInProcessClient, test_agents,
};
use agui_rs_core::types::RunAgentInput;
use axum::http::StatusCode;

use support::{collect_sse_body, user_input};

fn fresh_state() -> BridgeAppState {
    let client: Arc<dyn AcpClient> = Arc::new(InProcessAcpClient::new());
    BridgeAppState::new(client, PathBuf::from("/"))
}

#[tokio::test]
async fn echo_roundtrip_emits_run_lifecycle_events() {
    let state = fresh_state();
    let input = user_input("thread-A", "run-1", "hello world");
    let (status, body) = collect_sse_body(state, input).await;

    assert_eq!(status, StatusCode::OK, "expected 200, got {status}: {body}");
    assert!(
        body.contains("\"type\":\"RUN_STARTED\""),
        "missing RUN_STARTED in body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"TEXT_MESSAGE_START\""),
        "missing TEXT_MESSAGE_START in body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"TEXT_MESSAGE_CONTENT\""),
        "missing TEXT_MESSAGE_CONTENT in body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"TEXT_MESSAGE_END\""),
        "missing TEXT_MESSAGE_END in body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "missing RUN_FINISHED in body:\n{body}"
    );
}

#[tokio::test]
async fn empty_messages_emits_clean_noop_run() {
    // The bridge's contract: a run carries a fresh prompt only when
    // the tail of `messages[]` is a `User` text message. Empty
    // `messages[]` therefore collapses to a clean RUN_STARTED →
    // RUN_FINISHED pair (a noop run) — never RUN_ERROR. This matches
    // the broader trailing-user-only rule: AG-UI runtimes occasionally
    // post empty / non-user-tail runs (CopilotKit-style follow-ups,
    // state syncs, picker refreshes); those must not be treated as
    // protocol failures.
    let state = fresh_state();
    let input = RunAgentInput::new("thread-empty", "run-empty");
    let (status, body) = collect_sse_body(state, input).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"type\":\"RUN_STARTED\""),
        "RUN_STARTED must always lead the stream:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "noop run must terminate cleanly with RUN_FINISHED:\n{body}"
    );
    assert!(
        !body.contains("\"type\":\"RUN_ERROR\""),
        "noop run must NOT emit RUN_ERROR:\n{body}"
    );
}

#[tokio::test]
async fn same_thread_id_reuses_session_across_runs() {
    let state = fresh_state();

    let (s1, b1) =
        collect_sse_body(state.clone(), user_input("thread-reuse", "run-1", "first")).await;
    assert_eq!(s1, StatusCode::OK);
    assert!(
        b1.contains("\"type\":\"RUN_FINISHED\""),
        "first run must finish:\n{b1}"
    );

    let (s2, b2) =
        collect_sse_body(state.clone(), user_input("thread-reuse", "run-2", "second")).await;
    assert_eq!(s2, StatusCode::OK);
    assert!(
        b2.contains("\"type\":\"RUN_FINISHED\""),
        "second run must finish:\n{b2}"
    );

    assert_eq!(
        state.session_count(),
        1,
        "thread-reuse should map to a single cached session, got {}",
        state.session_count()
    );
}

#[tokio::test]
async fn non_text_block_emits_custom_event_then_run_finished() {
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|s| {
        test_agents::run_image_agent(s)
    }));
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (status, body) =
        collect_sse_body(state, user_input("thread-img", "run-img", "send image")).await;

    assert_eq!(status, StatusCode::OK, "expected 200, got {status}: {body}");
    assert!(
        body.contains("\"type\":\"RUN_STARTED\""),
        "missing RUN_STARTED:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"CUSTOM\""),
        "non-text content block must surface as CUSTOM event:\n{body}"
    );
    assert!(
        body.contains("acp.session_update"),
        "custom event name must be acp.session_update:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "stream must terminate cleanly with RUN_FINISHED:\n{body}"
    );
    assert!(
        !body.contains("\"type\":\"TEXT_MESSAGE_START\""),
        "image-only chunk must NOT open a text message:\n{body}"
    );
}

#[tokio::test]
async fn acp_error_propagates_as_run_error_event() {
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|s| {
        test_agents::run_failing_prompt_agent(s)
    }));
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let (status, body) =
        collect_sse_body(state, user_input("thread-err", "run-err", "please fail")).await;

    assert_eq!(status, StatusCode::OK, "expected 200, got {status}: {body}");
    assert!(
        body.contains("\"type\":\"RUN_STARTED\""),
        "RUN_STARTED must always lead the stream:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"RUN_ERROR\""),
        "agent prompt error must surface as RUN_ERROR:\n{body}"
    );
    assert!(
        !body.contains("\"type\":\"RUN_FINISHED\""),
        "errored run must NOT emit RUN_FINISHED:\n{body}"
    );
}
