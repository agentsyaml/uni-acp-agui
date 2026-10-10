//! M0.5 HTTP/SSE roundtrip tests.
//!
//! Drive `agui-acp-bridge-server::build_router(state)` via `tower::ServiceExt::oneshot`
//! using the in-process echo agent. Asserts the AG-UI run lifecycle:
//! `RUN_STARTED` → `TEXT_MESSAGE_START` → `TEXT_MESSAGE_CONTENT`+ →
//! `TEXT_MESSAGE_END` → `RUN_FINISHED` arrives in the SSE response body.

#[path = "http_sse_roundtrip/media_type.rs"]
mod media_type;
mod support;

use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::schema::v1::StopReason;
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, InProcessAcpClient, acp::CustomAgentInProcessClient, test_agents,
};
use agui_rs_core::types::{
    InputContent, InputContentSource, Message, RunAgentInput, UserMessage, UserMessageContent,
};
use axum::http::StatusCode;
use serde_json::Value;

use support::{collect_sse_body, count_events, extract_event_types, user_input};

fn fresh_state() -> BridgeAppState {
    let client: Arc<dyn AcpClient> = Arc::new(InProcessAcpClient::new());
    BridgeAppState::new(client, PathBuf::from("/"))
}

fn assert_one_terminal_event(body: &str, expected: &str) {
    let terminal_count = count_events(body, "RUN_FINISHED") + count_events(body, "RUN_ERROR");
    assert_eq!(
        terminal_count, 1,
        "expected one terminal event, body:\n{body}"
    );
    assert_eq!(
        extract_event_types(body).last().map(String::as_str),
        Some(expected),
        "terminal event must be last, body:\n{body}"
    );
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
    assert_eq!(count_events(&body, "MESSAGES_SNAPSHOT"), 0);
    assert_one_terminal_event(&body, "RUN_FINISHED");
}

#[tokio::test]
async fn stop_reasons_emit_the_expected_single_terminal_event() {
    let cases = [
        (StopReason::EndTurn, None),
        (StopReason::Cancelled, Some("ACP_CANCELLED")),
        (StopReason::MaxTokens, Some("ACP_MAX_TOKENS")),
        (StopReason::MaxTurnRequests, Some("ACP_MAX_TURN_REQUESTS")),
        (StopReason::Refusal, Some("ACP_REFUSAL")),
    ];

    for (stop_reason, expected_code) in cases {
        let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
            test_agents::run_stop_reason_agent(stream, stop_reason)
        }));
        let state = BridgeAppState::new(client, PathBuf::from("/"));
        let (status, body) =
            collect_sse_body(state, user_input("thread-stop", "run-stop", "hello")).await;
        assert_eq!(status, StatusCode::OK, "body:\n{body}");

        let terminal = body
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
            .next_back()
            .expect("terminal event");
        match expected_code {
            None => {
                assert_eq!(terminal["type"], "RUN_FINISHED", "body:\n{body}");
                assert!(!body.contains("RUN_ERROR"), "body:\n{body}");
            }
            Some(code) => {
                assert_eq!(terminal["type"], "RUN_ERROR", "body:\n{body}");
                assert_eq!(terminal["code"], code, "body:\n{body}");
                assert!(!body.contains("RUN_FINISHED"), "body:\n{body}");
            }
        }
    }
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
    assert_eq!(count_events(&body, "MESSAGES_SNAPSHOT"), 0);
    assert_one_terminal_event(&body, "RUN_FINISHED");
}

#[tokio::test]
async fn input_state_is_request_context_without_generic_state_snapshot() {
    for (label, state_value) in [
        ("non-null", serde_json::json!({"requestContext": true})),
        ("null-default", Value::Null),
    ] {
        let mut input = user_input(
            &format!("thread-input-state-{label}"),
            &format!("run-input-state-{label}"),
            "hello",
        );
        input.state = state_value;

        let (status, body) = collect_sse_body(fresh_state(), input).await;
        assert_eq!(status, StatusCode::OK, "{label} state body:\n{body}");
        assert_eq!(
            count_events(&body, "STATE_SNAPSHOT"),
            0,
            "input state must not become a generic STATE_SNAPSHOT: {label}\n{body}"
        );
        assert_eq!(count_events(&body, "MESSAGES_SNAPSHOT"), 0);
        assert_one_terminal_event(&body, "RUN_FINISHED");
    }
}

#[tokio::test]
async fn multipart_user_input_is_rejected_before_session_creation() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_agent = opens.clone();
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        opens_for_agent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_single_chunk_agent(stream)
    }));
    let state = BridgeAppState::new(client, PathBuf::from("/"));
    let mut input = RunAgentInput::new("thread-multipart", "run-multipart");
    input.messages.push(Message::User(UserMessage {
        id: "multipart-1".into(),
        content: UserMessageContent::Parts(vec![InputContent::Image {
            source: InputContentSource::Data {
                value: "aGVsbG8=".into(),
                mime_type: "image/png".into(),
            },
            metadata: None,
        }]),
        name: None,
        encrypted_value: None,
    }));

    let (status, body) = collect_sse_body(state.clone(), input).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("\"type\":\"RUN_STARTED\""), "body:\n{body}");
    assert!(body.contains("\"type\":\"RUN_ERROR\""), "body:\n{body}");
    assert!(
        body.contains("\"code\":\"UNSUPPORTED_INPUT\""),
        "body:\n{body}"
    );
    assert!(!body.contains("RUN_FINISHED"), "body:\n{body}");
    assert_eq!(
        state.session_count(),
        0,
        "multipart input must not open a session"
    );
    assert_eq!(opens.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn agui_resume_is_rejected_without_becoming_a_history_noop() {
    let state = fresh_state();
    let mut input = RunAgentInput::new("thread-resume", "run-resume");
    input.resume = Some(Vec::new());

    let (status, body) = collect_sse_body(state.clone(), input).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("\"code\":\"AGUI_RESUME_UNSUPPORTED\""),
        "body:\n{body}"
    );
    assert!(
        !body.contains("RUN_FINISHED"),
        "resume must not become a noop:\n{body}"
    );
    assert_eq!(
        state.session_count(),
        0,
        "unsupported resume must not open a session"
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
async fn same_thread_concurrency_is_rejected_and_next_run_can_execute() {
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
        test_agents::run_slow_prompt_agent(stream, 250)
    }));
    let state = BridgeAppState::new(client, PathBuf::from("/"));

    let first_state = state.clone();
    let first = tokio::spawn(async move {
        collect_sse_body(first_state, user_input("thread-gate", "run-1", "first")).await
    });
    tokio::task::yield_now().await;
    let second =
        collect_sse_body(state.clone(), user_input("thread-gate", "run-2", "second")).await;
    let first = first.await.expect("first run task");

    let bodies = [&first.1, &second.1];
    assert_eq!(
        bodies
            .iter()
            .filter(|body| body.contains("\"code\":\"CONCURRENT_RUN\""))
            .count(),
        1,
        "exactly one concurrent run must be rejected: first={first:?}, second={second:?}"
    );
    assert!(
        bodies.iter().any(|body| body.contains("RUN_FINISHED")),
        "one run must execute normally: first={first:?}, second={second:?}"
    );

    let (status, body) = collect_sse_body(state, user_input("thread-gate", "run-3", "after")).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("RUN_FINISHED"),
        "later run must execute:\n{body}"
    );
}

#[tokio::test]
async fn non_text_block_emits_raw_event_then_run_finished() {
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
        body.contains("\"type\":\"RAW\""),
        "non-text content block must surface as RAW event:\n{body}"
    );
    assert!(
        body.contains("\"source\":\"acp\""),
        "raw event source must be acp:\n{body}"
    );
    assert!(
        body.contains("\"sessionUpdate\":\"agent_message_chunk\"")
            && body.contains("\"mimeType\":\"image/png\""),
        "raw event must preserve the ACP image update payload:\n{body}"
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
    assert_eq!(count_events(&body, "MESSAGES_SNAPSHOT"), 0);
    assert_one_terminal_event(&body, "RUN_ERROR");
}
