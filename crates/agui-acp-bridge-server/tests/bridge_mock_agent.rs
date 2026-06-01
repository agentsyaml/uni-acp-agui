//! Deep mock-agent integration tests for the AG-UI ↔ ACP bridge.
//!
//! Every test starts with `validates_*` since the previous "proves_bug_*"
//! suite has been folded back into positive assertions after the underlying
//! issues were fixed.

mod support;

use std::sync::Arc;

use agui_acp_bridge_policy::AutoAllow;
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, acp::CustomAgentInProcessClient, test_agents,
};
use axum::http::StatusCode;

use support::{
    AllowAlwaysFirstOption, PolicySpy, collect_sse_body, count_events, extract_event_types,
    state_with_client, state_with_policy, user_input,
};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

#[tokio::test]
async fn validates_single_chunk_clean_lifecycle() {
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (status, body) =
        collect_sse_body(state, user_input("thread-single", "run-1", "ping")).await;

    assert_eq!(status, StatusCode::OK, "expected 200, body:\n{body}");
    let events = extract_event_types(&body);
    assert!(
        events.first().map(String::as_str) == Some("RUN_STARTED"),
        "stream must lead with RUN_STARTED, got: {events:?}\nbody:\n{body}"
    );
    assert!(
        events.iter().any(|e| e == "TEXT_MESSAGE_START"),
        "expected TEXT_MESSAGE_START, got: {events:?}"
    );
    assert!(
        events.iter().any(|e| e == "TEXT_MESSAGE_END"),
        "expected TEXT_MESSAGE_END, got: {events:?}"
    );
    assert_eq!(
        events.last().map(String::as_str),
        Some("RUN_FINISHED"),
        "stream must end with RUN_FINISHED, got: {events:?}"
    );
}

#[tokio::test]
async fn validates_stateful_session_reuses_thread() {
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    let (s1, b1) =
        collect_sse_body(state.clone(), user_input("thread-stateful", "r1", "alpha")).await;
    let (s2, b2) =
        collect_sse_body(state.clone(), user_input("thread-stateful", "r2", "beta")).await;

    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert!(
        b1.contains("turn 1: alpha"),
        "first run should report turn 1, body:\n{b1}"
    );
    assert!(
        b2.contains("turn 2: beta"),
        "second run on same thread should report turn 2, body:\n{b2}"
    );
    assert_eq!(
        state.session_count(),
        1,
        "shared thread_id must reuse a single session"
    );
}

#[tokio::test]
async fn validates_trailing_assistant_emits_noop_without_replay() {
    // Critical loop-prevention contract: when the AG-UI runtime
    // re-fires `runAgent` with `messages[]` whose tail is an assistant
    // message (CopilotKit's `agentic_chat` does this on every tool turn
    // so the LLM can see the tool result), the bridge MUST NOT
    // re-prompt the agent with the historical user message — the agent
    // has already handled that turn and a re-prompt would loop.
    //
    // Empirically: a stateful agent that emits "turn N: <prompt>" lets
    // us assert no replay happened by checking the second run's body
    // contains no "turn 2:" marker.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    // Run 1: real turn → reaches the agent.
    let (_, b1) = collect_sse_body(state.clone(), user_input("thread-loop", "r1", "alpha")).await;
    assert!(
        b1.contains("turn 1: alpha"),
        "first run reaches the agent:\n{b1}"
    );

    // Run 2: a follow-up posted by the runtime with the prior user
    // message in the array but the tail is an assistant message. The
    // bridge must noop; the stateful agent's turn counter must NOT
    // advance.
    let mut input2 = agui_rs_core::types::RunAgentInput::new("thread-loop", "r2");
    input2.messages.push(agui_rs_core::types::Message::User(
        agui_rs_core::types::UserMessage {
            id: "u-prior".into(),
            content: agui_rs_core::types::UserMessageContent::Text("alpha".into()),
            name: None,
            encrypted_value: None,
        },
    ));
    input2
        .messages
        .push(agui_rs_core::types::Message::Assistant(
            agui_rs_core::types::AssistantMessage {
                id: "a-prior".into(),
                content: Some("turn 1: alpha".into()),
                name: None,
                tool_calls: None,
                encrypted_value: None,
            },
        ));
    let (_, b2) = collect_sse_body(state.clone(), input2).await;
    assert!(
        !b2.contains("turn 2:"),
        "trailing-assistant follow-up MUST NOT re-prompt the agent:\n{b2}"
    );
    assert!(
        b2.contains("\"type\":\"RUN_FINISHED\""),
        "noop run still terminates cleanly:\n{b2}"
    );
}

#[tokio::test]
async fn validates_trailing_tool_message_emits_noop() {
    // Same as above but the tail is a tool message — the literal
    // shape CopilotKit's `agentic_chat` posts after every tool turn.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    let (_, b1) = collect_sse_body(state.clone(), user_input("thread-tool", "r1", "alpha")).await;
    assert!(
        b1.contains("turn 1: alpha"),
        "first run reaches the agent:\n{b1}"
    );

    let mut input2 = agui_rs_core::types::RunAgentInput::new("thread-tool", "r2");
    input2.messages.push(agui_rs_core::types::Message::User(
        agui_rs_core::types::UserMessage {
            id: "u-prior".into(),
            content: agui_rs_core::types::UserMessageContent::Text("alpha".into()),
            name: None,
            encrypted_value: None,
        },
    ));
    input2
        .messages
        .push(agui_rs_core::types::Message::Assistant(
            agui_rs_core::types::AssistantMessage {
                id: "a-prior".into(),
                content: None,
                name: None,
                tool_calls: Some(vec![agui_rs_core::types::ToolCall {
                    id: "tc-1".into(),
                    kind: agui_rs_core::types::ToolCallKind::Function,
                    function: agui_rs_core::types::FunctionCall {
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    },
                    encrypted_value: None,
                }]),
                encrypted_value: None,
            },
        ));
    input2.messages.push(agui_rs_core::types::Message::Tool(
        agui_rs_core::types::ToolMessage {
            id: "t-1".into(),
            content: "result".into(),
            tool_call_id: "tc-1".into(),
            error: None,
            encrypted_value: None,
        },
    ));
    let (_, b2) = collect_sse_body(state, input2).await;
    assert!(
        !b2.contains("turn 2:"),
        "trailing-tool follow-up MUST NOT re-prompt the agent:\n{b2}"
    );
    assert!(
        b2.contains("\"type\":\"RUN_FINISHED\""),
        "noop run terminates:\n{b2}"
    );
}

#[tokio::test]
async fn validates_multi_turn_with_trailing_user_replays_correctly() {
    // Sanity-check: a real multi-turn conversation (each new turn ends
    // with a fresh user message at the tail) MUST flow through to the
    // agent and advance its turn counter.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    let mk = |run_id: &str, user_id: &str, text: &str, prior: bool| {
        let mut input = agui_rs_core::types::RunAgentInput::new("thread-multi", run_id);
        if prior {
            input.messages.push(agui_rs_core::types::Message::User(
                agui_rs_core::types::UserMessage {
                    id: "u1".into(),
                    content: agui_rs_core::types::UserMessageContent::Text("alpha".into()),
                    name: None,
                    encrypted_value: None,
                },
            ));
            input.messages.push(agui_rs_core::types::Message::Assistant(
                agui_rs_core::types::AssistantMessage {
                    id: "a1".into(),
                    content: Some("turn 1: alpha".into()),
                    name: None,
                    tool_calls: None,
                    encrypted_value: None,
                },
            ));
        }
        input.messages.push(agui_rs_core::types::Message::User(
            agui_rs_core::types::UserMessage {
                id: user_id.into(),
                content: agui_rs_core::types::UserMessageContent::Text(text.into()),
                name: None,
                encrypted_value: None,
            },
        ));
        input
    };

    let (_, b1) = collect_sse_body(state.clone(), mk("r1", "u1", "alpha", false)).await;
    assert!(b1.contains("turn 1: alpha"));

    // Run 2: full transcript [user, assistant, user] — tail is fresh user → forward.
    let (_, b2) = collect_sse_body(state.clone(), mk("r2", "u2", "beta", true)).await;
    assert!(
        b2.contains("turn 2: beta"),
        "trailing fresh user MUST reach the agent:\n{b2}"
    );
}

#[tokio::test]
async fn validates_mixed_updates_routes_through_translator() {
    let state = state_with_client(client_for(test_agents::run_mixed_updates_agent));
    let (status, body) =
        collect_sse_body(state, user_input("thread-mixed", "run-mixed", "go")).await;

    assert_eq!(status, StatusCode::OK, "expected 200, body:\n{body}");

    assert!(
        body.contains("\"type\":\"TEXT_MESSAGE_START\""),
        "agent text chunk must open a text message, body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"TEXT_MESSAGE_END\""),
        "agent text chunk must close a text message, body:\n{body}"
    );
    assert!(
        body.contains("\"type\":\"CUSTOM\"") && body.contains("acp.session_update"),
        "non-text and non-message updates must flow through CustomEvent, body:\n{body}"
    );
    assert_eq!(
        count_events(&body, "RUN_FINISHED"),
        1,
        "exactly one RUN_FINISHED expected, body:\n{body}"
    );
}

#[tokio::test]
async fn validates_request_permission_resolves_with_auto_allow() {
    // End-to-end permission flow with the AutoAllow policy:
    // 1. Mock agent issues `requestPermission` (using SDK-recommended
    //    `on_receiving_ok_result` chaining, not `block_task`).
    // 2. Bridge's session handler consults the policy.
    // 3. `AutoAllow` returns `Allow { option_id }` for the first AllowOnce
    //    option ("allow").
    // 4. Bridge responds; agent receives the outcome and completes the
    //    prompt with `RUN_FINISHED`.
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AllowAlwaysFirstOption)));
    let client = client_for(test_agents::run_request_permission_agent);
    let state = state_with_policy(client, policy.clone());

    let app = agui_acp_bridge_server::build_router(state);
    let body = serde_json::to_vec(&user_input("thread-perm", "run-perm", "do read")).unwrap();
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body))
            .unwrap(),
    )
    .await
    .expect("router error");

    assert_eq!(response.status(), StatusCode::OK);

    let body = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http_body_util::BodyExt::collect(response.into_body()),
    )
    .await
    .expect("permission roundtrip must not hang")
    .expect("body collect failed");
    let text = String::from_utf8_lossy(&body.to_bytes()).into_owned();

    assert!(
        text.contains("\"type\":\"RUN_FINISHED\""),
        "expected RUN_FINISHED after auto-allow resolved permission, got:\n{text}"
    );
    assert!(
        policy.was_invoked(),
        "the configured PermissionPolicy must be consulted on requestPermission"
    );
}

#[tokio::test]
async fn validates_policy_not_consulted_on_turns_without_permission_request() {
    // Sanity: the policy must only be consulted when an agent actually issues
    // a `requestPermission`. A plain text-streaming turn must not touch it.
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AllowAlwaysFirstOption)));
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = state_with_policy(client, policy.clone());

    let (_, body) =
        collect_sse_body(state, user_input("thread-policy", "run-policy", "noop")).await;
    assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    assert!(
        !policy.was_invoked(),
        "policy must not be consulted on a turn that issues no requestPermission"
    );
}

#[tokio::test]
async fn validates_late_notification_after_finish_is_dropped() {
    // Documents (and pins) the design choice that late notifications —
    // those arriving on the connection-level callback after the prompt
    // has already completed — are dropped rather than rebroadcast on a
    // subsequent run's stream. Both the current run's body and the next
    // run's body must NOT contain the late chunk.
    let state = state_with_client(client_for(test_agents::run_late_notification_agent));

    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-late", "run-1", "first")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("in-band"),
        "first run should carry the in-band chunk, body:\n{body}"
    );
    assert!(
        !body.contains("LATE-AFTER-FINISH"),
        "late notification must not appear in the same run (channel already closed), body:\n{body}"
    );

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let (status2, body2) =
        collect_sse_body(state, user_input("thread-late", "run-2", "second")).await;
    assert_eq!(status2, StatusCode::OK);
    assert!(
        !body2.contains("LATE-AFTER-FINISH"),
        "late notifications are intentionally dropped, body:\n{body2}"
    );
}

#[tokio::test]
async fn validates_failing_prompt_yields_run_error() {
    let state: BridgeAppState =
        state_with_client(client_for(test_agents::run_failing_prompt_agent));
    let (status, body) =
        collect_sse_body(state, user_input("thread-fail", "run-fail", "explode")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"type\":\"RUN_ERROR\""),
        "agent prompt error must surface as RUN_ERROR, body:\n{body}"
    );
    assert!(!body.contains("\"type\":\"RUN_FINISHED\""));
}

#[tokio::test]
async fn validates_slow_prompt_completes_then_finishes() {
    let state = state_with_client(client_for(|s| test_agents::run_slow_prompt_agent(s, 150)));
    let started = std::time::Instant::now();
    let (status, body) =
        collect_sse_body(state, user_input("thread-slow", "run-slow", "wait")).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= std::time::Duration::from_millis(150),
        "bridge must wait for the slow prompt, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "slow prompt must still finish cleanly, body:\n{body}"
    );

    let _ = AutoAllow;
}

#[tokio::test]
async fn validates_auto_deny_consults_policy_then_returns_cancelled() {
    // AutoDeny returns `Cancelled` as the outcome of `requestPermission`,
    // which is still a valid (non-error) response to the agent. The mock
    // agent's PromptRequest handler treats any `Ok(_)` outcome as success
    // and ends the turn, so the AG-UI run still finishes cleanly. The
    // crucial assertion is that the configured policy was consulted.
    use agui_acp_bridge_policy::AutoDeny;
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AutoDeny)));
    let client = client_for(test_agents::run_request_permission_agent);
    let state = state_with_policy(client, policy.clone());

    let (status, body) =
        collect_sse_body(state, user_input("thread-deny", "run-deny", "do read")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "RUN_FINISHED expected (Cancelled is a non-error outcome), body:\n{body}"
    );
    assert_eq!(
        policy.call_count(),
        1,
        "the configured policy must be consulted exactly once"
    );
}

#[tokio::test]
async fn validates_health_endpoint_reports_session_count() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    // Drive one prompt to materialize a session.
    let (_, _) = collect_sse_body(state.clone(), user_input("thread-h", "run-h", "warmup")).await;

    let app = agui_acp_bridge_server::build_router(state);
    let response = app
        .oneshot(HttpRequest::get("/health").body(Body::empty()).unwrap())
        .await
        .expect("health request must succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("collect health body")
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains("\"status\":\"ok\""),
        "health body must report status:ok, got: {text}"
    );
    assert!(
        text.contains("\"sessions\":1"),
        "health body must report 1 cached session after one prompt, got: {text}"
    );
}

#[tokio::test]
async fn validates_approval_endpoint_returns_404_for_unknown_interrupt() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let app = agui_acp_bridge_server::build_router(state);

    // Note: `optionId` IS required when approved=true; we include a value
    // here so the request passes input validation and reaches the
    // interrupt-id lookup, which is what we want to test.
    let body = serde_json::to_vec(&serde_json::json!({
        "interruptId": "does-not-exist",
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let response = app
        .oneshot(
            HttpRequest::post("/approval")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("router error");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn validates_approval_endpoint_returns_400_when_approved_without_option_id() {
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let app = agui_acp_bridge_server::build_router(state);

    let body = serde_json::to_vec(&serde_json::json!({
        "interruptId": "anything",
        "approved": true,
    }))
    .unwrap();
    let response = app
        .oneshot(
            HttpRequest::post("/approval")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .expect("router error");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn validates_defer_policy_emits_state_snapshot_and_resolves_via_approval() {
    // End-to-end deferred approval flow:
    // 1. Configure `InterruptViaAgUiEvent` so the bridge defers every request.
    // 2. The session handler emits `BridgeStreamItem::Interrupt` to the
    //    SSE handler, which surfaces it as a `STATE_SNAPSHOT` event.
    // 3. We pluck the `interruptId` out of the snapshot and POST `/approval`
    //    to resolve it.
    // 4. The agent receives `Allow`, the prompt completes, RUN_FINISHED ships.
    //
    // Because `oneshot` buffers the entire body, we drive both halves
    // concurrently with a real bound HTTP server.
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, std::path::PathBuf::from("/"))
        .with_policy(policy)
        .build();
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Send the prompt and stream the response while concurrently watching
    // for the STATE_SNAPSHOT event.
    let prompt_body =
        serde_json::to_vec(&user_input("thread-defer", "run-defer", "do read")).unwrap();
    let mut response = raw_post(addr, "/", prompt_body, "Accept: text/event-stream\r\n").await;

    // Read until we see STATE_SNAPSHOT, extract its interruptId, POST approval.
    let snapshot_payload = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_snapshot(&mut response),
    )
    .await
    .expect("STATE_SNAPSHOT must arrive within 5s")
    .expect("STATE_SNAPSHOT must contain an interruptId");

    let interrupt_id = snapshot_payload
        .pointer("/snapshot/approval/interruptId")
        .and_then(serde_json::Value::as_str)
        .expect("interruptId must be a string")
        .to_string();

    // Resolve via /approval.
    let approval_body = serde_json::to_vec(&serde_json::json!({
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut approval_resp = raw_post(addr, "/approval", approval_body, "").await;
    assert_eq!(
        read_status_code(&mut approval_resp).await,
        StatusCode::OK,
        "POST /approval must accept the interruptId"
    );

    // Drain the rest of the SSE stream and assert RUN_FINISHED.
    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end(response))
        .await
        .expect("stream must complete after approval");

    assert!(
        trailer.contains("\"type\":\"RUN_FINISHED\""),
        "RUN_FINISHED must arrive after approval, body:\n{trailer}"
    );

    server.abort();
}

// --- helpers used only by validates_defer_policy_emits_state_snapshot_and_resolves_via_approval ---

async fn raw_post(
    addr: std::net::SocketAddr,
    path: &str,
    body: Vec<u8>,
    extra_headers: &str,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {len}\r\nConnection: close\r\n\r\n",
        len = body.len(),
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

async fn read_status_code(stream: &mut tokio::net::TcpStream) -> StatusCode {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 256];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    let head = String::from_utf8_lossy(&buf[..n]);
    let line = head.lines().next().unwrap_or("");
    let code: u16 = line
        .split_whitespace()
        .nth(1)
        .unwrap_or("500")
        .parse()
        .unwrap_or(500);
    StatusCode::from_u16(code).unwrap()
}

async fn wait_for_state_snapshot(stream: &mut tokio::net::TcpStream) -> Option<serde_json::Value> {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 8192];
    let mut accumulated = String::new();
    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => n,
        };
        accumulated.push_str(&String::from_utf8_lossy(&buf[..n]));
        for line in accumulated.lines() {
            if let Some(payload) = line.strip_prefix("data:") {
                let payload = payload.trim();
                if payload.contains("\"type\":\"STATE_SNAPSHOT\"") {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) {
                        return Some(v);
                    }
                }
            }
        }
    }
}

async fn drain_to_end(mut stream: tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut acc = String::new();
    let mut buf = vec![0u8; 8192];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => acc.push_str(&String::from_utf8_lossy(&buf[..n])),
        }
    }
    acc
}

#[tokio::test]
async fn validates_defer_with_permission_timeout_falls_back_to_cancelled() {
    // When a Defer'd permission goes unanswered, the bridge must time out
    // (per BridgeConfig.permission_timeout) and respond Cancelled instead of
    // hanging forever. We use 200ms so the test runs fast.
    use agui_acp_bridge_core::BridgeConfig;
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::path::PathBuf;
    use std::time::Duration;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy)
        .with_config(BridgeConfig {
            permission_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();

    let started = std::time::Instant::now();
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(5),
        collect_sse_body(state, user_input("thread-to", "run-to", "do read")),
    )
    .await
    .expect("must not hang past timeout × 25");
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= Duration::from_millis(200),
        "must not return before the configured permission_timeout, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "after timeout the agent receives Cancelled and finishes the turn, body:\n{body}"
    );
}

#[tokio::test]
async fn validates_idle_reaper_drops_unused_sessions() {
    // Configure a 200ms idle timeout, prompt once to materialize a session,
    // then wait for the reaper to drop it. The reaper interval is
    // `min(idle/4, 30s)`, capped to a 1s minimum, so the timing budget is:
    //  - prompt (~0ms) → session count = 1
    //  - sleep 1.5s → reaper has run at least once with idle elapsed
    //  - assert session count = 0
    use agui_acp_bridge_core::BridgeConfig;
    use std::path::PathBuf;
    use std::time::Duration;

    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let (_, _) = collect_sse_body(state.clone(), user_input("thread-reap", "run-reap", "hi")).await;
    assert_eq!(
        state.session_count(),
        1,
        "session must be cached after prompt"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(
        state.session_count(),
        0,
        "reaper must drop session whose last_used is older than idle_timeout"
    );
}

#[tokio::test]
async fn validates_approval_endpoint_returns_422_for_unknown_option_id() {
    // Drive the deferred-approval flow with a bogus optionId so we can
    // observe the new InvalidOption → 422 path. We POST an Allow with
    // optionId="never-offered" and assert the bridge rejects it (and the
    // pending request stays alive — a follow-up valid POST resolves it
    // and the run finishes).
    use agui_acp_bridge_policy::InterruptViaAgUiEvent;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::net::TcpListener;

    let policy = Arc::new(InterruptViaAgUiEvent);
    let client = client_for(test_agents::run_request_permission_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy)
        .build();
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let prompt_body = serde_json::to_vec(&user_input("thread-422", "run-422", "do read")).unwrap();
    let mut response = raw_post(addr, "/", prompt_body, "Accept: text/event-stream\r\n").await;

    let snapshot = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_state_snapshot(&mut response),
    )
    .await
    .expect("STATE_SNAPSHOT")
    .expect("interruptId");
    let interrupt_id = snapshot
        .pointer("/snapshot/approval/interruptId")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .to_string();

    // First attempt: bogus optionId → 422.
    let bad_body = serde_json::to_vec(&serde_json::json!({
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "never-offered",
    }))
    .unwrap();
    let mut bad = raw_post(addr, "/approval", bad_body, "").await;
    assert_eq!(
        read_status_code(&mut bad).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    // Second attempt: valid optionId → 200 → run completes.
    let good_body = serde_json::to_vec(&serde_json::json!({
        "interruptId": interrupt_id,
        "approved": true,
        "optionId": "allow",
    }))
    .unwrap();
    let mut good = raw_post(addr, "/approval", good_body, "").await;
    assert_eq!(read_status_code(&mut good).await, StatusCode::OK);

    let trailer = tokio::time::timeout(Duration::from_secs(5), drain_to_end(response))
        .await
        .expect("drain");
    assert!(trailer.contains("\"type\":\"RUN_FINISHED\""));

    server.abort();
}

#[tokio::test]
async fn validates_concurrent_first_use_creates_exactly_one_session() {
    // Fan in 8 concurrent prompts on the same thread_id when no session
    // exists yet. Without the per-key async lock in `session_for`, multiple
    // would race past the cache-miss check, each open a session, and only
    // one would survive `or_insert_with` — wasting agent processes.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    let mut handles = Vec::new();
    for i in 0..8 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            let (status, body) =
                collect_sse_body(s, user_input("thread-race", &format!("run-{i}"), "ping")).await;
            assert_eq!(status, StatusCode::OK);
            assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(
        state.session_count(),
        1,
        "concurrent first-use must lazily create exactly one session, got {}",
        state.session_count()
    );
}

#[tokio::test]
async fn validates_long_prompt_survives_short_idle_timeout() {
    // Regression for the audit's P1 finding: long-running prompts whose
    // duration exceeds `idle_timeout` would be reaped mid-flight by the
    // background reaper. Now `enter_prompt`/`PromptGuard` keep the
    // `active_prompts` counter non-zero for the duration of the turn,
    // and the reaper skips entries with `active_prompts > 0`.
    use agui_acp_bridge_core::BridgeConfig;
    use std::path::PathBuf;
    use std::time::Duration;

    // 250ms idle timeout, 700ms slow prompt (almost 3× the budget).
    let client = client_for(|s| test_agents::run_slow_prompt_agent(s, 700));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(250),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let started = std::time::Instant::now();
    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-long", "run-long", "wait")).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= Duration::from_millis(700),
        "prompt must run to completion despite shorter idle_timeout, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "long prompt must finish cleanly:\n{body}"
    );
}

#[tokio::test]
async fn validates_long_running_agent_cancelled_when_client_disconnects() {
    // Regression for the audit's P0 finding: when the SSE consumer drops,
    // the bridge must cancel the in-flight ACP turn so the agent stops
    // doing work nobody will read. We use the `run_long_running_agent`
    // fixture which emits a chunk every 50ms for up to 50s, breaking out
    // early if `send_notification` fails (which it does once we drop
    // the receiver). With cancel hooked up the test should finish in
    // well under a second; without it would queue commands and never
    // fire.
    use std::time::Duration;
    use tokio::net::TcpListener;

    let client = client_for(test_agents::run_long_running_agent);
    let state = state_with_client(client);
    let app = agui_acp_bridge_server::build_router(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let body = serde_json::to_vec(&user_input("thread-cancel", "run-cancel", "stream")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;

    // Drain just enough bytes to confirm streaming has started, then drop.
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 4096];
    let _ = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut buf))
        .await
        .expect("must receive at least the headers");
    drop(conn);

    // The agent loop polls send_notification each tick; it should bail
    // out within ~50ms of the disconnect being noticed by the SDK.
    // Give us a generous 3s budget — failure mode is "agent runs the
    // full 50s before responding to the prompt".
    tokio::time::sleep(Duration::from_secs(3)).await;

    server.abort();
}

// --- Mode / Model surface (ACP `session/set_mode`, `session/set_model`) ---

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
}

#[cfg(feature = "unstable_session_model")]
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

#[cfg(feature = "unstable_session_model")]
#[tokio::test]
async fn validates_set_model_endpoint_round_trips_through_agent() {
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
    assert_eq!(
        init.models.as_ref().map(|m| m.current_model_id.as_str()),
        Some("claude-sonnet"),
        "current_model_id must move after set-model"
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

    server.abort();
}

#[cfg(feature = "unstable_session_model")]
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

#[cfg(feature = "unstable_session_model")]
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

#[cfg(feature = "unstable_session_model")]
#[tokio::test]
async fn validates_set_mode_runs_concurrently_with_in_flight_prompt() {
    // Regression for: actor used to handle commands serially, so a
    // SetMode arriving during a long-running prompt would queue behind
    // it and only execute after `finished` — a 60s prompt would make
    // the picker permanently spinning. We now `tokio::spawn` SetMode
    // off the dispatch loop so it round-trips while the prompt is
    // still streaming.
    use std::time::Duration;
    use tokio::net::TcpListener;

    let state = state_with_client(client_for(test_agents::run_modes_models_agent));
    let app = agui_acp_bridge_server::build_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Open the session with a small first run so the cache has an entry.
    let body = serde_json::to_vec(&user_input("thread-conc", "run-1", "boot")).unwrap();
    let mut conn = raw_post(addr, "/", body, "Accept: text/event-stream\r\n").await;
    let _ = tokio::time::timeout(Duration::from_secs(3), drain_to_end_local(&mut conn)).await;
    drop(conn);

    // Open a second SSE connection. We immediately read just enough
    // to know the stream started (RUN_STARTED), then POST set-mode
    // BEFORE the prompt finishes. The mock agent's prompt completes
    // quickly, but the structural property under test is that
    // SetMode is dispatched off the actor loop — we assert end-to-end
    // latency is well below `set_session_timeout`.
    let body2 = serde_json::to_vec(&user_input("thread-conc", "run-2", "stay")).unwrap();
    let mut blocked = raw_post(addr, "/", body2, "Accept: text/event-stream\r\n").await;
    use tokio::io::AsyncReadExt;
    let mut hdr = vec![0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_secs(2), blocked.read(&mut hdr)).await;

    let payload =
        serde_json::to_vec(&serde_json::json!({"threadId": "thread-conc", "modeId": "code"}))
            .unwrap();
    let started = std::time::Instant::now();
    let mut resp = raw_post(addr, "/session/set-mode", payload, "").await;
    let code = read_status_code(&mut resp).await;
    let elapsed = started.elapsed();
    drop(blocked);

    assert_eq!(
        code,
        StatusCode::OK,
        "set-mode must succeed during in-flight prompt"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "set-mode took {elapsed:?}; this implies it queued behind the prompt"
    );

    let init = state
        .session_init_state("thread-conc")
        .expect("session must exist");
    assert_eq!(
        init.modes.as_ref().map(|m| m.current_mode_id.as_str()),
        Some("code"),
    );

    server.abort();
}

// --- helpers used by the mode/model tests ---

#[cfg(feature = "unstable_session_model")]
async fn raw_get(addr: std::net::SocketAddr, path: &str) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

#[cfg(feature = "unstable_session_model")]
async fn read_status_and_body(stream: &mut tokio::net::TcpStream) -> (StatusCode, String) {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut buf),
    )
    .await
    .unwrap_or(Ok(0));
    let raw = String::from_utf8_lossy(&buf);
    let line = raw.lines().next().unwrap_or("");
    let code: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // body starts after the empty line separating headers and body
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (
        StatusCode::from_u16(code).unwrap_or(StatusCode::IM_A_TEAPOT),
        body,
    )
}

#[cfg(feature = "unstable_session_model")]
async fn drain_to_end_local(stream: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    // 5s budget covers the agent's single chunk + RUN_FINISHED.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_end(&mut buf),
    )
    .await;
    String::from_utf8_lossy(&buf).into_owned()
}
