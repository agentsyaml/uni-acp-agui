use super::*;

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
        body.contains("\"type\":\"RAW\"") && body.contains("\"source\":\"acp\""),
        "non-text and non-message updates must flow through RawEvent, body:\n{body}"
    );
    assert_eq!(
        count_events(&body, "RUN_FINISHED"),
        1,
        "exactly one RUN_FINISHED expected, body:\n{body}"
    );
}
