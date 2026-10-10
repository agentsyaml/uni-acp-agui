use super::*;

#[test]
fn tool_raw_input_only_emits_args_and_raw_output_emits_result() {
    let mut t = Translator::new();
    let call = ToolCall::new(ToolCallId::new("tc-raw"), "Read file")
        .raw_input(serde_json::json!({"path": "a.txt"}))
        .raw_output(serde_json::json!({"text": "first"}));
    let start_events = t.translate(SessionUpdate::ToolCall(call));

    assert!(
        !start_events
            .iter()
            .any(|event| matches!(event, Event::ToolCallArgs(_)))
    );
    assert!(
        !start_events
            .iter()
            .any(|event| matches!(event, Event::ToolCallResult(_)))
    );
    assert!(!start_events.iter().any(|event| matches!(
        event,
        Event::ToolCallArgs(args) if args.delta.contains("first")
    )));

    let progress = ToolCallUpdate::new(
        "tc-raw",
        ToolCallUpdateFields::new()
            .raw_input(serde_json::json!({"offset": 1}))
            .raw_output(serde_json::json!({"text": "latest"})),
    );
    let args_events = t.translate(SessionUpdate::ToolCallUpdate(progress));
    assert!(
        !args_events
            .iter()
            .any(|event| matches!(event, Event::ToolCallArgs(_)))
    );
    assert!(
        !args_events
            .iter()
            .any(|event| matches!(event, Event::ToolCallResult(_)))
    );

    let terminal = ToolCallUpdate::new(
        "tc-raw",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    let terminal_events = t.translate(SessionUpdate::ToolCallUpdate(terminal));
    assert!(matches!(
        terminal_events.as_slice(),
        [
            Event::ToolCallArgs(_),
            Event::ToolCallEnd(_),
            Event::ToolCallResult(_)
        ]
    ));
    assert!(terminal_events.iter().any(|event| matches!(
        event,
        Event::ToolCallArgs(args) if args.delta == r#"{"offset":1}"#
    )));
    assert!(terminal_events.iter().any(|event| matches!(
        event,
        Event::ToolCallResult(result)
            if result.tool_call_id == "tc-raw"
                && result.content == r#"{"text":"latest"}"#
    )));
    assert!(!terminal_events.iter().any(|event| matches!(
        event,
        Event::ToolCallArgs(args) if args.delta.contains("latest")
    )));

    let duplicate = ToolCallUpdate::new(
        "tc-raw",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    assert!(
        t.translate(SessionUpdate::ToolCallUpdate(duplicate))
            .is_empty()
    );
    let unknown = ToolCallUpdate::new(
        "unknown",
        ToolCallUpdateFields::new()
            .status(ToolCallStatus::Failed)
            .raw_output(serde_json::json!({"ignored": true})),
    );
    assert!(
        t.translate(SessionUpdate::ToolCallUpdate(unknown))
            .is_empty()
    );
    let unknown_progress = ToolCallUpdate::new(
        "unknown-progress",
        ToolCallUpdateFields::new().raw_input(serde_json::json!({"ignored": true})),
    );
    assert!(
        t.translate(SessionUpdate::ToolCallUpdate(unknown_progress))
            .is_empty()
    );
    assert!(t.flush().is_empty());
}

#[test]
fn frontend_tool_call_emits_one_complete_args_payload() {
    let mut t = Translator::new();
    let events = t.translate_frontend_tool_call(
        "frontend-1",
        "say_hello",
        Some(&serde_json::json!({"name": "world"})),
    );
    assert!(matches!(
        events.as_slice(),
        [Event::ToolCallStart(_), Event::ToolCallArgs(args), Event::ToolCallEnd(end)]
            if args.delta == r#"{"name":"world"}"#
                && end.tool_call_id == "frontend-1"
    ));
    assert!(t.open_tool_calls.is_empty());
    assert!(t.translate_frontend_tool_end("frontend-1").is_empty());
    assert!(t.flush().is_empty());
}

#[test]
fn frontend_tool_call_without_args_still_emits_end_and_tracks_no_state() {
    let mut t = Translator::new();
    for (id, arguments, has_args) in [
        ("no-args", None, false),
        ("null-args", Some(serde_json::Value::Null), false),
        ("empty-args", Some(serde_json::json!({})), true),
    ] {
        let events = t.translate_frontend_tool_call(id, "tool", arguments.as_ref());
        if has_args {
            assert!(matches!(
                events.as_slice(),
                [Event::ToolCallStart(_), Event::ToolCallArgs(args), Event::ToolCallEnd(end)]
                    if args.delta == "{}" && end.tool_call_id == id
            ));
        } else {
            assert!(matches!(
                events.as_slice(),
                [Event::ToolCallStart(_), Event::ToolCallEnd(end)]
                    if end.tool_call_id == id
            ));
        }
        assert!(t.open_tool_calls.is_empty());
        assert!(t.translate_frontend_tool_end(id).is_empty());
    }
    assert!(t.flush().is_empty());
}

#[test]
fn tool_call_closes_open_message_and_emits_start() {
    let mut t = Translator::new();
    // Open a text message
    let _ = t.translate(SessionUpdate::AgentMessageChunk(chunk("hello")));
    assert!(t.agent.is_some());

    // Tool call should close the message first
    let tc = ToolCall::new(ToolCallId::new("tc-1"), "Read file");
    let evs = t.translate(SessionUpdate::ToolCall(tc));

    // Should have: TextMessageEnd, ToolCallStart
    assert!(evs.len() >= 2, "expected >=2 events, got {:?}", evs);
    assert!(
        matches!(evs[0], Event::TextMessageEnd(_)),
        "first event should be TextMessageEnd, got {:?}",
        evs[0]
    );
    assert!(
        matches!(evs[1], Event::ToolCallStart(_)),
        "second event should be ToolCallStart, got {:?}",
        evs[1]
    );
    assert!(t.agent.is_none(), "agent message should be closed");
    assert!(t.open_tool_calls.contains("tc-1"));
}

#[test]
fn tool_call_update_completed_emits_end() {
    let mut t = Translator::new();
    // First open a tool call
    let tc = ToolCall::new(ToolCallId::new("tc-1"), "Read file");
    let _ = t.translate(SessionUpdate::ToolCall(tc));

    // Now complete it
    let update = ToolCallUpdate::new(
        "tc-1",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    let evs = t.translate(SessionUpdate::ToolCallUpdate(update));

    assert_eq!(evs.len(), 1);
    match &evs[0] {
        Event::ToolCallEnd(e) => assert_eq!(e.tool_call_id, "tc-1"),
        other => panic!("expected ToolCallEnd, got {other:?}"),
    }
    assert!(!t.open_tool_calls.contains("tc-1"));
}

#[test]
fn flush_closes_open_tool_calls() {
    let mut t = Translator::new();
    let tc = ToolCall::new(ToolCallId::new("tc-1"), "Read file")
        .raw_output(serde_json::json!({"one": 1}));
    let _ = t.translate(SessionUpdate::ToolCall(tc));
    let tc2 = ToolCall::new(ToolCallId::new("tc-2"), "Write file")
        .raw_output(serde_json::json!({"two": 2}));
    let _ = t.translate(SessionUpdate::ToolCall(tc2));

    let flushed = t.flush();
    assert!(matches!(
        flushed.as_slice(),
        [
            Event::ToolCallEnd(_),
            Event::ToolCallResult(_),
            Event::ToolCallEnd(_),
            Event::ToolCallResult(_)
        ]
    ));
    assert_eq!(
        flushed
            .iter()
            .filter(|event| matches!(event, Event::ToolCallEnd(_)))
            .count(),
        2
    );
    assert_eq!(
        flushed
            .iter()
            .filter(|event| matches!(event, Event::ToolCallResult(_)))
            .count(),
        2
    );
    assert!(t.flush().is_empty(), "flush must be idempotent");
}

#[test]
fn suppressed_tool_call_emits_no_envelope() {
    let mut t = Translator::new();
    t.set_suppressed_titles(["agui-acp-bridge_say_hello", "say_hello"]);

    // Open an agent message first so we can verify it gets closed
    // (the AG-UI rule still holds — the next event will be the
    // bridge's MCP-driven envelope).
    let _ = t.translate(SessionUpdate::AgentMessageChunk(ContentChunk::new(
        ContentBlock::Text(TextContent::new("preamble")),
    )));

    let tc = ToolCall::new(ToolCallId::new("agent-uuid-1"), "agui-acp-bridge_say_hello");
    let evs = t.translate(SessionUpdate::ToolCall(tc));

    // Only the message-end (one event), no TOOL_CALL_* — the MCP path
    // owns this.
    assert_eq!(evs.len(), 1, "expected only the message-end, got {evs:?}");
    assert!(matches!(evs[0], Event::TextMessageEnd(_)));
    assert!(!t.open_tool_calls.contains("agent-uuid-1"));
}

#[test]
fn suppressed_tool_call_update_is_dropped_until_terminal() {
    let mut t = Translator::new();
    t.set_suppressed_titles(["agui-acp-bridge_say_hello"]);

    // Suppressed tool call seeds the id into the suppression set.
    let tc = ToolCall::new(ToolCallId::new("call-1"), "agui-acp-bridge_say_hello");
    let _ = t.translate(SessionUpdate::ToolCall(tc));

    // In-progress update for the same id: still suppressed.
    let in_progress = ToolCallUpdate::new(
        "call-1",
        ToolCallUpdateFields::new().status(ToolCallStatus::InProgress),
    );
    let evs = t.translate(SessionUpdate::ToolCallUpdate(in_progress));
    assert!(
        evs.is_empty(),
        "in-progress update must be dropped: {evs:?}"
    );

    // Terminal update: still emits nothing, but clears the id.
    let completed = ToolCallUpdate::new(
        "call-1",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    let evs = t.translate(SessionUpdate::ToolCallUpdate(completed));
    assert!(
        evs.is_empty(),
        "completed update for suppressed id emits nothing: {evs:?}"
    );

    // Now a *fresh* tool call update for the same id (re-used) would
    // surface normally. We don't test re-use because ACP ids are
    // expected to be unique per call.
}

#[test]
fn unsuppressed_tool_call_still_emits_normally() {
    let mut t = Translator::new();
    t.set_suppressed_titles(["agui-acp-bridge_say_hello"]);

    // A different tool name passes through.
    let tc = ToolCall::new(ToolCallId::new("read-1"), "Read file");
    let evs = t.translate(SessionUpdate::ToolCall(tc));
    let kinds: Vec<&str> = evs
        .iter()
        .map(|e| match e {
            Event::ToolCallStart(_) => "start",
            Event::ToolCallArgs(_) => "args",
            Event::ToolCallEnd(_) => "end",
            _ => "other",
        })
        .collect();
    assert!(kinds.contains(&"start"), "got {kinds:?}");
}
