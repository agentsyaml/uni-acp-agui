use super::*;

#[test]
fn tool_tracking_limit_closes_every_started_call_once() {
    let mut t = Translator::new();
    let mut trace = Vec::new();
    // Push more fresh open calls than the cap. The oldest are evicted
    // together with their cached raw input/output — no dangling partial
    // state.
    for i in 0..(MAX_OPEN_TOOL_CALLS + 64) {
        let call = ToolCall::new(ToolCallId::new(format!("flood-{i}")), "Read file")
            .raw_input(serde_json::json!({"i": i}))
            .raw_output(serde_json::json!({"out": i}));
        let events = t.translate(SessionUpdate::ToolCall(call));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Event::ToolCallStart(_)))
        );
        trace.extend(events);
        assert_eq!(t.open_tool_calls.len(), t.open_tool_call_order.len());
        assert!(t.open_tool_calls.len() <= MAX_OPEN_TOOL_CALLS);
        assert!(t.raw_tool_inputs.len() <= MAX_OPEN_TOOL_CALLS);
        assert!(t.raw_tool_outputs.len() <= MAX_OPEN_TOOL_CALLS);
    }

    let replacement_start = trace.iter().position(|e| matches!(e, Event::ToolCallStart(s) if s.tool_call_id == format!("flood-{MAX_OPEN_TOOL_CALLS}"))).unwrap();
    let first_end = trace
        .iter()
        .position(|e| matches!(e, Event::ToolCallEnd(end) if end.tool_call_id == "flood-0"))
        .unwrap();
    assert!(first_end < replacement_start);
    assert!(
        matches!(&trace[first_end-1], Event::ToolCallArgs(a) if a.tool_call_id == "flood-0" && a.delta == r#"{"i":0}"#)
    );
    assert!(
        matches!(&trace[first_end+1], Event::ToolCallResult(r) if r.tool_call_id == "flood-0" && r.content == r#"{"out":0}"#)
    );

    // An evicted id's later terminal update must not resurrect state.
    let stale = ToolCallUpdate::new(
        "flood-0",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    assert!(t.translate(SessionUpdate::ToolCallUpdate(stale)).is_empty());
    assert!(!t.open_tool_calls.contains("flood-0"));

    // Flushing the survivors emits one well-formed END per open call,
    // never a dangling partial envelope.
    let flushed = t.flush();
    trace.extend(flushed.clone());
    assert_eq!(
        flushed
            .iter()
            .filter(|event| matches!(event, Event::ToolCallEnd(_)))
            .count(),
        MAX_OPEN_TOOL_CALLS
    );
    for end in flushed.iter().filter_map(|event| match event {
        Event::ToolCallEnd(end) => Some(end.tool_call_id.as_str()),
        _ => None,
    }) {
        assert!(end.starts_with("flood-"));
    }
    assert!(t.flush().is_empty(), "flush must be idempotent after flood");
    for i in 0..(MAX_OPEN_TOOL_CALLS + 64) {
        let id = format!("flood-{i}");
        assert_eq!(
            trace
                .iter()
                .filter(|e| matches!(e, Event::ToolCallStart(s) if s.tool_call_id == id))
                .count(),
            1
        );
        assert_eq!(
            trace
                .iter()
                .filter(|e| matches!(e, Event::ToolCallEnd(e) if e.tool_call_id == id))
                .count(),
            1
        );
    }

    let mut frontend = Translator::new();
    let normal = ToolCall::new(ToolCallId::new("normal-open"), "Read file")
        .raw_input(serde_json::json!({"path": "file"}))
        .raw_output(serde_json::json!({"ok": true}));
    frontend.translate(SessionUpdate::ToolCall(normal));
    let mut frontend_trace = Vec::new();
    for i in 0..(MAX_OPEN_TOOL_CALLS + 1) {
        let events = frontend.translate_frontend_tool_call(format!("frontend-{i}"), "tool", None);
        assert!(matches!(
            events.as_slice(),
            [Event::ToolCallStart(_), Event::ToolCallEnd(end)]
                if end.tool_call_id == format!("frontend-{i}")
        ));
        frontend_trace.extend(events);
        assert_eq!(frontend.open_tool_calls.len(), 1);
        assert!(frontend.open_tool_calls.contains("normal-open"));
        assert_eq!(frontend.raw_tool_inputs.len(), 1);
        assert_eq!(frontend.raw_tool_outputs.len(), 1);
    }
    assert!(
        frontend
            .translate_frontend_tool_end("frontend-0")
            .is_empty()
    );
    frontend_trace.extend(frontend.flush());
    assert_eq!(
        frontend_trace
            .iter()
            .filter(|e| matches!(e, Event::ToolCallStart(_)))
            .count(),
        MAX_OPEN_TOOL_CALLS + 1
    );
    assert_eq!(
        frontend_trace
            .iter()
            .filter(|e| matches!(e, Event::ToolCallEnd(_)))
            .count(),
        MAX_OPEN_TOOL_CALLS + 2
    );
    let old_end = frontend_trace
        .iter()
        .position(|e| matches!(e, Event::ToolCallEnd(end) if end.tool_call_id == "frontend-0"))
        .unwrap();
    let new_start = frontend_trace.iter().position(|e| matches!(e, Event::ToolCallStart(start) if start.tool_call_id == format!("frontend-{MAX_OPEN_TOOL_CALLS}"))).unwrap();
    assert!(old_end < new_start);
    assert_eq!(
        frontend_trace
            .iter()
            .filter(|event| matches!(event, Event::ToolCallResult(result) if result.tool_call_id == "normal-open"))
            .count(),
        1
    );
    assert!(!frontend_trace.iter().any(|event| matches!(
        event,
        Event::ToolCallResult(result) if result.tool_call_id.starts_with("frontend-")
    )));

    let mut snapshots = Translator::new();
    let pending =
        ToolCall::new(ToolCallId::new("repeat"), "tool").raw_input(serde_json::json!({"v":1}));
    snapshots.translate(SessionUpdate::ToolCall(pending));
    let completed = ToolCall::new(ToolCallId::new("repeat"), "tool")
        .status(ToolCallStatus::Completed)
        .raw_input(serde_json::json!({"v":2}))
        .raw_output(serde_json::json!({"ok":true}));
    let closed = snapshots.translate(SessionUpdate::ToolCall(completed));
    assert!(!closed.iter().any(|e| matches!(e, Event::ToolCallStart(_))));
    assert!(matches!(
        closed.as_slice(),
        [
            Event::ToolCallArgs(_),
            Event::ToolCallEnd(_),
            Event::ToolCallResult(_)
        ]
    ));
    assert!(snapshots.flush().is_empty());
    assert!(
        snapshots
            .translate(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                "repeat",
                ToolCallUpdateFields::new().status(ToolCallStatus::Completed)
            )))
            .is_empty()
    );
}

#[test]
fn suppressed_ids_stay_bounded_when_calls_never_terminate() {
    let mut t = Translator::new();
    t.set_suppressed_titles(["agui-acp-bridge_say_hello"]);
    for i in 0..(MAX_SUPPRESSED_IDS + 64) {
        let tc = ToolCall::new(
            ToolCallId::new(format!("ghost-{i}")),
            "agui-acp-bridge_say_hello",
        );
        assert!(t.translate(SessionUpdate::ToolCall(tc)).is_empty());
        assert!(t.suppressed_ids.len() <= MAX_SUPPRESSED_IDS);
    }
    let repeated = ToolCall::new(
        ToolCallId::new(format!("ghost-{}", MAX_SUPPRESSED_IDS + 63)),
        "agui-acp-bridge_say_hello",
    );
    t.translate(SessionUpdate::ToolCall(repeated));
    assert_eq!(
        t.suppressed_ids
            .iter()
            .filter(|id| id.as_str() == format!("ghost-{}", MAX_SUPPRESSED_IDS + 63))
            .count(),
        0
    );
    let terminal = ToolCall::new(
        ToolCallId::new("terminal-ghost"),
        "agui-acp-bridge_say_hello",
    )
    .status(ToolCallStatus::Completed);
    t.translate(SessionUpdate::ToolCall(terminal));
    assert!(!t.suppressed_ids.contains(&"terminal-ghost".to_string()));
    // Terminating a surviving (non-evicted) suppressed id still prunes it.
    let terminal = ToolCallUpdate::new(
        format!("ghost-{}", MAX_SUPPRESSED_IDS + 63),
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    assert!(
        t.translate(SessionUpdate::ToolCallUpdate(terminal))
            .is_empty()
    );
}
