use super::*;

#[test]
fn acp_message_id_is_forwarded_to_agent_lifecycle() {
    let mut t = Translator::new();
    let events = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "hello",
        "acp-agent-1",
    )));
    match (&events[0], &events[1]) {
        (Event::TextMessageStart(start), Event::TextMessageContent(content)) => {
            assert_eq!(start.message_id, "acp-agent-1");
            assert_eq!(content.message_id, "acp-agent-1");
        }
        other => panic!("expected start/content, got {other:?}"),
    }
    match t.flush().as_slice() {
        [Event::TextMessageEnd(end)] => assert_eq!(end.message_id, "acp-agent-1"),
        other => panic!("expected one matching end, got {other:?}"),
    }
}

#[test]
fn same_acp_message_id_reuses_one_agent_lifecycle() {
    let mut t = Translator::new();
    let first = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "one",
        "acp-agent-1",
    )));
    let second = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "two",
        "acp-agent-1",
    )));
    assert!(matches!(
        first.as_slice(),
        [Event::TextMessageStart(_), Event::TextMessageContent(_)]
    ));
    assert!(
        matches!(second.as_slice(), [Event::TextMessageContent(content)] if content.message_id == "acp-agent-1")
    );
    assert!(
        matches!(t.flush().as_slice(), [Event::TextMessageEnd(end)] if end.message_id == "acp-agent-1")
    );
}

#[test]
fn changed_acp_message_id_ends_old_before_starting_new() {
    let mut t = Translator::new();
    let _ = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "one",
        "acp-agent-1",
    )));
    let events = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "two",
        "acp-agent-2",
    )));
    match events.as_slice() {
        [
            Event::TextMessageEnd(old_end),
            Event::TextMessageStart(new_start),
            Event::TextMessageContent(new_content),
        ] => {
            assert_eq!(old_end.message_id, "acp-agent-1");
            assert_eq!(new_start.message_id, "acp-agent-2");
            assert_eq!(new_content.message_id, "acp-agent-2");
        }
        other => panic!("expected old end/new start/content, got {other:?}"),
    }
}

#[test]
fn agent_user_and_reasoning_message_ids_are_independent() {
    let mut t = Translator::new();
    let agent = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "agent",
        "same-acp-id",
    )));
    let user = t.translate(SessionUpdate::UserMessageChunk(chunk_with_id(
        "user",
        "same-acp-id",
    )));
    let thought = t.translate(SessionUpdate::AgentThoughtChunk(chunk_with_id(
        "thought",
        "same-acp-id",
    )));

    assert!(
        matches!(agent.as_slice(), [Event::TextMessageStart(start), Event::TextMessageContent(content)] if start.message_id == "same-acp-id" && content.message_id == "same-acp-id")
    );
    assert!(
        matches!(user.as_slice(), [Event::TextMessageStart(start), Event::TextMessageContent(content)] if start.role == TextMessageRole::User && start.message_id == "same-acp-id" && content.message_id == "same-acp-id")
    );
    assert!(
        matches!(thought.as_slice(), [Event::ReasoningMessageStart(start), Event::ReasoningMessageContent(content)] if start.message_id == "same-acp-id" && content.message_id == "same-acp-id")
    );
}

#[test]
fn none_message_id_continues_open_and_gets_new_fallback_after_close() {
    let mut t = Translator::new();
    let known = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "known",
        "acp-known",
    )));
    assert!(matches!(
        known.as_slice(),
        [Event::TextMessageStart(start), Event::TextMessageContent(content)]
            if start.message_id == "acp-known" && content.message_id == "acp-known"
    ));
    let continued = t.translate(SessionUpdate::AgentMessageChunk(chunk("continued")));
    assert!(matches!(
        continued.as_slice(),
        [Event::TextMessageContent(content)] if content.message_id == "acp-known"
    ));
    assert!(matches!(
        t.flush().as_slice(),
        [Event::TextMessageEnd(end)] if end.message_id == "acp-known"
    ));

    let first = t.translate(SessionUpdate::AgentMessageChunk(chunk("one")));
    let first_id = match &first[0] {
        Event::TextMessageStart(start) => start.message_id.clone(),
        other => panic!("expected fallback start, got {other:?}"),
    };
    let second = t.translate(SessionUpdate::AgentMessageChunk(chunk("two")));
    assert!(
        matches!(second.as_slice(), [Event::TextMessageContent(content)] if content.message_id == first_id)
    );
    assert!(
        matches!(t.flush().as_slice(), [Event::TextMessageEnd(end)] if end.message_id == first_id)
    );

    let next = t.translate(SessionUpdate::AgentMessageChunk(chunk("three")));
    match &next[0] {
        Event::TextMessageStart(start) => assert_ne!(start.message_id, first_id),
        other => panic!("expected new fallback start, got {other:?}"),
    }
}

#[test]
fn tool_boundary_closes_text_and_clears_none_fallback_identity() {
    let mut t = Translator::new();
    let first = t.translate(SessionUpdate::AgentMessageChunk(chunk_with_id(
        "before tool",
        "acp-agent-message",
    )));
    let first_id = match &first[0] {
        Event::TextMessageStart(start) => start.message_id.clone(),
        other => panic!("expected text start, got {other:?}"),
    };
    let tool = t.translate(SessionUpdate::ToolCall(ToolCall::new(
        ToolCallId::new("tool-call-1"),
        "Read file",
    )));
    assert!(
        matches!(tool.as_slice(), [Event::TextMessageEnd(end), Event::ToolCallStart(start)] if end.message_id == first_id && start.tool_call_id == "tool-call-1")
    );

    let after = t.translate(SessionUpdate::AgentMessageChunk(chunk("after tool")));
    match &after[0] {
        Event::TextMessageStart(start) => assert_ne!(start.message_id, first_id),
        other => panic!("expected fresh fallback start, got {other:?}"),
    }
}

#[test]
fn agent_chunk_emits_start_then_content_then_end_on_flush() {
    let mut t = Translator::new();
    let evs1 = t.translate(SessionUpdate::AgentMessageChunk(chunk("Hello ")));
    assert_eq!(evs1.len(), 2);
    assert!(matches!(evs1[0], Event::TextMessageStart(_)));
    assert!(matches!(evs1[1], Event::TextMessageContent(_)));

    let evs2 = t.translate(SessionUpdate::AgentMessageChunk(chunk("world")));
    assert_eq!(evs2.len(), 1);
    assert!(matches!(evs2[0], Event::TextMessageContent(_)));

    let flushed = t.flush();
    assert_eq!(flushed.len(), 1);
    assert!(matches!(flushed[0], Event::TextMessageEnd(_)));
}

#[test]
fn agent_start_carries_assistant_role() {
    let mut t = Translator::new();
    let evs = t.translate(SessionUpdate::AgentMessageChunk(chunk("x")));
    match &evs[0] {
        Event::TextMessageStart(s) => assert_eq!(s.role, TextMessageRole::Assistant),
        other => panic!("{other:?}"),
    }
}

#[test]
fn user_chunk_uses_user_role_and_separate_id() {
    let mut t = Translator::new();
    let a = t.translate(SessionUpdate::AgentMessageChunk(chunk("a")));
    let u = t.translate(SessionUpdate::UserMessageChunk(chunk("u")));
    let agent_id = match &a[0] {
        Event::TextMessageStart(s) => s.message_id.clone(),
        _ => unreachable!(),
    };
    match &u[0] {
        Event::TextMessageStart(s) => {
            assert_eq!(s.role, TextMessageRole::User);
            assert_ne!(s.message_id, agent_id);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn empty_text_yields_no_events() {
    let mut t = Translator::new();
    assert!(
        t.translate(SessionUpdate::AgentMessageChunk(chunk("")))
            .is_empty()
    );
}

#[test]
fn thought_chunk_emits_reasoning_start_then_content() {
    let mut t = Translator::new();
    let evs = t.translate(SessionUpdate::AgentThoughtChunk(chunk("hmm")));
    assert_eq!(evs.len(), 2);
    assert!(matches!(evs[0], Event::ReasoningMessageStart(_)));
    assert!(matches!(evs[1], Event::ReasoningMessageContent(_)));

    let evs2 = t.translate(SessionUpdate::AgentThoughtChunk(chunk("...")));
    assert_eq!(evs2.len(), 1);
    assert!(matches!(evs2[0], Event::ReasoningMessageContent(_)));

    let flushed = t.flush();
    assert!(matches!(flushed[0], Event::ReasoningMessageEnd(_)));
}

#[test]
fn flush_on_empty_translator_is_noop() {
    let mut t = Translator::new();
    assert!(t.flush().is_empty());
}
