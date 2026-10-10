use super::*;

#[cfg(feature = "unstable_session_usage")]
#[test]
fn usage_update_emits_agent_usage_custom_event() {
    // opencode emits a `usage_update` SessionUpdate on every LLM
    // round-trip carrying token-context-window stats. The bridge
    // must translate it to a CUSTOM `agent:usage_update` event so
    // frontends can render token meters; before the
    // `unstable_session_usage` feature was wired the ACP client
    // library raised JSON-RPC `Invalid params` back to the agent
    // every time, polluting logs and confusing the agent.
    use agent_client_protocol::schema::v1::UsageUpdate;
    let mut t = Translator::new();
    let usage = UsageUpdate::new(28_239, 200_000);
    let evs = t.translate(SessionUpdate::UsageUpdate(usage));
    assert_eq!(evs.len(), 1);
    match &evs[0] {
        Event::Custom(c) => {
            assert_eq!(c.name, "agent:usage_update");
            assert_eq!(c.value["used"], 28_239);
            assert_eq!(c.value["size"], 200_000);
        }
        other => panic!("expected Custom(agent:usage_update), got {other:?}"),
    }
}

#[test]
fn opaque_non_text_update_emits_one_raw_after_closing_text() {
    let update = SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Image(
        ImageContent::new("aGVsbG8=", "image/png"),
    )));
    let mut t = Translator::new();
    let _ = t.translate(SessionUpdate::AgentMessageChunk(chunk("before image")));
    let expected = serde_json::to_value(&update).expect("ACP update must serialize");
    let evs = t.translate(update);

    assert!(matches!(
        evs.as_slice(),
        [Event::TextMessageEnd(_), Event::Raw(_)]
    ));
    match &evs[1] {
        Event::Raw(raw) => {
            assert_eq!(raw.source.as_deref(), Some("acp"));
            assert_eq!(raw.event, expected);
            assert_eq!(raw.base, BaseEventFields::default());
        }
        other => panic!("expected Raw for non-text image chunk, got {other:?}"),
    }
    assert!(
        t.flush().is_empty(),
        "opaque update must close the open text boundary"
    );
}

#[test]
fn config_option_update_emits_complete_custom_bridge_snapshot() {
    let update = SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(vec![
        agent_client_protocol::schema::v1::SessionConfigOption::boolean("enabled", "Enabled", true),
    ]));
    let expected = serde_json::to_value(&update).expect("config update must serialize");
    let mut t = Translator::new();
    let events = t.translate(update);

    match events.as_slice() {
        [Event::Custom(custom)] => {
            assert_eq!(custom.name, "acp.session_update");
            assert_eq!(custom.value, expected);
            assert_eq!(custom.value["sessionUpdate"], "config_option_update");
            assert!(custom.value["configOptions"][0].is_object());
        }
        other => panic!("expected one config CUSTOM event, got {other:?}"),
    }
}
