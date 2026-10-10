use super::*;

pub(super) fn extract_text(
    content: &agent_client_protocol::schema::v1::ContentBlock,
) -> Option<String> {
    use agent_client_protocol::schema::v1::ContentBlock;
    match content {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    }
}

pub(super) fn push_text_message(
    state: &mut Option<MessageState>,
    current_acp_message_id: &mut Option<String>,
    text: String,
    message_id: Option<&MessageId>,
    role: TextMessageRole,
) -> Vec<Event> {
    let incoming_id = message_id.map(|id| id.0.to_string());
    let needs_boundary = incoming_id.as_deref().is_some_and(|id| {
        state.as_ref().is_some_and(|current| current.is_open())
            && current_acp_message_id.as_deref() != Some(id)
    });
    let mut out = Vec::new();
    if needs_boundary {
        if let Some(mut current) = state.take() {
            out.append(&mut close_text(&mut current));
        }
        *current_acp_message_id = None;
    }
    if state.as_ref().is_none_or(|current| !current.is_open()) {
        let id = incoming_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        *state = Some(MessageState::new(id));
        *current_acp_message_id = incoming_id;
    }
    if let Some(current) = state.as_mut() {
        out.extend(push_text(current, text, role));
    }
    out
}

pub(super) fn push_text(
    state: &mut MessageState,
    text: String,
    role: TextMessageRole,
) -> Vec<Event> {
    state
        .push_chunk(text)
        .into_iter()
        .map(|ev| match ev {
            Event::TextMessageStart(s) => {
                Event::TextMessageStart(TextMessageStartEvent { role, ..s })
            }
            other => other,
        })
        .collect()
}

pub(super) fn close_text(state: &mut MessageState) -> Vec<Event> {
    state.close()
}

pub(super) fn raw_passthrough(update: &SessionUpdate) -> Event {
    let value =
        serde_json::to_value(update).unwrap_or_else(|_| json!({"error": "serialize_failed"}));
    Event::Raw(RawEvent {
        event: value,
        source: Some("acp".to_string()),
        base: BaseEventFields::default(),
    })
}

pub(super) fn config_option_update_event(update: &ConfigOptionUpdate) -> Event {
    let value = serde_json::to_value(SessionUpdate::ConfigOptionUpdate(update.clone()))
        .unwrap_or_else(|_| json!({"error": "serialize_failed"}));
    Event::Custom(CustomEvent {
        name: "acp.session_update".to_string(),
        value,
        base: BaseEventFields::default(),
    })
}

pub(super) fn tool_call_args_event(
    tool_call_id: &str,
    raw_input: &serde_json::Value,
) -> Option<Event> {
    let args_json = serde_json::to_string(raw_input).unwrap_or_default();
    (!args_json.is_empty() && args_json != "null").then(|| {
        Event::ToolCallArgs(ToolCallArgsEvent {
            tool_call_id: tool_call_id.to_string(),
            delta: args_json,
            base: BaseEventFields::default(),
        })
    })
}

pub(super) fn tool_call_result_event(tool_call_id: &str, raw_output: &serde_json::Value) -> Event {
    let message_id = format!("tool-result-{tool_call_id}");
    match serde_json::to_string(raw_output) {
        Ok(content) => Event::ToolCallResult(ToolCallResultEvent {
            message_id,
            tool_call_id: tool_call_id.to_string(),
            content,
            role: Some(ToolResultRole::Tool),
            base: BaseEventFields::default(),
        }),
        Err(error) => Event::Custom(CustomEvent {
            name: "acp.tool_call_raw_output".to_string(),
            value: json!({
                "toolCallId": tool_call_id,
                "rawOutput": raw_output,
                "error": error.to_string(),
            }),
            base: BaseEventFields::default(),
        }),
    }
}
