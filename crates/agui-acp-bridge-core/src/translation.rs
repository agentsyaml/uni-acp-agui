use std::collections::HashSet;

use crate::message_state::MessageState;
use crate::stream::{SessionModelsInit, SessionModesInit};
use agent_client_protocol::schema::{SessionUpdate, ToolCallStatus};
use agui_rs_core::events::{
    BaseEventFields, CustomEvent, Event, ReasoningMessageContentEvent, ReasoningMessageEndEvent,
    ReasoningMessageRole, ReasoningMessageStartEvent, TextMessageStartEvent, ToolCallArgsEvent,
    ToolCallEndEvent, ToolCallStartEvent,
};
use agui_rs_core::types::TextMessageRole;
use serde_json::json;

/// Build the AG-UI CUSTOM event a fresh prompt should emit before any agent
/// updates so the frontend can render mode / model pickers.
///
/// The shape is intentionally stable and matches the JSON returned by the
/// bridge's `GET /session/init` endpoint:
/// `{ "modes": SessionModesInit | null, "models": SessionModelsInit | null }`.
/// Either field is `null` when the agent did not advertise that capability;
/// frontends can treat them identically across both transports.
#[must_use]
pub fn session_init_event(
    modes: Option<&SessionModesInit>,
    models: Option<&SessionModelsInit>,
) -> Event {
    let modes_value = modes
        .and_then(|m| serde_json::to_value(m).ok())
        .unwrap_or(serde_json::Value::Null);
    let models_value = models
        .and_then(|m| serde_json::to_value(m).ok())
        .unwrap_or(serde_json::Value::Null);
    let mut payload = serde_json::Map::new();
    payload.insert("modes".to_string(), modes_value);
    payload.insert("models".to_string(), models_value);
    Event::Custom(CustomEvent {
        name: "agent:session_init".to_string(),
        value: serde_json::Value::Object(payload),
        base: BaseEventFields::default(),
    })
}

#[derive(Debug, Default)]
pub struct Translator {
    agent: Option<MessageState>,
    user: Option<MessageState>,
    thought: Option<String>,
    open_tool_calls: HashSet<String>,
    /// Tool titles the agent will report on `session/update` for tool calls
    /// that the bridge is *also* driving via its in-process MCP endpoint.
    /// We suppress those native session-side events so frontend hooks see
    /// exactly one logical tool call per invocation. Otherwise, agents
    /// that proxy MCP tool calls through their own session/update channel
    /// (opencode does this) would surface duplicate `TOOL_CALL_*` events
    /// — one from the agent's UI tracking, one from our MCP path —
    /// causing the frontend's `useFrontendTool`-style hook to fire
    /// against an unknown id.
    suppressed_titles: HashSet<String>,
    /// Tool call ids (by `tool_call_id`) that originated on the
    /// agent's session/update path and were suppressed. Their later
    /// `ToolCallUpdate` events must also be suppressed so we don't emit
    /// stray `TOOL_CALL_END` for an id the frontend never saw.
    suppressed_ids: HashSet<String>,
}

impl Translator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure a set of tool titles whose ACP-side `ToolCall` /
    /// `ToolCallUpdate` events should be suppressed in favour of the
    /// bridge's MCP-driven `FrontendToolCall` items.
    ///
    /// Pass both the short tool name AND any prefixed variant the agent
    /// might use (typically `<mcp-server-name>_<tool-name>`). Repeated
    /// calls *replace* the set, matching the per-run lifecycle of
    /// `RunAgentInput.tools`.
    pub fn set_suppressed_titles<I, S>(&mut self, titles: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.suppressed_titles = titles.into_iter().map(Into::into).collect();
    }

    /// Whether a given title is currently being suppressed.
    pub fn is_suppressed(&self, title: &str) -> bool {
        self.suppressed_titles.contains(title)
    }

    /// Emit an AG-UI `TOOL_CALL_START` (and `TOOL_CALL_ARGS` if any) for a
    /// tool call the bridge is driving directly through its in-process
    /// MCP endpoint.
    ///
    /// Unlike [`Translator::translate`] for [`SessionUpdate::ToolCall`],
    /// this **bypasses the suppression filter**: the suppression set
    /// exists to drop *agent-side echoes* of these calls, so the canonical
    /// event must always come through. The translator's open-tool-calls
    /// tracking is updated so [`Translator::flush`] still emits a
    /// `TOOL_CALL_END` if the call never sees its terminal update.
    pub fn translate_frontend_tool_call(
        &mut self,
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Option<&serde_json::Value>,
    ) -> Vec<Event> {
        let tool_call_id = tool_call_id.into();
        let tool_name = tool_name.into();

        let mut events = self.close_open_messages();

        events.push(Event::ToolCallStart(ToolCallStartEvent {
            tool_call_id: tool_call_id.clone(),
            tool_call_name: tool_name,
            parent_message_id: None,
            base: BaseEventFields::default(),
        }));

        if let Some(value) = arguments {
            let args_json = serde_json::to_string(value).unwrap_or_default();
            if !args_json.is_empty() && args_json != "null" {
                events.push(Event::ToolCallArgs(ToolCallArgsEvent {
                    tool_call_id: tool_call_id.clone(),
                    delta: args_json,
                    base: BaseEventFields::default(),
                }));
            }
        }

        self.open_tool_calls.insert(tool_call_id);
        events
    }

    /// Emit an AG-UI `TOOL_CALL_END` for a bridge-driven tool call. Used
    /// by the MCP endpoint pathway after the frontend posts its result.
    /// Idempotent — emits nothing if the id is unknown (the call was
    /// already closed by `flush()` or a prior call).
    pub fn translate_frontend_tool_end(&mut self, tool_call_id: &str) -> Vec<Event> {
        if !self.open_tool_calls.remove(tool_call_id) {
            return Vec::new();
        }
        vec![Event::ToolCallEnd(ToolCallEndEvent {
            tool_call_id: tool_call_id.to_string(),
            base: BaseEventFields::default(),
        })]
    }

    pub fn translate(&mut self, update: SessionUpdate) -> Vec<Event> {
        match update {
            SessionUpdate::AgentMessageChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_agent(text),
                None => {
                    let mut out = self.close_open_messages();
                    out.push(raw_passthrough(&update));
                    out
                }
            },
            SessionUpdate::UserMessageChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_user(text),
                None => {
                    let mut out = self.close_open_messages();
                    out.push(raw_passthrough(&update));
                    out
                }
            },
            SessionUpdate::AgentThoughtChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_thought(text),
                None => {
                    let mut out = self.close_open_messages();
                    out.push(raw_passthrough(&update));
                    out
                }
            },
            SessionUpdate::ToolCall(ref tc) => self.handle_tool_call(tc),
            SessionUpdate::ToolCallUpdate(ref update) => self.handle_tool_call_update(update),
            SessionUpdate::CurrentModeUpdate(ref mode) => {
                let mut out = self.close_open_messages();
                let mode_id = mode.current_mode_id.0.to_string();
                out.push(Event::Custom(CustomEvent {
                    name: "agent:mode_update".to_string(),
                    value: json!({ "modeId": mode_id }),
                    base: BaseEventFields::default(),
                }));
                out
            }
            SessionUpdate::AvailableCommandsUpdate(ref cmds) => {
                let mut out = self.close_open_messages();
                let value = serde_json::to_value(cmds)
                    .unwrap_or_else(|_| json!({"error": "serialize_failed"}));
                out.push(Event::Custom(CustomEvent {
                    name: "agent:commands_available".to_string(),
                    value,
                    base: BaseEventFields::default(),
                }));
                out
            }
            #[cfg(feature = "unstable_session_usage")]
            SessionUpdate::UsageUpdate(ref usage) => {
                // Token / cost telemetry. Some agents (opencode) emit
                // this on every LLM round-trip; surfacing it as a
                // dedicated CUSTOM event lets frontends render token
                // meters and cost badges without having to parse the
                // raw passthrough envelope.
                let mut out = self.close_open_messages();
                let value = serde_json::to_value(usage)
                    .unwrap_or_else(|_| json!({"error": "serialize_failed"}));
                out.push(Event::Custom(CustomEvent {
                    name: "agent:usage_update".to_string(),
                    value,
                    base: BaseEventFields::default(),
                }));
                out
            }
            SessionUpdate::Plan(ref plan) => {
                // Translate plan entries to step events where possible
                let mut events = self.close_open_messages();
                let mut had_step = false;
                for entry in &plan.entries {
                    match entry.status {
                        agent_client_protocol::schema::PlanEntryStatus::InProgress => {
                            events.push(Event::StepStarted(
                                agui_rs_core::events::StepStartedEvent {
                                    step_name: entry.content.clone(),
                                    base: BaseEventFields::default(),
                                },
                            ));
                            had_step = true;
                        }
                        agent_client_protocol::schema::PlanEntryStatus::Completed => {
                            events.push(Event::StepFinished(
                                agui_rs_core::events::StepFinishedEvent {
                                    step_name: entry.content.clone(),
                                    base: BaseEventFields::default(),
                                },
                            ));
                            had_step = true;
                        }
                        _ => {}
                    }
                }
                if !had_step {
                    events.push(raw_passthrough(&SessionUpdate::Plan(plan.clone())));
                }
                events
            }
            other => {
                let mut out = self.close_open_messages();
                out.push(raw_passthrough(&other));
                out
            }
        }
    }

    pub fn flush(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut s) = self.agent.take() {
            out.append(&mut close_text(&mut s));
        }
        if let Some(mut s) = self.user.take() {
            out.append(&mut close_text(&mut s));
        }
        if let Some(id) = self.thought.take() {
            out.push(Event::ReasoningMessageEnd(ReasoningMessageEndEvent {
                message_id: id,
                base: BaseEventFields::default(),
            }));
        }
        // Close all open tool calls
        for tc_id in self.open_tool_calls.drain() {
            out.push(Event::ToolCallEnd(ToolCallEndEvent {
                tool_call_id: tc_id,
                base: BaseEventFields::default(),
            }));
        }
        out
    }

    /// Handle a ToolCall session update.
    ///
    /// AG-UI rule: tool call arrival must close any open text message first.
    fn handle_tool_call(&mut self, tc: &agent_client_protocol::schema::ToolCall) -> Vec<Event> {
        let tool_name = tc.title.clone();
        let tool_call_id = tc.tool_call_id.0.to_string();

        // Suppress agent-side echoes of bridge-driven frontend tool calls
        // (see `suppressed_titles` doc on Translator). We still close any
        // open text message so the AG-UI rule about message boundaries is
        // preserved — the *next* event on the stream (ours, from the MCP
        // path) will then open the canonical TOOL_CALL_* envelope.
        if self.suppressed_titles.contains(&tool_name) {
            self.suppressed_ids.insert(tool_call_id);
            return self.close_open_messages();
        }

        let mut events = Vec::new();

        // Close open messages before starting a tool call (AG-UI protocol rule)
        events.append(&mut self.close_open_messages());

        // Emit TOOL_CALL_START
        events.push(Event::ToolCallStart(ToolCallStartEvent {
            tool_call_id: tool_call_id.clone(),
            tool_call_name: tool_name,
            parent_message_id: None,
            base: BaseEventFields::default(),
        }));

        // Emit TOOL_CALL_ARGS with raw_input if present
        if let Some(ref raw_input) = tc.raw_input {
            let args_json = serde_json::to_string(raw_input).unwrap_or_default();
            if !args_json.is_empty() && args_json != "null" {
                events.push(Event::ToolCallArgs(ToolCallArgsEvent {
                    tool_call_id: tool_call_id.clone(),
                    delta: args_json,
                    base: BaseEventFields::default(),
                }));
            }
        }

        self.open_tool_calls.insert(tool_call_id);
        events
    }

    /// Handle a ToolCallUpdate session update.
    fn handle_tool_call_update(
        &mut self,
        update: &agent_client_protocol::schema::ToolCallUpdate,
    ) -> Vec<Event> {
        let tool_call_id = update.tool_call_id.0.to_string();

        // Drop updates for ids that originated on a suppressed ToolCall.
        // The bridge's MCP path owns the lifecycle for those.
        if self.suppressed_ids.contains(&tool_call_id) {
            // If this update reports terminal status, also clear the id
            // so the suppression set doesn't grow unbounded over a long
            // session.
            if let Some(ref status) = update.fields.status {
                if matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed) {
                    self.suppressed_ids.remove(&tool_call_id);
                }
            }
            return Vec::new();
        }

        // Check status in the fields
        if let Some(ref status) = update.fields.status {
            match status {
                ToolCallStatus::Completed | ToolCallStatus::Failed => {
                    self.open_tool_calls.remove(&tool_call_id);
                    return vec![Event::ToolCallEnd(ToolCallEndEvent {
                        tool_call_id,
                        base: BaseEventFields::default(),
                    })];
                }
                ToolCallStatus::InProgress => {
                    // Emit progress as TOOL_CALL_ARGS if there's raw_output
                    if let Some(ref raw_output) = update.fields.raw_output {
                        let delta = serde_json::to_string(raw_output).unwrap_or_default();
                        if !delta.is_empty() && delta != "null" {
                            return vec![Event::ToolCallArgs(ToolCallArgsEvent {
                                tool_call_id,
                                delta,
                                base: BaseEventFields::default(),
                            })];
                        }
                    }
                }
                _ => {}
            }
        }

        // If there's raw_output but no status change, emit as args
        if let Some(ref raw_output) = update.fields.raw_output {
            let delta = serde_json::to_string(raw_output).unwrap_or_default();
            if !delta.is_empty() && delta != "null" {
                return vec![Event::ToolCallArgs(ToolCallArgsEvent {
                    tool_call_id,
                    delta,
                    base: BaseEventFields::default(),
                })];
            }
        }

        vec![]
    }

    /// Close all open text messages and thought streams.
    /// Used when a tool call arrives (AG-UI requires messages to be closed first).
    fn close_open_messages(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut s) = self.agent.take() {
            out.append(&mut close_text(&mut s));
        }
        if let Some(mut s) = self.user.take() {
            out.append(&mut close_text(&mut s));
        }
        if let Some(id) = self.thought.take() {
            out.push(Event::ReasoningMessageEnd(ReasoningMessageEndEvent {
                message_id: id,
                base: BaseEventFields::default(),
            }));
        }
        out
    }

    fn push_agent(&mut self, text: String) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        let state = self
            .agent
            .get_or_insert_with(|| MessageState::new(uuid::Uuid::new_v4().to_string()));
        push_text(state, text, TextMessageRole::Assistant)
    }

    fn push_user(&mut self, text: String) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        let state = self
            .user
            .get_or_insert_with(|| MessageState::new(uuid::Uuid::new_v4().to_string()));
        push_text(state, text, TextMessageRole::User)
    }

    fn push_thought(&mut self, text: String) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        let mut events = Vec::new();
        let id = match &self.thought {
            Some(existing) => existing.clone(),
            None => {
                let new_id = uuid::Uuid::new_v4().to_string();
                events.push(Event::ReasoningMessageStart(ReasoningMessageStartEvent {
                    message_id: new_id.clone(),
                    role: ReasoningMessageRole::Reasoning,
                    base: BaseEventFields::default(),
                }));
                self.thought = Some(new_id.clone());
                new_id
            }
        };
        events.push(Event::ReasoningMessageContent(
            ReasoningMessageContentEvent {
                message_id: id,
                delta: text,
                base: BaseEventFields::default(),
            },
        ));
        events
    }
}

fn extract_text(content: &agent_client_protocol::schema::ContentBlock) -> Option<String> {
    use agent_client_protocol::schema::ContentBlock;
    match content {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    }
}

fn push_text(state: &mut MessageState, text: String, role: TextMessageRole) -> Vec<Event> {
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

fn close_text(state: &mut MessageState) -> Vec<Event> {
    state.close()
}

fn raw_passthrough(update: &SessionUpdate) -> Event {
    let value =
        serde_json::to_value(update).unwrap_or_else(|_| json!({"error": "serialize_failed"}));
    Event::Custom(CustomEvent {
        name: "acp.session_update".to_string(),
        value,
        base: BaseEventFields::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::{
        ContentBlock, ContentChunk, ImageContent, Plan, PlanEntry, PlanEntryPriority,
        PlanEntryStatus, TextContent, ToolCall, ToolCallId, ToolCallUpdate, ToolCallUpdateFields,
    };

    fn chunk(text: &str) -> ContentChunk {
        ContentChunk::new(ContentBlock::Text(TextContent::new(text)))
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
    fn plan_with_in_progress_emits_step_started() {
        let plan = Plan::new(vec![
            PlanEntry::new(
                "step one",
                PlanEntryPriority::High,
                PlanEntryStatus::InProgress,
            ),
            PlanEntry::new(
                "step two",
                PlanEntryPriority::Medium,
                PlanEntryStatus::Pending,
            ),
        ]);
        let mut t = Translator::new();
        let evs = t.translate(SessionUpdate::Plan(plan));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            Event::StepStarted(s) => assert_eq!(s.step_name, "step one"),
            other => panic!("expected StepStarted, got {other:?}"),
        }
    }

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
        use agent_client_protocol::schema::UsageUpdate;
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
    fn plan_all_pending_falls_through_as_custom() {
        let plan = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )]);
        let mut t = Translator::new();
        let evs = t.translate(SessionUpdate::Plan(plan));
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Event::Custom(_)));
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
        let tc = ToolCall::new(ToolCallId::new("tc-1"), "Read file");
        let _ = t.translate(SessionUpdate::ToolCall(tc));
        let tc2 = ToolCall::new(ToolCallId::new("tc-2"), "Write file");
        let _ = t.translate(SessionUpdate::ToolCall(tc2));

        let flushed = t.flush();
        let end_count = flushed
            .iter()
            .filter(|e| matches!(e, Event::ToolCallEnd(_)))
            .count();
        assert_eq!(end_count, 2, "flush should close both tool calls");
    }

    #[test]
    fn flush_on_empty_translator_is_noop() {
        let mut t = Translator::new();
        assert!(t.flush().is_empty());
    }

    #[test]
    fn agent_message_chunk_with_image_emits_custom_event() {
        let img = ContentChunk::new(ContentBlock::Image(ImageContent::new(
            "aGVsbG8=",
            "image/png",
        )));
        let mut t = Translator::new();
        let evs = t.translate(SessionUpdate::AgentMessageChunk(img));
        assert_eq!(evs.len(), 1, "non-text chunk must emit exactly one event");
        match &evs[0] {
            Event::Custom(c) => {
                assert_eq!(c.name, "acp.session_update");
                let serialized = c.value.to_string();
                assert!(
                    serialized.contains("image/png"),
                    "custom event must preserve original image payload, got {serialized}"
                );
            }
            other => panic!("expected Custom for non-text image chunk, got {other:?}"),
        }
        assert!(
            t.flush().is_empty(),
            "no text message was opened, so flush must be a no-op"
        );
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
}
