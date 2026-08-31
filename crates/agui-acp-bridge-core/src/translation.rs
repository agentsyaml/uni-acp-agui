use std::collections::{HashMap, HashSet};

use crate::message_state::MessageState;
use crate::stream::{SessionModelsInit, SessionModesInit};
use agent_client_protocol::schema::v1::{
    ConfigOptionUpdate, MessageId, SessionUpdate, ToolCallStatus,
};
use agui_rs_core::events::{
    BaseEventFields, CustomEvent, Event, RawEvent, ReasoningMessageContentEvent,
    ReasoningMessageEndEvent, ReasoningMessageRole, ReasoningMessageStartEvent,
    TextMessageStartEvent, ToolCallArgsEvent, ToolCallEndEvent, ToolCallResultEvent,
    ToolCallStartEvent, ToolResultRole,
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
    agent_acp_message_id: Option<String>,
    user: Option<MessageState>,
    user_acp_message_id: Option<String>,
    thought: Option<String>,
    thought_acp_message_id: Option<String>,
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
    /// Last complete plan snapshot, keyed by entry content (ACP exposes no
    /// separate entry id). Only state edges produce AG-UI step events.
    plan_entries: Vec<PlanEntryState>,
    plan_meta: Option<agent_client_protocol::schema::v1::Meta>,
    plan_seen: bool,
    /// Latest raw output for each open tool call. A later ACP snapshot replaces
    /// the earlier value; it is emitted only when the call closes.
    raw_tool_outputs: HashMap<String, serde_json::Value>,
    /// Latest complete raw input for each open tool call. ACP updates replace
    /// this snapshot; AG-UI receives one args payload when the call closes.
    raw_tool_inputs: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
struct PlanEntryState {
    entry: agent_client_protocol::schema::v1::PlanEntry,
    started: bool,
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
            self.raw_tool_inputs.remove(tool_call_id);
            self.raw_tool_outputs.remove(tool_call_id);
            return Vec::new();
        }
        self.raw_tool_inputs.remove(tool_call_id);
        self.raw_tool_outputs.remove(tool_call_id);
        vec![Event::ToolCallEnd(ToolCallEndEvent {
            tool_call_id: tool_call_id.to_string(),
            base: BaseEventFields::default(),
        })]
    }

    pub fn translate(&mut self, update: SessionUpdate) -> Vec<Event> {
        match update {
            SessionUpdate::AgentMessageChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_agent(text, chunk.message_id.as_ref()),
                None => {
                    let mut out = self.close_open_messages();
                    out.push(raw_passthrough(&update));
                    out
                }
            },
            SessionUpdate::UserMessageChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_user(text, chunk.message_id.as_ref()),
                None => {
                    let mut out = self.close_open_messages();
                    out.push(raw_passthrough(&update));
                    out
                }
            },
            SessionUpdate::AgentThoughtChunk(ref chunk) => match extract_text(&chunk.content) {
                Some(text) => self.push_thought(text, chunk.message_id.as_ref()),
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
            // Config snapshots are a known bridge CUSTOM extension consumed by
            // the demo UI; other opaque ACP updates remain lossless RAW events.
            SessionUpdate::ConfigOptionUpdate(ref update) => {
                let mut out = self.close_open_messages();
                out.push(config_option_update_event(update));
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
            SessionUpdate::Plan(ref plan) => self.handle_plan(plan),
            other => {
                let mut out = self.close_open_messages();
                out.push(raw_passthrough(&other));
                out
            }
        }
    }

    pub fn flush(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        out.append(&mut self.close_agent());
        out.append(&mut self.close_user());
        out.append(&mut self.close_thought());
        // Close all open plan steps before clearing their lifecycle state.
        out.extend(self.finish_open_plan_steps());
        self.plan_entries.clear();
        self.plan_meta = None;
        self.plan_seen = false;

        // Close all open tool calls in stable id order. END must precede the
        // single cached RESULT for each call.
        let mut tool_ids: Vec<String> = self.open_tool_calls.drain().collect();
        tool_ids.sort();
        for tc_id in tool_ids {
            if let Some(raw_input) = self.raw_tool_inputs.remove(&tc_id)
                && let Some(event) = tool_call_args_event(&tc_id, &raw_input)
            {
                out.push(event);
            }
            out.push(Event::ToolCallEnd(ToolCallEndEvent {
                tool_call_id: tc_id.clone(),
                base: BaseEventFields::default(),
            }));
            if let Some(raw_output) = self.raw_tool_outputs.remove(&tc_id) {
                out.push(tool_call_result_event(&tc_id, &raw_output));
            }
        }
        self.raw_tool_inputs.clear();
        self.raw_tool_outputs.clear();
        self.suppressed_ids.clear();
        out
    }

    /// Handle a ToolCall session update.
    ///
    /// AG-UI rule: tool call arrival must close any open text message first.
    fn handle_tool_call(&mut self, tc: &agent_client_protocol::schema::v1::ToolCall) -> Vec<Event> {
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

        // ACP raw_input is a complete replacement snapshot, while AG-UI args
        // are append-only deltas. Cache it and emit one payload at close so
        // replacement snapshots can never be concatenated into invalid JSON.
        if let Some(ref raw_input) = tc.raw_input {
            self.raw_tool_inputs
                .insert(tool_call_id.clone(), raw_input.clone());
        }
        if let Some(ref raw_output) = tc.raw_output {
            self.raw_tool_outputs
                .insert(tool_call_id.clone(), raw_output.clone());
        }

        if matches!(
            tc.status,
            ToolCallStatus::Completed | ToolCallStatus::Failed
        ) {
            if let Some(raw_input) = self.raw_tool_inputs.remove(&tool_call_id)
                && let Some(event) = tool_call_args_event(&tool_call_id, &raw_input)
            {
                events.push(event);
            }
            events.push(Event::ToolCallEnd(ToolCallEndEvent {
                tool_call_id: tool_call_id.clone(),
                base: BaseEventFields::default(),
            }));
            if let Some(raw_output) = self.raw_tool_outputs.remove(&tool_call_id) {
                events.push(tool_call_result_event(&tool_call_id, &raw_output));
            }
        } else {
            self.open_tool_calls.insert(tool_call_id);
        }
        events
    }

    /// Handle a ToolCallUpdate session update.
    fn handle_tool_call_update(
        &mut self,
        update: &agent_client_protocol::schema::v1::ToolCallUpdate,
    ) -> Vec<Event> {
        let tool_call_id = update.tool_call_id.0.to_string();

        // Drop updates for ids that originated on a suppressed ToolCall.
        // The bridge's MCP path owns the lifecycle for those.
        if self.suppressed_ids.contains(&tool_call_id) {
            // If this update reports terminal status, also clear the id
            // so the suppression set doesn't grow unbounded over a long
            // session.
            if let Some(ref status) = update.fields.status
                && matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed)
            {
                self.suppressed_ids.remove(&tool_call_id);
            }
            return Vec::new();
        }

        let terminal = matches!(
            update.fields.status,
            Some(ToolCallStatus::Completed | ToolCallStatus::Failed)
        );
        if !self.open_tool_calls.contains(&tool_call_id) {
            self.raw_tool_inputs.remove(&tool_call_id);
            self.raw_tool_outputs.remove(&tool_call_id);
            return Vec::new();
        }

        let mut events = Vec::new();
        if let Some(ref raw_input) = update.fields.raw_input {
            self.raw_tool_inputs
                .insert(tool_call_id.clone(), raw_input.clone());
        }
        if let Some(ref raw_output) = update.fields.raw_output {
            self.raw_tool_outputs
                .insert(tool_call_id.clone(), raw_output.clone());
        }

        if terminal {
            self.open_tool_calls.remove(&tool_call_id);
            if let Some(raw_input) = self.raw_tool_inputs.remove(&tool_call_id)
                && let Some(event) = tool_call_args_event(&tool_call_id, &raw_input)
            {
                events.push(event);
            }
            events.push(Event::ToolCallEnd(ToolCallEndEvent {
                tool_call_id: tool_call_id.clone(),
                base: BaseEventFields::default(),
            }));
            if let Some(raw_output) = self.raw_tool_outputs.remove(&tool_call_id) {
                events.push(tool_call_result_event(&tool_call_id, &raw_output));
            }
        }

        events
    }

    fn handle_plan(&mut self, plan: &agent_client_protocol::schema::v1::Plan) -> Vec<Event> {
        let mut events = self.close_open_messages();
        let identity_safe = {
            let mut seen_names = HashSet::new();
            plan.entries
                .iter()
                .all(|entry| !entry.content.is_empty() && seen_names.insert(entry.content.as_str()))
        };
        if !identity_safe {
            events.extend(self.finish_open_plan_steps());
            events.push(raw_passthrough(&SessionUpdate::Plan(plan.clone())));
            self.plan_entries.clear();
            self.plan_meta = plan.meta.clone();
            self.plan_seen = true;
            return events;
        }

        let structure_same = self.plan_entries.len() == plan.entries.len()
            && self.plan_meta == plan.meta
            && self
                .plan_entries
                .iter()
                .zip(&plan.entries)
                .all(|(previous, current)| {
                    previous.entry.content == current.content
                        && previous.entry.priority == current.priority
                        && previous.entry.meta == current.meta
                });

        let current_names: HashSet<&str> = plan
            .entries
            .iter()
            .map(|entry| entry.content.as_str())
            .collect();
        let mut actions = Vec::new();
        for previous in &self.plan_entries {
            if previous.started && !current_names.contains(previous.entry.content.as_str()) {
                actions.push((previous.entry.content.clone(), false));
            }
        }

        let mut needs_raw = !self.plan_seen || !structure_same;
        let mut next_entries = Vec::with_capacity(plan.entries.len());
        for entry in &plan.entries {
            let previous = self
                .plan_entries
                .iter()
                .find(|previous| previous.entry.content == entry.content);
            let previous_status = previous.map(|previous| &previous.entry.status);
            let was_started = previous.is_some_and(|previous| previous.started);
            let mut started = was_started;

            match (previous_status, &entry.status) {
                (
                    Some(agent_client_protocol::schema::v1::PlanEntryStatus::Pending),
                    agent_client_protocol::schema::v1::PlanEntryStatus::InProgress,
                ) if !was_started => {
                    actions.push((entry.content.clone(), true));
                    started = true;
                }
                (None, agent_client_protocol::schema::v1::PlanEntryStatus::InProgress) => {
                    actions.push((entry.content.clone(), true));
                    started = true;
                }
                (_, agent_client_protocol::schema::v1::PlanEntryStatus::Completed)
                    if was_started =>
                {
                    actions.push((entry.content.clone(), false));
                    started = false;
                }
                (Some(previous), current) if previous == current => {}
                _ => {
                    needs_raw = true;
                    if matches!(
                        &entry.status,
                        agent_client_protocol::schema::v1::PlanEntryStatus::Completed
                    ) {
                        started = false;
                    }
                }
            }
            next_entries.push(PlanEntryState {
                entry: entry.clone(),
                started,
            });
        }

        actions.sort_by(|left, right| left.0.cmp(&right.0));
        for (name, start) in actions {
            if start {
                events.push(Event::StepStarted(agui_rs_core::events::StepStartedEvent {
                    step_name: name,
                    base: BaseEventFields::default(),
                }));
            } else {
                events.push(Event::StepFinished(
                    agui_rs_core::events::StepFinishedEvent {
                        step_name: name,
                        base: BaseEventFields::default(),
                    },
                ));
            }
        }
        if needs_raw {
            events.push(raw_passthrough(&SessionUpdate::Plan(plan.clone())));
        }

        self.plan_entries = next_entries;
        self.plan_meta = plan.meta.clone();
        self.plan_seen = true;
        events
    }

    fn finish_open_plan_steps(&mut self) -> Vec<Event> {
        let mut names: Vec<String> = self
            .plan_entries
            .iter()
            .filter(|entry| entry.started)
            .map(|entry| entry.entry.content.clone())
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|step_name| {
                Event::StepFinished(agui_rs_core::events::StepFinishedEvent {
                    step_name,
                    base: BaseEventFields::default(),
                })
            })
            .collect()
    }

    /// Close all open text messages and thought streams.
    /// Used when a tool call arrives (AG-UI requires messages to be closed first).
    fn close_open_messages(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        out.append(&mut self.close_agent());
        out.append(&mut self.close_user());
        out.append(&mut self.close_thought());
        out
    }

    fn push_agent(&mut self, text: String, message_id: Option<&MessageId>) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        push_text_message(
            &mut self.agent,
            &mut self.agent_acp_message_id,
            text,
            message_id,
            TextMessageRole::Assistant,
        )
    }

    fn push_user(&mut self, text: String, message_id: Option<&MessageId>) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        push_text_message(
            &mut self.user,
            &mut self.user_acp_message_id,
            text,
            message_id,
            TextMessageRole::User,
        )
    }

    fn push_thought(&mut self, text: String, message_id: Option<&MessageId>) -> Vec<Event> {
        if text.is_empty() {
            return vec![];
        }
        let incoming_id = message_id.map(|id| id.0.to_string());
        let needs_boundary = incoming_id.as_deref().is_some_and(|id| {
            self.thought.is_some() && self.thought_acp_message_id.as_deref() != Some(id)
        });
        let mut events = if needs_boundary {
            self.close_thought()
        } else {
            Vec::new()
        };
        let id = match &self.thought {
            Some(existing) => existing.clone(),
            None => {
                let new_id = incoming_id
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                events.push(Event::ReasoningMessageStart(ReasoningMessageStartEvent {
                    message_id: new_id.clone(),
                    role: ReasoningMessageRole::Reasoning,
                    base: BaseEventFields::default(),
                }));
                self.thought = Some(new_id.clone());
                self.thought_acp_message_id = incoming_id;
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

    fn close_agent(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut state) = self.agent.take() {
            out.append(&mut close_text(&mut state));
        }
        self.agent_acp_message_id = None;
        out
    }

    fn close_user(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(mut state) = self.user.take() {
            out.append(&mut close_text(&mut state));
        }
        self.user_acp_message_id = None;
        out
    }

    fn close_thought(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(id) = self.thought.take() {
            out.push(Event::ReasoningMessageEnd(ReasoningMessageEndEvent {
                message_id: id,
                base: BaseEventFields::default(),
            }));
        }
        self.thought_acp_message_id = None;
        out
    }
}

fn extract_text(content: &agent_client_protocol::schema::v1::ContentBlock) -> Option<String> {
    use agent_client_protocol::schema::v1::ContentBlock;
    match content {
        ContentBlock::Text(t) => Some(t.text.clone()),
        _ => None,
    }
}

fn push_text_message(
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
    Event::Raw(RawEvent {
        event: value,
        source: Some("acp".to_string()),
        base: BaseEventFields::default(),
    })
}

fn config_option_update_event(update: &ConfigOptionUpdate) -> Event {
    let value = serde_json::to_value(SessionUpdate::ConfigOptionUpdate(update.clone()))
        .unwrap_or_else(|_| json!({"error": "serialize_failed"}));
    Event::Custom(CustomEvent {
        name: "acp.session_update".to_string(),
        value,
        base: BaseEventFields::default(),
    })
}

fn tool_call_args_event(tool_call_id: &str, raw_input: &serde_json::Value) -> Option<Event> {
    let args_json = serde_json::to_string(raw_input).unwrap_or_default();
    (!args_json.is_empty() && args_json != "null").then(|| {
        Event::ToolCallArgs(ToolCallArgsEvent {
            tool_call_id: tool_call_id.to_string(),
            delta: args_json,
            base: BaseEventFields::default(),
        })
    })
}

fn tool_call_result_event(tool_call_id: &str, raw_output: &serde_json::Value) -> Event {
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

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        ContentBlock, ContentChunk, ImageContent, Plan, PlanEntry, PlanEntryPriority,
        PlanEntryStatus, TextContent, ToolCall, ToolCallId, ToolCallUpdate, ToolCallUpdateFields,
    };

    fn chunk(text: &str) -> ContentChunk {
        ContentChunk::new(ContentBlock::Text(TextContent::new(text)))
    }

    fn chunk_with_id(text: &str, message_id: &str) -> ContentChunk {
        chunk(text).message_id(message_id)
    }

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
    fn plan_with_in_progress_emits_step_started() {
        let pending = Plan::new(vec![
            PlanEntry::new(
                "step one",
                PlanEntryPriority::High,
                PlanEntryStatus::Pending,
            ),
            PlanEntry::new(
                "step two",
                PlanEntryPriority::Medium,
                PlanEntryStatus::Pending,
            ),
        ]);
        let mut t = Translator::new();
        assert!(matches!(
            t.translate(SessionUpdate::Plan(pending)).as_slice(),
            [Event::Raw(_)]
        ));

        let in_progress = Plan::new(vec![
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
        let evs = t.translate(SessionUpdate::Plan(in_progress));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            Event::StepStarted(s) => assert_eq!(s.step_name, "step one"),
            other => panic!("expected StepStarted, got {other:?}"),
        }
    }

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
            [Event::ToolCallStart(_), Event::ToolCallArgs(args)]
                if args.delta == r#"{"name":"world"}"#
        ));
    }

    #[test]
    fn repeated_plan_snapshots_emit_no_duplicate_step_events() {
        let mut t = Translator::new();
        let pending = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )]);
        let in_progress = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::InProgress,
        )]);
        let completed = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Completed,
        )]);

        let _ = t.translate(SessionUpdate::Plan(pending));
        assert!(matches!(
            t.translate(SessionUpdate::Plan(in_progress.clone()))
                .as_slice(),
            [Event::StepStarted(_)]
        ));
        assert!(
            t.translate(SessionUpdate::Plan(in_progress))
                .iter()
                .all(|event| !matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
        );
        assert!(matches!(
            t.translate(SessionUpdate::Plan(completed.clone()))
                .as_slice(),
            [Event::StepFinished(_)]
        ));
        assert!(
            t.translate(SessionUpdate::Plan(completed))
                .iter()
                .all(|event| !matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
        );
    }

    #[test]
    fn first_completed_plan_entry_is_raw_but_in_progress_starts_and_finishes() {
        let mut completed_translator = Translator::new();
        let completed = Plan::new(vec![PlanEntry::new(
            "already done",
            PlanEntryPriority::High,
            PlanEntryStatus::Completed,
        )]);
        let completed_events = completed_translator.translate(SessionUpdate::Plan(completed));
        assert!(matches!(completed_events.as_slice(), [Event::Raw(_)]));
        assert!(
            !completed_events
                .iter()
                .any(|event| matches!(event, Event::StepFinished(_)))
        );

        let mut in_progress_translator = Translator::new();
        let in_progress = Plan::new(vec![PlanEntry::new(
            "already running",
            PlanEntryPriority::High,
            PlanEntryStatus::InProgress,
        )]);
        let in_progress_events = in_progress_translator.translate(SessionUpdate::Plan(in_progress));
        assert!(matches!(
            in_progress_events.as_slice(),
            [Event::StepStarted(_), Event::Raw(_)]
        ));

        let completed_after_unpaired = Plan::new(vec![PlanEntry::new(
            "already running",
            PlanEntryPriority::High,
            PlanEntryStatus::Completed,
        )]);
        let completion_events =
            in_progress_translator.translate(SessionUpdate::Plan(completed_after_unpaired));
        assert!(matches!(
            completion_events.as_slice(),
            [Event::StepFinished(_)]
        ));
    }

    #[test]
    fn removed_open_plan_entry_finishes_before_snapshot_raw() {
        let mut t = Translator::new();
        let pending = Plan::new(vec![
            PlanEntry::new("keep", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
            PlanEntry::new(
                "removed",
                PlanEntryPriority::Medium,
                PlanEntryStatus::Pending,
            ),
        ]);
        let active = Plan::new(vec![
            PlanEntry::new(
                "keep",
                PlanEntryPriority::Medium,
                PlanEntryStatus::InProgress,
            ),
            PlanEntry::new(
                "removed",
                PlanEntryPriority::Medium,
                PlanEntryStatus::InProgress,
            ),
        ]);
        let _ = t.translate(SessionUpdate::Plan(pending));
        let _ = t.translate(SessionUpdate::Plan(active));

        let events = t.translate(SessionUpdate::Plan(Plan::new(vec![PlanEntry::new(
            "keep",
            PlanEntryPriority::Medium,
            PlanEntryStatus::InProgress,
        )])));
        assert!(matches!(
            events.as_slice(),
            [Event::StepFinished(_), Event::Raw(_)]
        ));
        match &events[0] {
            Event::StepFinished(step) => assert_eq!(step.step_name, "removed"),
            _ => unreachable!(),
        }
        assert!(matches!(
            t.translate(SessionUpdate::Plan(Plan::new(Vec::new())))
                .as_slice(),
            [Event::StepFinished(_), Event::Raw(_)]
        ));
        assert!(
            t.translate(SessionUpdate::Plan(Plan::new(Vec::new())))
                .is_empty()
        );
    }

    #[test]
    fn duplicate_plan_identity_falls_back_without_steps() {
        let mut t = Translator::new();
        let duplicate = Plan::new(vec![
            PlanEntry::new("same", PlanEntryPriority::High, PlanEntryStatus::Pending),
            PlanEntry::new("same", PlanEntryPriority::Low, PlanEntryStatus::InProgress),
        ]);
        let events = t.translate(SessionUpdate::Plan(duplicate));
        assert!(matches!(events.as_slice(), [Event::Raw(_)]));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::StepStarted(_) | Event::StepFinished(_)))
        );
    }

    #[test]
    fn plan_step_event_order_is_sorted_by_entry_name() {
        let mut t = Translator::new();
        let pending = Plan::new(vec![
            PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
            PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Pending),
        ]);
        let active = Plan::new(vec![
            PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::InProgress),
            PlanEntry::new(
                "alpha",
                PlanEntryPriority::High,
                PlanEntryStatus::InProgress,
            ),
        ]);
        let _ = t.translate(SessionUpdate::Plan(pending));
        let events = t.translate(SessionUpdate::Plan(active));
        let names: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                Event::StepStarted(step) => Some(step.step_name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(names, ["alpha", "zeta"]);

        let completed = Plan::new(vec![
            PlanEntry::new("zeta", PlanEntryPriority::Low, PlanEntryStatus::Completed),
            PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Completed),
        ]);
        let events = t.translate(SessionUpdate::Plan(completed));
        let finished_names: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                Event::StepFinished(step) => Some(step.step_name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(finished_names, ["alpha", "zeta"]);
    }

    #[test]
    fn flush_resets_plan_state_between_runs() {
        let mut t = Translator::new();
        let pending = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )]);
        let in_progress = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::InProgress,
        )]);
        let completed = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Completed,
        )]);

        let _ = t.translate(SessionUpdate::Plan(pending));
        assert!(matches!(
            t.translate(SessionUpdate::Plan(in_progress)).as_slice(),
            [Event::StepStarted(_)]
        ));
        assert!(matches!(t.flush().as_slice(), [Event::StepFinished(_)]));
        assert!(t.flush().is_empty());

        let after_reset = t.translate(SessionUpdate::Plan(completed));
        assert!(matches!(after_reset.as_slice(), [Event::Raw(_)]));
        assert!(
            !after_reset
                .iter()
                .any(|event| matches!(event, Event::StepFinished(_)))
        );
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
    fn plan_snapshot_structure_changes_emit_full_raw_snapshot() {
        let mut t = Translator::new();
        let initial = Plan::new(vec![
            PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
            PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
        ]);
        let _ = t.translate(SessionUpdate::Plan(initial));

        let partial = Plan::new(vec![PlanEntry::new(
            "alpha",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )]);
        assert!(matches!(
            t.translate(SessionUpdate::Plan(partial)).as_slice(),
            [Event::Raw(_)]
        ));

        let priority_changed = Plan::new(vec![PlanEntry::new(
            "alpha",
            PlanEntryPriority::High,
            PlanEntryStatus::Pending,
        )]);
        assert!(matches!(
            t.translate(SessionUpdate::Plan(priority_changed))
                .as_slice(),
            [Event::Raw(_)]
        ));

        let mut meta = serde_json::Map::new();
        meta.insert("source".to_string(), serde_json::json!("changed"));
        let meta_changed = Plan::new(vec![
            PlanEntry::new("alpha", PlanEntryPriority::High, PlanEntryStatus::Pending).meta(meta),
        ]);
        assert!(matches!(
            t.translate(SessionUpdate::Plan(meta_changed)).as_slice(),
            [Event::Raw(_)]
        ));

        let mut reordered_translator = Translator::new();
        let ordered = Plan::new(vec![
            PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
            PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
        ]);
        let _ = reordered_translator.translate(SessionUpdate::Plan(ordered));
        let reordered = Plan::new(vec![
            PlanEntry::new("beta", PlanEntryPriority::Low, PlanEntryStatus::Pending),
            PlanEntry::new("alpha", PlanEntryPriority::Medium, PlanEntryStatus::Pending),
        ]);
        assert!(matches!(
            reordered_translator
                .translate(SessionUpdate::Plan(reordered))
                .as_slice(),
            [Event::Raw(_)]
        ));
    }

    #[test]
    fn plan_top_level_meta_change_emits_raw_once() {
        let mut t = Translator::new();
        let entries = vec![PlanEntry::new(
            "same entry",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )];

        assert!(matches!(
            t.translate(SessionUpdate::Plan(Plan::new(entries.clone())))
                .as_slice(),
            [Event::Raw(_)]
        ));
        assert!(
            t.translate(SessionUpdate::Plan(Plan::new(entries.clone())))
                .is_empty()
        );

        let mut meta = serde_json::Map::new();
        meta.insert("source".to_string(), serde_json::json!("changed"));
        let changed = Plan::new(entries.clone()).meta(meta.clone());
        assert!(matches!(
            t.translate(SessionUpdate::Plan(changed)).as_slice(),
            [Event::Raw(_)]
        ));
        assert!(
            t.translate(SessionUpdate::Plan(Plan::new(entries).meta(meta)))
                .is_empty()
        );
    }

    #[test]
    fn plan_all_pending_falls_through_as_raw() {
        let plan = Plan::new(vec![PlanEntry::new(
            "step",
            PlanEntryPriority::Medium,
            PlanEntryStatus::Pending,
        )]);
        let mut t = Translator::new();
        let evs = t.translate(SessionUpdate::Plan(plan));
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], Event::Raw(_)));
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
    fn flush_on_empty_translator_is_noop() {
        let mut t = Translator::new();
        assert!(t.flush().is_empty());
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
            agent_client_protocol::schema::v1::SessionConfigOption::boolean(
                "enabled", "Enabled", true,
            ),
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
