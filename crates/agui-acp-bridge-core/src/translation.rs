use std::collections::{HashMap, HashSet, VecDeque};

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

/// Cap on concurrently open tool calls the translator tracks for one AG-UI
/// run. Evicts the oldest when exceeded.
const MAX_OPEN_TOOL_CALLS: usize = 256;
/// Cap on tracked suppressed tool-call ids (see `suppressed_ids`).
const MAX_SUPPRESSED_IDS: usize = 256;

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
    // ponytail: insertion order of open tool calls, only used to evict the
    // OLDEST entry when MAX_OPEN_TOOL_CALLS is exceeded. Upgrade to a
    // proper LRU (e.g. `lru` crate) if access-order eviction ever matters.
    open_tool_call_order: VecDeque<String>,
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
    suppressed_ids: VecDeque<String>,
    /// Last complete plan snapshot, keyed by entry content (ACP exposes no
    /// separate entry id). Only state edges produce AG-UI step events.
    plan_entries: Vec<PlanEntryState>,
    plan_meta: Option<agent_client_protocol::schema::v1::Meta>,
    plan_seen: bool,
    /// Latest raw output for each open tool call. A later ACP snapshot replaces
    /// the earlier value; it is emitted only when the call closes.
    ///
    /// Bounded by [`MAX_OPEN_TOOL_CALLS`]: an agent that streams tool calls
    /// with fresh ids and never closes them cannot grow this map for the
    /// lifetime of the run. Oldest entries are evicted along with their
    /// open-call tracking.
    raw_tool_outputs: HashMap<String, serde_json::Value>,
    /// Latest complete raw input for each open tool call. ACP updates replace
    /// this snapshot; AG-UI receives one args payload when the call closes.
    ///
    /// Bounded by [`MAX_OPEN_TOOL_CALLS`] like `raw_tool_outputs`.
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
        let mut tool_ids: Vec<String> = self.open_tool_call_order.drain(..).collect();
        tool_ids.sort();
        for tc_id in tool_ids {
            out.extend(self.close_tool_call(&tc_id));
        }
        self.open_tool_calls.clear();
        self.raw_tool_inputs.clear();
        self.raw_tool_outputs.clear();
        self.suppressed_ids.clear();
        out
    }
}

mod eventhelpers;
mod plans;
mod text;
mod tools;
use eventhelpers::*;

#[cfg(test)]
mod tests;
