use agent_client_protocol::schema::{RequestPermissionRequest, SessionUpdate, StopReason};
use serde_json::Value;

/// ACP-protocol view of a session's mode / model offering, surfaced to the
/// AG-UI client via [`BridgeStreamItem::SessionInit`] so frontends can render
/// pickers without speaking ACP themselves.
///
/// All fields are protocol-typed mirrors of `SessionModeState` /
/// `SessionModelState` from `agent-client-protocol-schema` 0.12, with `Arc<str>`
/// flattened to plain `String` to keep them serde-serializable into AG-UI
/// CUSTOM event payloads.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ModeOffering {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ModelOffering {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionModesInit {
    pub current_mode_id: String,
    pub available_modes: Vec<ModeOffering>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionModelsInit {
    pub current_model_id: String,
    pub available_models: Vec<ModelOffering>,
}

/// Serializable summary of one persisted ACP session, mirroring
/// `agent_client_protocol::schema::SessionInfo`. Returned by the bridge's
/// `GET /sessions` endpoint (backed by ACP `session/list`) so AG-UI
/// frontends can render a conversation-history list without speaking ACP.
///
/// The bridge holds **no** session state itself: this is a pass-through view
/// of what the agent reports. `session_id` doubles as the AG-UI `threadId`
/// the frontend should use to resume the conversation.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    /// ACP `SessionId`. Use this as the AG-UI `threadId` to resume.
    pub session_id: String,
    /// Absolute working directory the session was created in.
    pub cwd: String,
    /// Human-readable title the agent assigned, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// ISO-8601 timestamp of last activity, if the agent reports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Internal item the session actor and the MCP endpoint pass to the
/// `BridgeHandler`'s SSE task. These are translated into AG-UI events
/// (`TOOL_CALL_*`, `STATE_SNAPSHOT`, `TEXT_MESSAGE_*`, lifecycle, …)
/// before being flushed to the client.
#[derive(Debug)]
pub enum BridgeStreamItem {
    /// A `session/update` notification streamed by the agent.
    Update(SessionUpdate),
    /// Initial offering of modes and (optionally) models advertised by the
    /// agent in `NewSessionResponse`. Emitted **once per session**, ahead of
    /// the first `Update`. The translator surfaces this as a CUSTOM
    /// `agent:session_init` event so frontends can populate pickers.
    SessionInit {
        modes: Option<SessionModesInit>,
        models: Option<SessionModelsInit>,
    },
    /// A permission request that the policy chose to defer to the client.
    Interrupt {
        id: String,
        request: RequestPermissionRequest,
    },
    /// Start of a frontend-defined tool call routed through the bridge's
    /// in-process MCP endpoint. The SSE task translates this into AG-UI
    /// `TOOL_CALL_START` / `TOOL_CALL_ARGS`. The matching MCP request is
    /// parked on a oneshot keyed by `tool_call_id`; the `/tool-response`
    /// endpoint resolves it once the frontend posts back, at which point
    /// the MCP endpoint emits a [`BridgeStreamItem::FrontendToolEnd`].
    FrontendToolCall {
        tool_call_id: String,
        tool_name: String,
        arguments: Value,
    },
    /// Terminal half of a frontend tool call. Translated into
    /// `TOOL_CALL_END`. Emitted by the MCP endpoint after the frontend
    /// posts a result back, regardless of success — failures are
    /// reflected in the MCP envelope returned to the agent, not in this
    /// signal.
    FrontendToolEnd { tool_call_id: String },
    /// Terminal error for the run; converts into AG-UI `RUN_ERROR`.
    RunError { message: String },
    /// Terminal success-or-cancel for the run; converts into `RUN_FINISHED`.
    Finished { stop_reason: StopReason },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_stream_item_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<BridgeStreamItem>();
    }

    #[test]
    fn run_error_variant_constructs_with_message() {
        let item = BridgeStreamItem::RunError {
            message: "boom".into(),
        };
        match item {
            BridgeStreamItem::RunError { message } => assert_eq!(message, "boom"),
            _ => panic!("expected RunError"),
        }
    }

    #[test]
    fn frontend_tool_call_carries_args() {
        let item = BridgeStreamItem::FrontendToolCall {
            tool_call_id: "tc-1".into(),
            tool_name: "show_alert".into(),
            arguments: serde_json::json!({"text": "hi"}),
        };
        match item {
            BridgeStreamItem::FrontendToolCall {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                assert_eq!(tool_call_id, "tc-1");
                assert_eq!(tool_name, "show_alert");
                assert_eq!(arguments["text"], "hi");
            }
            _ => panic!("expected FrontendToolCall"),
        }
    }

    #[test]
    fn frontend_tool_end_carries_id() {
        let item = BridgeStreamItem::FrontendToolEnd {
            tool_call_id: "tc-2".into(),
        };
        match item {
            BridgeStreamItem::FrontendToolEnd { tool_call_id } => {
                assert_eq!(tool_call_id, "tc-2");
            }
            _ => panic!("expected FrontendToolEnd"),
        }
    }
}
