use agent_client_protocol::schema::v1::{
    RequestPermissionRequest, SessionConfigOption, SessionUpdate, StopReason,
};
use serde_json::Value;

/// ACP-protocol view of a session's mode / model offering, surfaced to the
/// AG-UI client via [`BridgeStreamItem::SessionInit`] so frontends can render
/// pickers without speaking ACP themselves.
///
/// All fields are protocol-typed mirrors of ACP v1 mode and model config
/// offerings, with `Arc<str>` flattened to plain `String` to keep them
/// serde-serializable into AG-UI CUSTOM event payloads.
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
/// `agent_client_protocol::schema::v1::SessionInfo`. Returned by the bridge's
/// `GET /sessions` endpoint (backed by ACP `session/list`) so AG-UI
/// frontends can render a conversation-history list without speaking ACP.
///
/// The bridge holds no persisted history itself: this is a pass-through view
/// of what the agent reports. ACP session IDs are separate from AG-UI
/// `threadId`; the live bridge cache owns the explicit mapping between them.
/// A listed `session_id` is supplied only as
/// `forwardedProps.acpResume.sessionId` for a private load, never as a thread
/// alias. Boolean resume markers and load fallbacks are not supported.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    /// ACP `SessionId`, separate from the AG-UI `threadId`.
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
        config_options: Option<Vec<SessionConfigOption>>,
    },
    /// A permission request that the policy chose to defer to the client.
    Interrupt {
        id: String,
        request: RequestPermissionRequest,
    },
    /// Complete frontend-defined invocation routed through the bridge's
    /// in-process MCP endpoint. The SSE task sends its AG-UI start, optional
    /// complete args, and end events in order before waiting for browser
    /// execution. The matching MCP request remains parked on a oneshot keyed
    /// by `tool_call_id` until `/tool-response` resolves it.
    FrontendToolCall {
        tool_call_id: String,
        tool_name: String,
        arguments: Value,
    },
    /// Legacy terminal signal for frontend tool calls. Canonical
    /// [`BridgeStreamItem::FrontendToolCall`] items already contain their end
    /// event; this variant remains for compatibility with older producers.
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
