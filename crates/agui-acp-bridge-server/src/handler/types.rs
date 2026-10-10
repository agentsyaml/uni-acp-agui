use super::*;

/// Outcome of resolving a deferred permission request.
///
/// Surfaced through `BridgeAppState::resolve_permission` so HTTP handlers can
/// distinguish "no such interrupt" (404) from "invalid option" (422) from
/// "ok" (200) without losing the pending entry on validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Permission was found, validated, and delivered to the session actor.
    Resolved,
    /// The supplied `option_id` is not one of the choices the agent offered.
    /// The pending request stays in the map so the caller can retry.
    InvalidOption,
    /// No pending permission with that id (already resolved, timed out, or
    /// never existed).
    NotFound,
}

/// Outcome of the server-facing session setting operations. The HTTP routes
/// map these to the status codes documented on the route itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetSessionStatus {
    /// No session for the supplied thread id (caller must create one first
    /// by issuing a normal AG-UI run).
    NotFound,
    /// Another lifecycle operation owns the thread claim.
    Busy,
    /// Agent rejected the request, or no matching discovered config option was
    /// available for a mode/model alias.
    Acp(String),
    /// Agent did not respond within `BridgeConfig.set_session_timeout`.
    /// The session is left intact and the caller can retry.
    Timeout,
    /// Underlying session actor terminated; cache entry is evicted.
    SessionClosed,
}

/// JSON body for the server-facing config-option route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetSessionConfigOptionBody {
    pub thread_id: String,
    pub config_id: String,
    #[serde(deserialize_with = "deserialize_string_or_boolean")]
    pub value: serde_json::Value,
}

pub(super) fn deserialize_string_or_boolean<'de, D>(
    deserializer: D,
) -> Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
    match value {
        serde_json::Value::Bool(_) | serde_json::Value::String(_) => Ok(value),
        _ => Err(serde::de::Error::custom(
            "value must be a JSON string or boolean",
        )),
    }
}

pub(super) fn typed_config_option_value(value: serde_json::Value) -> SessionConfigOptionValue {
    match value {
        serde_json::Value::Bool(value) => SessionConfigOptionValue::boolean(value),
        serde_json::Value::String(value) => SessionConfigOptionValue::value_id(value),
        _ => unreachable!("config option input was validated during deserialization"),
    }
}

pub(super) fn config_option_value_type_matches(
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> bool {
    matches!(
        (&option.kind, value),
        (
            SessionConfigKind::Boolean(_),
            SessionConfigOptionValue::Boolean { .. }
        ) | (
            SessionConfigKind::Select(_),
            SessionConfigOptionValue::ValueId { .. }
        )
    )
}

pub(super) fn config_option_value_matches(
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> bool {
    if !config_option_value_type_matches(option, value) {
        return false;
    }
    match (&option.kind, value) {
        (SessionConfigKind::Boolean(_), SessionConfigOptionValue::Boolean { .. }) => true,
        (SessionConfigKind::Select(select), SessionConfigOptionValue::ValueId { value }) => {
            match &select.options {
                SessionConfigSelectOptions::Ungrouped(options) => options
                    .iter()
                    .any(|option| option.value.0.as_ref() == value.0.as_ref()),
                SessionConfigSelectOptions::Grouped(groups) => groups.iter().any(|group| {
                    group
                        .options
                        .iter()
                        .any(|option| option.value.0.as_ref() == value.0.as_ref())
                }),
                _ => false,
            }
        }
        _ => false,
    }
}

pub(super) fn validate_discovered_config_option(
    config_id: &str,
    option: &SessionConfigOption,
    value: &SessionConfigOptionValue,
) -> Result<(), SetSessionStatus> {
    if !config_option_value_type_matches(option, value) {
        Err(SetSessionStatus::Acp(format!(
            "config option `{config_id}` does not accept this value type"
        )))
    } else if config_option_value_matches(option, value) {
        Ok(())
    } else {
        Err(SetSessionStatus::Acp(format!(
            "config option `{config_id}` does not advertise this value"
        )))
    }
}

pub(super) fn validate_legacy_mode(
    modes: Option<&agui_acp_bridge_core::SessionModesInit>,
    mode_id: &str,
) -> Result<(), SetSessionStatus> {
    let Some(modes) = modes else {
        return Err(SetSessionStatus::Acp(
            "agent did not advertise a legacy mode capability".into(),
        ));
    };
    if !modes.available_modes.iter().any(|mode| mode.id == mode_id) {
        return Err(SetSessionStatus::Acp(format!(
            "mode `{mode_id}` is not advertised"
        )));
    }
    Ok(())
}

/// JSON body for the server-facing cancel route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelSessionBody {
    pub thread_id: String,
}

/// JSON body for the explicit ACP session-close route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CloseSessionBody {
    pub thread_id: String,
}

/// JSON body for the explicit ACP session-delete route.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteSessionBody {
    pub thread_id: String,
}

/// Outcome of closing a cached ACP session. The route maps each variant to a
/// stable HTTP status without collapsing unsupported close into a destructive
/// local drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseSessionStatus {
    /// No cached session exists for the supplied thread id.
    NotFound,
    /// A run, setting, queued turn, or pending frontend/permission operation
    /// prevents a terminal close.
    Busy,
    /// The agent did not advertise `sessionCapabilities.close`.
    Unsupported,
    /// The bounded ACP close request timed out.
    Timeout,
    /// The agent rejected the close request.
    Acp(String),
    /// The local actor was already closed.
    SessionClosed,
}

/// Outcome of deleting a persisted ACP session mapped to an exact AG-UI
/// thread. A missing bridge mapping is a local not-found response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteSessionStatus {
    /// The request did not contain a non-empty AG-UI thread id.
    InvalidInput,
    /// No bridge-owned session is mapped to the supplied AG-UI thread id.
    NotFound,
    /// A run, setting, queued turn, or pending frontend/permission operation
    /// prevents deletion.
    Busy,
    /// The agent did not advertise `sessionCapabilities.delete`.
    Unsupported,
    /// The bounded ACP delete request timed out.
    Timeout,
    /// The agent or transient transport rejected the request.
    Acp(String),
}
