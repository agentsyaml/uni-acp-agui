use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("ACP error: {0}")]
    Acp(#[from] agent_client_protocol::Error),

    #[error("AG-UI error: {0}")]
    AgUi(#[from] agui_rs_core::AgUiError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    #[error("session closed")]
    SessionClosed,

    /// The agent does not advertise the capability required for this
    /// operation (e.g. `session/list` or `session/load`). Surfaced by the
    /// HTTP layer as `501 Not Implemented` so frontends can hide the
    /// corresponding UI.
    #[error("agent does not support: {0}")]
    Unsupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_agui_preserves_message() {
        let inner = agui_rs_core::AgUiError::Validation("boom".into());
        let bridge: BridgeError = inner.into();
        assert!(format!("{bridge}").contains("boom"));
    }

    #[test]
    fn timeout_renders_with_duration() {
        let err = BridgeError::Timeout(Duration::from_millis(1500));
        let msg = format!("{err}");
        assert!(msg.contains("1.5s"), "got: {msg}");
    }
}
