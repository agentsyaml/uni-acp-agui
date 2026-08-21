use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
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

    #[error("unsupported ACP protocol version: agent negotiated {actual}, expected {expected}")]
    ProtocolVersionMismatch {
        expected: ProtocolVersion,
        actual: ProtocolVersion,
    },

    #[error("session closed")]
    SessionClosed,

    /// The agent does not advertise the capability required for this
    /// operation (e.g. `session/list` or `session/load`). Surfaced by the
    /// HTTP layer as `501 Not Implemented` so frontends can hide the
    /// corresponding UI.
    #[error("agent does not support: {0}")]
    Unsupported(String),

    #[error("ACP resume unsupported: {0}")]
    ResumeUnsupported(String),

    #[error("ACP resume failed: {0}")]
    ResumeFailed(String),

    #[error("prompt turn queue is full (limit {max_queued_turns})")]
    QueueCapacity { max_queued_turns: usize },
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
