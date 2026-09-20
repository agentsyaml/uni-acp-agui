use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("ACP error: {0}")]
    Acp(#[from] agent_client_protocol::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    /// The agent did not acknowledge `session/cancel` within the configured
    /// grace window. The ACP session state is unknowable afterward; the
    /// session actor marks itself unusable so the handle is evicted rather
    /// than reused.
    #[error("cancel grace expired after {0:?}: agent did not acknowledge session/cancel")]
    CancelGraceExpired(Duration),

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
    fn cancel_grace_expired_message_names_the_condition() {
        let err = BridgeError::CancelGraceExpired(Duration::from_millis(750));
        let msg = format!("{err}");
        assert!(msg.contains("cancel grace"), "got: {msg}");
        // Downstream (server/handler.rs) maps uncoded errors to a generic
        // RUN_ERROR with this Display text, so it must stay readable.
        assert!(msg.contains("session/cancel"), "got: {msg}");
    }

    #[test]
    fn timeout_renders_with_duration() {
        let err = BridgeError::Timeout(Duration::from_millis(1500));
        let msg = format!("{err}");
        assert!(msg.contains("1.5s"), "got: {msg}");
    }
}
