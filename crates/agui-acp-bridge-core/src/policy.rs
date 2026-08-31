use std::fmt::Debug;

use agent_client_protocol::schema::v1::{
    FileSystemCapabilities, PermissionOptionId, RequestPermissionRequest,
};
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow {
        option_id: PermissionOptionId,
    },
    Deny,
    /// Defer to the AG-UI client by emitting a custom event and awaiting a
    /// resume call. Carries the bridge-assigned interrupt id.
    Defer {
        interrupt_id: String,
    },
}

#[async_trait]
pub trait PermissionPolicy: Send + Sync + Debug + 'static {
    async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision;

    /// File operations the live session may expose to the agent.
    ///
    /// Filesystem access is opt-in and both operations are disabled unless a
    /// policy explicitly enables them. Existing policy implementations remain
    /// source-compatible through this default.
    fn filesystem_capabilities(&self) -> FileSystemCapabilities {
        FileSystemCapabilities::default()
    }

    /// Whether the live session may expose the ACP `terminal/*` methods.
    ///
    /// Terminal access is opt-in. Existing policies remain terminal-disabled
    /// through this default.
    fn terminal_capability(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trait_object_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn PermissionPolicy>();
    }

    #[test]
    fn decision_defer_roundtrips_through_match() {
        let d = PermissionDecision::Defer {
            interrupt_id: "abc".into(),
        };
        match d {
            PermissionDecision::Defer { interrupt_id } => assert_eq!(interrupt_id, "abc"),
            _ => panic!("expected Defer"),
        }
    }

    #[derive(Debug)]
    struct DefaultPolicy;

    #[async_trait]
    impl PermissionPolicy for DefaultPolicy {
        async fn decide(&self, _request: &RequestPermissionRequest) -> PermissionDecision {
            PermissionDecision::Deny
        }
    }

    #[test]
    fn filesystem_capabilities_default_to_disabled() {
        assert_eq!(
            DefaultPolicy.filesystem_capabilities(),
            FileSystemCapabilities::default()
        );
    }

    #[test]
    fn terminal_capability_defaults_to_disabled() {
        assert!(!DefaultPolicy.terminal_capability());
    }

    #[derive(Debug)]
    struct ReadOnlyPolicy;

    #[async_trait]
    impl PermissionPolicy for ReadOnlyPolicy {
        async fn decide(&self, _request: &RequestPermissionRequest) -> PermissionDecision {
            PermissionDecision::Deny
        }

        fn filesystem_capabilities(&self) -> FileSystemCapabilities {
            FileSystemCapabilities::new().read_text_file(true)
        }
    }

    #[derive(Debug)]
    struct WriteOnlyPolicy;

    #[async_trait]
    impl PermissionPolicy for WriteOnlyPolicy {
        async fn decide(&self, _request: &RequestPermissionRequest) -> PermissionDecision {
            PermissionDecision::Deny
        }

        fn filesystem_capabilities(&self) -> FileSystemCapabilities {
            FileSystemCapabilities::new().write_text_file(true)
        }
    }

    #[test]
    fn filesystem_capabilities_are_independent() {
        let read = ReadOnlyPolicy.filesystem_capabilities();
        assert!(read.read_text_file);
        assert!(!read.write_text_file);

        let write = WriteOnlyPolicy.filesystem_capabilities();
        assert!(!write.read_text_file);
        assert!(write.write_text_file);
    }
}
