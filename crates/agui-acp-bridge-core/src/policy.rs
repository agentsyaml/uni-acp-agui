use std::fmt::Debug;

use agent_client_protocol::schema::v1::{PermissionOptionId, RequestPermissionRequest};
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
}
