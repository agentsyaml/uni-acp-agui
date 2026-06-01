#![doc = "Built-in PermissionPolicy implementations for the AG-UI to ACP bridge."]

use agent_client_protocol::schema::{
    PermissionOptionId, PermissionOptionKind, RequestPermissionRequest,
};
use agui_acp_bridge_core::{PermissionDecision, PermissionPolicy};
use async_trait::async_trait;

fn pick_allow(req: &RequestPermissionRequest) -> Option<PermissionOptionId> {
    req.options
        .iter()
        .find(|o| {
            matches!(
                o.kind,
                PermissionOptionKind::AllowOnce | PermissionOptionKind::AllowAlways
            )
        })
        .map(|o| o.option_id.clone())
}

#[derive(Debug, Default)]
pub struct AutoAllow;

#[async_trait]
impl PermissionPolicy for AutoAllow {
    async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
        match pick_allow(request) {
            Some(option_id) => PermissionDecision::Allow { option_id },
            None => PermissionDecision::Deny,
        }
    }
}

#[derive(Debug, Default)]
pub struct AutoDeny;

#[async_trait]
impl PermissionPolicy for AutoDeny {
    async fn decide(&self, _request: &RequestPermissionRequest) -> PermissionDecision {
        PermissionDecision::Deny
    }
}

/// Allows tool calls whose `title` is in the configured set; denies all others.
/// Title is the human-readable display name set by the agent in
/// `SessionUpdate::ToolCall { title, .. }`.
#[derive(Debug, Clone)]
pub struct Allowlist {
    titles: std::collections::HashSet<String>,
}

impl Allowlist {
    pub fn new<I, S>(titles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            titles: titles.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl PermissionPolicy for Allowlist {
    async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
        let title_match = request
            .tool_call
            .fields
            .title
            .as_deref()
            .is_some_and(|t| self.titles.contains(t));
        if !title_match {
            return PermissionDecision::Deny;
        }
        match pick_allow(request) {
            Some(option_id) => PermissionDecision::Allow { option_id },
            None => PermissionDecision::Deny,
        }
    }
}

/// Defers every decision to the AG-UI client by minting a fresh interrupt id
/// the bridge will surface as a `CustomEvent { name: "acp.permission_request" }`.
#[derive(Debug, Default)]
pub struct InterruptViaAgUiEvent;

#[async_trait]
impl PermissionPolicy for InterruptViaAgUiEvent {
    async fn decide(&self, _request: &RequestPermissionRequest) -> PermissionDecision {
        PermissionDecision::Defer {
            interrupt_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::{PermissionOption, ToolCallUpdate, ToolCallUpdateFields};

    fn req_with(title: Option<&str>, options: Vec<PermissionOption>) -> RequestPermissionRequest {
        let mut fields = ToolCallUpdateFields::new();
        if let Some(t) = title {
            fields = fields.title(t.to_string());
        }
        RequestPermissionRequest::new("s1", ToolCallUpdate::new("tc1", fields), options)
    }

    fn opt(id: &str, kind: PermissionOptionKind) -> PermissionOption {
        PermissionOption::new(PermissionOptionId::new(id), id.to_string(), kind)
    }

    #[tokio::test]
    async fn auto_allow_picks_allow_once() {
        let r = req_with(
            None,
            vec![
                opt("deny", PermissionOptionKind::RejectOnce),
                opt("yes", PermissionOptionKind::AllowOnce),
            ],
        );
        let d = AutoAllow.decide(&r).await;
        assert_eq!(
            d,
            PermissionDecision::Allow {
                option_id: PermissionOptionId::new("yes")
            }
        );
    }

    #[tokio::test]
    async fn auto_allow_falls_back_to_deny_when_no_allow_option() {
        let r = req_with(None, vec![opt("no", PermissionOptionKind::RejectAlways)]);
        assert_eq!(AutoAllow.decide(&r).await, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn auto_deny_always_denies() {
        let r = req_with(None, vec![opt("yes", PermissionOptionKind::AllowAlways)]);
        assert_eq!(AutoDeny.decide(&r).await, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn allowlist_allows_matching_title() {
        let policy = Allowlist::new(["Read file"]);
        let r = req_with(
            Some("Read file"),
            vec![opt("ok", PermissionOptionKind::AllowOnce)],
        );
        assert_eq!(
            policy.decide(&r).await,
            PermissionDecision::Allow {
                option_id: PermissionOptionId::new("ok")
            }
        );
    }

    #[tokio::test]
    async fn allowlist_denies_non_matching_title() {
        let policy = Allowlist::new(["Read file"]);
        let r = req_with(
            Some("Write file"),
            vec![opt("ok", PermissionOptionKind::AllowOnce)],
        );
        assert_eq!(policy.decide(&r).await, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn allowlist_denies_when_title_missing() {
        let policy = Allowlist::new(["Read file"]);
        let r = req_with(None, vec![opt("ok", PermissionOptionKind::AllowOnce)]);
        assert_eq!(policy.decide(&r).await, PermissionDecision::Deny);
    }

    #[tokio::test]
    async fn interrupt_policy_returns_unique_defer_ids() {
        let p = InterruptViaAgUiEvent;
        let r = req_with(None, vec![]);
        let d1 = p.decide(&r).await;
        let d2 = p.decide(&r).await;
        match (d1, d2) {
            (
                PermissionDecision::Defer { interrupt_id: a },
                PermissionDecision::Defer { interrupt_id: b },
            ) => {
                assert_ne!(a, b);
                assert_eq!(a.len(), 36, "uuid v4 hyphenated len");
            }
            other => panic!("expected two Defers, got {other:?}"),
        }
    }
}
