//! Shared helpers for the bridge's integration tests.
//!
//! Kept intentionally small: SSE event splitting, a `RunAgentInput` builder,
//! a oneshot router driver, and a `PolicySpy` that records every call to
//! [`agui_acp_bridge_core::PermissionPolicy::decide`].
//!
//! Each integration test file (e.g. `bridge_mock_agent.rs`,
//! `http_sse_roundtrip.rs`) compiles its own copy of this module — Rust
//! integration tests are independent crates — so any helper that one file
//! consumes but the other doesn't will be flagged as dead. We blanket-allow
//! the warnings here rather than annotating every item.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use agent_client_protocol::schema::v1::{PermissionOptionId, RequestPermissionRequest};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, PermissionDecision, PermissionPolicy, build_router,
};
use agui_rs_core::types::{Message, RunAgentInput, UserMessage, UserMessageContent};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode, header::CONTENT_TYPE};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Build a minimal valid `RunAgentInput` carrying one user-text message.
pub fn user_input(thread_id: &str, run_id: &str, text: &str) -> RunAgentInput {
    let mut input = RunAgentInput::new(thread_id, run_id);
    input.messages.push(Message::User(UserMessage {
        id: "msg-1".into(),
        content: UserMessageContent::Text(text.into()),
        name: None,
        encrypted_value: None,
    }));
    input
}

/// Drive `state` via `tower::oneshot` and return `(status, full_body_string)`.
///
/// Use this for scenarios that don't need real socket-level semantics.
/// `oneshot` buffers the full body, so it cannot exercise client-disconnect
/// paths — use [`with_bound_server`] for that.
pub async fn collect_sse_body(state: BridgeAppState, input: RunAgentInput) -> (StatusCode, String) {
    let app = build_router(state);
    let body = serde_json::to_vec(&input).expect("serialize input");
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        app.oneshot(
            HttpRequest::post("/")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        ),
    )
    .await
    .expect("router deadlocked")
    .expect("router error");

    let status = response.status();
    let bytes = tokio::time::timeout(Duration::from_secs(10), response.into_body().collect())
        .await
        .expect("body collect deadlocked")
        .expect("body collect failed")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Split an SSE response body into the ordered list of `event-name` strings.
///
/// AG-UI events appear as `data: {"type":"X", ...}` lines. We extract the
/// `"type"` field by simple string scan to avoid a serde_json round-trip per
/// frame (and to keep the helper robust to schema additions).
pub fn extract_event_types(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in body.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim_start();
        let Some(idx) = payload.find("\"type\":\"") else {
            continue;
        };
        let rest = &payload[idx + "\"type\":\"".len()..];
        if let Some(end) = rest.find('"') {
            out.push(rest[..end].to_string());
        }
    }
    out
}

/// Count occurrences of a specific event type in an SSE body.
pub fn count_events(body: &str, event_type: &str) -> usize {
    extract_event_types(body)
        .iter()
        .filter(|t| *t == event_type)
        .count()
}

/// A `PermissionPolicy` that records every `decide` call. Wrap any inner
/// policy to verify whether the bridge actually consults policies in a
/// given scenario. The audit asserts it does NOT.
#[derive(Debug)]
pub struct PolicySpy {
    inner: Arc<dyn PermissionPolicy>,
    call_count: Arc<AtomicUsize>,
    invoked: Arc<AtomicBool>,
}

impl PolicySpy {
    pub fn new(inner: Arc<dyn PermissionPolicy>) -> Self {
        Self {
            inner,
            call_count: Arc::new(AtomicUsize::new(0)),
            invoked: Arc::new(AtomicBool::new(false)),
        }
    }

    /// `true` iff `decide` has been called at least once.
    pub fn was_invoked(&self) -> bool {
        self.invoked.load(Ordering::SeqCst)
    }

    /// Number of `decide` calls since the spy was created.
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PermissionPolicy for PolicySpy {
    async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.invoked.store(true, Ordering::SeqCst);
        self.inner.decide(request).await
    }
}

/// A trivial allow-all policy used as `PolicySpy`'s inner for tests that just
/// want to detect bridge invocation without altering decision semantics.
#[derive(Debug, Default)]
pub struct AllowAlwaysFirstOption;

#[async_trait]
impl PermissionPolicy for AllowAlwaysFirstOption {
    async fn decide(&self, request: &RequestPermissionRequest) -> PermissionDecision {
        let id = request
            .options
            .first()
            .map(|o| o.option_id.clone())
            .unwrap_or_else(|| PermissionOptionId::new("allow"));
        PermissionDecision::Allow { option_id: id }
    }
}

/// Helper builder constructing a `BridgeAppState` from any `AcpClient` plus
/// a default cwd of `/`.
pub fn state_with_client(client: Arc<dyn AcpClient>) -> BridgeAppState {
    BridgeAppState::new(client, PathBuf::from("/"))
}

/// Helper constructing a `BridgeAppState` from any `AcpClient` plus a
/// pre-built `Arc<PolicySpy>` (so the test can read counters after the run).
pub fn state_with_policy(client: Arc<dyn AcpClient>, policy: Arc<PolicySpy>) -> BridgeAppState {
    BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(policy as Arc<dyn PermissionPolicy>)
        .build()
}
