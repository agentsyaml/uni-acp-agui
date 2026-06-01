//! Per-thread registry that bridges AG-UI `useFrontendTool`-style tools into
//! the in-flight ACP session via MCP-over-HTTP.
//!
//! # Why this exists
//!
//! AG-UI lets the frontend expose tools that live in the user's browser
//! (CopilotKit's `useFrontendTool`, `useCopilotAction`, etc.). For the agent's
//! LLM to *decide* to call one of these tools, it needs a tool-list it can
//! reason about. ACP standardises this through `NewSessionRequest.mcp_servers`:
//! the agent connects to the MCP servers we hand it and treats their tools as
//! first-class. So the bridge mounts a tiny in-process MCP HTTP endpoint and
//! advertises **only that URL** to **only the session that owns it**.
//!
//! Nothing global is touched: no `mcp.json`, no `~/.config/opencode/...`, no
//! extra port. The MCP endpoint lives on the same axum router as the AG-UI
//! `POST /` route, scoped under a per-thread token in the path.
//!
//! # Per-thread state
//!
//! [`ThreadEntry`] holds:
//! - the latest `tools[]` list (replaced on every `RunAgentInput`);
//! - the active prompt's event sender, used by the MCP handler to stream
//!   `TOOL_CALL_*` AG-UI events back into the SSE response;
//! - a map of pending `tool_call_id → oneshot` so the `/tool-response` route
//!   can resolve calls posted back by the browser.
//!
//! The registry also keeps a global reverse index `tool_call_id → thread_id`
//! so [`FrontendToolRegistry::resolve_anywhere`] resolves in O(1) regardless
//! of how many threads are live. The index is maintained automatically on
//! [`ThreadEntry::register_pending`] / [`ThreadEntry::resolve_pending`] /
//! [`ThreadEntry::drop`], so callers never need to manage it.
//!
//! Dropping a [`ThreadEntry`] (e.g. session reaped) drains all pending
//! oneshots with an error so the MCP handler tasks exit promptly instead of
//! hanging on a closed channel.

use std::sync::{Arc, Mutex, MutexGuard, Weak};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::stream::BridgeStreamItem;

/// One AG-UI tool definition forwarded into the registry.
///
/// Mirrors the relevant fields of `agui_rs_core::types::Tool` — kept local so
/// this module stays a pure transport layer (no AG-UI types in the wire
/// format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendToolDef {
    /// MCP-visible tool name. Must be non-empty and unique across the list.
    pub name: String,
    /// Human-readable description; surfaced to the LLM.
    #[serde(default)]
    pub description: String,
    /// JSON Schema for the tool's parameters.
    #[serde(default = "default_params")]
    pub parameters: Value,
}

fn default_params() -> Value {
    serde_json::json!({"type": "object", "properties": {}})
}

/// Outcome of a frontend tool execution as posted back by the browser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendToolResponse {
    /// Plain-text content. The MCP `tools/call` response wraps this as a
    /// single text content block. If `is_error` is `true`, this is treated
    /// as the error message (still surfaced to the LLM).
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
}

impl FrontendToolResponse {
    #[must_use]
    pub fn ok(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
        }
    }

    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            content: message.into(),
            is_error: true,
        }
    }
}

/// Shared reverse-index handle threaded through every `ThreadEntry` so
/// `register_pending` / `resolve_pending` can keep the global index in
/// sync without holding a strong cycle back to the registry.
#[derive(Debug, Default)]
struct ReverseIndex {
    /// `tool_call_id → thread_id`. Populated on `register_pending`,
    /// removed on `resolve_pending` or `drain_pending`.
    map: DashMap<String, String>,
}

impl ReverseIndex {
    fn lookup(&self, tool_call_id: &str) -> Option<String> {
        self.map.get(tool_call_id).map(|v| v.clone())
    }

    fn insert(&self, tool_call_id: String, thread_id: String) {
        self.map.insert(tool_call_id, thread_id);
    }

    fn remove(&self, tool_call_id: &str) {
        self.map.remove(tool_call_id);
    }
}

/// Per-thread registry entry: tool list + active prompt sender + pending calls.
#[derive(Debug)]
pub struct ThreadEntry {
    thread_id: String,
    tools: Mutex<Vec<FrontendToolDef>>,
    active_sender: Mutex<Option<mpsc::Sender<BridgeStreamItem>>>,
    pending: DashMap<String, oneshot::Sender<FrontendToolResponse>>,
    /// Weak back-reference into the registry's reverse index. `Weak` so a
    /// reaped registry doesn't keep stale entries alive; if upgrade fails
    /// we silently skip the index update — the registry is going away too.
    /// `None` for stand-alone entries created via [`ThreadEntry::orphaned`]
    /// (test fixtures).
    reverse_index: Weak<ReverseIndex>,
}

impl Default for ThreadEntry {
    fn default() -> Self {
        Self {
            thread_id: String::new(),
            tools: Mutex::new(Vec::new()),
            active_sender: Mutex::new(None),
            pending: DashMap::new(),
            reverse_index: Weak::new(),
        }
    }
}

impl ThreadEntry {
    fn new_in_registry(thread_id: String, reverse_index: Weak<ReverseIndex>) -> Self {
        Self {
            thread_id,
            tools: Mutex::new(Vec::new()),
            active_sender: Mutex::new(None),
            pending: DashMap::new(),
            reverse_index,
        }
    }

    /// Build a stand-alone entry not connected to any registry. Useful for
    /// unit tests that exercise `ThreadEntry` semantics in isolation.
    #[doc(hidden)]
    #[must_use]
    pub fn orphaned() -> Self {
        Self::default()
    }

    /// Replace the tool list. Called once per `RunAgentInput`.
    pub fn set_tools(&self, tools: Vec<FrontendToolDef>) {
        *self.lock_tools() = tools;
    }

    /// Snapshot the current tools (for MCP `tools/list`).
    #[must_use]
    pub fn tools(&self) -> Vec<FrontendToolDef> {
        self.lock_tools().clone()
    }

    /// Install / clear the active prompt's event sender.
    ///
    /// The session actor calls this with `Some(tx)` when a prompt begins and
    /// `None` when the prompt's event channel is closed; the MCP handler
    /// reads it to route `FrontendToolCall` items into the live SSE stream.
    pub fn set_active_sender(&self, sender: Option<mpsc::Sender<BridgeStreamItem>>) {
        *self.lock_sender() = sender;
    }

    /// Clear the active sender **only if** it still points at `sender`.
    /// Returns `true` if the slot was ours and was cleared, `false` otherwise.
    ///
    /// This is the safe teardown path for a finishing run: when two runs on
    /// the same `thread_id` overlap (page refresh reusing the thread, a
    /// CopilotKit follow-up run, a reconnect), the newer run installs its own
    /// sender via [`set_active_sender`]. An unconditional clear from the older
    /// run's teardown would then wipe the newer run's sender, stranding any
    /// in-flight MCP `tools/call` (the browser never sees `TOOL_CALL_*`, never
    /// posts back, and the call times out). Comparing channels with
    /// [`mpsc::Sender::same_channel`] ensures a run only ever clears the slot
    /// it actually installed.
    pub fn clear_active_sender_if_same(&self, sender: &mpsc::Sender<BridgeStreamItem>) -> bool {
        let mut guard = self.lock_sender();
        let is_ours = guard.as_ref().is_some_and(|cur| cur.same_channel(sender));
        if is_ours {
            *guard = None;
        }
        is_ours
    }

    /// Take a clone of the current active sender, if any.
    #[must_use]
    pub fn active_sender(&self) -> Option<mpsc::Sender<BridgeStreamItem>> {
        self.lock_sender().clone()
    }

    /// The thread id this entry is keyed by. Empty string for orphaned
    /// entries.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Park an MCP `tools/call` request on a oneshot keyed by `tool_call_id`.
    /// Returns the receiver to await the frontend's response.
    pub fn register_pending(
        &self,
        tool_call_id: String,
    ) -> oneshot::Receiver<FrontendToolResponse> {
        let (tx, rx) = oneshot::channel();
        // If the same id was somehow registered twice (extremely unlikely
        // since we mint UUIDs ourselves), the older Sender just gets
        // dropped and its waiter receives Closed — safer than panicking.
        self.pending.insert(tool_call_id.clone(), tx);
        if let Some(idx) = self.reverse_index.upgrade() {
            idx.insert(tool_call_id, self.thread_id.clone());
        }
        rx
    }

    /// Resolve a parked tool call by id. Returns `true` if a pending entry
    /// existed and was successfully notified.
    pub fn resolve_pending(&self, tool_call_id: &str, response: FrontendToolResponse) -> bool {
        match self.pending.remove(tool_call_id) {
            Some((_, tx)) => {
                if let Some(idx) = self.reverse_index.upgrade() {
                    idx.remove(tool_call_id);
                }
                tx.send(response).is_ok()
            }
            None => false,
        }
    }

    /// Number of pending tool calls. For tests and observability.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Abort every pending tool call with an error, unblocking any MCP
    /// `tools/call` handler currently parked on its oneshot.
    ///
    /// Called when the run that owns this entry's SSE stream ends — most
    /// importantly on **client disconnect**. Without this, a browser that
    /// navigates away (refresh / tab close) while the agent is awaiting a
    /// frontend-tool result leaves the MCP handler parked for the full
    /// `frontend_tool_timeout`, which keeps `active_prompts > 0` and so
    /// prevents the idle reaper from releasing the session. Aborting the
    /// pending calls lets the agent's turn unwind promptly so the session
    /// becomes reapable.
    pub fn abort_pending_calls(&self, reason: &str) {
        self.drain_pending(reason);
    }

    /// Drain every pending request with an error response so MCP handler
    /// tasks awaiting them exit immediately.
    fn drain_pending(&self, reason: &str) {
        let keys: Vec<String> = self.pending.iter().map(|e| e.key().clone()).collect();
        let idx = self.reverse_index.upgrade();
        for k in keys {
            if let Some((_, tx)) = self.pending.remove(&k) {
                if let Some(ref idx) = idx {
                    idx.remove(&k);
                }
                let _ = tx.send(FrontendToolResponse::error(format!(
                    "frontend tool aborted: {reason}"
                )));
            }
        }
    }

    fn lock_tools(&self) -> MutexGuard<'_, Vec<FrontendToolDef>> {
        self.tools.lock().expect("frontend tool list poisoned")
    }

    fn lock_sender(&self) -> MutexGuard<'_, Option<mpsc::Sender<BridgeStreamItem>>> {
        self.active_sender
            .lock()
            .expect("frontend active sender poisoned")
    }
}

impl Drop for ThreadEntry {
    fn drop(&mut self) {
        self.drain_pending("thread entry dropped");
    }
}

/// Shared, cheap-to-clone registry of per-thread frontend-tool state.
#[derive(Debug, Clone, Default)]
pub struct FrontendToolRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    threads: DashMap<String, Arc<ThreadEntry>>,
    reverse_index: Arc<ReverseIndex>,
}

impl FrontendToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get (or lazily create) the per-thread entry.
    pub fn entry(&self, thread_id: &str) -> Arc<ThreadEntry> {
        if let Some(existing) = self.inner.threads.get(thread_id) {
            return existing.clone();
        }
        let weak_index = Arc::downgrade(&self.inner.reverse_index);
        self.inner
            .threads
            .entry(thread_id.to_string())
            .or_insert_with(|| {
                Arc::new(ThreadEntry::new_in_registry(
                    thread_id.to_string(),
                    weak_index,
                ))
            })
            .clone()
    }

    /// Whether a thread entry exists. Used by the MCP route to gate access.
    #[must_use]
    pub fn has(&self, thread_id: &str) -> bool {
        self.inner.threads.contains_key(thread_id)
    }

    /// Resolve a pending call without knowing its thread up-front.
    ///
    /// Uses the registry-wide reverse index for an O(1) lookup keyed by
    /// `tool_call_id`. Returns `true` if a pending entry existed and was
    /// consumed.
    pub fn resolve_anywhere(&self, tool_call_id: &str, response: FrontendToolResponse) -> bool {
        let Some(thread_id) = self.inner.reverse_index.lookup(tool_call_id) else {
            return false;
        };
        let Some(entry) = self.inner.threads.get(&thread_id) else {
            // Thread vanished between index lookup and now (reaped). Clean
            // up the dangling index entry so a retry doesn't keep matching.
            self.inner.reverse_index.remove(tool_call_id);
            return false;
        };
        entry.value().resolve_pending(tool_call_id, response)
    }

    /// Drop a thread entry. Called when its session is reaped.
    pub fn drop_thread(&self, thread_id: &str) {
        if let Some((_, entry)) = self.inner.threads.remove(thread_id) {
            entry.drain_pending("thread closed");
        }
    }

    /// Live thread count (test/observability).
    #[must_use]
    pub fn thread_count(&self) -> usize {
        self.inner.threads.len()
    }

    /// Number of entries in the reverse index. Test-only.
    #[doc(hidden)]
    #[must_use]
    pub fn reverse_index_len(&self) -> usize {
        self.inner.reverse_index.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_creation_is_idempotent() {
        let registry = FrontendToolRegistry::new();
        let a = registry.entry("t1");
        let b = registry.entry("t1");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn set_and_get_tools_round_trip() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        entry.set_tools(vec![FrontendToolDef {
            name: "alert".into(),
            description: "show an alert".into(),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        let got = entry.tools();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "alert");
    }

    #[tokio::test]
    async fn resolve_pending_unblocks_waiter() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        let rx = entry.register_pending("call-1".into());
        assert!(entry.resolve_pending("call-1", FrontendToolResponse::ok("ok")));
        let resp = rx.await.expect("oneshot");
        assert_eq!(resp.content, "ok");
        assert!(!resp.is_error);
    }

    #[tokio::test]
    async fn resolve_anywhere_finds_across_threads() {
        let registry = FrontendToolRegistry::new();
        let _ = registry.entry("ta");
        let entry_b = registry.entry("tb");
        let rx = entry_b.register_pending("call-2".into());
        assert!(registry.resolve_anywhere("call-2", FrontendToolResponse::ok("x")));
        assert_eq!(rx.await.expect("oneshot").content, "x");
    }

    #[tokio::test]
    async fn reverse_index_is_maintained_on_register_and_resolve() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        assert_eq!(registry.reverse_index_len(), 0);

        let rx = entry.register_pending("call-rev".into());
        assert_eq!(registry.reverse_index_len(), 1);

        assert!(registry.resolve_anywhere("call-rev", FrontendToolResponse::ok("ok")));
        assert_eq!(
            registry.reverse_index_len(),
            0,
            "index entry must clear after resolve"
        );
        let _ = rx.await.expect("oneshot");
    }

    #[test]
    fn resolve_unknown_returns_false() {
        let registry = FrontendToolRegistry::new();
        let _ = registry.entry("t1");
        assert!(!registry.resolve_anywhere("nope", FrontendToolResponse::ok("x")));
    }

    #[tokio::test]
    async fn drop_thread_drains_pending_with_error() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        let rx = entry.register_pending("call-x".into());
        registry.drop_thread("t1");
        let resp = rx.await.expect("oneshot");
        assert!(resp.is_error);
        assert!(resp.content.contains("thread closed"));
        assert_eq!(
            registry.reverse_index_len(),
            0,
            "drop_thread must clear reverse index entries"
        );
    }

    #[tokio::test]
    async fn dropping_entry_drains_pending() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        let rx = entry.register_pending("call-y".into());
        // Force-drop the registry's only strong reference.
        drop(entry);
        registry.drop_thread("t1");
        let resp = rx.await.expect("oneshot");
        assert!(resp.is_error);
    }

    #[test]
    fn active_sender_round_trip() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        assert!(entry.active_sender().is_none());

        let (tx, _rx) = mpsc::channel::<BridgeStreamItem>(4);
        entry.set_active_sender(Some(tx));
        assert!(entry.active_sender().is_some());

        entry.set_active_sender(None);
        assert!(entry.active_sender().is_none());
    }

    #[test]
    fn clear_if_same_only_clears_matching_sender() {
        // Models two overlapping runs on one thread: run B installs its
        // sender after run A. When run A tears down, its conditional clear
        // must NOT wipe run B's sender — otherwise a tool call during B
        // would find an empty slot and time out.
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");

        let (tx_a, _rx_a) = mpsc::channel::<BridgeStreamItem>(4);
        let (tx_b, _rx_b) = mpsc::channel::<BridgeStreamItem>(4);

        // Run A installs, then run B overwrites (newest run owns the slot).
        entry.set_active_sender(Some(tx_a.clone()));
        entry.set_active_sender(Some(tx_b.clone()));

        // Run A tears down: must be a no-op because the slot is now B's.
        let cleared_a = entry.clear_active_sender_if_same(&tx_a);
        assert!(!cleared_a, "A's clear must report it did NOT own the slot");
        let cur = entry
            .active_sender()
            .expect("B's sender must survive A's teardown");
        assert!(
            cur.same_channel(&tx_b),
            "slot must still hold run B's sender after A's conditional clear"
        );

        // Run B tears down: now it matches, so the slot clears.
        let cleared_b = entry.clear_active_sender_if_same(&tx_b);
        assert!(cleared_b, "B's clear must report it owned the slot");
        assert!(
            entry.active_sender().is_none(),
            "B's own teardown must clear the slot"
        );
    }

    #[test]
    fn clear_if_same_on_empty_slot_is_noop() {
        let registry = FrontendToolRegistry::new();
        let entry = registry.entry("t1");
        let (tx, _rx) = mpsc::channel::<BridgeStreamItem>(4);
        // No sender installed; clearing must not panic and reports false.
        assert!(!entry.clear_active_sender_if_same(&tx));
        assert!(entry.active_sender().is_none());
    }

    #[test]
    fn orphaned_entry_skips_index_silently() {
        // A stand-alone entry has no registry — register/resolve must
        // still work locally without panicking.
        let entry = ThreadEntry::orphaned();
        assert_eq!(entry.thread_id(), "");
        let _rx = entry.register_pending("orphan-1".into());
        assert_eq!(entry.pending_len(), 1);
        assert!(entry.resolve_pending("orphan-1", FrontendToolResponse::ok("x")));
        assert_eq!(entry.pending_len(), 0);
    }
}
