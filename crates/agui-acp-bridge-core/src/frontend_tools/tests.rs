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

#[test]
fn tool_response_preserves_text_content_and_error_flag() {
    let ok = serde_json::to_value(FrontendToolResponse::ok("done")).unwrap();
    assert_eq!(
        ok,
        serde_json::json!({"content": "done", "is_error": false})
    );

    let error = serde_json::to_value(FrontendToolResponse::error("failed")).unwrap();
    assert_eq!(
        error,
        serde_json::json!({"content": "failed", "is_error": true})
    );
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
async fn resolve_for_thread_stays_within_the_owning_thread() {
    let registry = FrontendToolRegistry::new();
    let entry_a = registry.entry("ta");
    let entry_b = registry.entry("tb");
    let rx_a = entry_a.register_pending("call-a".into());
    let rx = entry_b.register_pending("call-2".into());
    assert!(!registry.resolve_for_thread("tb", "call-a", FrontendToolResponse::ok("wrong")));
    assert_eq!(entry_a.pending_len(), 1);
    assert_eq!(entry_b.pending_len(), 1);
    assert!(registry.resolve_for_thread("tb", "call-2", FrontendToolResponse::ok("x")));
    assert!(registry.resolve_for_thread("ta", "call-a", FrontendToolResponse::ok("a")));
    assert_eq!(rx.await.expect("oneshot").content, "x");
    assert_eq!(rx_a.await.expect("oneshot").content, "a");
}

#[tokio::test]
async fn unknown_thread_lookup_does_not_create_registry_state() {
    let registry = FrontendToolRegistry::new();
    assert_eq!(registry.thread_count(), 0);
    assert!(registry.get("unknown").is_none());
    assert!(!registry.resolve_for_thread("unknown", "call", FrontendToolResponse::ok("x")));
    assert_eq!(registry.thread_count(), 0);
}

#[test]
fn resolve_unknown_returns_false() {
    let registry = FrontendToolRegistry::new();
    let _ = registry.entry("t1");
    assert!(!registry.resolve_for_thread("t1", "nope", FrontendToolResponse::ok("x")));
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

#[tokio::test]
async fn atomic_pending_registration_is_aborted_by_sender_teardown() {
    let registry = FrontendToolRegistry::new();
    let entry = registry.entry("t1");
    let (sender, _events) = mpsc::channel::<BridgeStreamItem>(1);
    entry.set_active_sender(Some(sender.clone()));

    let (registered_sender, receiver) = entry
        .register_pending_on_active_sender("call-atomic".into())
        .expect("active sender must register the pending call");
    assert!(registered_sender.same_channel(&sender));
    assert_eq!(entry.pending_len(), 1);

    assert!(entry.clear_active_sender_if_same(&registered_sender));
    entry.drain_pending("sender cleared");
    let response = receiver.await.expect("abort response");
    assert!(response.is_error);
    assert_eq!(entry.pending_len(), 0);
}

#[test]
fn atomic_pending_registration_rejects_after_sender_clear() {
    let registry = FrontendToolRegistry::new();
    let entry = registry.entry("t1");
    let (sender, _events) = mpsc::channel::<BridgeStreamItem>(1);
    entry.set_active_sender(Some(sender.clone()));
    assert!(entry.clear_active_sender_if_same(&sender));

    assert!(
        entry
            .register_pending_on_active_sender("call-cleared".into())
            .is_none()
    );
    assert_eq!(entry.pending_len(), 0);
}

#[tokio::test]
async fn undisarmed_mcp_guard_falls_back_to_lifecycle_end_when_channel_is_full() {
    let registry = FrontendToolRegistry::new();
    let entry = registry.entry("t1");
    let (sender, mut events) = mpsc::channel::<BridgeStreamItem>(1);
    entry.set_active_sender(Some(sender.clone()));
    sender
        .try_send(BridgeStreamItem::FrontendToolCall {
            tool_call_id: "queued".into(),
            tool_name: "queued".into(),
            arguments: Value::Null,
        })
        .expect("fill bounded channel");
    let (_, _receiver) = entry
        .register_pending_on_active_sender("call-cancel-end".into())
        .expect("active sender");

    let guard = entry.pending_call_guard_with_sender("call-cancel-end", sender.clone());
    drop(guard);

    assert_eq!(entry.pending_len(), 0);
    assert!(matches!(
        events.recv().await,
        Some(BridgeStreamItem::FrontendToolCall { .. })
    ));
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("fallback end send")
            .expect("frontend lifecycle end item"),
        BridgeStreamItem::FrontendToolEnd { ref tool_call_id }
            if tool_call_id == "call-cancel-end"
    ));
}

#[tokio::test]
async fn disarmed_mcp_guard_drops_pending_without_synthetic_end() {
    let registry = FrontendToolRegistry::new();
    let entry = registry.entry("t1");
    let (sender, mut events) = mpsc::channel::<BridgeStreamItem>(1);
    entry.set_active_sender(Some(sender.clone()));
    sender
        .try_send(BridgeStreamItem::FrontendToolCall {
            tool_call_id: "queued".into(),
            tool_name: "queued".into(),
            arguments: Value::Null,
        })
        .expect("fill bounded channel");
    let (_, _receiver) = entry
        .register_pending_on_active_sender("call-disarmed".into())
        .expect("active sender");

    let mut guard = entry.pending_call_guard_with_sender("call-disarmed", sender);
    guard.complete();
    drop(guard);

    assert_eq!(entry.pending_len(), 0);
    assert!(matches!(
        events.recv().await,
        Some(BridgeStreamItem::FrontendToolCall { .. })
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
            .await
            .is_err()
    );
}

#[test]
fn orphaned_entry_resolves_locally() {
    // A stand-alone entry (no registry) must still register/resolve
    // locally without panicking. Built via Default, same as an entry
    // with an empty thread id.
    let entry = ThreadEntry::default();
    assert_eq!(entry.thread_id(), "");
    let _rx = entry.register_pending("orphan-1".into());
    assert_eq!(entry.pending_len(), 1);
    assert!(entry.resolve_pending("orphan-1", FrontendToolResponse::ok("x")));
    assert_eq!(entry.pending_len(), 0);
}
