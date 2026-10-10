use super::*;

#[test]
fn load_history_limits_events_and_bytes_without_reordering() {
    use agent_client_protocol::schema::v1::{CurrentModeUpdate, SessionUpdate};

    let first = SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("first"));
    let second = SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("second"));
    let mut history = LoadHistory::default();
    history.append(first.clone(), 1).expect("first fits");
    history.append(second.clone(), 1).expect("second fits");
    assert_eq!(history.updates, vec![first, second]);

    for _ in 2..MAX_LOAD_HISTORY_EVENTS {
        history
            .append(
                SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("event")),
                1,
            )
            .expect("event fits");
    }
    assert!(
        history
            .append(
                SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("over")),
                1,
            )
            .is_err()
    );
    assert_eq!(history.updates.len(), MAX_LOAD_HISTORY_EVENTS);
    assert!(history.exceeded);

    let mut bytes = LoadHistory::default();
    bytes
        .append(
            SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("bytes")),
            MAX_LOAD_HISTORY_BYTES,
        )
        .expect("byte limit itself fits");
    assert!(
        bytes
            .append(
                SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new("over")),
                1,
            )
            .is_err()
    );
    assert_eq!(bytes.bytes, MAX_LOAD_HISTORY_BYTES);
}

#[test]
fn session_list_limits_entries_bytes_and_pages_with_errors() {
    let summary = |id: &str| SessionSummary {
        session_id: id.into(),
        cwd: "/".into(),
        title: None,
        updated_at: None,
    };
    let mut entries = BoundedSessionList::default();
    for index in 0..MAX_LIST_SESSIONS {
        entries
            .push_with_size(summary(&index.to_string()), 1)
            .expect("entry fits");
    }
    let entry_error = entries
        .push_with_size(summary("over"), 1)
        .expect_err("entry limit must be reported");
    // Budget violations are internal errors with structured limit data,
    // not -32800 (request_cancelled) — the caller did not cancel.
    let (code, data) = match entry_error {
        BridgeError::Acp(error) => (i32::from(error.code), error.data),
        other => panic!("unexpected error: {other:?}"),
    };
    assert_eq!(code, -32603);
    assert_eq!(
        data,
        Some(serde_json::json!({"limit": "MAX_LIST_SESSIONS", "cap": MAX_LIST_SESSIONS}))
    );
    assert_eq!(entries.len(), MAX_LIST_SESSIONS);

    let mut bytes = BoundedSessionList::default();
    bytes
        .push_with_size(summary("bytes"), MAX_LIST_BYTES)
        .expect("byte limit itself fits");
    assert!(bytes.push_with_size(summary("over"), 1).is_err());
    assert_eq!(bytes.len(), 1);

    assert!(next_list_cursor(MAX_LIST_PAGES - 1, Some("next".into())).is_ok());
    assert!(next_list_cursor(MAX_LIST_PAGES, Some("next".into())).is_err());
    assert!(next_list_cursor(MAX_LIST_PAGES, None).is_ok());
}
