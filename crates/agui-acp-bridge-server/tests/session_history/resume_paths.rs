use super::*;

#[tokio::test]
async fn resume_loads_a_nested_allowed_working_directory() {
    let root = temporary_resume_root("nested");
    let nested = root.join("nested/project");
    fs::create_dir_all(&nested).expect("create nested cwd");

    let client = Arc::new(SharedHistoryClient::new());
    client.store.lock().expect("store poisoned").insert(
        "nested-session".into(),
        (
            Some("nested".into()),
            nested.clone(),
            vec!["user:nested history".into()],
        ),
    );
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let state = BridgeAppState::new(client, root.clone());

    let mut input = RunAgentInput::new("nested-thread", "r-nested");
    input.forwarded_props = serde_json::json!({
        "acpResume": {"sessionId": "nested-session"}
    });
    let (status, body) = collect_sse_body(state.clone(), input).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        body.contains("HISTORY:user:nested history"),
        "body:\n{body}"
    );
    assert_eq!(state.session_count(), 1);
    drop(state);
    fs::remove_dir_all(root).expect("remove temporary resume root");
}

#[tokio::test]
async fn resume_rejects_a_session_outside_the_bridge_root_before_load() {
    let root = temporary_resume_root("outside-root");
    let outside = temporary_resume_root("outside");
    let client = Arc::new(SharedHistoryClient::new());
    client.store.lock().expect("store poisoned").insert(
        "outside-session".into(),
        (
            Some("outside".into()),
            outside.clone(),
            vec!["user:must not load".into()],
        ),
    );
    let state = BridgeAppState::new(client, root.clone());

    let mut input = RunAgentInput::new("outside-thread", "r-outside");
    input.forwarded_props = serde_json::json!({
        "acpResume": {"sessionId": "outside-session"}
    });
    let (status, body) = collect_sse_body(state.clone(), input).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("ACP_RESUME_FAILED"), "body:\n{body}");
    assert!(
        !body.contains("HISTORY:"),
        "outside session was loaded:\n{body}"
    );
    assert_eq!(
        state.session_count(),
        0,
        "no actor should open before cwd validation"
    );
    drop(state);
    fs::remove_dir_all(root).expect("remove bridge root");
    fs::remove_dir_all(outside).expect("remove outside cwd");
}

#[tokio::test]
async fn invalid_resume_at_capacity_preserves_idle_session() {
    let root = temporary_resume_root("capacity-root");
    let outside = temporary_resume_root("capacity-outside");
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::builder(client.clone(), root.clone())
        .with_config(agui_acp_bridge_core::BridgeConfig {
            max_sessions: 1,
            ..Default::default()
        })
        .build();
    let (status, body) =
        collect_sse_body(state.clone(), user_input("original", "r1", "keep me")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let original_id = state.list_sessions().await.expect("list original")[0]
        .session_id
        .clone();

    let mut missing = RunAgentInput::new("missing", "r-missing");
    missing.forwarded_props = serde_json::json!({"acpResume":{"sessionId":"no-such-session"}});
    let (status, body) = collect_sse_body(state.clone(), missing).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("ACP_RESUME_FAILED") && body.contains("was not found"),
        "{body}"
    );

    client.store.lock().expect("store poisoned").insert(
        "outside-at-capacity".into(),
        (
            Some("outside".into()),
            outside.clone(),
            vec!["user:must not load".into()],
        ),
    );
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let mut escaped = RunAgentInput::new("outside", "r-outside");
    escaped.forwarded_props = serde_json::json!({"acpResume":{"sessionId":"outside-at-capacity"}});
    let (status, body) = collect_sse_body(state.clone(), escaped).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("ACP_RESUME_FAILED") && body.contains("outside the bridge root"),
        "{body}"
    );
    assert!(!body.contains("must not load"), "{body}");

    let sessions = state.list_sessions().await.expect("list after failures");
    assert_eq!(sessions.len(), 2);
    assert!(
        sessions
            .iter()
            .any(|session| session.session_id == original_id)
    );
    assert_eq!(state.session_count(), 1);
    let (status, body) =
        collect_sse_body(state.clone(), user_input("original", "r2", "still here")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("echo: still here"), "{body}");
    assert_eq!(state.session_count(), 1);
    drop(state);
    fs::remove_dir_all(root).expect("remove root");
    fs::remove_dir_all(outside).expect("remove outside cwd");
}

#[tokio::test]
async fn spoofed_resume_cwd_is_ignored() {
    let root = temporary_resume_root("spoofed-cwd");
    let outside = temporary_resume_root("spoofed-outside");
    let client = Arc::new(SharedHistoryClient::new());
    let state = BridgeAppState::new(client, root.clone());

    let (_, _) =
        collect_sse_body(state.clone(), user_input("source", "r-source", "persisted")).await;
    let session_id = state.list_sessions().await.expect("list")[0]
        .session_id
        .clone();

    let mut input = RunAgentInput::new("spoofed-thread", "r-spoofed");
    input.forwarded_props = serde_json::json!({
        "acpResume": {
            "sessionId": session_id,
            "cwd": outside.to_string_lossy()
        }
    });
    let (status, body) = collect_sse_body(state.clone(), input).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(body.contains("HISTORY:user:persisted"), "body:\n{body}");
    assert!(!body.contains("ACP_RESUME_FAILED"), "body:\n{body}");
    drop(state);
    fs::remove_dir_all(root).expect("remove bridge root");
    fs::remove_dir_all(outside).expect("remove spoofed cwd");
}
