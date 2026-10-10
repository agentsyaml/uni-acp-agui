use super::*;

#[tokio::test]
async fn validates_request_permission_resolves_with_auto_allow() {
    // End-to-end permission flow with the AutoAllow policy:
    // 1. Mock agent issues `requestPermission` (using SDK-recommended
    //    `on_receiving_ok_result` chaining, not `block_task`).
    // 2. Bridge's session handler consults the policy.
    // 3. `AutoAllow` returns `Allow { option_id }` for the first AllowOnce
    //    option ("allow").
    // 4. Bridge responds; agent receives the outcome and completes the
    //    prompt with `RUN_FINISHED`.
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AllowAlwaysFirstOption)));
    let client = client_for(test_agents::run_request_permission_agent);
    let state = state_with_policy(client, policy.clone());

    let app = agui_acp_bridge_server::build_router(state);
    let body = serde_json::to_vec(&user_input("thread-perm", "run-perm", "do read")).unwrap();
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body))
            .unwrap(),
    )
    .await
    .expect("router error");

    assert_eq!(response.status(), StatusCode::OK);

    let body = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        http_body_util::BodyExt::collect(response.into_body()),
    )
    .await
    .expect("permission roundtrip must not hang")
    .expect("body collect failed");
    let text = String::from_utf8_lossy(&body.to_bytes()).into_owned();

    assert!(
        text.contains("\"type\":\"RUN_FINISHED\""),
        "expected RUN_FINISHED after auto-allow resolved permission, got:\n{text}"
    );
    assert!(
        policy.was_invoked(),
        "the configured PermissionPolicy must be consulted on requestPermission"
    );
}

#[tokio::test]
async fn validates_policy_not_consulted_on_turns_without_permission_request() {
    // Sanity: the policy must only be consulted when an agent actually issues
    // a `requestPermission`. A plain text-streaming turn must not touch it.
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AllowAlwaysFirstOption)));
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = state_with_policy(client, policy.clone());

    let (_, body) =
        collect_sse_body(state, user_input("thread-policy", "run-policy", "noop")).await;
    assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    assert!(
        !policy.was_invoked(),
        "policy must not be consulted on a turn that issues no requestPermission"
    );
}

#[tokio::test]
async fn validates_no_events_after_terminal_before_spill_drain() {
    // Pins half of the spill contract: late notifications — those arriving
    // on the connection-level callback after the prompt has completed — do
    // NOT appear retroactively in the run that already terminated (no
    // events after the terminal event). They are spilled and drained by
    // the NEXT run; see `validates_spilled_late_notifications_drain_on_next_run`.
    let state = state_with_client(client_for(test_agents::run_late_notification_agent));

    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-late", "run-1", "first")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("in-band"),
        "first run should carry the in-band chunk, body:\n{body}"
    );
    assert!(
        !body.contains("LATE-AFTER-FINISH"),
        "late notification must not appear in the same run (channel already closed), body:\n{body}"
    );
}

#[tokio::test]
async fn validates_spilled_late_notifications_drain_on_next_run() {
    // The other half of the spill contract: a late notification arriving
    // after the prompt response is preserved in the bounded spill buffer and
    // rebroadcast at the START of the second run's stream — before that
    // run's own in-band chunk from the new prompt. Never silently dropped.
    let state = state_with_client(client_for(test_agents::run_late_notification_agent));

    // Run 1: terminates; its late notification lands in the spill buffer.
    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-spill", "run-1", "first")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body.contains("LATE-AFTER-FINISH"),
        "no events after run 1's terminal event, body:\n{body}"
    );

    // Give the agent's spawned late-notification task time to deliver.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Run 2: opens with the spilled update, then emits its own turn text
    // (this agent labels every in-turn chunk "in-band").
    let (status2, body2) =
        collect_sse_body(state, user_input("thread-spill", "run-2", "second")).await;
    assert_eq!(status2, StatusCode::OK, "body:\n{body2}");
    let late = body2
        .find("LATE-AFTER-FINISH")
        .expect("spilled late notification must be rebroadcast on the next run, body:\n{body2}");
    let own = body2
        .rfind("in-band")
        .expect("second run's own in-band chunk must be present, body:\n{body2}");
    assert!(
        late < own,
        "spilled update must precede run 2's own turn events, body:\n{body2}"
    );
}

#[tokio::test]
async fn validates_spill_overflow_drops_with_warning_and_session_stays_usable() {
    // 40 late updates against a 32-entry spill buffer: the first 32 are
    // preserved, the rest are warned and dropped. The session itself stays
    // usable — run 2 completes normally with the drained prefix.
    let state = state_with_client(client_for(|stream| {
        test_agents::run_late_notification_flood_agent(stream, 40)
    }));

    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-flood", "run-1", "first")).await;
    assert_eq!(status, StatusCode::OK, "body:\n{body}");
    assert!(
        !body.contains("LATE-AFTER-FINISH"),
        "no events after run 1's terminal event, body:\n{body}"
    );

    // Let all 40 late notifications reach the notification handler.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let (status2, body2) =
        collect_sse_body(state, user_input("thread-flood", "run-2", "second")).await;
    assert_eq!(
        status2,
        StatusCode::OK,
        "overflow must not poison the session, body:\n{body2}"
    );
    assert!(
        body2.contains("RUN_FINISHED"),
        "run 2 must finish cleanly despite spill overflow, body:\n{body2}"
    );
    let kept = (0..32)
        .filter(|index| body2.contains(&format!("LATE-AFTER-FINISH-{index}")))
        .count();
    let dropped = (32..40)
        .filter(|index| body2.contains(&format!("LATE-AFTER-FINISH-{index}")))
        .count();
    assert_eq!(
        kept, 32,
        "spill must retain exactly its capacity, body:\n{body2}"
    );
    assert_eq!(
        dropped, 0,
        "updates beyond capacity must be dropped, body:\n{body2}"
    );
}

#[tokio::test]
async fn validates_failing_prompt_yields_run_error() {
    let state: BridgeAppState =
        state_with_client(client_for(test_agents::run_failing_prompt_agent));
    let (status, body) =
        collect_sse_body(state, user_input("thread-fail", "run-fail", "explode")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"type\":\"RUN_ERROR\""),
        "agent prompt error must surface as RUN_ERROR, body:\n{body}"
    );
    assert!(!body.contains("\"type\":\"RUN_FINISHED\""));
}

#[tokio::test]
async fn validates_slow_prompt_completes_then_finishes() {
    let state = state_with_client(client_for(|s| test_agents::run_slow_prompt_agent(s, 150)));
    let started = std::time::Instant::now();
    let (status, body) =
        collect_sse_body(state, user_input("thread-slow", "run-slow", "wait")).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert!(
        elapsed >= std::time::Duration::from_millis(150),
        "bridge must wait for the slow prompt, elapsed={elapsed:?}"
    );
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "slow prompt must still finish cleanly, body:\n{body}"
    );

    let _ = AutoAllow;
}

#[tokio::test]
async fn validates_auto_deny_consults_policy_then_returns_cancelled() {
    // AutoDeny returns `Cancelled` as the outcome of `requestPermission`,
    // which is still a valid (non-error) response to the agent. The mock
    // agent's PromptRequest handler treats any `Ok(_)` outcome as success
    // and ends the turn, so the AG-UI run still finishes cleanly. The
    // crucial assertion is that the configured policy was consulted.
    use agui_acp_bridge_policy::AutoDeny;
    let policy: Arc<PolicySpy> = Arc::new(PolicySpy::new(Arc::new(AutoDeny)));
    let client = client_for(test_agents::run_request_permission_agent);
    let state = state_with_policy(client, policy.clone());

    let (status, body) =
        collect_sse_body(state, user_input("thread-deny", "run-deny", "do read")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("\"type\":\"RUN_FINISHED\""),
        "RUN_FINISHED expected (Cancelled is a non-error outcome), body:\n{body}"
    );
    assert_eq!(
        policy.call_count(),
        1,
        "the configured policy must be consulted exactly once"
    );
}
