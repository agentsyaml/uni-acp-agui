use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_call_resolves_on_happy_path() {
    // Baseline: a single run drives the MCP tool call and the browser posts
    // the result. The run must finish with the tool result echoed.
    let (bound, state) = spawn_bridge(BridgeConfig::default()).await;

    let input = input_with_tool("t-happy", "r1");
    let resp = post_run(bound, &input).await;

    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut body = String::new();
    let mut posted = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(b))) => body.push_str(&String::from_utf8_lossy(&b)),
            Ok(Some(Err(e))) => panic!("sse error: {e}"),
            Ok(None) => break,
            Err(_) => {}
        }
        if !posted && let Some(id) = first_ended_tool_call_id(&body) {
            assert_eq!(state.frontend_tools().pending_len("t-happy"), 1);
            assert_eq!(
                post_tool_response(bound, "t-happy", &id, "hi world").await,
                reqwest::StatusCode::OK
            );
            posted = true;
        }
        if body.contains("\"type\":\"RUN_FINISHED\"") {
            break;
        }
    }

    assert!(
        posted,
        "should have observed a TOOL_CALL_START and posted a response"
    );
    assert!(
        body.contains("TOOL_RESULT=hi world"),
        "agent must receive the browser's tool result, body:\n{body}"
    );
    assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    let _ = state;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_session_recreates_frontend_tool_registry() {
    let (bound, state) = spawn_replacement_bridge(BridgeConfig {
        set_session_timeout: Duration::from_millis(100),
        ..BridgeConfig::default()
    })
    .await;
    let thread_id = "t-replacement-tools";

    let first = input_with_tool(thread_id, "r1");
    let first_events = drain_run(bound, &first).await;
    assert!(
        first_events.contains("\"type\":\"RUN_FINISHED\""),
        "{first_events}"
    );
    assert!(state.frontend_tools().has(thread_id));

    assert!(matches!(
        state.set_session_mode(thread_id, "code").await,
        Err(agui_acp_bridge_server::SetSessionStatus::Timeout)
    ));
    assert_eq!(state.session_count(), 1);

    // Do not probe session_init_state here: that API intentionally evicts an
    // unusable entry. The replacement run must repair the registry itself.
    let replacement = input_with_tool(thread_id, "r2");
    let replacement_events = run_with_tool_response(bound, &state, &replacement).await;
    assert!(
        replacement_events.contains("\"type\":\"TOOL_CALL_START\"")
            && replacement_events.contains("\"type\":\"TOOL_CALL_END\"")
            && replacement_events.contains("\"type\":\"RUN_FINISHED\""),
        "replacement MCP flow must complete: {replacement_events}"
    );
    assert!(
        replacement_events.contains("TOOL_RESULT=hi world"),
        "replacement tools/call must reach the browser response: {replacement_events}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_thread_tool_response_is_404_and_leaves_call_pending() {
    let (bound, state) = spawn_bridge(BridgeConfig::default()).await;
    let input = input_with_tool("t-owner", "r1");
    let resp = post_run(bound, &input).await;

    // Keep a second registry entry live so this proves a known wrong thread
    // cannot fall back to any global tool-call-id lookup.
    state.frontend_tools().entry("t-other");

    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut body = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let tool_call_id = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "tool call did not start"
        );
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => body.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(error))) => panic!("SSE chunk error: {error}"),
            Ok(None) => panic!("SSE ended before tool call"),
            Err(_) => {}
        }
        if let Some(id) = first_ended_tool_call_id(&body) {
            break id;
        }
    };

    assert_eq!(state.frontend_tools().pending_len("t-owner"), 1);
    assert_eq!(
        post_tool_response(bound, "t-owner", "unknown-call", "unknown").await,
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(state.frontend_tools().pending_len("t-owner"), 1);
    assert_eq!(
        post_tool_response(bound, "t-other", &tool_call_id, "wrong").await,
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(state.frontend_tools().pending_len("t-owner"), 1);

    assert_eq!(
        post_tool_response(bound, "t-owner", &tool_call_id, "right").await,
        reqwest::StatusCode::OK
    );
    let finish_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < finish_deadline && !body.contains("RUN_FINISHED") {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => body.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(error))) => panic!("SSE chunk error: {error}"),
            Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(body.contains("TOOL_RESULT=right"), "body:\n{body}");
    assert!(body.contains("RUN_FINISHED"), "body:\n{body}");
    assert_eq!(state.frontend_tools().pending_len("t-owner"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_while_tool_parked_releases_session_quickly() {
    // The core Bug 2 scenario. A run starts a tool call; the agent parks
    // awaiting the browser. We then DROP the SSE connection (simulating a
    // page refresh / tab close) WITHOUT posting a tool response.
    //
    // Before the fix: the stream task parked too, never noticed the dead
    // client, `active_prompts` stayed > 0, and the reaper could not release
    // the session until frontend_tool_timeout (here 60s) fired.
    //
    // After the fix: the stream loop's `tx.closed()` branch fires on
    // disconnect, cancels the turn, and aborts the pending tool call, so the
    // prompt unwinds and the session becomes idle. With a short idle_timeout
    // the reaper then drops it well within a few seconds.
    let (bound, state) = spawn_bridge(BridgeConfig {
        // Long enough that, if the disconnect path were broken, the session
        // would stay pinned far past our assertion window.
        frontend_tool_timeout: Duration::from_secs(60),
        idle_timeout: Duration::from_millis(300),
        ..BridgeConfig::default()
    })
    .await;

    let input = input_with_tool("t-disc", "r1");
    let resp = post_run(bound, &input).await;

    // Read until the tool call starts (agent is now parked), then drop.
    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut body = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(b))) => body.push_str(&String::from_utf8_lossy(&b)),
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {}
        }
        if first_ended_tool_call_id(&body).is_some() {
            break;
        }
    }
    assert!(
        first_ended_tool_call_id(&body).is_some(),
        "agent must emit TOOL_CALL_END while the MCP call remains parked, body:\n{body}"
    );
    assert_eq!(
        state.session_count(),
        1,
        "session is live during the parked call"
    );
    assert_eq!(state.frontend_tools().pending_len("t-disc"), 1);

    // Simulate the browser going away mid-call: dropping the body stream
    // closes the underlying TCP connection.
    drop(stream);

    // The session must become reapable and get dropped well before the 60s
    // frontend_tool_timeout. Poll for up to 5s.
    let release_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut released = false;
    while tokio::time::Instant::now() < release_deadline {
        if state.session_count() == 0 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        released,
        "session must be released within 5s of disconnect (not pinned until frontend_tool_timeout)"
    );
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "frontend-tools registry entry must be dropped too"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bridge_stays_responsive_after_a_parked_disconnect() {
    // After one run is abandoned mid-tool-call, a fresh run on a NEW thread
    // must still work normally — proving the abandoned call didn't wedge any
    // shared state.
    let (bound, state) = spawn_bridge(BridgeConfig {
        frontend_tool_timeout: Duration::from_secs(60),
        idle_timeout: Duration::from_millis(300),
        ..BridgeConfig::default()
    })
    .await;

    // Run 1: abandon mid-call.
    {
        let input = input_with_tool("t-aband", "r1");
        let resp = post_run(bound, &input).await;
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut body = String::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(b))) => body.push_str(&String::from_utf8_lossy(&b)),
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => {}
            }
            if first_ended_tool_call_id(&body).is_some() {
                assert_eq!(state.frontend_tools().pending_len("t-aband"), 1);
                break;
            }
        }
        drop(stream); // disconnect
    }

    // Run 2: fresh thread, post the response, expect clean completion.
    let input = input_with_tool("t-fresh", "r1");
    let resp = post_run(bound, &input).await;
    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut body = String::new();
    let mut posted = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(b))) => body.push_str(&String::from_utf8_lossy(&b)),
            Ok(Some(Err(e))) => panic!("sse error: {e}"),
            Ok(None) => break,
            Err(_) => {}
        }
        if !posted && let Some(id) = first_ended_tool_call_id(&body) {
            assert!(state.frontend_tools().pending_len("t-fresh") > 0);
            assert_eq!(
                post_tool_response(bound, "t-fresh", &id, "ok").await,
                reqwest::StatusCode::OK
            );
            posted = true;
        }
        if body.contains("\"type\":\"RUN_FINISHED\"") {
            break;
        }
    }

    assert!(
        body.contains("TOOL_RESULT=ok") && body.contains("\"type\":\"RUN_FINISHED\""),
        "a fresh run must complete cleanly after an abandoned one, body:\n{body}"
    );
}
