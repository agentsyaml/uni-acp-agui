use super::*;

#[tokio::test]
async fn frontend_tool_call_with_no_active_prompt_returns_mcp_error() {
    // The name claims the no-active-prompt `tools/call` branch: a thread
    // whose tools are registered (entry exists) but whose SSE prompt is
    // NOT running must get an MCP `isError: true` *result* (HTTP 200,
    // JSON-RPC ok) with the "no active AG-UI prompt" text — not a
    // JSON-RPC error and not a hang.
    //
    // We create the entry by POSTing a RunAgentInput: the handler stores
    // the tool list before opening the session, so the registry entry
    // exists. The mock agent immediately finishes its prompt and the
    // sender slot clears, leaving no active prompt by the time we call.
    let (bound, _captured) = spawn_bridge().await;
    let thread_id = "thread-no-active-prompt";
    let url = format!("http://{}/", bound);
    let mcp_url = format!("http://{}/mcp/{}", bound, thread_id);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Populate the registry entry with `say_hello` (the entry exists,
    // but nobody is holding an active SSE prompt for the thread).
    let input = input_with_tool(thread_id, "run-no-active", say_hello_tool());
    let resp = http
        .post(&url)
        .header("Accept", "text/event-stream")
        .json(&input)
        .send()
        .await
        .expect("post run");
    drop(resp);

    // initialize first (unknown-thread-friendly), then tools/call.
    let init: Value = http
        .post(&mcp_url)
        .json(&json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2024-11-05","capabilities":{}}
        }))
        .send()
        .await
        .expect("init")
        .json()
        .await
        .expect("parse init");
    assert!(init.get("result").is_some(), "init must succeed: {init}");

    // Deterministic sync: dropping `resp` does not guarantee the bridge has
    // cleared its SSE sender slot yet (no sleep-based ordering). If a call
    // races against the still-live sender it parks on `/tool-response`
    // instead of returning the no-active-prompt error, so retry the call
    // until the sender is observed cleared. A bounded deadline means a real
    // regression fails as an assertion here, not as a transport panic.
    // ponytail: 2s per attempt (a parked call eats one attempt), 15s budget.
    let mcp_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let body = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "tools/call never returned the no-active-prompt MCP error within 15s"
        );
        let posted = mcp_http
            .post(&mcp_url)
            .json(&json!({
                "jsonrpc":"2.0","id":2,"method":"tools/call",
                "params":{"name":"say_hello","arguments":{"name":"world"}}
            }))
            .send()
            .await;
        let Ok(response) = posted else {
            // Transport-level failure (parked call timing out): retry.
            continue;
        };
        let Ok(parsed) = response.json::<Value>().await else {
            continue; // unparseable mid-teardown response: retry.
        };
        // Done only when the sender slot is cleared: JSON-RPC ok whose
        // result is the no-active-prompt MCP error envelope.
        let is_expected = parsed.get("error").is_none()
            && parsed["result"]["isError"] == serde_json::Value::Bool(true)
            && parsed["result"]["content"][0]["text"]
                .as_str()
                .is_some_and(|t| t.contains("no active"));
        if is_expected {
            break parsed;
        }
    };

    // The bridge must answer with a *successful* JSON-RPC response whose
    // result is an MCP error envelope — that is how MCP surfaces tool
    // failure to the LLM. A JSON-RPC-level error here would be wrong.
    assert!(
        body.get("error").is_none(),
        "tools/call must not be a JSON-RPC error, got: {body}"
    );
    assert_eq!(body["result"]["isError"], true, "got: {body}");
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        text.contains("no active"),
        "error text should mention the missing active prompt, got: {text}"
    );
}

#[tokio::test]
async fn frontend_tool_error_response_propagates_as_is_error() {
    // The frontend handler returns a *failing* result (is_error=true).
    // The bridge must convert it into an MCP error envelope — HTTP 200,
    // JSON-RPC ok, `isError: true` and our content preserved verbatim —
    // so the agent's LLM sees the failure instead of a success.
    let (bound, _captured) = spawn_bridge().await;
    let input = input_with_tool("thread-ft-err", "run-ft-err", say_hello_tool());

    let on_tool_call: OnToolCall = Box::new(|_id, _name, _args| {
        // Simulate a frontend handler that errored.
        Box::new(|| (json!({"this":"is the result"}), true))
    });

    let (events, agent_text) = tokio::time::timeout(
        Duration::from_secs(20),
        drive_run(bound, &input, on_tool_call),
    )
    .await
    .expect("test deadlocked");

    assert!(events.iter().any(|e| e == "TOOL_CALL_END"), "{events:?}");
    assert!(events.iter().any(|e| e == "RUN_FINISHED"), "{events:?}");

    // The agent's echo carries the MCP envelope: the failing flag AND the
    // exact content we posted, surviving the round trip.
    let result_line = marker_value(&agent_text, "TOOL_RESULT=")
        .unwrap_or_else(|| panic!("no TOOL_RESULT in agent transcript: {agent_text:?}"));
    let is_error_line = marker_value(&agent_text, "TOOL_IS_ERROR=")
        .unwrap_or_else(|| panic!("no TOOL_IS_ERROR in agent transcript: {agent_text:?}"));
    let payload: Value = serde_json::from_str(result_line).expect("tool result must be valid JSON");
    assert_eq!(payload["this"], "is the result");
    assert_eq!(
        is_error_line.trim_start_matches("TOOL_IS_ERROR="),
        "true",
        "agent must see isError=true, transcript: {agent_text:?}"
    );
}

#[tokio::test]
async fn unknown_tool_call_returns_mcp_error() {
    use serde_json::json;

    // Stand up a bridge, populate the registry by POSTing an empty AG-UI
    // run (the handler stores tools before returning), then directly
    // call /mcp/{thread} with an unknown tool name.
    let (bound, _captured) = spawn_bridge().await;
    let thread_id = "thread-unknown-tool";
    let url = format!("http://{}/", bound);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Trigger tool-list registration via a normal RunAgentInput. The
    // mock agent immediately hits MCP back, so we drive the SSE just
    // enough to reach RUN_FINISHED — but our concern here is just that
    // the registry has the thread and the configured tool list.
    let input = input_with_tool(thread_id, "run-unknown", say_hello_tool());
    drop(tokio::spawn({
        let url = url.clone();
        let http = http.clone();
        async move {
            // Drain the SSE in the background.
            let resp = http
                .post(&url)
                .header("Accept", "text/event-stream")
                .json(&input)
                .send()
                .await
                .unwrap();
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;
            while (stream.next().await).is_some() {}
        }
    }));

    // Give the bridge a moment to populate the registry. The handler
    // calls `frontend_tools.entry(thread).set_tools(...)` before
    // session_for, so the entry is available almost immediately.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mcp_url = format!("http://{}/mcp/{}", bound, thread_id);
    // initialize, just to be polite.
    let _ = http
        .post(&mcp_url)
        .json(&json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2024-11-05","capabilities":{}}
        }))
        .send()
        .await
        .expect("init");

    let resp = http
        .post(&mcp_url)
        .json(&json!({
            "jsonrpc":"2.0","id":2,"method":"tools/call",
            "params":{"name":"definitely_not_registered","arguments":{}}
        }))
        .send()
        .await
        .expect("tools/call");
    let body: Value = resp.json().await.unwrap();
    assert!(
        body.get("error").is_some(),
        "expected JSON-RPC error envelope for unknown tool, got: {body}"
    );
}

#[tokio::test]
async fn structured_json_result_round_trips_to_agent() {
    // Generative-UI-style tool: handler returns a JSON object, the bridge
    // wraps it as MCP text content. The mock agent re-emits the textual
    // payload so the test can assert the JSON survived the roundtrip —
    // including the `marker` field, proving no truncation/mangling.
    let (bound, _captured) = spawn_bridge().await;
    let input = input_with_tool("thread-genui", "run-genui", say_hello_tool());

    let on_tool_call: OnToolCall = Box::new(|_id, _name, args| {
        // Echo the structured args back as a structured result. The bridge
        // serialises the value with `serde_json::to_string` for MCP, so
        // we need the test agent to surface that string and we'll assert
        // it parses back to the same JSON.
        let payload = json!({
            "echoed": args.unwrap_or(json!({})),
            "marker": "GENUI_OK",
        });
        Box::new(move || (payload.clone(), false))
    });

    let (events, agent_text) = tokio::time::timeout(
        Duration::from_secs(20),
        drive_run(bound, &input, on_tool_call),
    )
    .await
    .expect("deadlocked");

    assert!(events.contains(&"TOOL_CALL_END".into()), "{events:?}");
    assert!(events.contains(&"RUN_FINISHED".into()), "{events:?}");

    // The structured payload must arrive at the agent intact: parseable
    // as JSON, with both fields present and correct.
    let result_line = marker_value(&agent_text, "TOOL_RESULT=")
        .unwrap_or_else(|| panic!("no TOOL_RESULT in agent transcript: {agent_text:?}"));
    let payload: Value = serde_json::from_str(result_line).unwrap_or_else(|e| {
        panic!("round-tripped payload must be valid JSON ({e}): {agent_text:?}")
    });
    assert_eq!(payload["marker"], "GENUI_OK");
    assert_eq!(
        payload["echoed"],
        json!({"name": "world"}),
        "args must survive the round trip too"
    );
}

#[tokio::test]
async fn frontend_tool_handler_failure_propagates_as_mcp_error_envelope() {
    // The browser-side handler throws (modeled as is_error=true with an
    // error message). The bridge must convert that into an MCP isError
    // envelope and the agent's `tools/call` must still return cleanly:
    // JSON-RPC ok, `isError: true`, and the error message preserved so
    // the LLM can react to it.
    let (bound, _captured) = spawn_bridge().await;
    let input = input_with_tool("thread-err", "run-err", say_hello_tool());

    const FAILURE_MESSAGE: &str = "FRONTEND_HANDLER_THREW: cannot render widget";

    let on_tool_call: OnToolCall = Box::new(|_id, _name, _args| {
        // Model a thrown browser-side handler as an is_error response
        // carrying the failure message.
        Box::new(move || (json!(FAILURE_MESSAGE), true))
    });

    let (events, agent_text) = tokio::time::timeout(
        Duration::from_secs(20),
        drive_run(bound, &input, on_tool_call),
    )
    .await
    .expect("deadlocked");

    assert!(events.contains(&"TOOL_CALL_END".into()), "{events:?}");
    assert!(events.contains(&"RUN_FINISHED".into()), "{events:?}");

    // The failure message must reach the agent verbatim inside the error
    // envelope, and the envelope must be flagged as an error.
    let result_line = marker_value(&agent_text, "TOOL_RESULT=")
        .unwrap_or_else(|| panic!("no TOOL_RESULT in agent transcript: {agent_text:?}"));
    let message = result_line.trim_matches('"');
    assert!(
        message.contains(FAILURE_MESSAGE),
        "error message must reach the agent verbatim, got: {message:?}"
    );
    let is_error_line = marker_value(&agent_text, "TOOL_IS_ERROR=")
        .unwrap_or_else(|| panic!("no TOOL_IS_ERROR in agent transcript: {agent_text:?}"));
    assert_eq!(
        is_error_line.trim_start_matches("TOOL_IS_ERROR="),
        "true",
        "handler failure must surface as isError=true"
    );
}
