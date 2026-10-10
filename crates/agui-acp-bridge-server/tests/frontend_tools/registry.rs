use super::*;

#[tokio::test]
async fn tool_list_change_mid_thread_is_visible_via_mcp_list() {
    // Simulate a re-prompt that changes the registered tool set. The
    // bridge logs a warning (which we don't capture here) but more
    // importantly the next /mcp/{thread} `tools/list` call must reflect
    // the new set. This proves the registry is updated even when the
    // agent caches its own list.
    let (bound, _captured) = spawn_bridge().await;
    let thread_id = "thread-tool-list-change";
    let url = format!("http://{}/", bound);
    let mcp_url = format!("http://{}/mcp/{}", bound, thread_id);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // First prompt: register `say_hello` only. We don't actually drain
    // the SSE — opening the run creates the registry entry, which is
    // all we need. Drop the response immediately to free the connection.
    let input1 = input_with_tool(thread_id, "run-1", say_hello_tool());
    let resp1 = http
        .post(&url)
        .header("Accept", "text/event-stream")
        .json(&input1)
        .send()
        .await
        .expect("post 1");
    drop(resp1);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // tools/list must return [say_hello].
    let body: Value = http
        .post(&mcp_url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}))
        .send()
        .await
        .expect("list 1")
        .json()
        .await
        .expect("parse 1");
    let names: Vec<String> = body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["say_hello".to_string()], "got {names:?}");

    // Second prompt: replace with a different tool. (Same thread id so
    // the registry update path runs.)
    let mut input2 = agui_rs_core::types::RunAgentInput::new(thread_id, "run-2");
    input2.tools.push(Tool {
        name: "estimate_order".into(),
        description: "estimate cost".into(),
        parameters: json!({"type":"object"}),
        metadata: None,
    });
    input2.messages.push(Message::User(UserMessage {
        id: "u2".into(),
        content: UserMessageContent::Text("hi".into()),
        name: None,
        encrypted_value: None,
    }));
    let resp2 = http
        .post(&url)
        .header("Accept", "text/event-stream")
        .json(&input2)
        .send()
        .await
        .expect("post 2");
    drop(resp2);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // tools/list must now return [estimate_order]. The registry is
    // authoritative even if the agent caches; this test pins that
    // contract.
    let body: Value = http
        .post(&mcp_url)
        .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
        .send()
        .await
        .expect("list 2")
        .json()
        .await
        .expect("parse 2");
    let names: Vec<String> = body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["estimate_order".to_string()], "got {names:?}");
}

#[tokio::test]
async fn frontend_tool_initialize_works_for_unknown_thread() {
    // A defensive property: even before any AG-UI run has populated the
    // thread, MCP `initialize` must succeed. opencode probes initialize
    // before knowing whether tools/list will be useful, so a hard error
    // here breaks the connection.
    let (bound, _captured) = spawn_bridge().await;
    let mcp_url = format!("http://{}/mcp/never-prompted", bound);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let body: Value = http
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
        .expect("parse");
    assert!(body.get("result").is_some(), "got {body}");
    assert_eq!(body["result"]["serverInfo"]["name"], "agui-acp-bridge");
}
