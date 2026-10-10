use super::*;

type McpUrlCell = Arc<TokioMutex<Option<String>>>;

/// Mock agent: on prompt, connects to the bridge's MCP endpoint and calls
/// the first advertised tool, blocking on its result (which only arrives
/// when the browser posts `/tool-response`). On success it emits a
/// `TOOL_RESULT=<text>` chunk so tests can assert resolution.
async fn run_tool_calling_agent(
    stream: DuplexStream,
    captured_url: McpUrlCell,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("tool-calling-mock")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new()
                            .mcp_capabilities(McpCapabilities::new().http(true)),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let captured_url = captured_url.clone();
                async move |req: NewSessionRequest, responder, _cx| {
                    for server in &req.mcp_servers {
                        if let McpServer::Http(http) = server {
                            captured_url.lock().await.replace(http.url.clone());
                            break;
                        }
                    }
                    responder.respond(NewSessionResponse::new(SessionId::from(
                        Uuid::new_v4().to_string(),
                    )))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let captured_url = captured_url.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let url = captured_url.lock().await.clone();
                    let Some(url) = url else {
                        cx.send_notification(SessionNotification::new(
                            req.session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new("AGENT_BUG: no mcp url")),
                            )),
                        ))?;
                        return responder.respond(PromptResponse::new(StopReason::EndTurn));
                    };

                    let client = match build_mcp_client(&url).await {
                        Ok(c) => c,
                        Err(e) => {
                            cx.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(format!(
                                        "MCP_INIT_FAIL: {e}"
                                    ))),
                                )),
                            ))?;
                            return responder.respond(PromptResponse::new(StopReason::EndTurn));
                        }
                    };

                    let tools = client.tools_list().await.unwrap_or_default();
                    let Some(first) = tools
                        .iter()
                        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
                        .next()
                        .map(String::from)
                    else {
                        cx.send_notification(SessionNotification::new(
                            req.session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new("NO_TOOLS")),
                            )),
                        ))?;
                        return responder.respond(PromptResponse::new(StopReason::EndTurn));
                    };

                    // This blocks until the browser posts /tool-response, the
                    // run tears down (abort), or frontend_tool_timeout fires.
                    let call_result = match client
                        .tools_call(&first, json!({"name": "world"}))
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            cx.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(format!("CALL_FAIL: {e}"))),
                                )),
                            ))?;
                            return responder.respond(PromptResponse::new(StopReason::EndTurn));
                        }
                    };

                    let echo = call_result
                        .get("content")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|first| first.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("<none>")
                        .to_string();
                    cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!("TOOL_RESULT={echo}")),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)?;
    Ok(())
}

// --- Tiny MCP client (subset) --------------------------------------------

struct McpClient {
    url: String,
    http: reqwest::Client,
    next_id: std::sync::atomic::AtomicI64,
}

async fn build_mcp_client(url: &str) -> Result<McpClient, String> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| format!("reqwest build: {e}"))?;
    let client = McpClient {
        url: url.to_string(),
        http,
        next_id: std::sync::atomic::AtomicI64::new(1),
    };
    client
        .request(
            "initialize",
            json!({"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"0"}}),
        )
        .await?;
    Ok(client)
}

impl McpClient {
    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let resp = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("send {method}: {e}"))?;
        let status = resp.status();
        let json: Value = resp
            .json()
            .await
            .map_err(|e| format!("parse {method}: {e}"))?;
        if !status.is_success() {
            return Err(format!("{method} HTTP {status}: {json}"));
        }
        if let Some(err) = json.get("error") {
            return Err(format!("{method} JSON-RPC error: {err}"));
        }
        json.get("result")
            .cloned()
            .ok_or_else(|| format!("{method} missing result: {json}"))
    }

    async fn tools_list(&self) -> Result<Vec<Value>, String> {
        let r = self.request("tools/list", json!({})).await?;
        Ok(r.get("tools")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default())
    }

    async fn tools_call(&self, name: &str, arguments: Value) -> Result<Value, String> {
        self.request("tools/call", json!({"name":name,"arguments":arguments}))
            .await
    }
}

// --- Test harness ---------------------------------------------------------

async fn spawn_bridge(config: BridgeConfig) -> (SocketAddr, BridgeAppState) {
    let captured_url: McpUrlCell = Arc::new(TokioMutex::new(None));
    let captured = captured_url.clone();
    let factory = move |stream: DuplexStream| {
        let captured = captured.clone();
        async move { run_tool_calling_agent(stream, captured).await }
    };
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(factory));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let bound = listener.local_addr().expect("addr");
    let self_url = format!("http://{bound}");

    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_self_url(self_url)
        .with_config(config)
        .build();
    state.spawn_reaper();
    let app = build_router(state.clone());

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (bound, state)
}

async fn spawn_replacement_bridge(config: BridgeConfig) -> (SocketAddr, BridgeAppState) {
    let captured_url: McpUrlCell = Arc::new(TokioMutex::new(None));
    let captured = captured_url.clone();
    let opens = Arc::new(AtomicUsize::new(0));
    let opens_for_factory = opens.clone();
    let factory = move |stream: DuplexStream| {
        let captured = captured.clone();
        let first = opens_for_factory.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            if first {
                test_agents::run_unresponsive_setting_agent(stream).await
            } else {
                run_tool_calling_agent(stream, captured).await
            }
        }
    };
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(factory));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let bound = listener.local_addr().expect("addr");
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_self_url(format!("http://{bound}"))
        .with_config(config)
        .build();
    state.spawn_reaper();
    let app = build_router(state.clone());

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (bound, state)
}

fn input_with_tool(thread: &str, run: &str) -> RunAgentInput {
    let mut input = RunAgentInput::new(thread, run);
    input.tools.push(Tool {
        name: "say_hello".into(),
        description: "Greet the supplied name.".into(),
        parameters: json!({"type":"object","properties":{"name":{"type":"string"}}}),
        metadata: None,
    });
    input.messages.push(Message::User(UserMessage {
        id: "u1".into(),
        content: UserMessageContent::Text("call say_hello".into()),
        name: None,
        encrypted_value: None,
    }));
    input
}

/// Read the SSE stream until `needle` appears in the body or the deadline
/// passes. Returns the accumulated body. The `reqwest::Response` is consumed
/// by streaming; drop it (via the returned guard) to simulate disconnect.
async fn post_run(bound: SocketAddr, input: &RunAgentInput) -> reqwest::Response {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    http.post(format!("http://{bound}/"))
        .header("Accept", "text/event-stream")
        .json(input)
        .send()
        .await
        .expect("POST /")
}

async fn post_tool_response(
    bound: SocketAddr,
    thread_id: &str,
    tool_call_id: &str,
    content: &str,
) -> reqwest::StatusCode {
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://{bound}/tool-response"))
        .json(&json!({"threadId": thread_id, "toolCallId": tool_call_id, "content": content, "isError": false}))
        .send()
        .await
        .expect("POST /tool-response");
    resp.status()
}

async fn drain_run(bound: SocketAddr, input: &RunAgentInput) -> String {
    use futures::StreamExt;

    let mut stream = post_run(bound, input).await.bytes_stream();
    let mut body = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => body.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(error))) => panic!("SSE chunk error: {error}"),
            Ok(None) => break,
            Err(_) => {}
        }
        if body.contains("\"type\":\"RUN_FINISHED\"") || body.contains("\"type\":\"RUN_ERROR\"") {
            break;
        }
    }
    body
}

async fn run_with_tool_response(
    bound: SocketAddr,
    state: &BridgeAppState,
    input: &RunAgentInput,
) -> String {
    use futures::StreamExt;

    let mut stream = post_run(bound, input).await.bytes_stream();
    let mut body = String::new();
    let mut posted = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            Ok(Some(Ok(chunk))) => body.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(error))) => panic!("SSE chunk error: {error}"),
            Ok(None) => break,
            Err(_) => {}
        }
        if !posted && let Some(tool_call_id) = first_ended_tool_call_id(&body) {
            assert!(
                state.frontend_tools().pending_len(&input.thread_id) > 0,
                "MCP call must still be pending at TOOL_CALL_END"
            );
            assert_eq!(
                post_tool_response(bound, &input.thread_id, &tool_call_id, "hi world").await,
                reqwest::StatusCode::OK
            );
            posted = true;
        }
        if body.contains("\"type\":\"RUN_FINISHED\"") || body.contains("\"type\":\"RUN_ERROR\"") {
            break;
        }
    }
    assert!(
        posted,
        "replacement run must emit a frontend tool call: {body}"
    );
    body
}

fn first_ended_tool_call_id(body: &str) -> Option<String> {
    for line in body.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(payload.trim_start()) else {
            continue;
        };
        if event["type"] == "TOOL_CALL_END" {
            return event["toolCallId"].as_str().map(str::to_owned);
        }
    }
    None
}

#[path = "tests.rs"]
mod tests;
