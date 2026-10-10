use super::*;

// --------------------------------------------------------------------------
// Mock agent that exercises the bridge's MCP endpoint.
// --------------------------------------------------------------------------

/// Cell capturing the MCP URL the bridge advertised in `NewSessionRequest`.
/// Filled when the agent receives `session/new`; read on the matching prompt.
pub(super) type McpUrlCell = Arc<TokioMutex<Option<String>>>;

#[derive(Clone, Default)]
pub(super) struct ImmediateCallSignals {
    pub(super) prompt_started: Arc<Notify>,
    pub(super) call_started: Arc<Notify>,
}

pub(super) async fn run_mcp_using_agent(
    stream: DuplexStream,
    captured_url: McpUrlCell,
    immediate_call: bool,
    signals: Option<ImmediateCallSignals>,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("frontend-tool-mock")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new()
                            // Crucial: tell the bridge we accept HTTP MCP
                            // servers via `mcp_servers`.
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
                    // Capture the first HTTP MCP server URL the bridge gave us.
                    // We don't connect here — opening MCP synchronously inside
                    // the `session/new` handler would block the ACP dispatch
                    // loop unnecessarily, and the real flow is per-prompt
                    // anyway.
                    for server in &req.mcp_servers {
                        if let McpServer::Http(http) = server {
                            let mut slot = captured_url.lock().await;
                            slot.replace(http.url.clone());
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
                    // Drive the MCP flow and surface the tool result as an
                    // agent text message. The regression variant skips
                    // tools/list to issue tools/call as soon as prompt handling
                    // starts.
                    let url = {
                        let slot = captured_url.lock().await;
                        slot.clone()
                    };
                    let url = match url {
                        Some(u) => u,
                        None => {
                            // No MCP server configured — bail with a chunk
                            // explaining why so the test can fail loudly.
                            cx.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(
                                        "AGENT_BUG: no mcp_servers entry was given; \
                                     bridge did not advertise the URL",
                                    )),
                                )),
                            ))?;
                            return responder.respond(PromptResponse::new(StopReason::EndTurn));
                        }
                    };

                    if let Some(signals) = &signals {
                        signals.prompt_started.notify_one();
                    }

                    let client = match build_mcp_client(&url).await {
                        Ok(c) => c,
                        Err(e) => {
                            cx.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(format!(
                                        "MCP_INITIALIZE_FAILED: {e}"
                                    ))),
                                )),
                            ))?;
                            return responder.respond(PromptResponse::new(StopReason::EndTurn));
                        }
                    };

                    let first = if immediate_call {
                        // The regression path deliberately skips tools/list:
                        // once prompt handling starts, the agent calls the
                        // known frontend tool immediately.
                        "say_hello".to_string()
                    } else {
                        let tools = match client.tools_list().await {
                            Ok(t) => t,
                            Err(e) => {
                                cx.send_notification(SessionNotification::new(
                                    req.session_id.clone(),
                                    SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(format!(
                                            "MCP_TOOLS_LIST_FAILED: {e}"
                                        ))),
                                    )),
                                ))?;
                                return responder.respond(PromptResponse::new(StopReason::EndTurn));
                            }
                        };

                        // Surface the tools/list result as a marker line so the
                        // test can verify it independently of tools/call.
                        let tool_names: Vec<String> = tools
                            .iter()
                            .filter_map(|t| {
                                t.get("name").and_then(|n| n.as_str()).map(String::from)
                            })
                            .collect();
                        cx.send_notification(SessionNotification::new(
                            req.session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!(
                                    "TOOLS={}",
                                    tool_names.join(",")
                                ))),
                            )),
                        ))?;

                        // Use the first tool. The test always provides exactly one.
                        let Some(first) = tool_names.first().cloned() else {
                            cx.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new("NO_TOOLS_AVAILABLE")),
                                )),
                            ))?;
                            return responder.respond(PromptResponse::new(StopReason::EndTurn));
                        };
                        first
                    };

                    if let Some(signals) = &signals {
                        signals.call_started.notify_one();
                    }

                    let call_result =
                        match client.tools_call(&first, json!({"name": "world"})).await {
                            Ok(v) => v,
                            Err(e) => {
                                cx.send_notification(SessionNotification::new(
                                    req.session_id.clone(),
                                    SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                        ContentBlock::Text(TextContent::new(format!(
                                            "MCP_TOOLS_CALL_FAILED: {e}"
                                        ))),
                                    )),
                                ))?;
                                return responder.respond(PromptResponse::new(StopReason::EndTurn));
                            }
                        };

                    // Echo the tool result back as a text chunk that includes
                    // a recognisable prefix the test asserts on. The
                    // `TOOL_IS_ERROR` marker surfaces the MCP envelope's
                    // `isError` flag so error-propagation tests can pin it.
                    let is_error = call_result
                        .get("isError")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let echo = call_result
                        .get("content")
                        .and_then(|c| c.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|first| first.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("<no text content>")
                        .to_string();
                    cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!(
                                "TOOL_RESULT={echo}\nTOOL_IS_ERROR={is_error}"
                            )),
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

// --------------------------------------------------------------------------
// Tiny MCP-over-HTTP client used by the mock agent.
// --------------------------------------------------------------------------

struct McpClient {
    url: String,
    http: reqwest::Client,
    next_id: std::sync::atomic::AtomicI64,
}

async fn build_mcp_client(url: &str) -> Result<McpClient, String> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("reqwest build: {e}"))?;
    let client = McpClient {
        url: url.to_string(),
        http,
        next_id: std::sync::atomic::AtomicI64::new(1),
    };
    let _ = client
        .request(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name":"test-agent","version":"0"}
            }),
        )
        .await?;
    // Best-effort send the initialized notification (no id, no body
    // expected back). Errors are tolerated.
    let _ = client.notify("notifications/initialized").await;
    Ok(client)
}

impl McpClient {
    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body = json!({
            "jsonrpc":"2.0",
            "id": id,
            "method": method,
            "params": params,
        });
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
            return Err(format!("{method} returned HTTP {status}: {json}"));
        }
        if let Some(err) = json.get("error") {
            return Err(format!("{method} JSON-RPC error: {err}"));
        }
        json.get("result")
            .cloned()
            .ok_or_else(|| format!("{method} response missing result: {json}"))
    }

    async fn notify(&self, method: &str) -> Result<(), String> {
        let body = json!({
            "jsonrpc":"2.0",
            "method": method,
            "params": {}
        });
        let resp = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("notify {method}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("notify {method} HTTP {}", resp.status()));
        }
        Ok(())
    }

    async fn tools_list(&self) -> Result<Vec<Value>, String> {
        let r = self.request("tools/list", json!({})).await?;
        let tools = r
            .get("tools")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| format!("missing tools array: {r}"))?;
        Ok(tools)
    }

    async fn tools_call(&self, name: &str, arguments: Value) -> Result<Value, String> {
        self.request(
            "tools/call",
            json!({ "name": name, "arguments": arguments }),
        )
        .await
    }
}
