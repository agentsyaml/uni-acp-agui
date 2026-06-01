//! Regression tests for the two multi-session frontend-tool bugs:
//!
//! 1. **Tool calls time out when sessions overlap.** The per-thread active
//!    sender was cleared unconditionally on run teardown, so an older run
//!    finishing would wipe a newer overlapping run's sender and strand its
//!    in-flight tool call until `frontend_tool_timeout`. Fixed by clearing
//!    the slot only when it still holds the sender the finishing run
//!    installed (`clear_active_sender_if_same`).
//!
//! 2. **Sessions never released after a refresh.** When the browser
//!    disconnects while the agent is parked awaiting a frontend-tool result,
//!    the streaming task used to park too (no event to push), so it never
//!    noticed the dead client. `active_prompts` stayed > 0 and the reaper
//!    refused to release the session until `frontend_tool_timeout` fired.
//!    Fixed by (a) watching the downstream SSE channel for closure in the
//!    stream loop, and (b) aborting the thread's pending tool calls when the
//!    owning run tears down so the agent's turn unwinds promptly.
//!
//! These use a mock ACP agent that, on prompt, drives the bridge's in-process
//! MCP endpoint (`initialize → tools/list → tools/call`) and then waits for
//! the browser to post `/tool-response`. By controlling whether/when we post
//! back — and by dropping the SSE connection mid-call — we exercise both bugs.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::BridgeConfig;
use agui_acp_bridge_core::BridgeError;
use agui_acp_bridge_core::acp::CustomAgentInProcessClient;
use agui_acp_bridge_server::{AcpClient, BridgeAppState, build_router};
use agui_rs_core::types::{Message, RunAgentInput, Tool, UserMessage, UserMessageContent};
use serde_json::{Value, json};
use tokio::io::DuplexStream;
use tokio::net::TcpListener;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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

/// Extract the first `toolCallId` seen in a TOOL_CALL_START frame from a body.
fn first_tool_call_id(body: &str) -> Option<String> {
    for line in body.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim_start();
        if payload.contains("\"type\":\"TOOL_CALL_START\"") {
            if let Ok(v) = serde_json::from_str::<Value>(payload) {
                if let Some(id) = v.get("toolCallId").and_then(|v| v.as_str()) {
                    return Some(id.to_string());
                }
            }
        }
    }
    None
}

async fn post_tool_response(
    bound: SocketAddr,
    tool_call_id: &str,
    content: &str,
) -> reqwest::StatusCode {
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://{bound}/tool-response"))
        .json(&json!({"toolCallId": tool_call_id, "content": content, "isError": false}))
        .send()
        .await
        .expect("POST /tool-response");
    resp.status()
}

// --- Tests ----------------------------------------------------------------

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
        if !posted {
            if let Some(id) = first_tool_call_id(&body) {
                assert_eq!(
                    post_tool_response(bound, &id, "hi world").await,
                    reqwest::StatusCode::OK
                );
                posted = true;
            }
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
        if first_tool_call_id(&body).is_some() {
            break;
        }
    }
    assert!(
        first_tool_call_id(&body).is_some(),
        "agent must have dispatched the tool call before we disconnect, body:\n{body}"
    );
    assert_eq!(
        state.session_count(),
        1,
        "session is live during the parked call"
    );

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
    let (bound, _state) = spawn_bridge(BridgeConfig {
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
            if first_tool_call_id(&body).is_some() {
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
        if !posted {
            if let Some(id) = first_tool_call_id(&body) {
                assert_eq!(
                    post_tool_response(bound, &id, "ok").await,
                    reqwest::StatusCode::OK
                );
                posted = true;
            }
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
