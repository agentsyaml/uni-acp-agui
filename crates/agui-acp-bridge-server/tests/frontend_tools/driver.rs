use super::agent_fixture::{ImmediateCallSignals, McpUrlCell, run_mcp_using_agent};
use super::*;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct ToolCallTracker(HashMap<String, (String, String)>);

impl ToolCallTracker {
    pub(super) fn contains(&self, id: &str) -> bool {
        self.0.contains_key(id)
    }

    pub(super) fn start(&mut self, id: String, name: String) {
        self.0.insert(id, (name, String::new()));
    }

    pub(super) fn args(&mut self, id: &str, delta: &str) {
        if let Some((_, args)) = self.0.get_mut(id) {
            args.push_str(delta);
        }
    }

    pub(super) fn end(&mut self, id: &str) -> Option<(String, Value)> {
        let (name, args) = self.0.remove(id)?;
        let args = if args.is_empty() {
            json!({})
        } else {
            serde_json::from_str(&args).unwrap_or_else(|error| {
                panic!("invalid complete TOOL_CALL_ARGS JSON for {id}: {error}")
            })
        };
        Some((name, args))
    }
}

// --------------------------------------------------------------------------
// Test harness: bind a real bridge, run the mock agent, drive the run.
// --------------------------------------------------------------------------

pub(super) async fn spawn_bridge() -> (SocketAddr, McpUrlCell) {
    spawn_bridge_with_agent(false, None).await
}

pub(super) async fn spawn_immediate_bridge(
    signals: ImmediateCallSignals,
) -> (SocketAddr, McpUrlCell) {
    spawn_bridge_with_agent(true, Some(signals)).await
}

async fn spawn_bridge_with_agent(
    immediate_call: bool,
    signals: Option<ImmediateCallSignals>,
) -> (SocketAddr, McpUrlCell) {
    let captured_url: McpUrlCell = Arc::new(TokioMutex::new(None));
    let captured_url_factory = captured_url.clone();
    let signals_factory = signals.clone();

    // The factory is invoked each time the bridge opens an ACP session.
    // For these tests every run uses the same thread, so the agent
    // factory is invoked exactly once.
    let factory = move |stream: DuplexStream| {
        let captured = captured_url_factory.clone();
        let signals = signals_factory.clone();
        async move { run_mcp_using_agent(stream, captured, immediate_call, signals).await }
    };
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(factory));

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let bound = listener.local_addr().expect("local_addr");
    let self_url = format!("http://{}", bound);

    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_self_url(self_url.clone())
        .build();
    let app = build_router(state);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("bridge axum::serve error: {e}");
        }
    });

    (bound, captured_url)
}

pub(super) fn input_with_tool(thread: &str, run: &str, tool: Tool) -> RunAgentInput {
    let mut input = RunAgentInput::new(thread, run);
    input.tools.push(tool);
    input.messages.push(Message::User(UserMessage {
        id: "u1".into(),
        content: UserMessageContent::Text("please call the say_hello tool with name=world".into()),
        name: None,
        encrypted_value: None,
    }));
    input
}

pub(super) fn say_hello_tool() -> Tool {
    Tool {
        name: "say_hello".into(),
        description: "Greet the supplied name.".into(),
        parameters: json!({
            "type":"object",
            "properties":{"name":{"type":"string"}},
            "required":["name"],
        }),
        metadata: None,
    }
}

/// Closure type used by `drive_run` to react to a tool call. Returns a
/// fresh `(result, is_error)` pair on each invocation so the test can
/// express non-deterministic results — including *failing* handlers,
/// which the bridge must surface as an MCP `isError` envelope.
pub(super) type OnToolCall = Box<
    dyn Fn(String, String, Option<Value>) -> Box<dyn Fn() -> (Value, bool) + Send> + Send + Sync,
>;

/// Subscribe to SSE, parse `data: {...}` lines as events, drive a
/// `/tool-response` POST when a `TOOL_CALL_END` arrives. Collects all
/// event types in order and stops when `RUN_FINISHED` or `RUN_ERROR`
/// arrives, or after the supplied deadline.
///
/// Returns the event-type list plus the *agent-visible* text transcript:
/// every `TEXT_MESSAGE_CONTENT` delta concatenated in order. The mock
/// agent echoes tool results (and error text) into that transcript via
/// `TOOL_RESULT=...` / `TOOL_IS_ERROR=...` marker lines, so assertions on
/// what the agent actually received go through this string.
pub(super) async fn drive_run(
    bound: SocketAddr,
    input: &RunAgentInput,
    on_tool_call: OnToolCall,
) -> (Vec<String>, String) {
    use futures::StreamExt;

    let url = format!("http://{}/", bound);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let resp = http
        .post(&url)
        .header("Accept", "text/event-stream")
        .json(input)
        .send()
        .await
        .expect("POST /");
    assert!(resp.status().is_success(), "status={}", resp.status());

    let mut stream = resp.bytes_stream();
    let mut buf = Vec::<u8>::new();
    let mut events: Vec<String> = Vec::new();
    // Collect independently because ACP can interleave tool-call events.
    let mut calls = ToolCallTracker::default();
    // All agent-visible text content, concatenated in arrival order.
    let mut agent_text = String::new();

    let tool_response_url = format!("http://{}/tool-response", bound);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    while tokio::time::Instant::now() < deadline {
        let chunk = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        match chunk {
            Ok(Some(Ok(b))) => buf.extend_from_slice(&b),
            Ok(Some(Err(e))) => panic!("SSE chunk error: {e}"),
            Ok(None) => break,
            Err(_) => continue, // tick — keep checking deadline
        }

        // SSE frames are delimited by blank lines (`\n\n`).
        while let Some(idx) = find_double_newline(&buf) {
            let frame = String::from_utf8_lossy(&buf[..idx]).into_owned();
            buf.drain(..idx + 2);
            for line in frame.lines() {
                let Some(payload) = line.strip_prefix("data:") else {
                    continue;
                };
                let payload = payload.trim_start();
                let Ok(event): Result<Value, _> = serde_json::from_str(payload) else {
                    continue;
                };
                let ty = event
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<no-type>")
                    .to_string();
                events.push(ty.clone());
                if ty == "TEXT_MESSAGE_CONTENT"
                    && let Some(delta) = event.get("delta").and_then(|v| v.as_str())
                {
                    agent_text.push_str(delta);
                }
                match ty.as_str() {
                    "TOOL_CALL_START" => {
                        let id = event
                            .get("toolCallId")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let name = event
                            .get("toolCallName")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        calls.start(id, name);
                    }
                    "TOOL_CALL_ARGS" => {
                        if let (Some(id), Some(delta)) = (
                            event.get("toolCallId").and_then(Value::as_str),
                            event.get("delta").and_then(Value::as_str),
                        ) {
                            calls.args(id, delta);
                        }
                    }
                    "TOOL_CALL_END" => {
                        let id = event
                            .get("toolCallId")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some((name, parsed_args)) = calls.end(id) {
                            let factory = on_tool_call(id.to_string(), name, Some(parsed_args));
                            let (result, is_error) = factory();
                            let body = json!({
                                "threadId": input.thread_id,
                                "toolCallId": id,
                                "content": result.to_string(),
                                "isError": is_error,
                            });
                            let r = http
                                .post(&tool_response_url)
                                .json(&body)
                                .send()
                                .await
                                .expect("tool-response");
                            assert!(
                                r.status().is_success(),
                                "tool-response status={}",
                                r.status()
                            );
                        }
                    }
                    "RUN_FINISHED" | "RUN_ERROR" => return (events, agent_text),
                    _ => {}
                }
            }
        }
    }

    (events, agent_text)
}

fn find_double_newline(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

/// Extract the value after a `MARKER=` tag in the agent-visible transcript.
/// Chunks concatenate without separators, so we cannot rely on line splits.
pub(super) fn marker_value<'a>(transcript: &'a str, marker: &str) -> Option<&'a str> {
    let start = transcript.find(marker)? + marker.len();
    let end = transcript[start..]
        .find('\n')
        .map_or(transcript.len(), |i| start + i);
    Some(&transcript[start..end])
}
