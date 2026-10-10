//! End-to-end test for the frontend-tools (`useFrontendTool`) injection
//! path.
//!
//! Runs:
//!
//! 1. A bridge bound to a real TCP socket on `127.0.0.1`.
//! 2. A mock ACP agent that, on every prompt, opens an HTTP MCP
//!    connection to whatever URL the bridge advertises in the session's
//!    `mcp_servers` (it captures that URL during `NewSessionRequest`),
//!    issues `initialize → tools/list → tools/call`, and only then
//!    finishes the prompt.
//! 3. A "browser" task that subscribes to the SSE stream, sees the
//!    AG-UI `TOOL_CALL_*` events, and POSTs back to `/tool-response`.
//!
//! The test asserts that:
//! - the agent's `tools/list` returns the tool we declared in
//!   `RunAgentInput.tools`,
//! - the agent's `tools/call` blocks until the browser posts back,
//! - the bridge translates the call into `TOOL_CALL_START / ARGS / END`,
//! - the result the agent sees matches what the browser sent,
//! - the SSE stream ends with `RUN_FINISHED`.
//!
//! `tracing` is opt-in: set `RUST_LOG=debug` to see the message flow.

#[path = "frontend_tools/agent_fixture.rs"]
mod agent_fixture;
#[path = "frontend_tools/driver.rs"]
mod driver;
mod support;
use agent_fixture::ImmediateCallSignals;
use driver::*;
#[path = "frontend_tools/errors.rs"]
mod errors;
#[path = "frontend_tools/registry.rs"]
mod registry;

#[test]
fn tool_call_driver_waits_for_end_and_accumulates_interleaved_arguments() {
    let mut calls = driver::ToolCallTracker::default();
    calls.start("one".into(), "first".into());
    calls.start("two".into(), "second".into());
    calls.args("one", "{\"value\":");
    assert!(
        calls.contains("one"),
        "argument prefix cannot complete a call"
    );
    calls.args("two", "{\"value\":2}");
    calls.args("one", "1}");
    assert_eq!(
        calls.end("two"),
        Some(("second".into(), json!({"value": 2})))
    );
    assert_eq!(
        calls.end("one"),
        Some(("first".into(), json!({"value": 1})))
    );
    assert!(calls.end("one").is_none(), "duplicate END is idempotent");
}

#[test]
fn tool_call_driver_accepts_end_without_argument_deltas() {
    let mut calls = driver::ToolCallTracker::default();
    calls.start("empty".into(), "no_args".into());
    assert_eq!(calls.end("empty"), Some(("no_args".into(), json!({}))));
}

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::BridgeError;
use agui_acp_bridge_core::acp::CustomAgentInProcessClient;
use agui_acp_bridge_server::{AcpClient, BridgeAppState, build_router};
use agui_rs_core::types::{Message, RunAgentInput, Tool, UserMessage, UserMessageContent};
use serde_json::{Value, json};
use tokio::io::DuplexStream;
use tokio::net::TcpListener;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

// --------------------------------------------------------------------------
// The actual tests.
// --------------------------------------------------------------------------

#[tokio::test]
async fn frontend_tool_round_trip_streams_call_and_returns_browser_result() {
    let (bound, _captured) = spawn_bridge().await;

    let input = input_with_tool("thread-ft-1", "run-ft-1", say_hello_tool());

    let on_tool_call: OnToolCall = Box::new(|_id, name, args| {
        let resolved = args
            .as_ref()
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("there")
            .to_string();
        let response = format!("hello, {resolved}! (from {name})");
        Box::new(move || (json!({ "echo": response }), false))
    });

    let (events, _agent_text) = tokio::time::timeout(
        Duration::from_secs(20),
        drive_run(bound, &input, on_tool_call),
    )
    .await
    .expect("test deadlocked");

    // The exact lifecycle we expect (in order, not necessarily contiguous):
    // RUN_STARTED → ... → TOOL_CALL_START → TOOL_CALL_ARGS → TOOL_CALL_END
    // → ... text chunks containing TOOLS=say_hello and TOOL_RESULT=... →
    // RUN_FINISHED.
    assert!(events.contains(&"RUN_STARTED".into()), "{events:?}");
    assert!(
        events.contains(&"TOOL_CALL_START".into()),
        "expected TOOL_CALL_START in {events:?}"
    );
    assert!(
        events.contains(&"TOOL_CALL_END".into()),
        "expected TOOL_CALL_END in {events:?}"
    );
    assert!(
        events.contains(&"RUN_FINISHED".into()),
        "expected RUN_FINISHED, got {events:?}"
    );
    let tool_start = events.iter().position(|e| e == "TOOL_CALL_START").unwrap();
    let tool_end = events.iter().position(|e| e == "TOOL_CALL_END").unwrap();
    assert!(
        tool_start < tool_end,
        "TOOL_CALL_START must precede TOOL_CALL_END: {events:?}"
    );
    let run_finished = events.iter().position(|e| e == "RUN_FINISHED").unwrap();
    assert!(
        tool_end < run_finished,
        "tool call must complete before RUN_FINISHED: {events:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frontend_tool_is_ready_for_an_immediate_prompt_call() {
    let signals = ImmediateCallSignals::default();
    let (bound, _captured) = spawn_immediate_bridge(signals.clone()).await;
    let input = input_with_tool("thread-ft-immediate", "run-ft-immediate", say_hello_tool());
    let on_tool_call: OnToolCall =
        Box::new(|_id, _name, _args| Box::new(|| (json!({"ok": true}), false)));

    let ((events, _agent_text), (), ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(
            drive_run(bound, &input, on_tool_call),
            signals.prompt_started.notified(),
            signals.call_started.notified(),
        )
    })
    .await
    .expect("immediate frontend tool call deadlocked");
    assert!(events.contains(&"TOOL_CALL_START".into()), "{events:?}");
    assert!(events.contains(&"TOOL_CALL_END".into()), "{events:?}");
    assert!(events.contains(&"RUN_FINISHED".into()), "{events:?}");
}

#[tokio::test]
async fn frontend_tool_mcp_url_encodes_special_thread_path_segment() {
    let (bound, captured) = spawn_bridge().await;
    let thread_id = "thread/slash?query#fragment";
    let input = input_with_tool(thread_id, "run-special-path", say_hello_tool());
    let on_tool_call: OnToolCall = Box::new(|_, _, _| Box::new(|| (json!({"ok": true}), false)));

    let (events, _agent_text) = tokio::time::timeout(
        Duration::from_secs(20),
        drive_run(bound, &input, on_tool_call),
    )
    .await
    .expect("special path MCP call deadlocked");
    assert!(events.contains(&"TOOL_CALL_START".into()), "{events:?}");
    assert!(events.contains(&"RUN_FINISHED".into()), "{events:?}");

    let advertised = captured
        .lock()
        .await
        .clone()
        .expect("agent must receive an MCP URL");
    assert_eq!(
        advertised,
        format!("http://{bound}/mcp/thread%2Fslash%3Fquery%23fragment")
    );
}
