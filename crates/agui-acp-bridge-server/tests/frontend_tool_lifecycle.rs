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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::BridgeConfig;
use agui_acp_bridge_core::BridgeError;
use agui_acp_bridge_core::acp::CustomAgentInProcessClient;
use agui_acp_bridge_server::{AcpClient, BridgeAppState, build_router, test_agents};
use agui_rs_core::types::{Message, RunAgentInput, Tool, UserMessage, UserMessageContent};
use serde_json::{Value, json};
use tokio::io::DuplexStream;
use tokio::net::TcpListener;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

#[path = "frontend_tool_lifecycle/fixture.rs"]
mod fixture;
