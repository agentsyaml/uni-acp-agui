//! Focused ACP terminal-lane integration tests.

// Linux exercises the descriptor-relative cwd and pidfd/process-group paths.
// Windows uses the job-object path in `terminal.rs` and is not exercised by
// this host's focused test lane. Non-Linux/non-Windows runtimes are likewise
// unverified and fail closed because no equivalent cwd backend is initialized.
#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CreateTerminalRequest, EnvVariable, InitializeRequest, InitializeResponse,
    KillTerminalRequest, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
    ReleaseTerminalRequest, SessionId, StopReason, TerminalId, TerminalOutputRequest,
    WaitForTerminalExitRequest,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::{
    BridgeConfig, BridgeError, PermissionDecision, PermissionPolicy, SessionConfig,
    spawn_in_process_session_with,
};
use async_trait::async_trait;
use tokio::io::DuplexStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[path = "terminal/fixture.rs"]
mod fixture;
