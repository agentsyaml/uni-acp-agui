//! Per-session ACP terminal supervision.
//!
//! Terminal identifiers and process state live below one `TerminalRegistry`,
//! which is created for each live ACP session. The registry deliberately has
//! no process-global state: a terminal from one session cannot be looked up by
//! another session.

mod cleanup;
mod cwd;
mod process_tree;
mod registry;
mod requests;
mod runtime;
mod supervisor;
#[cfg(windows)]
mod windows;

pub(crate) use process_tree::kill_acp_process_group;
pub(crate) use registry::TerminalRegistry;
pub(crate) use requests::{
    create_request, kill_request, output_request, release_request, wait_request,
};

const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const READ_CHUNK_BYTES: usize = 8192;
const CONTROL_BUFFER: usize = 8;
const POST_EXIT_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_TERMINALS_PER_SESSION: usize = 32;
const PROCESS_CLEANUP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(1);
const PROCESS_CLEANUP_ATTEMPTS: usize = 8;
const PROCESS_CLEANUP_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
#[cfg(unix)]
const CHILD_EXIT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);
#[cfg(test)]
mod tests;
#[cfg(test)]
use agent_client_protocol::schema::v1::CreateTerminalRequest;
#[cfg(test)]
use agent_client_protocol::schema::v1::TerminalOutputResponse;
#[cfg(test)]
use cleanup::retry_cleanup;
#[cfg(all(test, target_os = "linux"))]
use cwd::ApprovedCwd;
#[cfg(all(test, target_os = "linux"))]
use registry::wire_error;
#[cfg(test)]
use registry::{TerminalError, output_limit};
#[cfg(test)]
use runtime::TerminalState;
#[cfg(test)]
use std::io;
#[cfg(all(test, target_os = "linux"))]
use std::{path::PathBuf, process::Stdio};
#[cfg(all(test, target_os = "linux"))]
use tokio::process::Command;
