//! `agui-acp-bridge` — command-line entry point.
//!
//! Wraps an ACP agent (or the built-in echo agent) as an AG-UI HTTP/SSE
//! endpoint. The defaults are tuned for local development; flags expose every
//! production-relevant knob (port, working directory, permission policy,
//! timeouts, event buffer size).
//!
//! # Usage
//!
//! ```text
//! # In-process echo agent (no external binary required).
//! agui-acp-bridge --in-process
//!
//! # Wrap a real ACP agent.
//! agui-acp-bridge ./path/to/my-agent --policy auto-allow
//!
//! # Wrap an agent that takes its own arguments.
//! agui-acp-bridge --policy allowlist --allow "Read file" --allow "List directory" \
//!     -- ./my-agent --foo bar
//! ```
//!
//! Use `agui-acp-bridge --help` for the full flag list.

use std::net::IpAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use agui_acp_bridge_core::PermissionPolicy;
use agui_acp_bridge_policy::{
    Allowlist, AutoAllow, AutoDeny, FilesystemAccessPolicy, InterruptViaAgUiEvent,
};
use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use runtime::run;

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum PolicyKind {
    /// Approve every `requestPermission` automatically.
    AutoAllow,
    /// Reject every `requestPermission`.
    AutoDeny,
    /// Approve only tool calls whose `title` is in the `--allow` set.
    Allowlist,
    /// Defer decisions to the AG-UI client via a custom event.
    Interrupt,
}

impl PolicyKind {
    fn build(self, allowlist: Vec<String>) -> Result<Arc<dyn PermissionPolicy>> {
        Ok(match self {
            PolicyKind::AutoAllow => Arc::new(AutoAllow),
            PolicyKind::AutoDeny => Arc::new(AutoDeny),
            PolicyKind::Allowlist => {
                if allowlist.is_empty() {
                    bail!("--policy allowlist requires at least one --allow <TITLE>");
                }
                Arc::new(Allowlist::new(allowlist))
            }
            PolicyKind::Interrupt => Arc::new(InterruptViaAgUiEvent),
        })
    }
}

fn apply_filesystem_flags(
    policy: Arc<dyn PermissionPolicy>,
    allow_read: bool,
    allow_write: bool,
) -> Arc<dyn PermissionPolicy> {
    if allow_read || allow_write {
        Arc::new(FilesystemAccessPolicy::new(policy, allow_read, allow_write))
    } else {
        policy
    }
}

/// AG-UI ⇄ ACP bridge.
///
/// Starts an HTTP/SSE server that translates AG-UI `RunAgentInput` POSTs into
/// ACP `prompt` turns and streams the response back as AG-UI events.
#[derive(Debug, Parser)]
#[command(name = "agui-acp-bridge", version, about, long_about = None)]
struct Cli {
    /// Run with an in-process echo agent (no external binary).
    ///
    /// Mutually exclusive with `<AGENT_COMMAND>`. Useful for local frontend
    /// development and smoke tests.
    #[arg(long, conflicts_with = "agent_command")]
    in_process: bool,

    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// Permit binding a non-loopback address without
    /// `AGUI_ACP_BRIDGE_TOKEN`. Unsafe for production; disabled by default.
    #[arg(long)]
    allow_unauthenticated_non_loopback: bool,

    /// Bind port.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Working directory passed to each ACP session as the initial cwd.
    #[arg(short = 'w', long, default_value = ".")]
    cwd: PathBuf,

    /// Permission policy applied to ACP `requestPermission` requests.
    #[arg(long, value_enum, default_value_t = PolicyKind::AutoDeny)]
    policy: PolicyKind,

    /// Tool-call titles approved by `--policy allowlist`. Repeat to allow
    /// multiple titles, or pass a comma-separated list.
    #[arg(long, value_delimiter = ',')]
    allow: Vec<String>,

    /// Allow live ACP sessions to read text files under `--cwd`.
    #[arg(long = "allow-fs-read")]
    allow_fs_read: bool,

    /// Allow live ACP sessions to create or overwrite text files under `--cwd`.
    #[arg(long = "allow-fs-write")]
    allow_fs_write: bool,

    /// Permission-request timeout in seconds. Applies to `--policy interrupt`
    /// (deferred decisions); after this many seconds with no resolution the
    /// bridge responds Cancelled to the agent.
    #[arg(long, default_value_t = 300)]
    permission_timeout: u64,

    /// Idle-session timeout in seconds. Sessions whose `last_used` instant
    /// is older than this are dropped by the background reaper, terminating
    /// the underlying actor (and, for subprocess clients, killing the child).
    /// In-flight prompts are never reaped regardless of this value.
    #[arg(long, default_value_t = 120)]
    idle_timeout: u64,

    /// Session-creation timeout in seconds. If the agent does not finish
    /// the ACP handshake within this many seconds the bridge returns an
    /// error to the client rather than hanging the request.
    #[arg(long, default_value_t = 30)]
    open_session_timeout: u64,

    /// Session setting request timeout in seconds. If the agent does not
    /// respond within this budget the bridge returns 408 to the HTTP caller.
    /// Independent from `open_session_timeout` so config changes that take
    /// longer than session creation can be tuned separately.
    #[arg(long, default_value_t = 30)]
    set_session_timeout: u64,

    /// Grace period in seconds after `session/cancel` before the bridge
    /// closes and evicts an agent session that never returns a prompt result.
    #[arg(long, default_value_t = 5)]
    cancel_grace_timeout: u64,

    /// Per-prompt event-channel buffer size in `BridgeStreamItem`s.
    /// Tune up for very chatty agents; tune down to reduce memory.
    #[arg(long, default_value_t = 64)]
    event_buffer: usize,

    /// Maximum time in seconds each SSE event send waits for the downstream
    /// consumer. A stalled consumer cancels the current turn instead of
    /// pinning the session. Set to `0` only to disable this protection.
    #[arg(long, default_value_t = 30)]
    slow_consumer_timeout: u64,

    /// Frontend-tool response timeout in seconds. Applied by the
    /// in-process MCP endpoint when awaiting a browser POST to
    /// `/tool-response`. On timeout the bridge returns an MCP-side
    /// error to the agent so the LLM can react.
    #[arg(long, default_value_t = 120)]
    frontend_tool_timeout: u64,

    /// Maximum number of concurrently cached ACP sessions. When the cap is
    /// reached, opening a new session first evicts the least-recently-used
    /// idle session (one with no in-flight prompt), terminating its agent
    /// (and subprocess). If every cached session is busy, the new request is
    /// rejected before an ACP actor is opened. Set to `0` for unlimited. The
    /// default of 128 bounds resource usage when clients churn through many
    /// distinct `threadId`s (e.g. a browser minting a fresh thread on every
    /// page refresh).
    #[arg(long, default_value_t = 128)]
    max_sessions: usize,

    /// Maximum number of active plus queued prompt turns per ACP session.
    /// Set to `0` for explicit unlimited development mode. The default of 32
    /// rejects excess turns before they reach the actor command queue.
    #[arg(long, default_value_t = 32)]
    max_queued_turns: usize,

    /// Public URL the agent will use to reach the bridge's built-in MCP
    /// endpoint for `useFrontendTool`-style tool injection. Defaults to
    /// `http://<host>:<port>` derived from the bind address; override only
    /// when running behind a reverse proxy that rewrites the origin.
    /// Set to the empty string to *disable* frontend-tool injection.
    #[arg(long)]
    public_url: Option<String>,

    /// Explicit origin allowed on MCP requests. Repeat for multiple origins.
    /// Missing Origin headers remain allowed for non-browser agents; with no
    /// configured origins, every present Origin is rejected.
    #[arg(long = "mcp-allowed-origin")]
    mcp_allowed_origins: Vec<String>,

    /// Agent command and its arguments.
    ///
    /// The first positional value is the binary path; subsequent values are
    /// passed through as argv. Use `--` to separate bridge flags from agent
    /// flags when the agent takes its own `-` prefixed arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, num_args = 0..)]
    agent_command: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,agui_acp_bridge=debug".into()),
        )
        .init();

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Write the full error chain so users can diagnose without RUST_LOG.
            eprintln!("agui-acp-bridge: {e:#}");
            ExitCode::from(1)
        }
    }
}

mod runtime;
#[cfg(test)]
mod tests;
