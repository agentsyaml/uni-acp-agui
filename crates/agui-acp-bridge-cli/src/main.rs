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

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use agui_acp_bridge_core::{BridgeConfig, PermissionPolicy};
use agui_acp_bridge_policy::{Allowlist, AutoAllow, AutoDeny, InterruptViaAgUiEvent};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, InProcessAcpClient, ProcessAcpClient, build_router,
};
use anyhow::{Context as _, Result, bail};
use clap::{Parser, ValueEnum};
use tokio::net::TcpListener;
use tokio::signal;

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
    #[arg(long, default_value = "0.0.0.0")]
    host: IpAddr,

    /// Bind port.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Working directory passed to each ACP session as the initial cwd.
    #[arg(short = 'w', long, default_value = ".")]
    cwd: PathBuf,

    /// Permission policy applied to ACP `requestPermission` requests.
    #[arg(long, value_enum, default_value_t = PolicyKind::AutoAllow)]
    policy: PolicyKind,

    /// Tool-call titles approved by `--policy allowlist`. Repeat to allow
    /// multiple titles, or pass a comma-separated list.
    #[arg(long, value_delimiter = ',')]
    allow: Vec<String>,

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

    /// `session/set_mode` and `session/set_model` request timeout in
    /// seconds. If the agent does not respond within this budget the
    /// bridge returns 408 to the HTTP caller. Independent from
    /// `open_session_timeout` so model switches that take longer than
    /// session creation can be tuned separately.
    #[arg(long, default_value_t = 30)]
    set_session_timeout: u64,

    /// Per-prompt event-channel buffer size in `BridgeStreamItem`s.
    /// Tune up for very chatty agents; tune down to reduce memory.
    #[arg(long, default_value_t = 64)]
    event_buffer: usize,

    /// Frontend-tool response timeout in seconds. Applied by the
    /// in-process MCP endpoint when awaiting a browser POST to
    /// `/tool-response`. On timeout the bridge returns an MCP-side
    /// error to the agent so the LLM can react.
    #[arg(long, default_value_t = 120)]
    frontend_tool_timeout: u64,

    /// Maximum number of concurrently cached ACP sessions. When the cap is
    /// reached, opening a new session first evicts the least-recently-used
    /// idle session (one with no in-flight prompt), terminating its agent
    /// (and subprocess). Set to `0` for unlimited. The default of 128 bounds
    /// resource usage when clients churn through many distinct `threadId`s
    /// (e.g. a browser minting a fresh thread on every page refresh).
    #[arg(long, default_value_t = 128)]
    max_sessions: usize,

    /// Public URL the agent will use to reach the bridge's built-in MCP
    /// endpoint for `useFrontendTool`-style tool injection. Defaults to
    /// `http://<host>:<port>` derived from the bind address; override only
    /// when running behind a reverse proxy that rewrites the origin.
    /// Set to the empty string to *disable* frontend-tool injection.
    #[arg(long)]
    public_url: Option<String>,

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

async fn run(cli: Cli) -> Result<()> {
    if !cli.in_process && cli.agent_command.is_empty() {
        bail!(
            "no agent specified.\n\
             \n\
             Either pass an agent command:\n\
             \n    agui-acp-bridge ./path/to/agent\n\n\
             or use the in-process echo agent for local development:\n\
             \n    agui-acp-bridge --in-process\n"
        );
    }

    let client: Arc<dyn AcpClient> = if cli.in_process {
        Arc::new(InProcessAcpClient::new())
    } else {
        let mut iter = cli.agent_command.iter();
        let command = iter
            .next()
            .expect("agent_command non-empty (checked above)")
            .clone();
        let args: Vec<String> = iter.cloned().collect();
        let mut process = ProcessAcpClient::new(command);
        if !args.is_empty() {
            process = process.with_args(args);
        }
        Arc::new(process)
    };

    let policy = cli.policy.build(cli.allow.clone())?;

    let config = BridgeConfig {
        permission_timeout: Duration::from_secs(cli.permission_timeout),
        idle_timeout: Duration::from_secs(cli.idle_timeout),
        event_buffer: cli.event_buffer,
        open_session_timeout: Duration::from_secs(cli.open_session_timeout),
        frontend_tool_timeout: Duration::from_secs(cli.frontend_tool_timeout),
        set_session_timeout: Duration::from_secs(cli.set_session_timeout),
        max_sessions: cli.max_sessions,
    };

    let state = BridgeAppState::builder(client, cli.cwd.clone())
        .with_config(config)
        .with_policy(policy);

    // Frontend-tool injection is enabled by default. Users can opt out by
    // passing `--public-url ""`; otherwise we either honour the supplied
    // URL or compute one from the bind socket below (after we know the
    // actual port for `:0` binds).
    let state = match cli.public_url.as_deref() {
        Some("") => state, // explicit opt-out
        Some(url) => state.with_self_url(url),
        None => state, // filled in after bind; see below
    };

    // We need the bound address to compute the default self_url, so build
    // the listener first and then plumb the URL into the state.
    let addr = SocketAddr::new(cli.host, cli.port);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    let bound = listener.local_addr().unwrap_or(addr);

    let state = if cli.public_url.is_none() {
        // Default self_url uses the host the operator told us to bind on,
        // not the wildcard. If the user bound on `0.0.0.0` we substitute
        // `127.0.0.1` so the URL is meaningfully reachable; remote
        // deployments should pass `--public-url` explicitly.
        let host = if cli.host.is_unspecified() {
            "127.0.0.1".to_string()
        } else {
            cli.host.to_string()
        };
        state.with_self_url(format!("http://{host}:{}", bound.port()))
    } else {
        state
    };
    let state = state.build();

    // Background task that drops sessions whose last-used time exceeds
    // `idle_timeout`. The reaper holds a `Weak` to the inner state and
    // auto-exits when the last `BridgeAppState` clone drops, so no manual
    // shutdown is required.
    state.spawn_reaper();

    log_startup(&cli, bound);

    let router = build_router(state);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum::serve failed")?;

    tracing::info!("bridge stopped");
    Ok(())
}

fn log_startup(cli: &Cli, addr: SocketAddr) {
    let agent = if cli.in_process {
        "in-process echo agent".to_string()
    } else {
        cli.agent_command.join(" ")
    };
    tracing::info!(
        addr = %addr,
        agent = %agent,
        cwd = %cli.cwd.display(),
        policy = ?cli.policy,
        "agui-acp-bridge listening"
    );
    tracing::info!("POST RunAgentInput JSON to http://{addr}/  →  SSE stream");
}

/// Shut down on Ctrl-C (cross-platform) or SIGTERM (Unix only).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::warn!(error = %e, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("received Ctrl-C, shutting down"),
        () = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_parses_minimal_in_process() {
        let cli = Cli::parse_from(["agui-acp-bridge", "--in-process"]);
        assert!(cli.in_process);
        assert!(cli.agent_command.is_empty());
        assert_eq!(cli.port, 8080);
        assert!(matches!(cli.policy, PolicyKind::AutoAllow));
    }

    #[test]
    fn cli_parses_subprocess_with_args() {
        let cli = Cli::parse_from([
            "agui-acp-bridge",
            "--port",
            "9090",
            "--",
            "./agent",
            "--flag",
            "value",
        ]);
        assert!(!cli.in_process);
        assert_eq!(cli.port, 9090);
        assert_eq!(cli.agent_command, vec!["./agent", "--flag", "value"]);
    }

    #[test]
    fn cli_parses_allowlist_repeated_and_csv() {
        let cli = Cli::parse_from([
            "agui-acp-bridge",
            "--policy",
            "allowlist",
            "--allow",
            "Read file,List directory",
            "--allow",
            "Write file",
            "--in-process",
        ]);
        assert_eq!(cli.allow, vec!["Read file", "List directory", "Write file"]);
    }

    #[test]
    fn cli_rejects_in_process_with_command() {
        let res = Cli::try_parse_from(["agui-acp-bridge", "--in-process", "./agent"]);
        assert!(res.is_err(), "should reject conflicting args, got {res:?}");
    }

    #[test]
    fn cli_help_does_not_panic() {
        // Catch breakage in clap derive attribute combinations.
        let mut cmd = Cli::command();
        let _ = cmd.render_help();
    }

    #[tokio::test]
    async fn run_rejects_when_no_agent_specified() {
        let cli = Cli::parse_from(["agui-acp-bridge"]);
        let err = run(cli).await.expect_err("should error when no agent");
        assert!(
            err.to_string().contains("no agent specified"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn run_rejects_allowlist_without_titles() {
        let cli = Cli::parse_from(["agui-acp-bridge", "--in-process", "--policy", "allowlist"]);
        let err = run(cli).await.expect_err("allowlist needs --allow");
        assert!(
            err.to_string().contains("--allow"),
            "unexpected error: {err}"
        );
    }
}
