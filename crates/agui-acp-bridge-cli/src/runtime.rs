use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::{Cli, apply_filesystem_flags};
use agui_acp_bridge_core::BridgeConfig;
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, InProcessAcpClient, ProcessAcpClient, build_router,
};
use anyhow::{Context as _, Result, bail};
use tokio::net::TcpListener;
use tokio::signal;

pub(super) async fn run(cli: Cli) -> Result<()> {
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

    // Auth pairing: the Rust server reads AGUI_ACP_BRIDGE_TOKEN; the demo
    // proxy (examples/copilotkit-acp-demo) reads AGUI_BRIDGE_TOKEN — set
    // both in production (same value).
    let bearer_token = match std::env::var("AGUI_ACP_BRIDGE_TOKEN") {
        Ok(token) => Some(token),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => bail!("failed to read AGUI_ACP_BRIDGE_TOKEN: {err}"),
    };
    require_non_loopback_auth(
        cli.host,
        bearer_token.is_some(),
        cli.allow_unauthenticated_non_loopback,
    )?;

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

    let policy = apply_filesystem_flags(
        cli.policy.build(cli.allow.clone())?,
        cli.allow_fs_read,
        cli.allow_fs_write,
    );

    let config = BridgeConfig {
        permission_timeout: Duration::from_secs(cli.permission_timeout),
        idle_timeout: Duration::from_secs(cli.idle_timeout),
        event_buffer: cli.event_buffer,
        slow_consumer_timeout: Duration::from_secs(cli.slow_consumer_timeout),
        open_session_timeout: Duration::from_secs(cli.open_session_timeout),
        frontend_tool_timeout: Duration::from_secs(cli.frontend_tool_timeout),
        set_session_timeout: Duration::from_secs(cli.set_session_timeout),
        cancel_grace_timeout: Duration::from_secs(cli.cancel_grace_timeout),
        max_sessions: cli.max_sessions,
        max_queued_turns: cli.max_queued_turns,
    };

    let state = BridgeAppState::builder(client, cli.cwd.clone())
        .with_config(config)
        .with_policy(policy)
        .with_mcp_allowed_origins(cli.mcp_allowed_origins.clone())
        .map_err(anyhow::Error::msg)?;
    let state = if let Some(token) = bearer_token {
        state.with_bearer_token(token).map_err(anyhow::Error::msg)?
    } else {
        state
    };

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
    // Bounded graceful shutdown: `with_graceful_shutdown` alone waits
    // indefinitely on open SSE connections after SIGTERM, which pushes
    // container runtimes past their stop timeout into SIGKILL. Race the
    // serving loop against the shutdown signal (the server MUST keep
    // accepting connections while healthy — awaiting the signal before
    // serving would accept nothing), then bound only the remaining drain:
    // once the signal arrives, resolve graceful shutdown immediately so
    // in-flight streams get a fixed window before forced stop.
    //
    // ponytail: hardcoded 10s rather than min(idle_timeout, …) —
    // idle_timeout (default 120s) is a session-reaping knob, not a drain
    // budget, and no operator tunes it expecting it to govern SIGTERM
    // latency. 10s comfortably covers normal SSE teardown; in-flight turns
    // that cannot finish by then are lost either way once the container is
    // SIGKILLed, so failing fast is the honest behavior.
    const DRAIN_WINDOW: Duration = Duration::from_secs(10);
    let serve = async {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
    };
    tokio::pin!(serve);
    // Race serving against the signal. While healthy the server runs forever;
    // once the signal arrives, keep polling the SAME serve future under the
    // drain window (a fresh axum::serve would fork accept loops; tokio's
    // TcpListener can't be cloned).
    let drained = tokio::select! {
        biased;
        result = &mut serve => Ok(result),
        () = shutdown_signal() => {
            serve_with_drain(serve.as_mut(), DRAIN_WINDOW).await
        }
    };
    match drained {
        Ok(result) => {
            result.context("axum::serve failed")?;
        }
        Err(()) => {
            tracing::warn!(
                drain = ?DRAIN_WINDOW,
                "drain window elapsed with streams still open; forcing stop"
            );
            // Graceful teardown did not finish; the Linux-only sweep is a
            // best-effort fallback after guarded-connection cleanup.
            if !cli.in_process {
                kill_agent_children();
            }
        }
    }

    tracing::info!("bridge stopped");
    Ok(())
}

/// Run `serve` under a bounded drain window.
///
/// Returns `Err(())` if the drain window elapsed first (streams still open);
/// otherwise returns the serve result. The caller must already have received
/// the shutdown signal — this bounds only the post-signal drain. Split out so
/// tests can drive it with paused time.
pub(crate) async fn serve_with_drain<S>(
    serve: S,
    drain: Duration,
) -> std::result::Result<S::Output, ()>
where
    S: Future,
{
    tokio::time::timeout(drain, serve)
        .await
        .map_err(|_: tokio::time::error::Elapsed| ())
}

/// Best-effort Linux-only sweep for direct agent children after the bounded
/// drain expires. Normal cleanup is owned by the guarded transport connection;
/// this fallback does not promise cleanup on other platforms or after forced
/// termination of the bridge.
///
/// ponytail: one-level `/proc` scan of direct children only, not a process
/// manager or a guarantee that every descendant will be stopped.
#[cfg(target_os = "linux")]
fn kill_agent_children() {
    let Some(children) = (|| -> Option<Vec<i32>> {
        let my_pid = std::process::id() as i32;
        let mut found = Vec::new();
        for entry in std::fs::read_dir("/proc").ok()?.flatten() {
            // Only numeric entries are processes.
            let Ok(pid) = entry.file_name().to_str().unwrap_or("").parse::<i32>() else {
                continue;
            };
            // /proc/<pid>/stat fields are space-separated but comm may contain
            // spaces AND ')' itself; split after the FINAL ')' so ppid is
            // field 4 of the remainder.
            let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, rest)) = stat.rsplit_once(')') else {
                continue;
            };
            let ppid = rest.split_whitespace().nth(1);
            if ppid.and_then(|p| p.parse::<i32>().ok()) == Some(my_pid) {
                found.push(pid);
            }
        }
        Some(found)
    })() else {
        tracing::warn!(
            "agent-child sweep unavailable (/proc unreadable); guarded-connection cleanup may still run"
        );
        return;
    };

    for pid in children {
        // Agents run as their own process-group leader (pgid == pid), so
        // killing the group reaches tool grandchildren; fall back to the
        // direct pid in case the group is already gone. Exact pids — no
        // pattern matching, so unrelated processes are never touched.
        // Direct syscalls via libc — no dependence on coreutils being on PATH.
        let group_gone = unsafe { libc::kill(-pid, libc::SIGKILL) } == 0;
        if !group_gone {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        tracing::info!(pid, "best-effort kill attempted on agent child");
    }
}

#[cfg(not(target_os = "linux"))]
fn kill_agent_children() {
    tracing::debug!(
        "agent-child sweep skipped (Linux-only /proc scan); guarded-connection cleanup is platform-specific"
    );
}

pub(super) fn require_non_loopback_auth(
    host: IpAddr,
    token_configured: bool,
    allow_unauthenticated: bool,
) -> Result<()> {
    if !host.is_loopback() && !token_configured && !allow_unauthenticated {
        bail!(
            "refusing unauthenticated non-loopback bind {host}; set \
             AGUI_ACP_BRIDGE_TOKEN or pass --allow-unauthenticated-non-loopback"
        );
    }
    Ok(())
}

/// Log-safe description of the agent command: the executable name and the
/// argument count only. Agent argv routinely carries credentials
/// (`--api-key sk-…`), so raw arguments must never reach the logs.
///
/// Element 0 is treated with extra care: it is *usually* the program path,
/// but a mistyped invocation (e.g. `agui-acp-bridge -- --api-key=sk-…`) makes
/// it a flag token that can itself be the credential. So we log only its file
/// name (a full path can also carry a token, e.g. `/tmp/sk-…/my-agent`), and
/// if element 0 starts with `-` it is not a program name at all, so we elide
/// it rather than echo it back — parsing mistakes belong in an error message,
/// not in a log line that must stay credential-free.
pub(super) fn agent_summary(agent_command: &[String]) -> String {
    match agent_command.split_first() {
        Some((bin, args)) => {
            // ponytail: basename only; no secret-redaction framework —
            // pattern-matching secret-looking flags is unreliable, the real
            // fix is that raw argv never reaches the log at all.
            let name = if bin.starts_with('-') {
                "<malformed>".to_string()
            } else {
                Path::new(bin)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("<unknown>")
                    .to_string()
            };
            format!("{name} ({} args)", args.len())
        }
        None => "<none>".to_string(),
    }
}

fn log_startup(cli: &Cli, addr: SocketAddr) {
    let agent = if cli.in_process {
        "in-process echo agent".to_string()
    } else {
        agent_summary(&cli.agent_command)
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
