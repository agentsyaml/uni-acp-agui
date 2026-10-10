use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::{agent_summary, require_non_loopback_auth, run, serve_with_drain};
use crate::{Cli, PolicyKind, apply_filesystem_flags};
use agui_acp_bridge_policy::AutoDeny;
use clap::{CommandFactory, Parser};

#[test]
fn cli_parses_minimal_in_process() {
    let cli = Cli::parse_from(["agui-acp-bridge", "--in-process"]);
    assert!(cli.in_process);
    assert!(cli.agent_command.is_empty());
    assert_eq!(cli.port, 8080);
    assert_eq!(cli.slow_consumer_timeout, 30);
    assert_eq!(cli.max_sessions, 128);
    assert_eq!(cli.max_queued_turns, 32);
    assert_eq!(cli.host, "127.0.0.1".parse::<IpAddr>().unwrap());
    assert!(matches!(cli.policy, PolicyKind::AutoDeny));
    assert!(!cli.allow_fs_read);
    assert!(!cli.allow_fs_write);
}

#[test]
fn cli_keeps_explicit_auto_allow_available() {
    let cli = Cli::parse_from(["agui-acp-bridge", "--in-process", "--policy", "auto-allow"]);
    assert!(matches!(cli.policy, PolicyKind::AutoAllow));
}

#[test]
fn non_loopback_requires_token_unless_explicitly_allowed() {
    let remote = "192.0.2.10".parse().unwrap();
    assert!(require_non_loopback_auth(remote, false, false).is_err());
    assert!(require_non_loopback_auth(remote, true, false).is_ok());
    assert!(require_non_loopback_auth(remote, false, true).is_ok());
    assert!(require_non_loopback_auth("127.0.0.1".parse().unwrap(), false, false).is_ok());
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
fn cli_parses_repeated_mcp_allowed_origins() {
    let cli = Cli::parse_from([
        "agui-acp-bridge",
        "--in-process",
        "--mcp-allowed-origin",
        "https://one.example",
        "--mcp-allowed-origin",
        "https://two.example:8443",
    ]);
    assert_eq!(
        cli.mcp_allowed_origins,
        vec!["https://one.example", "https://two.example:8443"]
    );
}

#[test]
fn cli_parses_filesystem_flags_independently() {
    let read = Cli::parse_from(["agui-acp-bridge", "--in-process", "--allow-fs-read"]);
    assert!(read.allow_fs_read);
    assert!(!read.allow_fs_write);

    let write = Cli::parse_from(["agui-acp-bridge", "--in-process", "--allow-fs-write"]);
    assert!(!write.allow_fs_read);
    assert!(write.allow_fs_write);

    let both = Cli::parse_from([
        "agui-acp-bridge",
        "--in-process",
        "--allow-fs-read",
        "--allow-fs-write",
    ]);
    assert!(both.allow_fs_read);
    assert!(both.allow_fs_write);
}

#[test]
fn filesystem_flags_are_applied_after_policy_build() {
    let policy = apply_filesystem_flags(Arc::new(AutoDeny), true, false);
    let capabilities = policy.filesystem_capabilities();
    assert!(capabilities.read_text_file);
    assert!(!capabilities.write_text_file);

    let policy = apply_filesystem_flags(Arc::new(AutoDeny), false, false);
    let capabilities = policy.filesystem_capabilities();
    assert!(!capabilities.read_text_file);
    assert!(!capabilities.write_text_file);
}

#[test]
fn agent_summary_never_logs_argument_values() {
    // Agents are routinely launched with inline credentials; the log
    // summary must expose only the binary name and argument count.
    let secret = "sk-fake-secret-0123456789abcdef";
    let cli = Cli::parse_from([
        "agui-acp-bridge",
        "--",
        "./my-agent",
        "--api-key",
        secret,
        "--token",
        "hunter2",
    ]);
    let summary = agent_summary(&cli.agent_command);
    assert_eq!(summary, "my-agent (4 args)");
    assert!(!summary.contains(secret));
    assert!(!summary.contains("hunter2"));
    assert!(!summary.contains("--api-key"));

    let empty = Cli::parse_from(["agui-acp-bridge"]);
    assert_eq!(agent_summary(&empty.agent_command), "<none>");
}

#[test]
fn agent_summary_elides_flag_like_element_zero_and_basenames_paths() {
    // A mistyped invocation (`agui-acp-bridge -- --api-key=sk-…`) makes
    // the flag token element 0 of `agent_command`; it must never be
    // echoed verbatim because that token may itself be the credential.
    let malformed = vec!["--api-key=sk-live-XXXX".to_string(), "extra".to_string()];
    let summary = agent_summary(&malformed);
    assert_eq!(summary, "<malformed> (1 args)");
    assert!(!summary.contains("sk-live-XXXX"));
    assert!(!summary.contains("--api-key"));

    // A full path in element 0 is reduced to its file name so a token
    // hidden in a directory component cannot leak either.
    let summary = agent_summary(&["/tmp/sk-dir-token/my-agent".to_string()]);
    assert_eq!(summary, "my-agent (0 args)");
    assert!(!summary.contains("sk-dir-token"));
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

#[tokio::test]
async fn bounded_drain_forces_stop_when_serve_hangs() {
    // Short real-time durations: `#[tokio::test(start_paused = true)]`
    // would need tokio's `test-util` feature, which this crate does not
    // enable. The proportions below still pin both behaviors in ~600ms.
    let drain_window = Duration::from_millis(100);

    // Serve future that never resolves; the drain window must win.
    let started = std::time::Instant::now();
    let result = serve_with_drain(
        std::future::pending::<std::future::Ready<()>>(),
        drain_window,
    )
    .await;
    assert!(result.is_err(), "drain window should force stop");
    assert!(
        started.elapsed() >= drain_window,
        "drain should have waited the full window"
    );

    // Serve future that resolves well inside the window completes
    // normally.
    let serve = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        "done"
    };
    let result = serve_with_drain(serve, drain_window).await;
    assert_eq!(result.expect("serve should finish first"), "done");

    // The window only starts counting at signal time — exactly the
    // sequence `run()` performs: idle far longer than DRAIN_WINDOW while
    // waiting for the shutdown signal (no timeout armed yet), then arm
    // the drain and let a healthy serve finish.
    tokio::time::sleep(drain_window * 3).await;
    let serve = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        "still fine"
    };
    let result = serve_with_drain(serve, drain_window).await;
    assert_eq!(
        result.expect("idle serve survives an idle period past DRAIN_WINDOW"),
        "still fine"
    );
}
