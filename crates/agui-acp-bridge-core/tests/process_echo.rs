//! Subprocess integration test: drive a real ACP agent (`simple_agent`
//! example from the sibling `acp-rust` checkout) over stdio and assert
//! the bridge streams `AgentMessageChunk` events and a `Finished` event.
//!
//! Skipped (returns early) when the `acp-rust` checkout is unavailable.

#[path = "common/build_example.rs"]
mod build_example;

use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::{SessionUpdate, StopReason};
use agui_acp_bridge_core::{
    AcpClient, BridgeConfig, BridgeStreamItem, ProcessAcpClient, SessionConfig,
};
use agui_acp_bridge_policy::AutoAllow;

use crate::build_example::build_example_agent;

fn cfg(cwd: std::path::PathBuf) -> SessionConfig {
    SessionConfig {
        cwd,
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        load_session_id: None,
    }
}

#[tokio::test]
async fn process_echo_round_trip_streams_chunks_and_finishes() {
    let Some(bin) = build_example_agent("simple_agent") else {
        eprintln!("skipping: acp-rust checkout or simple_agent example unavailable");
        return;
    };

    let client = ProcessAcpClient::new(bin.to_string_lossy());
    let cwd = std::env::current_dir().unwrap();

    let handle = tokio::time::timeout(Duration::from_secs(15), client.open_session(cfg(cwd)))
        .await
        .expect("open_session must not hang")
        .expect("open_session must succeed");

    let mut stream = tokio::time::timeout(Duration::from_secs(10), handle.prompt("hello"))
        .await
        .expect("prompt must not hang")
        .expect("prompt must succeed");

    let mut chunk_count = 0usize;
    let mut finished = false;
    let drain = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(item) = stream.events.recv().await {
            match item {
                BridgeStreamItem::Update(SessionUpdate::AgentMessageChunk(_)) => {
                    chunk_count += 1;
                }
                BridgeStreamItem::Finished { .. } => {
                    finished = true;
                    break;
                }
                BridgeStreamItem::RunError { message } => {
                    panic!("unexpected RunError: {message}");
                }
                _ => {}
            }
        }
    })
    .await;

    assert!(drain.is_ok(), "drain timed out");

    let stop = tokio::time::timeout(Duration::from_secs(5), stream.finished)
        .await
        .expect("finished must not hang")
        .expect("finished sender dropped")
        .expect("prompt must succeed");

    assert!(matches!(
        stop,
        StopReason::EndTurn
            | StopReason::MaxTokens
            | StopReason::MaxTurnRequests
            | StopReason::Refusal
            | StopReason::Cancelled
    ));
    assert!(
        chunk_count >= 2,
        "expected >=2 AgentMessageChunk events, got {chunk_count}"
    );
    assert!(finished, "expected Finished event");
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_handle_terminates_subprocess() {
    let Some(bin) = build_example_agent("simple_agent") else {
        eprintln!("skipping: acp-rust checkout unavailable");
        return;
    };

    let client = ProcessAcpClient::new(bin.to_string_lossy());
    let cwd = std::env::current_dir().unwrap();
    let handle = tokio::time::timeout(Duration::from_secs(15), client.open_session(cfg(cwd)))
        .await
        .expect("open_session must not hang")
        .expect("open_session must succeed");

    drop(handle);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let output = std::process::Command::new("pgrep")
        .args(["-f", "target/debug/examples/simple_agent"])
        .output()
        .expect("pgrep must run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.trim().is_empty(),
        "simple_agent still running after handle drop: {stdout}"
    );
}

#[tokio::test]
async fn open_session_with_invalid_command_fails_fast() {
    let client = ProcessAcpClient::new("/nonexistent/path/that/cannot/exist/agent");
    let cwd = std::env::current_dir().unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), client.open_session(cfg(cwd))).await;
    let opened = result.expect("must not hang on bad command");
    assert!(opened.is_err(), "expected open_session to fail, got Ok");
}
