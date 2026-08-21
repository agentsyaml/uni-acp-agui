//! Pins the M0.4 deliverable: `InProcessAcpClient` round-trips through an
//! embedded echo agent over `tokio::io::duplex(65536)`, exercising the full
//! byte-level wire path (serialization, framing, parsing) without spawning a
//! subprocess. Updated for the per-prompt `PromptStream` API.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{ContentBlock, SessionUpdate, StopReason};
use agui_acp_bridge_core::{BridgeConfig, BridgeStreamItem, SessionConfig};
use agui_acp_bridge_policy::AutoAllow;
use agui_acp_bridge_server::{AcpClient, InProcessAcpClient};
use tokio::time::timeout;

fn test_session_config() -> SessionConfig {
    SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    }
}

#[tokio::test]
async fn in_process_echo_round_trip_streams_chunks_and_finishes() {
    let client = InProcessAcpClient::new();
    let handle = timeout(
        Duration::from_secs(5),
        client.open_session(test_session_config()),
    )
    .await
    .expect("open_session deadlocked")
    .expect("open_session failed");

    let mut stream = timeout(Duration::from_secs(5), handle.prompt("hello"))
        .await
        .expect("prompt deadlocked")
        .expect("prompt failed");

    let mut chunk_count = 0usize;
    let mut saw_finished = false;
    while let Ok(Some(item)) = timeout(Duration::from_secs(2), stream.events.recv()).await {
        match item {
            BridgeStreamItem::Update(update) => {
                if let SessionUpdate::AgentMessageChunk(chunk) = update
                    && let ContentBlock::Text(_) = &chunk.content
                {
                    chunk_count += 1;
                }
            }
            BridgeStreamItem::Finished { .. } => {
                saw_finished = true;
                break;
            }
            BridgeStreamItem::RunError { message } => {
                panic!("unexpected RunError from in-process echo: {message}");
            }
            BridgeStreamItem::Interrupt { .. } => {
                panic!("echo agent should not request permissions");
            }
            BridgeStreamItem::FrontendToolCall { .. } => {
                panic!("echo agent should not invoke frontend tools");
            }
            BridgeStreamItem::FrontendToolEnd { .. } => {
                panic!("echo agent should not invoke frontend tools");
            }
            BridgeStreamItem::SessionInit {
                modes,
                models,
                config_options,
            } => {
                // Echo agent does not advertise any mode/model state; we
                // still expect the session actor to emit a SessionInit at
                // the start of every prompt so reconnecting clients always
                // see the picker frame (even if it's empty).
                assert!(modes.is_none(), "echo agent should not advertise modes");
                assert!(models.is_none(), "echo agent should not advertise models");
                assert!(
                    config_options.is_none(),
                    "echo agent should not advertise config options"
                );
            }
        }
    }

    let stop_reason = timeout(Duration::from_secs(2), stream.finished)
        .await
        .expect("finished deadlocked")
        .expect("finished sender dropped")
        .expect("prompt errored");

    assert!(
        matches!(
            stop_reason,
            StopReason::EndTurn
                | StopReason::MaxTokens
                | StopReason::MaxTurnRequests
                | StopReason::Refusal
                | StopReason::Cancelled
        ),
        "stop_reason should be a known variant, got {stop_reason:?}"
    );
    assert!(
        chunk_count >= 2,
        "echo agent should emit ≥2 text chunks, got {chunk_count}"
    );
    assert!(saw_finished, "Finished envelope must arrive after prompt");
}
