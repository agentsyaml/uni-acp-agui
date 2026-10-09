//! Subprocess integration test: drive a real ACP agent (`simple_agent`
//! example from the sibling `acp-rust` checkout) over stdio and assert
//! the bridge streams `AgentMessageChunk` events and a `Finished` event.
//!
//! Skipped (returns early) when the `acp-rust` checkout is unavailable.

#[path = "common/build_example.rs"]
mod build_example;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ImageContent, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, SessionId, SessionUpdate,
    StopReason, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::{
    AcpClient, BridgeConfig, BridgeError, BridgeStreamItem, CustomAgentInProcessClient,
    ProcessAcpClient, PromptStream, SessionConfig,
};
use agui_acp_bridge_policy::AutoAllow;
use tokio::io::DuplexStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::build_example::build_example_agent;

fn cfg(cwd: std::path::PathBuf) -> SessionConfig {
    SessionConfig {
        cwd,
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
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

#[tokio::test]
async fn prompt_wrappers_forward_ordered_blocks_without_repurposing_ids() {
    let captured = Arc::new(Mutex::new(Vec::<PromptRequest>::new()));
    let captured_for_client = captured.clone();
    let client = CustomAgentInProcessClient::new(move |stream| {
        run_capture_prompt_agent(stream, captured_for_client.clone())
    });
    let handle = client
        .open_session(cfg(std::env::current_dir().unwrap()))
        .await
        .expect("session must open");

    assert_eq!(handle.session_id().0.as_ref(), "agent-session");

    assert_finished_once(
        handle.prompt("compat text").await.unwrap(),
        StopReason::EndTurn,
    )
    .await;

    let blocks = vec![
        ContentBlock::Text(TextContent::new("first")),
        ContentBlock::Image(ImageContent::new("aGVsbG8=", "image/png")),
        ContentBlock::Text(TextContent::new("last")),
    ];
    assert_finished_once(
        handle.prompt_blocks(blocks.clone()).await.unwrap(),
        StopReason::EndTurn,
    )
    .await;

    let (text_stream, text_turn) = handle.prompt_with_turn("compat with turn").await.unwrap();
    assert_finished_once(text_stream, StopReason::EndTurn).await;
    let (blocks_stream, blocks_turn) = handle
        .prompt_blocks_with_turn(blocks.clone())
        .await
        .unwrap();
    assert_finished_once(blocks_stream, StopReason::EndTurn).await;
    assert_ne!(
        text_turn, blocks_turn,
        "turn IDs must remain distinct bridge IDs"
    );

    let captured = captured.lock().expect("capture lock");
    assert_eq!(captured.len(), 4);
    assert_eq!(
        captured[0].prompt,
        vec![ContentBlock::Text(TextContent::new("compat text"))]
    );
    assert_eq!(captured[1].prompt, blocks);
    assert_eq!(
        captured[2].prompt,
        vec![ContentBlock::Text(TextContent::new("compat with turn"))]
    );
    assert_eq!(captured[3].prompt, blocks);
    assert!(
        captured
            .iter()
            .all(|request| request.session_id.0.as_ref() == "agent-session"),
        "ACP session IDs must not be replaced with bridge turn/thread IDs"
    );
}

async fn assert_finished_once(stream: PromptStream, expected: StopReason) {
    let PromptStream {
        mut events,
        finished,
    } = stream;
    let mut terminal = Vec::new();
    while let Some(item) = events.recv().await {
        if let BridgeStreamItem::Finished { stop_reason } = item {
            terminal.push(stop_reason);
        }
    }
    assert_eq!(terminal, vec![expected]);
    assert_eq!(
        finished
            .await
            .expect("finished sender must remain")
            .expect("prompt must succeed"),
        expected
    );
}

async fn run_capture_prompt_agent(
    stream: DuplexStream,
    captured: Arc<Mutex<Vec<PromptRequest>>>,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-prompt-capture")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from("agent-session")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest, responder, _cx| {
                captured.lock().expect("capture lock").push(req);
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
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

#[cfg(unix)]
#[tokio::test]
async fn process_parent_exit_with_held_pipes_is_observed_and_group_killed() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let pid_file = std::env::temp_dir().join(format!(
        "agui-acp-descendant-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _cleanup = struct_drop_path(pid_file.clone());
    let script = "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; exit 17";
    let client = ProcessAcpClient::new("/bin/sh").with_args([
        "-c",
        script,
        "agui-test",
        pid_file.to_str().unwrap(),
    ]);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.open_session(cfg(std::env::current_dir().unwrap())),
    )
    .await;
    let result = result.expect("opener must observe parent exit while descendant holds pipes");
    let mut pid = None;
    for _ in 0..300 {
        if let Ok(value) = std::fs::read_to_string(&pid_file) {
            pid = value.parse::<u32>().ok();
            if pid.is_some() {
                break;
            }
        }
        tokio::task::yield_now().await;
    }
    let pid = pid.expect("launcher must publish descendant PID");
    let error = result
        .expect_err("agent exit 17 must fail opener")
        .to_string();
    assert!(error.contains("17"), "exit code must be reported: {error}");
    for _ in 0..300 {
        let alive = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "kill -0 \"$1\" 2>/dev/null",
                "agui-probe",
                &pid.to_string(),
            ])
            .status()
            .is_ok_and(|s| s.success());
        if !alive {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("descendant {pid} remained alive after process-group cleanup");
}

#[cfg(unix)]
fn struct_drop_path(path: std::path::PathBuf) -> impl Drop {
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    Cleanup(path)
}
