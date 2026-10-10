#[cfg(target_os = "linux")]
use super::filesystem_fixtures::FilesystemPolicy;
use super::*;
use crate::session::filesystem::write_content_validation_error;

#[cfg(target_os = "linux")]
async fn run_oversized_frame_agent(
    stream: tokio::io::DuplexStream,
    target_path: String,
    result_tx: oneshot::Sender<Option<String>>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PromptResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
    Agent
        .builder()
        .name("agui-bridge-oversized-wire-frame-test")
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
                responder.respond(NewSessionResponse::new(SessionId::from("wire-limit-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let target_path = Arc::new(target_path);
                let result_tx = Arc::new(Mutex::new(Some(result_tx)));
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let target_path = target_path.clone();
                    let result_tx = result_tx.clone();
                    let spawn_cx = cx.clone();
                    spawn_cx.spawn(async move {
                        let request = WriteTextFileRequest::new(
                            req.session_id,
                            target_path.as_str(),
                            "x".repeat(16 * 1024 * 1024 + 1),
                        );
                        let result = cx.send_request(request).block_task().await;
                        let result = match result {
                            Ok(_) => None,
                            Err(error) => Some(error.to_string()),
                        };
                        if let Some(tx) = result_tx.lock().expect("wire result poisoned").take() {
                            let _ = tx.send(result);
                        }
                        let _ = responder.respond(PromptResponse::new(StopReason::EndTurn));
                        Ok(())
                    })
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
#[test]
fn oversized_write_rpc_preflight_returns_invalid_params_without_creating_file() {
    let raw = std::env::temp_dir().join(format!(
        "agui-oversized-write-preflight-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&raw).unwrap();
    let target = raw.join("oversized.txt");
    let request = WriteTextFileRequest::new(
        SessionId::from("oversized-preflight-test"),
        target.to_string_lossy().into_owned(),
        "x".repeat(crate::file_ops::MAX_TEXT_FILE_BYTES + 1),
    );
    let error = write_content_validation_error(&request.content)
        .expect("oversized content must fail before filesystem access");
    assert_eq!(i32::from(error.code), -32602);
    assert!(!target.exists(), "invalid request cannot create its target");
    let _ = std::fs::remove_dir_all(raw);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn oversized_wire_frame_is_rejected_by_guarded_inprocess_transport() {
    let raw = std::env::temp_dir().join(format!(
        "agui-oversized-wire-frame-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&raw).unwrap();
    let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
    let target = cwd.join("must-not-exist.txt");
    let target_path = target.to_string_lossy().into_owned();
    let (agent_result_tx, agent_result_rx) = oneshot::channel();
    let cfg = SessionConfig {
        cwd,
        policy: Arc::new(FilesystemPolicy {
            capabilities: FileSystemCapabilities::new().write_text_file(true),
        }),
        config: crate::config::BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    };
    let handle = spawn_in_process_session_with(cfg, move |stream| {
        Box::pin(run_oversized_frame_agent(
            stream,
            target_path,
            agent_result_tx,
        ))
    })
    .await
    .expect("wire-boundary session opens");
    let mut prompt = handle.prompt("oversized-wire").await.expect("prompt opens");
    let run_error = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(item) = prompt.events.recv().await {
            match item {
                BridgeStreamItem::RunError { message } => return message,
                BridgeStreamItem::Finished { .. } => {
                    panic!("oversized frame must not produce RunFinished")
                }
                _ => {}
            }
        }
        panic!("oversized frame ended the stream without a terminal RunError")
    })
    .await
    .expect("guarded wire frame rejection finishes promptly");
    assert!(handle.is_unusable(), "oversized frame poisons the session");
    tokio::time::timeout(Duration::from_secs(5), handle.closed())
        .await
        .expect("oversized frame closes the session handle");
    assert!(
        !target.exists(),
        "oversized frame must be rejected before the write handler executes"
    );
    let agent_error = tokio::time::timeout(Duration::from_secs(10), agent_result_rx)
        .await
        .expect("agent outbound request observes the closed transport")
        .expect("agent reports its send result");
    assert!(
        agent_error.is_some(),
        "oversized write has no successful RPC reply"
    );
    assert!(
        run_error.contains("frame bytes limit exceeded")
            || agent_error
                .as_deref()
                .is_some_and(|error| error.contains("frame bytes limit exceeded")),
        "expected guarded transport frame diagnostic; run error={run_error:?}, agent error={agent_error:?}"
    );
    let _ = std::fs::remove_dir_all(raw);
}
