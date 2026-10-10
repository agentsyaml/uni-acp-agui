#[cfg(target_os = "linux")]
use super::filesystem_fixtures::FilesystemPolicy;
#[cfg(target_os = "linux")]
use super::*;

#[cfg(target_os = "linux")]
#[derive(Debug, Default, Clone)]
struct CancellationProbe {
    first_write: Option<Result<(), i32>>,
    second_write: Option<Result<(), i32>>,
}

#[cfg(target_os = "linux")]
async fn run_deterministic_write_cancellation_probe()
-> (CancellationProbe, (bool, bool), (bool, bool)) {
    let raw = std::env::temp_dir().join(format!("agui-filesystem-cancel-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&raw).unwrap();
    let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
    let first_target = cwd.join("first.txt");
    let second_target = cwd.join("second.txt");
    let first_path = first_target.to_string_lossy().into_owned();
    let second_path = second_target.to_string_lossy().into_owned();
    let gate = Arc::new(crate::file_ops::WriteGate {
        path: first_target.clone(),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    crate::file_ops::install_write_gate(gate.clone());
    let _gate_reset = WriteGateReset;
    let started = gate.started.notified();
    let probe = Arc::new(Mutex::new(CancellationProbe::default()));
    let probe_for_agent = probe.clone();
    let (cancel_done_tx, cancel_done_rx) = oneshot::channel();
    let gate_for_agent = gate.clone();

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
        Box::pin(run_cancellation_agent(
            stream,
            first_path,
            second_path,
            gate_for_agent,
            probe_for_agent,
            cancel_done_tx,
        ))
    })
    .await
    .expect("filesystem cancellation session opens");

    let mut prompt = handle.prompt("cancel").await.expect("prompt opens");
    tokio::time::timeout(std::time::Duration::from_secs(5), started)
        .await
        .expect("first write must reach the in-flight gate");
    tokio::time::timeout(std::time::Duration::from_secs(5), cancel_done_rx)
        .await
        .expect("cancellation must be sent")
        .expect("cancellation signal must remain connected");

    let before_release = (first_target.exists(), second_target.exists());
    gate.release.notify_waiters();

    while let Some(item) = prompt.events.recv().await {
        if matches!(item, BridgeStreamItem::Finished { .. }) {
            break;
        }
    }
    assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
    drop(handle);
    let result = probe.lock().unwrap().clone();
    crate::file_ops::clear_write_gate();
    let after_release = (first_target.exists(), second_target.exists());
    let _ = std::fs::remove_dir_all(raw);
    (result, before_release, after_release)
}
#[cfg(target_os = "linux")]
async fn run_cancellation_agent(
    stream: tokio::io::DuplexStream,
    first_path: String,
    second_path: String,
    gate: Arc<crate::file_ops::WriteGate>,
    probe: Arc<Mutex<CancellationProbe>>,
    cancel_done_tx: oneshot::Sender<()>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PromptResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-filesystem-cancellation-test")
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
                responder.respond(NewSessionResponse::new(SessionId::from("cancel-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let first_path = first_path.clone();
                let second_path = second_path.clone();
                let gate = gate.clone();
                let probe = probe.clone();
                let cancel_done_tx = Arc::new(Mutex::new(Some(cancel_done_tx)));
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let session_id = req.session_id;
                    let first_path = first_path.clone();
                    let second_path = second_path.clone();
                    let gate = gate.clone();
                    let probe = probe.clone();
                    let cancel_done_tx = cancel_done_tx.clone();
                    let cx_for_requests = cx.clone();
                    cx.spawn(async move {
                        let first = cx_for_requests.send_request(WriteTextFileRequest::new(
                            session_id.clone(),
                            first_path,
                            "first",
                        ));
                        gate.started.notified().await;

                        let second = cx_for_requests.send_request(WriteTextFileRequest::new(
                            session_id,
                            second_path,
                            "second",
                        ));
                        let _ = first.cancel();
                        let _ = second.cancel();
                        if let Some(tx) = cancel_done_tx.lock().unwrap().take() {
                            let _ = tx.send(());
                        }

                        let second_result = second.block_task().await;
                        probe.lock().unwrap().second_write = Some(match second_result {
                            Ok(_) => Ok(()),
                            Err(error) => Err(error.code.into()),
                        });
                        let first_result = first.block_task().await;
                        probe.lock().unwrap().first_write = Some(match first_result {
                            Ok(_) => Ok(()),
                            Err(error) => Err(error.code.into()),
                        });
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
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
                        .respond_with_error(agent_client_protocol::util::internal_error(
                            "unhandled request",
                        )),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_cancellation_is_deterministic_before_and_after_start() {
    let _gate_lock = FILESYSTEM_WRITE_GATE_TEST_LOCK.lock().await;
    if !crate::file_ops::read_text_file_supported() {
        return;
    }
    let (probe, before_release, after_release) = run_deterministic_write_cancellation_probe().await;
    assert_eq!(probe.first_write, Some(Ok(())));
    assert_eq!(probe.second_write, Some(Err(-32800)));
    assert_eq!(before_release, (false, false));
    assert_eq!(after_release, (true, false));
}
