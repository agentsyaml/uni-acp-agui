#[cfg(target_os = "linux")]
use super::filesystem_fixtures::FilesystemPolicy;
#[cfg(target_os = "linux")]
use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn spawned_write_quota_retires_actor_while_mutex_work_is_blocked() {
    let _gate_lock = FILESYSTEM_WRITE_GATE_TEST_LOCK.lock().await;
    let raw = std::env::temp_dir().join(format!("agui-work-admission-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&raw).unwrap();
    let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
    let paths: Vec<_> = ["first.txt", "second.txt", "third.txt"]
        .into_iter()
        .map(|name| cwd.join(name))
        .collect();
    let gate = Arc::new(crate::file_ops::WriteGate {
        path: paths[0].clone(),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    crate::file_ops::install_write_gate(gate.clone());
    let _gate_reset = WriteGateReset;
    let started = gate.started.notified();
    let work = WorkAdmission::with_limits(2, 4096);
    let mut retired = work.retire_tx.subscribe();
    let (third_tx, third_rx) = oneshot::channel();
    let agent_gate = gate.clone();
    let agent_paths: Vec<_> = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
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
    let handle = spawn_in_process_session_with_work(cfg, work.clone(), move |stream| {
        Box::pin(run_quota_write_agent(
            stream,
            agent_paths,
            agent_gate,
            third_rx,
        ))
    })
    .await
    .expect("filesystem quota session opens");
    let mut prompt = handle.prompt("quota").await.expect("prompt opens");
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("first write reaches existing filesystem gate");
    tokio::time::timeout(Duration::from_secs(5), async {
        while work.usage.lock().unwrap().items != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first gated and second mutex-queued requests are both charged");
    assert_eq!(work.usage.lock().unwrap().items, 2);
    third_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), retired.changed())
        .await
        .expect("third RPC retires work admission")
        .expect("work admission sender remains alive");

    let mut run_errors = 0;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(5), prompt.events.recv())
        .await
        .expect("quota error reaches the active prompt")
    {
        match item {
            BridgeStreamItem::RunError { message } => {
                run_errors += 1;
                assert!(message.contains("ACP_SPAWNED_WORK"), "{message}");
            }
            BridgeStreamItem::Finished { .. } => panic!("quota retirement emitted Finished"),
            _ => {}
        }
    }
    assert_eq!(run_errors, 1, "exactly one terminal quota error is emitted");
    assert!(
        prompt.finished.await.is_err(),
        "retired prompt is not successful"
    );
    tokio::time::timeout(Duration::from_secs(5), handle.closed())
        .await
        .expect("quota retirement closes the session handle");

    gate.release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), async {
        while work.usage.lock().unwrap().items != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both queued and blocking work credits release");
    assert!(
        !paths[2].exists(),
        "third request never reaches a write executor"
    );
    crate::file_ops::clear_write_gate();
    let _ = std::fs::remove_dir_all(raw);
}

#[cfg(target_os = "linux")]
async fn run_quota_write_agent(
    stream: tokio::io::DuplexStream,
    paths: Vec<String>,
    gate: Arc<crate::file_ops::WriteGate>,
    third_trigger: oneshot::Receiver<()>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PromptResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
    Agent
        .builder()
        .name("agui-bridge-work-admission-test")
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
                responder.respond(NewSessionResponse::new(SessionId::from("quota-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let paths = paths.clone();
                let gate = gate.clone();
                let third_trigger = Arc::new(Mutex::new(Some(third_trigger)));
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let paths = paths.clone();
                    let gate = gate.clone();
                    let third_trigger = third_trigger
                        .lock()
                        .expect("test trigger poisoned")
                        .take()
                        .unwrap();
                    let spawn_cx = cx.clone();
                    spawn_cx.spawn(async move {
                        let session_id = req.session_id;
                        let mut tasks = Vec::new();
                        let mut third_trigger = Some(third_trigger);
                        let first_started = gate.started.notified();
                        tokio::pin!(first_started);
                        for (index, path) in paths.into_iter().enumerate() {
                            let request_cx = cx.clone();
                            let request_spawn = request_cx.clone();
                            let session_id = session_id.clone();
                            let (done_tx, done_rx) = oneshot::channel();
                            tasks.push(done_rx);
                            request_spawn
                                .spawn(async move {
                                    let request = WriteTextFileRequest::new(
                                        session_id,
                                        path,
                                        format!("write-{index}"),
                                    );
                                    let _ = request_cx.send_request(request).block_task().await;
                                    let _ = done_tx.send(());
                                    Ok(())
                                })
                                .expect("agent outbound write task starts");
                            if index == 0 {
                                first_started.as_mut().await;
                            }
                            if index == 1 {
                                let _ = third_trigger.take().unwrap().await;
                            }
                        }
                        for task in tasks {
                            let _ = task.await;
                        }
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
