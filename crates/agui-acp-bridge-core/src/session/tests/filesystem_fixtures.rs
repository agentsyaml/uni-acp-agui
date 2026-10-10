use super::*;

#[derive(Debug)]
pub(super) struct FilesystemPolicy {
    pub(super) capabilities: FileSystemCapabilities,
}

#[async_trait::async_trait]
impl crate::policy::PermissionPolicy for FilesystemPolicy {
    async fn decide(
        &self,
        _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
    ) -> PermissionDecision {
        PermissionDecision::Deny
    }

    fn filesystem_capabilities(&self) -> FileSystemCapabilities {
        self.capabilities.clone()
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct FilesystemProbe {
    pub(super) capabilities: Option<FileSystemCapabilities>,
    pub(super) read: Option<Result<String, i32>>,
    pub(super) write: Option<Result<(), i32>>,
    pub(super) read_after_write: Option<Result<String, i32>>,
}

pub(super) async fn run_filesystem_probe(capabilities: FileSystemCapabilities) -> FilesystemProbe {
    let raw =
        std::env::temp_dir().join(format!("agui-filesystem-session-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&raw).unwrap();
    let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
    std::fs::write(cwd.join("roundtrip.txt"), "before\n\u{4e16}\u{754c}\n").unwrap();

    let probe = Arc::new(Mutex::new(FilesystemProbe::default()));
    let probe_for_agent = probe.clone();
    let path = cwd.join("roundtrip.txt").to_string_lossy().into_owned();
    let cfg = SessionConfig {
        cwd,
        policy: Arc::new(FilesystemPolicy { capabilities }),
        config: crate::config::BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    };

    let handle = spawn_in_process_session_with(cfg, move |stream| {
        Box::pin(run_filesystem_probe_agent(stream, path, probe_for_agent))
    })
    .await
    .expect("filesystem session opens");
    let mut prompt = handle.prompt("probe").await.expect("prompt opens");
    while let Some(item) = prompt.events.recv().await {
        if matches!(item, BridgeStreamItem::Finished { .. }) {
            break;
        }
    }
    assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
    drop(handle);
    let result = probe.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(raw);
    result
}

async fn run_filesystem_probe_agent(
    stream: tokio::io::DuplexStream,
    path: String,
    probe: Arc<Mutex<FilesystemProbe>>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PromptResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-filesystem-test")
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: InitializeRequest, responder, _cx| {
                    probe.lock().unwrap().capabilities = Some(req.client_capabilities.fs);
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from("filesystem-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let session_id = req.session_id;
                    let path = path.clone();
                    let probe = probe.clone();
                    let cx_for_requests = cx.clone();
                    cx.spawn(async move {
                        let read = cx_for_requests
                            .send_request(ReadTextFileRequest::new(
                                session_id.clone(),
                                path.clone(),
                            ))
                            .block_task()
                            .await;
                        probe.lock().unwrap().read = Some(match read {
                            Ok(response) => Ok(response.content),
                            Err(error) => Err(error.code.into()),
                        });

                        let write = cx_for_requests
                            .send_request(WriteTextFileRequest::new(
                                session_id.clone(),
                                path.clone(),
                                "after\n",
                            ))
                            .block_task()
                            .await;
                        let write_ok = write.is_ok();
                        probe.lock().unwrap().write = Some(match write {
                            Ok(_) => Ok(()),
                            Err(error) => Err(error.code.into()),
                        });

                        if write_ok {
                            let read_after = cx_for_requests
                                .send_request(ReadTextFileRequest::new(session_id, path))
                                .block_task()
                                .await;
                            probe.lock().unwrap().read_after_write = Some(match read_after {
                                Ok(response) => Ok(response.content),
                                Err(error) => Err(error.code.into()),
                            });
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
