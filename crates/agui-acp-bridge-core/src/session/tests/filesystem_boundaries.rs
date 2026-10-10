#[cfg(target_os = "linux")]
use super::filesystem_fixtures::FilesystemPolicy;
#[cfg(target_os = "linux")]
use super::*;

#[cfg(target_os = "linux")]
#[derive(Debug, Clone)]
struct BoundaryPaths {
    outside: String,
    missing: String,
    invalid_utf8: String,
    ordinary_io: String,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Default, Clone)]
struct BoundaryProbe {
    codes: Vec<i32>,
}

#[cfg(target_os = "linux")]
async fn run_filesystem_boundary_probe() -> BoundaryProbe {
    let raw =
        std::env::temp_dir().join(format!("agui-filesystem-boundary-{}", uuid::Uuid::new_v4()));
    let outside_raw = std::env::temp_dir().join(format!(
        "agui-filesystem-boundary-outside-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&raw).unwrap();
    std::fs::create_dir_all(&outside_raw).unwrap();
    let cwd = crate::file_ops::canonicalize_cwd(&raw).unwrap();
    let outside = crate::file_ops::canonicalize_cwd(&outside_raw).unwrap();
    std::fs::write(outside.join("outside.txt"), "outside").unwrap();
    std::fs::write(cwd.join("invalid.txt"), [0xff, 0xfe]).unwrap();
    std::fs::write(cwd.join("not-a-directory"), "file").unwrap();
    let paths = BoundaryPaths {
        outside: outside.join("outside.txt").to_string_lossy().into_owned(),
        missing: cwd.join("missing.txt").to_string_lossy().into_owned(),
        invalid_utf8: cwd.join("invalid.txt").to_string_lossy().into_owned(),
        ordinary_io: cwd
            .join("not-a-directory")
            .join("child.txt")
            .to_string_lossy()
            .into_owned(),
    };
    let probe = Arc::new(Mutex::new(BoundaryProbe::default()));
    let probe_for_agent = probe.clone();
    let cfg = SessionConfig {
        cwd,
        policy: Arc::new(FilesystemPolicy {
            capabilities: FileSystemCapabilities::new()
                .read_text_file(true)
                .write_text_file(true),
        }),
        config: crate::config::BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    };
    let handle = spawn_in_process_session_with(cfg, move |stream| {
        Box::pin(run_boundary_agent(stream, paths, probe_for_agent))
    })
    .await
    .expect("filesystem boundary session opens");
    let mut prompt = handle.prompt("boundary").await.expect("prompt opens");
    while let Some(item) = prompt.events.recv().await {
        if matches!(item, BridgeStreamItem::Finished { .. }) {
            break;
        }
    }
    assert_eq!(prompt.finished.await.unwrap().unwrap(), StopReason::EndTurn);
    drop(handle);
    let result = probe.lock().unwrap().clone();
    let _ = std::fs::remove_dir_all(raw);
    let _ = std::fs::remove_dir_all(outside_raw);
    result
}

#[cfg(target_os = "linux")]
async fn run_boundary_agent(
    stream: tokio::io::DuplexStream,
    paths: BoundaryPaths,
    probe: Arc<Mutex<BoundaryProbe>>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, InitializeResponse, NewSessionRequest, NewSessionResponse,
        PromptResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = agent_client_protocol::ByteStreams::new(write.compat_write(), read.compat());
    Agent
        .builder()
        .name("agui-bridge-filesystem-boundary-test")
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
                responder.respond(NewSessionResponse::new(SessionId::from("boundary-test")))
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
                    let paths = paths.clone();
                    let probe = probe.clone();
                    let cx_for_requests = cx.clone();
                    cx.spawn(async move {
                        let code = |result: Result<
                            agent_client_protocol::schema::v1::ReadTextFileResponse,
                            agent_client_protocol::Error,
                        >| match result {
                            Ok(_) => 0,
                            Err(error) => error.code.into(),
                        };
                        let mut codes = Vec::with_capacity(5);
                        codes.push(code(
                            cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    "relative.txt",
                                ))
                                .block_task()
                                .await,
                        ));
                        codes.push(code(
                            cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    paths.outside,
                                ))
                                .block_task()
                                .await,
                        ));
                        codes.push(code(
                            cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    paths.missing,
                                ))
                                .block_task()
                                .await,
                        ));
                        codes.push(code(
                            cx_for_requests
                                .send_request(ReadTextFileRequest::new(
                                    session_id.clone(),
                                    paths.invalid_utf8,
                                ))
                                .block_task()
                                .await,
                        ));

                        let ordinary_io = cx_for_requests
                            .send_request(WriteTextFileRequest::new(
                                session_id.clone(),
                                paths.ordinary_io,
                                "ordinary I/O error",
                            ))
                            .block_task()
                            .await;
                        codes.push(match ordinary_io {
                            Ok(_) => 0,
                            Err(error) => error.code.into(),
                        });

                        probe.lock().unwrap().codes = codes;
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
async fn filesystem_requests_return_exact_boundary_error_codes() {
    if !crate::file_ops::read_text_file_supported() {
        return;
    }
    let probe = run_filesystem_boundary_probe().await;
    assert_eq!(probe.codes, vec![-32602, -32602, -32002, -32603, -32603]);
}
