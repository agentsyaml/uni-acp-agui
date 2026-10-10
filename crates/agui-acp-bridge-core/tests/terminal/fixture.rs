use super::*;

#[derive(Debug, Default)]
struct TerminalPolicy;

#[async_trait]
impl PermissionPolicy for TerminalPolicy {
    async fn decide(
        &self,
        _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
    ) -> PermissionDecision {
        PermissionDecision::Deny
    }

    fn terminal_capability(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct Probe {
    capability: bool,
    ids: Vec<String>,
    combined_output: String,
    combined_exit_code: Option<u32>,
    partial_seen: bool,
    truncated_len: usize,
    truncated: bool,
    exit_code: Option<u32>,
    kill_preserved: bool,
    release_code: Option<i32>,
    unknown_code: Option<i32>,
    cwd_code: Option<i32>,
    command_code: Option<i32>,
    foreign_code: Option<i32>,
    cancelled_wait_code: Option<i32>,
    cancelled_wait_kept_terminal: bool,
}

fn error_code<T>(result: Result<T, agent_client_protocol::Error>) -> Option<i32> {
    result.err().map(|error| error.code.into())
}

fn record_id(probe: &Arc<Mutex<Probe>>, id: &TerminalId) {
    probe
        .lock()
        .expect("terminal probe lock")
        .ids
        .push(id.0.to_string());
}

fn session_config(cwd: PathBuf, policy: Arc<dyn PermissionPolicy>) -> SessionConfig {
    SessionConfig {
        cwd,
        policy,
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    }
}

async fn open_and_finish(
    cwd: PathBuf,
    probe: Arc<Mutex<Probe>>,
    mode: ProbeMode,
) -> agui_acp_bridge_core::AcpSessionHandle {
    let handle = spawn_in_process_session_with(
        session_config(cwd.clone(), Arc::new(TerminalPolicy)),
        move |stream| Box::pin(run_terminal_agent(stream, probe, mode)),
    )
    .await
    .expect("terminal session opens");
    let mut prompt = tokio::time::timeout(Duration::from_secs(30), handle.prompt("probe"))
        .await
        .expect("terminal prompt opens before timeout")
        .expect("terminal prompt opens");
    while let Some(item) = tokio::time::timeout(Duration::from_secs(30), prompt.events.recv())
        .await
        .expect("terminal prompt events arrive before timeout")
    {
        if matches!(
            item,
            agui_acp_bridge_core::BridgeStreamItem::Finished { .. }
        ) {
            break;
        }
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), prompt.finished)
            .await
            .expect("terminal prompt finishes before timeout")
            .expect("terminal prompt finished sender remains")
            .expect("terminal prompt succeeds"),
        StopReason::EndTurn
    );
    handle
}

#[derive(Debug)]
enum ProbeMode {
    Full { subdir: PathBuf, outside: PathBuf },
    Foreign(TerminalId),
    Cancellation,
    Teardown(PathBuf),
}

async fn run_terminal_agent(
    stream: DuplexStream,
    probe: Arc<Mutex<Probe>>,
    mode: ProbeMode,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-terminal-test")
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: InitializeRequest, responder, _cx| {
                    probe.lock().expect("terminal probe lock").capability =
                        req.client_capabilities.terminal;
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
                responder.respond(NewSessionResponse::new(SessionId::from("terminal-test")))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let probe = probe.clone();
                    let mode = match &mode {
                        ProbeMode::Full { subdir, outside } => ProbeMode::Full {
                            subdir: subdir.clone(),
                            outside: outside.clone(),
                        },
                        ProbeMode::Foreign(id) => ProbeMode::Foreign(id.clone()),
                        ProbeMode::Cancellation => ProbeMode::Cancellation,
                        ProbeMode::Teardown(marker) => ProbeMode::Teardown(marker.clone()),
                    };
                    let cx_for_task = cx.clone();
                    cx.spawn(async move {
                        match mode {
                            ProbeMode::Full { subdir, outside } => {
                                run_full_probe(
                                    &cx_for_task,
                                    req.session_id,
                                    probe,
                                    subdir,
                                    outside,
                                )
                                .await;
                            }
                            ProbeMode::Foreign(id) => {
                                run_foreign_probe(&cx_for_task, req.session_id, probe, id).await;
                            }
                            ProbeMode::Cancellation => {
                                run_cancellation_probe(&cx_for_task, req.session_id, probe).await;
                            }
                            ProbeMode::Teardown(marker) => {
                                run_teardown_probe(&cx_for_task, req.session_id, probe, marker)
                                    .await;
                            }
                        }
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    })
                }
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

#[path = "probes.rs"]
mod probes;
use probes::*;

#[path = "tests.rs"]
mod tests;
