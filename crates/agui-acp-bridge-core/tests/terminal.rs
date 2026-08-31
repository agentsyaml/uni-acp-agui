//! Focused ACP terminal-lane integration tests.

// Linux exercises the descriptor-relative cwd and pidfd/process-group paths.
// Windows uses the job-object path in `terminal.rs` and is not exercised by
// this host's focused test lane. Non-Linux/non-Windows runtimes are likewise
// unverified and fail closed because no equivalent cwd backend is initialized.
#![cfg(target_os = "linux")]

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CreateTerminalRequest, EnvVariable, InitializeRequest, InitializeResponse,
    KillTerminalRequest, NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse,
    ReleaseTerminalRequest, SessionId, StopReason, TerminalId, TerminalOutputRequest,
    WaitForTerminalExitRequest,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::{
    BridgeConfig, BridgeError, PermissionDecision, PermissionPolicy, SessionConfig,
    spawn_in_process_session_with,
};
use async_trait::async_trait;
use tokio::io::DuplexStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

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

async fn run_full_probe(
    cx: &ConnectionTo<agent_client_protocol::Client>,
    session_id: SessionId,
    probe: Arc<Mutex<Probe>>,
    subdir: PathBuf,
    outside: PathBuf,
) {
    let combined = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sh")
                .args(vec![
                    "-c".into(),
                    "printf stdout; printf stderr >&2; printf \"$ACP_TERM_TEST\"; pwd".into(),
                ])
                .env(vec![EnvVariable::new("ACP_TERM_TEST", "env")])
                .cwd(subdir.clone()),
        )
        .block_task()
        .await;
    let Ok(combined) = combined else {
        return;
    };
    record_id(&probe, &combined.terminal_id);
    let combined_wait = cx
        .send_request(WaitForTerminalExitRequest::new(
            session_id.clone(),
            combined.terminal_id.clone(),
        ))
        .block_task()
        .await;
    let combined_output = cx
        .send_request(TerminalOutputRequest::new(
            session_id.clone(),
            combined.terminal_id.clone(),
        ))
        .block_task()
        .await;
    if let (Ok(wait), Ok(output)) = (combined_wait, combined_output) {
        let mut probe = probe.lock().expect("terminal probe lock");
        probe.combined_output = output.output;
        probe.combined_exit_code = wait.exit_status.exit_code;
    }

    let partial = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sh").args(vec![
                "-c".into(),
                "printf partial; sleep 1; printf final".into(),
            ]),
        )
        .block_task()
        .await;
    if let Ok(partial) = partial {
        record_id(&probe, &partial.terminal_id);
        for _ in 0..100 {
            let output = cx
                .send_request(TerminalOutputRequest::new(
                    session_id.clone(),
                    partial.terminal_id.clone(),
                ))
                .block_task()
                .await;
            if let Ok(output) = output
                && output.output.contains("partial")
            {
                probe.lock().expect("terminal probe lock").partial_seen =
                    output.exit_status.is_none();
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = cx
            .send_request(WaitForTerminalExitRequest::new(
                session_id.clone(),
                partial.terminal_id,
            ))
            .block_task()
            .await;
    }

    let truncated = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "head")
                .args(vec!["-c".into(), "16777217".into(), "/dev/zero".into()])
                .output_byte_limit(4),
        )
        .block_task()
        .await
        .expect("truncation terminal creates");
    record_id(&probe, &truncated.terminal_id);
    let _ = cx
        .send_request(WaitForTerminalExitRequest::new(
            session_id.clone(),
            truncated.terminal_id.clone(),
        ))
        .block_task()
        .await;
    if let Ok(output) = cx
        .send_request(TerminalOutputRequest::new(
            session_id.clone(),
            truncated.terminal_id,
        ))
        .block_task()
        .await
    {
        let mut probe = probe.lock().expect("terminal probe lock");
        probe.truncated_len = output.output.len();
        probe.truncated = output.truncated;
    }

    let exit = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sh")
                .args(vec!["-c".into(), "exit 7".into()]),
        )
        .block_task()
        .await
        .expect("exit terminal creates");
    record_id(&probe, &exit.terminal_id);
    if let Ok(wait) = cx
        .send_request(WaitForTerminalExitRequest::new(
            session_id.clone(),
            exit.terminal_id,
        ))
        .block_task()
        .await
    {
        probe.lock().expect("terminal probe lock").exit_code = wait.exit_status.exit_code;
    }

    let kill = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sleep").args(vec!["30".into()]),
        )
        .block_task()
        .await
        .expect("kill terminal creates");
    record_id(&probe, &kill.terminal_id);
    let _ = cx
        .send_request(KillTerminalRequest::new(
            session_id.clone(),
            kill.terminal_id.clone(),
        ))
        .block_task()
        .await;
    let _ = cx
        .send_request(WaitForTerminalExitRequest::new(
            session_id.clone(),
            kill.terminal_id.clone(),
        ))
        .block_task()
        .await;
    probe.lock().expect("terminal probe lock").kill_preserved = cx
        .send_request(TerminalOutputRequest::new(
            session_id.clone(),
            kill.terminal_id,
        ))
        .block_task()
        .await
        .is_ok();

    let release = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sleep").args(vec!["30".into()]),
        )
        .block_task()
        .await
        .expect("release terminal creates");
    record_id(&probe, &release.terminal_id);
    let _ = cx
        .send_request(ReleaseTerminalRequest::new(
            session_id.clone(),
            release.terminal_id.clone(),
        ))
        .block_task()
        .await;
    probe.lock().expect("terminal probe lock").release_code = error_code(
        cx.send_request(TerminalOutputRequest::new(
            session_id.clone(),
            release.terminal_id,
        ))
        .block_task()
        .await,
    );

    probe.lock().expect("terminal probe lock").unknown_code = error_code(
        cx.send_request(TerminalOutputRequest::new(
            session_id.clone(),
            TerminalId::new("unknown"),
        ))
        .block_task()
        .await,
    );
    probe.lock().expect("terminal probe lock").cwd_code = error_code(
        cx.send_request(CreateTerminalRequest::new(session_id.clone(), "true").cwd(outside))
            .block_task()
            .await,
    );
    probe.lock().expect("terminal probe lock").command_code = error_code(
        cx.send_request(CreateTerminalRequest::new(session_id, ""))
            .block_task()
            .await,
    );
}

async fn run_foreign_probe(
    cx: &ConnectionTo<agent_client_protocol::Client>,
    session_id: SessionId,
    probe: Arc<Mutex<Probe>>,
    foreign_id: TerminalId,
) {
    probe.lock().expect("terminal probe lock").foreign_code = error_code(
        cx.send_request(TerminalOutputRequest::new(session_id.clone(), foreign_id))
            .block_task()
            .await,
    );
    let own = cx
        .send_request(CreateTerminalRequest::new(session_id.clone(), "true"))
        .block_task()
        .await
        .expect("second session terminal creates");
    record_id(&probe, &own.terminal_id);
    let _ = cx
        .send_request(WaitForTerminalExitRequest::new(session_id, own.terminal_id))
        .block_task()
        .await;
}

async fn run_cancellation_probe(
    cx: &ConnectionTo<agent_client_protocol::Client>,
    session_id: SessionId,
    probe: Arc<Mutex<Probe>>,
) {
    let terminal = cx
        .send_request(
            CreateTerminalRequest::new(session_id.clone(), "sleep").args(vec!["30".into()]),
        )
        .block_task()
        .await
        .expect("cancellation terminal creates");
    record_id(&probe, &terminal.terminal_id);

    let wait = cx.send_request(WaitForTerminalExitRequest::new(
        session_id.clone(),
        terminal.terminal_id.clone(),
    ));
    let _ = wait.cancel();
    probe
        .lock()
        .expect("terminal probe lock")
        .cancelled_wait_code = error_code(wait.block_task().await);
    probe
        .lock()
        .expect("terminal probe lock")
        .cancelled_wait_kept_terminal = cx
        .send_request(TerminalOutputRequest::new(
            session_id.clone(),
            terminal.terminal_id.clone(),
        ))
        .block_task()
        .await
        .is_ok();
    let _ = cx
        .send_request(KillTerminalRequest::new(
            session_id.clone(),
            terminal.terminal_id.clone(),
        ))
        .block_task()
        .await;
    let _ = cx
        .send_request(WaitForTerminalExitRequest::new(
            session_id,
            terminal.terminal_id,
        ))
        .block_task()
        .await;
}

async fn run_teardown_probe(
    cx: &ConnectionTo<agent_client_protocol::Client>,
    session_id: SessionId,
    probe: Arc<Mutex<Probe>>,
    marker: PathBuf,
) {
    let descendant_marker = marker.with_extension("descendant.pid");
    let script = format!(
        "echo $$ > {}; sleep 30 & echo $! > {}; exec sleep 30",
        marker.display(),
        descendant_marker.display()
    );
    if let Ok(terminal) = cx
        .send_request(CreateTerminalRequest::new(session_id, "sh").args(vec!["-c".into(), script]))
        .block_task()
        .await
    {
        record_id(&probe, &terminal.terminal_id);
    }
}

#[tokio::test]
async fn terminal_lane_covers_opt_in_process_and_registry_semantics() {
    let raw = std::env::temp_dir().join(format!("agui-terminal-test-{}", uuid::Uuid::new_v4()));
    let subdir = raw.join("subdir");
    let outside_raw = std::env::temp_dir().join(format!(
        "agui-terminal-test-outside-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&subdir).expect("terminal test subdir");
    std::fs::create_dir_all(&outside_raw).expect("terminal test outside dir");
    let cwd = std::fs::canonicalize(&raw).expect("terminal test cwd");
    let subdir = std::fs::canonicalize(subdir).expect("terminal test subdir canonicalizes");
    let outside = std::fs::canonicalize(outside_raw).expect("terminal test outside canonicalizes");
    let outside_for_probe = outside.clone();
    let probe = Arc::new(Mutex::new(Probe::default()));
    let handle = open_and_finish(
        cwd.clone(),
        probe.clone(),
        ProbeMode::Full {
            subdir,
            outside: outside_for_probe,
        },
    )
    .await;
    let result = probe.lock().expect("terminal probe lock");
    assert!(result.capability, "opt-in policy must advertise terminals");
    assert!(result.ids.len() >= 6);
    assert_ne!(result.ids[0], result.ids[1], "terminal IDs must be unique");
    assert!(result.combined_output.contains("stdout"));
    assert!(result.combined_output.contains("stderr"));
    assert!(result.combined_output.contains("env"));
    assert_eq!(result.combined_exit_code, Some(0));
    assert!(
        result.partial_seen,
        "output must be queryable while running"
    );
    assert_eq!(result.truncated_len, 4);
    assert!(result.truncated);
    assert_eq!(result.exit_code, Some(7));
    assert!(
        result.kill_preserved,
        "kill must preserve the registry entry"
    );
    assert_eq!(result.release_code, Some(-32002));
    assert_eq!(result.unknown_code, Some(-32002));
    assert_eq!(result.cwd_code, Some(-32602));
    assert_eq!(result.command_code, Some(-32602));
    drop(result);
    drop(handle);
    let _ = std::fs::remove_dir_all(raw);
    let _ = std::fs::remove_dir_all(outside);
}

#[tokio::test]
async fn terminal_ids_are_session_local_and_wait_cancellation_keeps_terminal_alive() {
    let raw =
        std::env::temp_dir().join(format!("agui-terminal-isolation-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&raw).expect("terminal isolation cwd");
    let cwd = std::fs::canonicalize(&raw).expect("terminal isolation canonical cwd");

    let first_probe = Arc::new(Mutex::new(Probe::default()));
    let first = open_and_finish(cwd.clone(), first_probe.clone(), ProbeMode::Cancellation).await;
    let first_id = TerminalId::new(
        first_probe
            .lock()
            .expect("first probe lock")
            .ids
            .first()
            .expect("first terminal ID")
            .clone(),
    );
    {
        let first_result = first_probe.lock().expect("first probe lock");
        assert_eq!(first_result.cancelled_wait_code, Some(-32800));
        assert!(first_result.cancelled_wait_kept_terminal);
    }

    let second_probe = Arc::new(Mutex::new(Probe::default()));
    let second = open_and_finish(
        cwd.clone(),
        second_probe.clone(),
        ProbeMode::Foreign(first_id),
    )
    .await;
    let second_result = second_probe.lock().expect("second probe lock");
    assert_eq!(second_result.foreign_code, Some(-32002));
    assert_ne!(
        first_probe.lock().expect("first probe lock").ids[0],
        second_result.ids[0],
        "separate sessions must receive opaque unique IDs"
    );
    drop(second_result);
    drop(second);
    drop(first);
    let _ = std::fs::remove_dir_all(raw);
}

#[tokio::test]
async fn dropping_session_kills_terminal_child() {
    let raw = std::env::temp_dir().join(format!("agui-terminal-teardown-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&raw).expect("terminal teardown cwd");
    let cwd = std::fs::canonicalize(&raw).expect("terminal teardown canonical cwd");
    let marker = cwd.join("child.pid");
    let descendant_marker = marker.with_extension("descendant.pid");
    let probe = Arc::new(Mutex::new(Probe::default()));
    let handle = open_and_finish(cwd, probe, ProbeMode::Teardown(marker.clone())).await;

    let (pid, descendant_pid) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let (Ok(pid), Ok(descendant_pid)) = (
                std::fs::read_to_string(&marker),
                std::fs::read_to_string(&descendant_marker),
            ) && let Ok(pid) = pid.trim().parse::<u32>()
                && let Ok(descendant_pid) = descendant_pid.trim().parse::<u32>()
            {
                break (pid, descendant_pid);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal child writes its PID");
    drop(handle);

    let alive = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let child_status = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("kill command runs");
            let descendant_status = std::process::Command::new("kill")
                .args(["-0", &descendant_pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("kill command runs");
            if !child_status.success() && !descendant_status.success() {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("terminal child teardown completes");
    if alive {
        let _ = std::process::Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = std::process::Command::new("kill")
            .args(["-KILL", &descendant_pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    assert!(
        !alive,
        "dropping the ACP session must kill terminal descendants"
    );
    let _ = std::fs::remove_dir_all(raw);
}
