use super::*;

pub(super) async fn run_full_probe(
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

pub(super) async fn run_foreign_probe(
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

pub(super) async fn run_cancellation_probe(
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

pub(super) async fn run_teardown_probe(
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
