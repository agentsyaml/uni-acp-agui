use super::*;

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
