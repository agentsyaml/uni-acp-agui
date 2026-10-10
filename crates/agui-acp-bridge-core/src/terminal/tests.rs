use super::*;

// ponytail: covers the fd-bound approved-cwd openat2 path; non-Linux fails closed by design, nothing to assert.
#[cfg(target_os = "linux")]
#[cfg(unix)]
#[tokio::test]
async fn approved_cwd_is_fd_bound_across_replacement() {
    use std::os::unix::fs::symlink;

    let root =
        std::env::temp_dir().join(format!("agui-terminal-cwd-race-{}", uuid::Uuid::new_v4()));
    let approved = root.join("approved");
    let moved = root.join("moved");
    let outside = root.join("outside");
    std::fs::create_dir_all(&approved).expect("approved cwd");
    std::fs::create_dir_all(&outside).expect("outside cwd");
    let root = std::fs::canonicalize(root).expect("root canonicalizes");
    let approved_path = std::fs::canonicalize(&approved).expect("approved canonicalizes");
    let outside = std::fs::canonicalize(outside).expect("outside canonicalizes");
    let cwd = ApprovedCwd::open(&root, &approved_path).expect("approved cwd opens");

    std::fs::rename(&approved_path, &moved).expect("approved cwd is replaced");
    symlink(&outside, &approved_path).expect("replacement symlink creates");
    assert!(
        ApprovedCwd::open(&root, &approved_path).is_err(),
        "a replacement must not pass the second identity check"
    );

    let mut command = Command::new("pwd");
    command.args(["-P"]).stdout(Stdio::piped());
    cwd.configure(&mut command);
    let output = command.output().await.expect("pwd runs");
    assert!(output.status.success());
    let actual = String::from_utf8_lossy(&output.stdout);
    let expected = std::fs::canonicalize(&moved).expect("moved cwd canonicalizes");
    assert_eq!(actual.trim(), expected.to_string_lossy());
    assert!(!actual.contains(outside.to_string_lossy().as_ref()));

    let _ = std::fs::remove_file(approved_path);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn completed_terminal_remains_releasable() {
    let (registry, guard) =
        TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
    let request = CreateTerminalRequest::new("session", "true");
    let (id, created) = registry.create(&request).expect("terminal creates");
    created.disarm();

    let terminal = registry.take(&id).expect("terminal remains registered");
    let status = terminal.wait().await.expect("terminal exits");
    assert_eq!(status.exit_code, Some(0));
    terminal
        .release()
        .await
        .expect("completed terminal releases");

    drop(terminal);
    drop(guard);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn output_after_wait_includes_final_bytes() {
    let (registry, guard) =
        TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
    let request = CreateTerminalRequest::new("session", "sh")
        .args(vec!["-c".into(), "printf final-output; exit 0".into()]);
    let (id, created) = registry.create(&request).expect("terminal creates");
    created.disarm();

    let terminal = registry.take(&id).expect("terminal remains registered");
    terminal.wait().await.expect("terminal exits");
    let output = terminal.output().await.expect("terminal output");
    assert_eq!(output.output, "final-output");

    drop(terminal);
    drop(guard);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn wait_output_and_release_do_not_require_inherited_pipe_eof() {
    let marker = std::env::temp_dir().join(format!(
        "agui-terminal-descendant-{}.pid",
        uuid::Uuid::new_v4()
    ));
    let (registry, guard) =
        TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
    let request = CreateTerminalRequest::new("session", "sh")
        .args(vec![
            "-c".into(),
            "printf final-output; sleep 30 & echo $! > \"$ACP_PID_FILE\"; exit 7".into(),
        ])
        .env(vec![agent_client_protocol::schema::v1::EnvVariable::new(
            "ACP_PID_FILE",
            marker.to_string_lossy(),
        )]);
    let (id, created) = registry.create(&request).expect("terminal creates");
    created.disarm();
    let terminal = registry.get(&id).expect("terminal remains registered");

    let wait = tokio::time::timeout(std::time::Duration::from_secs(2), terminal.wait()).await;
    let output = tokio::time::timeout(std::time::Duration::from_secs(2), terminal.output()).await;

    let descendant_stopped = if let Ok(pid) = std::fs::read_to_string(&marker)
        && let Ok(pid) = pid.trim().parse::<u32>()
    {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let status = std::process::Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .expect("kill command runs");
                if !status.success() {
                    break true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false)
    } else {
        false
    };
    let release = tokio::time::timeout(std::time::Duration::from_secs(2), terminal.release()).await;
    if !descendant_stopped
        && let Some(pid) = std::fs::read_to_string(&marker)
            .ok()
            .and_then(|pid| pid.trim().parse::<u32>().ok())
    {
        let _ = std::process::Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    let _ = std::fs::remove_file(marker);

    assert_eq!(
        wait.expect("wait must not hang")
            .expect("wait succeeds")
            .exit_code,
        Some(7)
    );
    assert_eq!(
        output
            .expect("output must not hang")
            .expect("output succeeds")
            .output,
        "final-output"
    );
    release
        .expect("release must not hang")
        .expect("release succeeds");
    assert!(
        descendant_stopped,
        "releasing a terminal must kill inherited descendants"
    );
    drop(terminal);
    drop(guard);
}

#[test]
fn output_buffer_retains_exact_suffix_and_marks_truncation() {
    let state = TerminalState::new(MAX_OUTPUT_BYTES);
    state.append_output(&vec![b'a'; MAX_OUTPUT_BYTES]);
    state.append_output(b"tail");
    let (output, truncated, _) = state.snapshot().expect("snapshot");
    assert_eq!(output.len(), MAX_OUTPUT_BYTES);
    assert!(truncated);
    assert_eq!(&output[MAX_OUTPUT_BYTES - 4..], b"tail");
}

#[test]
fn output_buffer_is_lazily_allocated() {
    let state = TerminalState::new(MAX_OUTPUT_BYTES);
    let inner = state.inner.lock().expect("state lock");
    assert_eq!(inner.output.capacity(), 0);
}

#[test]
fn output_limit_honors_request_and_zero_retains_nothing() {
    let default = CreateTerminalRequest::new("session", "true");
    assert_eq!(output_limit(&default), MAX_OUTPUT_BYTES);

    let requested = default.clone().output_byte_limit(4);
    assert_eq!(output_limit(&requested), 4);

    let zero = default.clone().output_byte_limit(0);
    assert_eq!(output_limit(&zero), 0);
    let state = TerminalState::new(output_limit(&zero));
    state.append_output(b"output");
    let (output, truncated, _) = state.snapshot().expect("snapshot");
    assert!(output.is_empty());
    assert!(truncated);

    let bounded = default.output_byte_limit(u64::MAX);
    assert_eq!(output_limit(&bounded), MAX_OUTPUT_BYTES);
}

#[test]
fn output_buffer_truncates_only_at_utf8_character_boundaries() {
    let state = TerminalState::new(4);
    state.append_output("abcéXYZ".as_bytes());
    let (output, truncated, _) = state.snapshot().expect("snapshot");

    assert!(truncated);
    assert_eq!(output, b"XYZ");
    assert!(std::str::from_utf8(&output).is_ok());
}

#[test]
fn output_buffer_keeps_invalid_utf8_until_response_building() {
    let state = TerminalState::new(MAX_OUTPUT_BYTES);
    state.append_output(&[0xff, b'x']);
    let (output, _, _) = state.snapshot().expect("snapshot");
    assert_eq!(output, vec![0xff, b'x']);
    let response = TerminalOutputResponse::new(String::from_utf8_lossy(&output), false);
    assert_eq!(response.output, "�x");
}

#[tokio::test]
async fn cleanup_failure_is_bounded_and_marks_terminal_unavailable() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let attempts = AtomicUsize::new(0);
    assert!(
        !retry_cleanup(|| {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::other("test cleanup failure"))
        })
        .await
    );
    assert_eq!(attempts.load(Ordering::Relaxed), PROCESS_CLEANUP_ATTEMPTS);

    let state = TerminalState::new(0);
    state.mark_unavailable();
    assert_eq!(state.completion_result(), Err(()));
    assert!(matches!(state.snapshot(), Err(TerminalError::Internal)));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn terminal_registry_enforces_per_session_capacity() {
    let (registry, guard) =
        TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
    let request = CreateTerminalRequest::new("session", "true");
    let mut created = Vec::new();
    for _ in 0..MAX_TERMINALS_PER_SESSION {
        let (_, terminal) = registry
            .create(&request)
            .expect("terminal creates below cap");
        created.push(terminal);
    }

    assert!(matches!(
        registry.create(&request),
        Err(TerminalError::Capacity)
    ));
    let capacity = wire_error(TerminalError::Capacity);
    // Budget violation is a truthful internal error, not -32800.
    assert_eq!(i32::from(capacity.code), -32603);
    assert_eq!(
        capacity.data,
        Some(serde_json::json!({
            "limit": "MAX_TERMINALS_PER_SESSION",
            "cap": MAX_TERMINALS_PER_SESSION
        }))
    );

    drop(created);
    drop(guard);
}
