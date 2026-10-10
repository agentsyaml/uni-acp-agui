use super::filesystem_fixtures::run_filesystem_probe;
use super::initialization::{DenyPolicy, run_unsupported_client_methods_agent};
use super::*;

#[tokio::test]
async fn unadvertised_filesystem_and_terminal_methods_are_not_found() {
    let unsupported = Arc::new(AtomicUsize::new(0));
    let capabilities_ok = Arc::new(AtomicBool::new(false));
    let unsupported_for_agent = unsupported.clone();
    let capabilities_for_agent = capabilities_ok.clone();
    let cfg = SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(DenyPolicy),
        config: crate::config::BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    };
    let handle = spawn_in_process_session_with(cfg, move |stream| {
        Box::pin(run_unsupported_client_methods_agent(
            stream,
            unsupported_for_agent,
            capabilities_for_agent,
        ))
    })
    .await
    .expect("session opens");

    let mut prompt =
        tokio::time::timeout(std::time::Duration::from_secs(5), handle.prompt("probe"))
            .await
            .expect("prompt opens before timeout")
            .expect("prompt opens");
    while let Some(item) =
        tokio::time::timeout(std::time::Duration::from_secs(5), prompt.events.recv())
            .await
            .expect("prompt event arrives before timeout")
    {
        if matches!(item, BridgeStreamItem::Finished { .. }) {
            break;
        }
    }
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), prompt.finished)
            .await
            .expect("finished result arrives before timeout")
            .expect("finished sender remains")
            .expect("prompt succeeds"),
        StopReason::EndTurn
    );
    assert!(capabilities_ok.load(Ordering::SeqCst));
    assert_eq!(unsupported.load(Ordering::SeqCst), 7);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_and_write_capabilities_are_independent_and_roundtrip() {
    if !crate::file_ops::read_text_file_supported() {
        let unsupported = run_filesystem_probe(
            FileSystemCapabilities::new()
                .read_text_file(true)
                .write_text_file(true),
        )
        .await;
        assert_eq!(
            unsupported.capabilities,
            Some(FileSystemCapabilities::default())
        );
        assert_eq!(unsupported.read, Some(Err(-32601)));
        assert_eq!(unsupported.write, Some(Err(-32601)));
        assert_eq!(unsupported.read_after_write, None);
        return;
    }

    let both = run_filesystem_probe(
        FileSystemCapabilities::new()
            .read_text_file(true)
            .write_text_file(true),
    )
    .await;
    assert_eq!(
        both.capabilities,
        Some(
            FileSystemCapabilities::new()
                .read_text_file(true)
                .write_text_file(true)
        )
    );
    assert_eq!(both.read, Some(Ok("before\n\u{4e16}\u{754c}\n".into())));
    assert_eq!(both.write, Some(Ok(())));
    assert_eq!(both.read_after_write, Some(Ok("after\n".into())));

    let read_only = run_filesystem_probe(FileSystemCapabilities::new().read_text_file(true)).await;
    assert_eq!(
        read_only.capabilities,
        Some(FileSystemCapabilities::new().read_text_file(true))
    );
    assert_eq!(
        read_only.read,
        Some(Ok("before\n\u{4e16}\u{754c}\n".into()))
    );
    assert_eq!(read_only.write, Some(Err(-32601)));
    assert_eq!(read_only.read_after_write, None);

    let write_only =
        run_filesystem_probe(FileSystemCapabilities::new().write_text_file(true)).await;
    assert_eq!(
        write_only.capabilities,
        Some(FileSystemCapabilities::new().write_text_file(true))
    );
    assert_eq!(write_only.read, Some(Err(-32601)));
    assert_eq!(write_only.write, Some(Ok(())));
    assert_eq!(write_only.read_after_write, Some(Err(-32601)));
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn unsupported_filesystem_capabilities_are_not_advertised() {
    let probe = run_filesystem_probe(
        FileSystemCapabilities::new()
            .read_text_file(true)
            .write_text_file(true),
    )
    .await;
    assert_eq!(probe.capabilities, Some(FileSystemCapabilities::default()));
    assert_eq!(probe.read, Some(Err(-32601)));
    assert_eq!(probe.write, Some(Err(-32601)));
    assert_eq!(probe.read_after_write, None);
}

#[test]
fn filesystem_handler_error_constructors_use_exact_acp_codes() {
    assert_eq!(
        i32::from(agent_client_protocol::Error::method_not_found().code),
        -32601
    );
    assert_eq!(
        i32::from(agent_client_protocol::Error::invalid_params().code),
        -32602
    );
    assert_eq!(
        i32::from(agent_client_protocol::Error::resource_not_found(None).code),
        -32002
    );
    assert_eq!(
        i32::from(agent_client_protocol::Error::internal_error().code),
        -32603
    );
    assert_eq!(
        i32::from(agent_client_protocol::Error::request_cancelled().code),
        -32800
    );
}
