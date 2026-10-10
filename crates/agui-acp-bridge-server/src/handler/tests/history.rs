use super::*;
async fn collect_history_terminal(
    finished_result: Option<Result<StopReason, BridgeError>>,
) -> Vec<Value> {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let entry = state
        .session_for("history-test")
        .await
        .expect("history test session opens");
    let prompt_guard = entry.enter_prompt();
    let run_guard = state
        .try_claim_run("history-test", "history-run")
        .expect("history test claims thread");

    let (events_tx, events) = tokio::sync::mpsc::channel(1);
    drop(events_tx);
    let (finished_tx, finished) = tokio::sync::oneshot::channel();
    if let Some(result) = finished_result {
        finished_tx.send(result).expect("finished receiver is live");
    } else {
        drop(finished_tx);
    }

    let stream = build_history_stream(
        "history-test".into(),
        "history-run".into(),
        PromptStream { events, finished },
        1,
        Duration::from_secs(30),
        entry.handle.clone(),
        prompt_guard,
        run_guard,
    );
    stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|event| {
            serde_json::to_value(event.expect("event stream result")).expect("event serializes")
        })
        .collect()
}

#[tokio::test]
async fn history_drain_error_and_closed_channel_never_finish_successfully() {
    for (finished_result, expected_code) in [
        (
            Some(Err(BridgeError::SessionClosed)),
            "ACP_HISTORY_DRAIN_ERROR",
        ),
        (None, "ACP_HISTORY_DRAIN_CLOSED"),
    ] {
        let events = collect_history_terminal(finished_result).await;
        assert!(!events.iter().any(|event| event["type"] == "RUN_FINISHED"));
        assert!(
            events
                .iter()
                .any(|event| { event["type"] == "RUN_ERROR" && event["code"] == expected_code })
        );
    }
}

#[tokio::test]
async fn sse_send_helper_distinguishes_success_closed_and_timeout() {
    let (tx, mut rx) = mpsc::channel(1);
    assert_eq!(
        send_sse_with_timeout(&tx, 1_u8, Duration::from_millis(20)).await,
        Ok(())
    );
    assert_eq!(rx.recv().await, Some(1));

    let (tx, mut rx) = mpsc::channel(1);
    tx.send(1_u8).await.expect("fill bounded channel");
    assert_eq!(
        send_sse_with_timeout(&tx, 2_u8, Duration::from_millis(5)).await,
        Err(SseSendError::TimedOut)
    );
    assert_eq!(rx.recv().await, Some(1));

    let (tx, rx) = mpsc::channel::<u8>(1);
    drop(rx);
    assert_eq!(
        send_sse_with_timeout(&tx, 1_u8, Duration::from_secs(1)).await,
        Err(SseSendError::Closed)
    );

    let (tx, mut rx) = mpsc::channel(1);
    assert_eq!(
        send_sse_with_timeout(&tx, 3_u8, Duration::ZERO).await,
        Ok(())
    );
    assert_eq!(rx.recv().await, Some(3));
}
