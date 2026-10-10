use super::*;
async fn disconnectable_test_session() -> (
    Arc<AcpSessionHandle>,
    Arc<tokio::sync::Notify>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let disconnect = Arc::new(tokio::sync::Notify::new());
    let disconnect_agent = disconnect.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let ready_tx = Arc::new(std::sync::Mutex::new(Some(ready_tx)));
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        let disconnect = disconnect_agent.clone();
        let ready_tx = ready_tx.clone();
        async move {
            if let Some(tx) = ready_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            tokio::select! {
                result = crate::test_agents::run_cancel_aware_slow_agent(stream) => result,
                () = disconnect.notified() => Ok(()),
            }
        }
    }));
    let state = BridgeAppState::new(client, PathBuf::from("/"));
    let session = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("disconnectable"))
            .await
            .expect("in-process actor opens"),
    );
    (session, disconnect, ready_rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_zero_send_unblocks_when_actor_retires() {
    for prompt_path in [true, false] {
        let (session, disconnect, agent_ready) = disconnectable_test_session().await;
        tokio::time::timeout(Duration::from_secs(1), agent_ready)
            .await
            .expect("agent runner starts")
            .expect("ready signal");
        let turn_id = if prompt_path {
            Some(
                session
                    .prompt_with_turn("blocked send")
                    .await
                    .expect("turn starts")
                    .1,
            )
        } else {
            None
        };
        let (tx, _rx) = mpsc::channel(1);
        tx.send(Ok(keepalive_event()))
            .await
            .expect("fill SSE channel");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let send_session = session.clone();
        let mut retired = RetiredSseDrain::default();
        let task = tokio::spawn(async move {
            let mut started_tx = Some(started_tx);
            std::future::poll_fn(|_cx| {
                if let Some(signal) = started_tx.take() {
                    let _ = signal.send(());
                }
                std::task::Poll::Ready(())
            })
            .await;
            if let Some(turn_id) = turn_id {
                send_prompt_sse(
                    &tx,
                    Ok(keepalive_event()),
                    Duration::ZERO,
                    &send_session,
                    "t",
                    "r",
                    turn_id,
                    &mut retired,
                )
                .await
            } else {
                send_history_sse(
                    &tx,
                    Ok(keepalive_event()),
                    Duration::ZERO,
                    "t",
                    "r",
                    &send_session,
                    &mut retired,
                )
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .expect("send task reaches helper")
            .expect("signal");
        // Polling the helper establishes the full-channel send is pending before actor EOF.
        tokio::task::yield_now().await;
        disconnect.notify_one();
        tokio::time::timeout(Duration::from_secs(2), session.closed())
            .await
            .expect("real actor observes agent EOF");
        assert_eq!(
            tokio::time::timeout(RETIRED_SSE_DRAIN_GRACE + Duration::from_secs(1), task)
                .await
                .expect("blocked SSE send unblocks")
                .expect("send task")
                .unwrap_err(),
            SseSendError::ActorClosed,
        );
    }
}

#[tokio::test]
async fn legacy_zero_send_keeps_waiting_for_live_actor() {
    let (session, _disconnect, agent_ready) = disconnectable_test_session().await;
    agent_ready.await.expect("agent runner starts");
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(Ok(keepalive_event()))
        .await
        .expect("fill SSE channel");
    let send_session = session.clone();
    let mut retired = RetiredSseDrain::default();
    let mut send = tokio::spawn(async move {
        send_history_sse(
            &tx,
            Ok(keepalive_event()),
            Duration::ZERO,
            "t",
            "r",
            &send_session,
            &mut retired,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut send)
            .await
            .is_err(),
        "healthy actor must not trigger an arbitrary zero-timeout"
    );
    assert!(rx.recv().await.unwrap().is_ok());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .expect("send completes after capacity frees")
            .expect("task"),
        Ok(())
    );
    assert!(rx.recv().await.unwrap().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_prompt_sse_send_cancels_turn_and_aborts_frontend_call() {
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(|stream| {
        Box::pin(crate::test_agents::run_cancel_aware_slow_agent(stream))
    }));
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            event_buffer: 1,
            slow_consumer_timeout: Duration::from_millis(10),
            ..BridgeConfig::default()
        })
        .build();
    let session = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("slow-consumer"))
            .await
            .expect("session opens"),
    );
    let (prompt_stream, turn_id) = session
        .prompt_with_turn("slow consumer test")
        .await
        .expect("prompt opens");
    let entry = Arc::new(SessionEntry::new(session.clone(), None));
    let registry_entry = state.inner.frontend_tools.entry("slow-consumer");
    let pending = registry_entry.register_pending("pending-tool".into());
    let frontend_stream = install_frontend_sender(&registry_entry, 1);
    let prompt_guard = entry.enter_prompt();
    let run_guard = state
        .try_claim_run("slow-consumer", "slow-run")
        .expect("run admission");

    // Do not poll the returned SSE stream. RUN_STARTED fills its internal
    // one-slot channel; the next SessionInit send must hit the timeout.
    let _stream = build_event_stream(
        "slow-consumer".into(),
        "slow-run".into(),
        prompt_stream,
        EventStreamContext {
            session,
            state: state.clone(),
            registry_entry,
            turn_id,
            frontend_stream,
        },
        1,
        prompt_guard,
        run_guard,
    );

    let response = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .expect("slow consumer must tear down the stream")
        .expect("pending frontend call response");
    assert!(response.is_error);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while entry.active_prompts() != 0 && std::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        entry.active_prompts(),
        0,
        "timed out waiting for detached stream task to drop PromptGuard"
    );
    assert!(
        !entry.handle.is_unusable(),
        "slow consumer cleanup must cancel the turn, not unconditionally kill a healthy session"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_stalled_prompt_releases_claim_after_drain_grace() {
    let (session, disconnect, agent_ready) = disconnectable_test_session().await;
    tokio::time::timeout(Duration::from_secs(1), agent_ready)
        .await
        .expect("agent runner starts")
        .expect("ready signal");
    let (actual_prompt, turn_id) = session
        .prompt_with_turn("stalled retired stream")
        .await
        .expect("prompt starts");
    let PromptStream {
        events: actual_events,
        finished,
    } = actual_prompt;

    let state = BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
        .with_config(BridgeConfig {
            event_buffer: 1,
            slow_consumer_timeout: Duration::ZERO,
            ..BridgeConfig::default()
        })
        .build();
    let entry = Arc::new(SessionEntry::new(session.clone(), None));
    let registry_entry = state.inner.frontend_tools.entry("retired-stall");
    let frontend_stream = install_frontend_sender(&registry_entry, 1);
    let prompt_guard = entry.enter_prompt();
    let run_guard = state
        .try_claim_run("retired-stall", "stalled-run")
        .expect("run claim acquired");
    assert_eq!(entry.active_prompts(), 1);
    assert!(state.try_claim_run("retired-stall", "overlap").is_none());

    let (events_tx, events) = mpsc::channel(1);
    let _stream = build_event_stream(
        "retired-stall".into(),
        "stalled-run".into(),
        PromptStream { events, finished },
        EventStreamContext {
            session: session.clone(),
            state: state.clone(),
            registry_entry,
            turn_id,
            frontend_stream,
        },
        1,
        prompt_guard,
        run_guard,
    );

    events_tx
        .send(BridgeStreamItem::SessionInit {
            modes: None,
            models: None,
            config_options: None,
        })
        .await
        .expect("queue first translated event");
    let queued = tokio::time::timeout(Duration::from_secs(1), events_tx.reserve())
        .await
        .expect("stream task consumes first source event")
        .expect("source event channel remains open");
    queued.send(BridgeStreamItem::SessionInit {
        modes: None,
        models: None,
        config_options: None,
    });
    assert!(matches!(
        events_tx.try_send(BridgeStreamItem::SessionInit {
            modes: None,
            models: None,
            config_options: None,
        }),
        Err(mpsc::error::TrySendError::Full(_))
    ));

    // The internal output capacity is one: RUN_STARTED occupies it before
    // the source channel is consumed. The queued source event plus Full
    // result above prove the forwarding task is stalled behind that slot.
    tokio::task::yield_now().await;
    assert_eq!(entry.active_prompts(), 1);
    assert!(state.try_claim_run("retired-stall", "overlap").is_none());

    disconnect.notify_one();
    tokio::time::timeout(Duration::from_secs(2), session.closed())
        .await
        .expect("real ACP actor observes fixture EOF");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if let Some(claim) = state.try_claim_run("retired-stall", "early") {
                    return claim;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "claim must remain held during retired-stream drain grace"
    );

    let claim = tokio::time::timeout(RETIRED_SSE_DRAIN_GRACE + Duration::from_secs(1), async {
        let claim = loop {
            if let Some(claim) = state.try_claim_run("retired-stall", "after-retirement") {
                break claim;
            }
            tokio::task::yield_now().await;
        };
        // The independent guards drop non-atomically; wait for prompt release after claim acquisition.
        while entry.active_prompts() != 0 {
            tokio::task::yield_now().await;
        }
        claim
    })
    .await
    .expect("retired stalled stream releases same-thread run claim");
    assert_eq!(
        entry.active_prompts(),
        0,
        "PromptGuard released on actor retirement"
    );
    drop(claim);
    drop(_stream);
    drop(actual_events);
}

#[tokio::test]
async fn stale_cleanup_does_not_remove_replacement_session() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let old_handle = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("race"))
            .await
            .expect("old session opens"),
    );
    let replacement_handle = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("race"))
            .await
            .expect("replacement session opens"),
    );
    let stale_entry = Arc::new(SessionEntry::new(old_handle, None));
    let replacement_entry = Arc::new(SessionEntry::new(replacement_handle, None));
    state
        .inner
        .sessions
        .insert("race".to_string(), replacement_entry.clone());

    assert!(!remove_session_if_same(&state.inner, "race", &stale_entry));
    let current = state
        .inner
        .sessions
        .get("race")
        .expect("replacement must remain cached")
        .clone();
    assert!(Arc::ptr_eq(&current, &replacement_entry));

    assert!(remove_session_if_same(
        &state.inner,
        "race",
        &replacement_entry
    ));
    assert!(!state.inner.sessions.contains_key("race"));
}
