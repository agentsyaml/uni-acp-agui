use super::*;

pub(super) struct WorkerState {
    pub(super) cwd: Arc<PathBuf>,
    pub(super) ready_tx:
        std::sync::Mutex<Option<oneshot::Sender<Result<SessionReady, BridgeError>>>>,
    pub(super) event_slot: EventSlot,
    pub(super) error_slot: EventSlot,
    pub(super) pending_permissions: PendingPermissions,
    pub(super) pending_for_drain: PendingPermissions,
    pub(super) turn_queue: Arc<SessionTurnQueue>,
    pub(super) unusable: Arc<AtomicBool>,
    pub(super) cmd_rx: mpsc::Receiver<SessionCommand>,
    pub(super) mcp_url: Option<String>,
    pub(super) mcp_headers: Vec<HttpHeader>,
    pub(super) init_state: Arc<Mutex<SessionInitState>>,
    pub(super) load_session_id: Option<SessionId>,
    pub(super) load_buffer: LoadBuffer,
    pub(super) spill: SpillBuffer,
    pub(super) event_mailbox_tx: EventMailboxTx,
    pub(super) filesystem_capabilities: FileSystemCapabilities,
    pub(super) terminal_capability: bool,
    pub(super) config: crate::config::BridgeConfig,
    pub(super) work_retired: tokio::sync::watch::Receiver<Option<agent_client_protocol::Error>>,
}

pub(super) async fn run(
    cx: ConnectionTo<Agent>,
    state: WorkerState,
) -> Result<(), agent_client_protocol::Error> {
    let WorkerState {
        cwd,
        ready_tx,
        event_slot,
        error_slot,
        pending_permissions,
        pending_for_drain,
        turn_queue,
        unusable,
        cmd_rx,
        mcp_url,
        mcp_headers,
        init_state,
        load_session_id,
        load_buffer,
        spill,
        event_mailbox_tx,
        filesystem_capabilities,
        terminal_capability,
        config,
        mut work_retired,
    } = state;
    let cwd = cwd.clone();
    let ready_slot = Arc::new(ready_tx);
    let event_slot = event_slot;
    let error_slot = error_slot;
    let pending_for_drain = pending_for_drain;
    let turn_queue = turn_queue;
    let mut cmd_rx = cmd_rx;
    let mcp_headers = mcp_headers;
    let init_state = init_state;
    let load_buffer = load_buffer;
    let spill = spill;
    let event_mailbox_tx = event_mailbox_tx;
    let ready_slot_for_foreground = ready_slot.clone();
    let foreground = async {
        let (session_id, supports_close) = match initialize(
            &cx,
            cwd.as_ref().clone(),
            mcp_url,
            mcp_headers,
            load_session_id,
            &load_buffer,
            &init_state,
            filesystem_capabilities,
            terminal_capability,
        )
        .await
        {
            Ok((id, init, supports_close)) => {
                *init_state.lock().expect("init_state poisoned") = init.clone();
                if let Some(tx) = ready_slot_for_foreground
                    .lock()
                    .expect("ready slot poisoned")
                    .take()
                {
                    let _ = tx.send(Ok(SessionReady {
                        session_id: id.clone(),
                        supports_close,
                    }));
                }
                (id, supports_close)
            }
            Err(err) => {
                if let Some(tx) = ready_slot_for_foreground
                    .lock()
                    .expect("ready slot poisoned")
                    .take()
                {
                    let _ = tx.send(Err(err));
                }
                return Ok(());
            }
        };

        loop {
            let cmd = tokio::select! {
                biased;
                () = cx.incoming_closed() => break,
                cmd = cmd_rx.recv() => match cmd { Some(cmd) => cmd, None => break },
            };
            match cmd {
                SessionCommand::Prompt {
                    prompt,
                    events_tx,
                    finished_tx,
                    turn,
                } => {
                    let keep_running = prompt::run_prompt(
                        prompt,
                        events_tx,
                        finished_tx,
                        turn,
                        prompt::PromptState {
                            cx: &cx,
                            session_id: &session_id,
                            event_slot: &event_slot,
                            error_slot: &error_slot,
                            init_state: &init_state,
                            load_buffer: &load_buffer,
                            spill: &spill,
                            pending_permissions: &pending_permissions,
                            event_mailbox: &event_mailbox_tx,
                            turn_queue: &turn_queue,
                            unusable: &unusable,
                            cancel_grace_timeout: config.cancel_grace_timeout,
                        },
                    )
                    .await;
                    if !keep_running {
                        break;
                    }
                }
                SessionCommand::SetMode { mode_id, ack } => {
                    let kept = run_setting_command(
                        ack,
                        config.set_session_timeout,
                        &unusable,
                        send_set_mode(&cx, &session_id, &mode_id, init_state.clone()),
                    )
                    .await;
                    if !kept {
                        break;
                    }
                }
                SessionCommand::SetConfigOption {
                    config_id,
                    value,
                    ack,
                } => {
                    let kept = run_setting_command(
                        ack,
                        config.set_session_timeout,
                        &unusable,
                        send_set_config_option(
                            &cx,
                            &session_id,
                            &config_id,
                            &value,
                            init_state.clone(),
                        ),
                    )
                    .await;
                    if !kept {
                        break;
                    }
                }
                SessionCommand::Close { ack } => {
                    if !supports_close {
                        let _ = ack.send(Err(BridgeError::Unsupported("session/close".into())));
                        continue;
                    }

                    // Closing is a terminal actor operation. The
                    // request uses the real ACP SessionId returned by
                    // initialize, never the bridge thread/run ids.
                    let result = match tokio::time::timeout(
                        config.set_session_timeout,
                        cx.send_request(CloseSessionRequest::new(session_id.clone()))
                            .block_task(),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(error)) => Err(BridgeError::Acp(error)),
                        Err(_) => Err(BridgeError::Timeout(config.set_session_timeout)),
                    };

                    // A close response error/timeout leaves the ACP
                    // state uncertain, so all terminal close outcomes
                    // make this actor unusable and release local work.
                    unusable.store(true, Ordering::Release);
                    turn_queue.clear();
                    drain_pending_permissions(&pending_for_drain);
                    let _ = ack.send(result);
                    break;
                }
                SessionCommand::DrainHistory {
                    events_tx,
                    finished_tx,
                } => {
                    history_drain::drain_history(
                        events_tx,
                        finished_tx,
                        &error_slot,
                        &init_state,
                        &load_buffer,
                        &spill,
                    )
                    .await;
                }
            }
        }
        // Connection closing — fail any pending permission waiters
        // (their spawned tasks will respond Cancelled to the agent
        // and exit) so memory and tasks don't linger past idle
        // reaping.
        drain_pending_permissions(&pending_for_drain);
        Ok(())
    };
    tokio::select! {
        biased;
        changed = work_retired.changed() => {
            let _ = changed;
            unusable.store(true, Ordering::Release);
            drain_pending_permissions(&pending_for_drain);
            let error = work_retired.borrow().clone().unwrap_or_else(agent_client_protocol::Error::internal_error);
            if let Some(tx) = ready_slot.lock().expect("ready slot poisoned").take() {
                let _ = tx.send(Err(BridgeError::Acp(error.clone())));
            }
            Err(error)
        }
        result = foreground => result,
    }
}
