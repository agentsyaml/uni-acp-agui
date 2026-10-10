use super::*;

pub(super) fn mailbox_limit_error(limit: EventLimit) -> BridgeError {
    BridgeError::Acp(
        agent_client_protocol::Error::internal_error().data(serde_json::json!({
            "limit": limit.name(),
            "items": EVENT_ITEM_LIMIT,
            "bytes": EVENT_BYTE_LIMIT,
        })),
    )
}

pub(super) struct SessionActorState {
    pub(super) pending_permissions: PendingPermissions,
    pub(super) turn_queue: Arc<SessionTurnQueue>,
    pub(super) unusable: Arc<AtomicBool>,
    pub(super) init_state: Arc<Mutex<SessionInitState>>,
}

pub(super) struct SessionReady {
    pub(super) session_id: SessionId,
    pub(super) supports_close: bool,
}

/// Last-resort cleanup for actor cancellation or panic. The explicit cleanup
/// at the normal return site runs before error-event backpressure, while this
/// guard covers task aborts that never reach that site.
struct ActorExitGuard {
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    unusable: Arc<AtomicBool>,
}

impl Drop for ActorExitGuard {
    fn drop(&mut self) {
        self.unusable.store(true, Ordering::Release);
        self.turn_queue.clear();
        drain_pending_permissions(&self.pending_permissions);
    }
}

pub(super) async fn run_actor<T>(
    connector: T,
    cfg: SessionConfig,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    ready_tx: oneshot::Sender<Result<SessionReady, BridgeError>>,
    state: SessionActorState,
    work_admission: Arc<WorkAdmission>,
) where
    T: ConnectTo<Client> + Send + 'static,
{
    let SessionActorState {
        pending_permissions,
        turn_queue,
        unusable,
        init_state,
    } = state;
    let _exit_guard = ActorExitGuard {
        pending_permissions: pending_permissions.clone(),
        turn_queue: turn_queue.clone(),
        unusable: unusable.clone(),
    };
    let event_slot: EventSlot = Arc::new(Mutex::new(None));
    let error_slot: EventSlot = Arc::new(Mutex::new(None));
    let event_slot_for_notif = event_slot.clone();
    let event_slot_for_perm = event_slot.clone();
    let event_slot_for_session = event_slot.clone();
    let error_slot_for_session = error_slot.clone();
    let (event_mailbox_tx, event_mailbox_rx) = EventMailbox::new();
    let work_retired = work_admission.retire_tx.subscribe();
    let mut event_driver = AbortOnDrop::new(tokio::spawn(run_event_mailbox(
        event_mailbox_rx,
        unusable.clone(),
    )));
    let mailbox_for_notif = event_mailbox_tx.clone();
    let mailbox_for_perm = event_mailbox_tx.clone();
    let mailbox_for_prompt = event_mailbox_tx.clone();
    let spill: SpillBuffer = Arc::new(Mutex::new(Vec::new()));
    let spill_for_notif = spill.clone();
    let spill_for_teardown = spill.clone();
    let load_buffer: LoadBuffer = Arc::new(Mutex::new(None));
    let load_buffer_for_notif = load_buffer.clone();
    let load_buffer_for_session = load_buffer.clone();
    let SessionConfig {
        cwd,
        policy,
        config,
        mcp_url,
        mcp_headers,
        load_session_id,
    } = cfg;
    let permission_timeout = config.permission_timeout;
    let filesystem_capabilities = policy.filesystem_capabilities();
    let read_filesystem_enabled =
        filesystem_capabilities.read_text_file && crate::file_ops::read_text_file_supported();
    let write_filesystem_enabled =
        filesystem_capabilities.write_text_file && crate::file_ops::write_text_file_supported();
    let filesystem_capabilities = filesystem_capabilities
        .read_text_file(read_filesystem_enabled)
        .write_text_file(write_filesystem_enabled);
    let (filesystem_root, filesystem_root_guard) =
        if read_filesystem_enabled || write_filesystem_enabled {
            match tokio::task::spawn_blocking({
                let cwd = cwd.clone();
                move || {
                    let root = std::fs::canonicalize(cwd)?;
                    let guard = crate::file_ops::pin_filesystem_root(&root)?;
                    Ok::<_, std::io::Error>((root, guard))
                }
            })
            .await
            {
                Ok(Ok((root, guard))) => (Arc::new(root), Some(guard)),
                Ok(Err(error)) => {
                    let ready_tx = ready_tx;
                    let _ = ready_tx.send(Err(BridgeError::Io(error)));
                    return;
                }
                Err(error) => {
                    let ready_tx = ready_tx;
                    let _ = ready_tx.send(Err(BridgeError::Io(std::io::Error::other(error))));
                    return;
                }
            }
        } else {
            (Arc::new(cwd.clone()), None)
        };
    // Do not advertise terminal access unless the policy opts in and the
    // platform-specific cwd/process backend initialized successfully.
    let terminal_backend = if policy.terminal_capability() {
        TerminalRegistry::new(cwd.clone()).ok()
    } else {
        None
    };
    let terminal_capability = terminal_backend.is_some();
    let cwd = Arc::new(cwd);
    let filesystem_lock = Arc::new(AsyncMutex::new(()));
    let (terminal_registry, terminal_registry_guard) = terminal_backend
        .map(|(registry, guard)| (Some(registry), Some(guard)))
        .unwrap_or((None, None));

    let pending_perms_for_events = pending_permissions.clone();
    let pending_perms_for_drain = pending_permissions.clone();
    let pending_perms_for_worker = pending_perms_for_drain.clone();
    let turns_for_session = turn_queue.clone();
    let unusable_for_session = unusable.clone();
    let read_filesystem_lock = filesystem_lock.clone();
    let write_filesystem_lock = filesystem_lock.clone();
    let read_filesystem_root = filesystem_root.clone();
    let write_filesystem_root = filesystem_root.clone();
    let ready_tx = std::sync::Mutex::new(Some(ready_tx));

    let callbacks = connection::CallbackState {
        init_state: init_state.clone(),
        load_buffer: load_buffer_for_notif,
        event_slot_notification: event_slot_for_notif,
        event_slot_permission: event_slot_for_perm,
        mailbox_notification: mailbox_for_notif,
        mailbox_permission: mailbox_for_perm,
        spill: spill_for_notif,
        pending_permissions: pending_perms_for_events,
        policy,
        permission_timeout,
        read_root: read_filesystem_root,
        read_lock: read_filesystem_lock,
        read_enabled: read_filesystem_enabled,
        write_root: write_filesystem_root,
        write_lock: write_filesystem_lock,
        write_enabled: write_filesystem_enabled,
        work: work_admission.clone(),
        terminal_registry: terminal_registry.clone(),
        terminal_enabled: terminal_capability,
    };
    let result = connection::connect(connector, callbacks, move |cx| {
        worker::run(
            cx,
            worker::WorkerState {
                cwd,
                ready_tx,
                event_slot: event_slot_for_session,
                error_slot: error_slot_for_session,
                pending_permissions: pending_permissions.clone(),
                pending_for_drain: pending_perms_for_worker,
                turn_queue: turns_for_session,
                unusable: unusable_for_session,
                cmd_rx,
                mcp_url,
                mcp_headers,
                init_state,
                load_session_id,
                load_buffer: load_buffer_for_session,
                spill,
                event_mailbox_tx: mailbox_for_prompt,
                filesystem_capabilities,
                terminal_capability,
                config,
                work_retired,
            },
        )
    })
    .await;

    if let Some(registry) = terminal_registry.as_ref() {
        registry.shutdown();
    }
    drop(terminal_registry_guard);
    // SDK callback closures (and their in-flight operations) are gone after
    // connect_with returns; release the session lease before mailbox draining.
    #[cfg(target_os = "linux")]
    drop(filesystem_root_guard);
    #[cfg(not(target_os = "linux"))]
    let _filesystem_root_guard = filesystem_root_guard;

    // Every exit path below represents a dead actor. Mark the shared handle
    // first so new prompts fail closed, then release every queued admission
    // before any potentially backpressured error event is sent.
    unusable.store(true, Ordering::Release);
    turn_queue.clear();

    // Actor shutdown with a non-empty spill buffer means those out-of-turn
    // updates will never be drained by a future prompt — surface the loss
    // instead of discarding silently.
    let remaining_spill = spill_for_teardown
        .lock()
        .expect("spill buffer poisoned")
        .len();
    if remaining_spill > 0 {
        tracing::warn!(
            count = remaining_spill,
            "session actor terminated with non-empty out-of-turn update spill buffer; \
             buffered updates discarded"
        );
    }

    drain_pending_permissions(&pending_perms_for_drain);
    if let Err(err) = result {
        let route = error_slot
            .lock()
            .expect("error route slot poisoned")
            .take()
            .or_else(|| event_slot.lock().expect("event slot poisoned").take());
        if let Some(route) = route {
            route.turn.fail();
            route.turn.cancel_and_drain(&pending_perms_for_drain);
            let terminal_ack = event_mailbox_tx.enqueue_terminal(
                &route,
                BridgeStreamItem::RunError {
                    message: if work_admission.retire_tx.borrow().is_some() {
                        "ACP_SPAWNED_WORK quota exceeded".into()
                    } else {
                        format!("acp connection terminated: {err}")
                    },
                },
            );
            let _ = terminal_ack.await;
        }
    }
    // Release every producer before closing the FIFO; the connection error,
    // when present, is queued behind all accepted events above.
    drop(event_slot.lock().expect("event slot poisoned").take());
    drop(event_mailbox_tx);
    if let Some(handle) = event_driver.handle.as_mut()
        && tokio::time::timeout(EVENT_DRIVER_SHUTDOWN_TIMEOUT, handle)
            .await
            .is_ok()
    {
        event_driver.disarm();
    }
}
