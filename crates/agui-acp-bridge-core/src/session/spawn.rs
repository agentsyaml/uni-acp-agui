use super::*;

pub(super) const COMMAND_BUFFER: usize = 8;
pub(super) const IN_PROCESS_DUPLEX_BUFFER: usize = 65_536;
pub(crate) async fn spawn_in_process_echo_session(
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError> {
    spawn_in_process_session_with(cfg, |s| Box::pin(echo_agent::run_echo_agent(s))).await
}

/// Spawn an in-process ACP session backed by a caller-provided agent runner.
///
/// `agent_runner` receives one half of a tokio duplex stream and is expected
/// to drive the ACP `Agent` builder loop until the stream is dropped. Used by
/// integration tests to plug in custom agents (image, failing, etc.) without
/// duplicating the duplex+transport plumbing.
#[doc(hidden)]
pub async fn spawn_in_process_session_with<F>(
    cfg: SessionConfig,
    agent_runner: F,
) -> Result<AcpSessionHandle, BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    let mut agent_guard = AbortOnDrop::new(tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process test agent terminated with error");
        }
    }));

    let (read, write) = tokio::io::split(client_stream);
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

    let result = spawn_session(transport, cfg).await;
    if result.is_ok() {
        agent_guard.disarm();
    }
    result
}

#[cfg(all(test, target_os = "linux"))]
pub(super) async fn spawn_in_process_session_with_work<F>(
    cfg: SessionConfig,
    work: Arc<WorkAdmission>,
    agent_runner: F,
) -> Result<AcpSessionHandle, BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);
    let mut agent_guard = AbortOnDrop::new(tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process test agent terminated with error");
        }
    }));
    let (read, write) = tokio::io::split(client_stream);
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);
    let result = spawn_session_with_work(transport, cfg, work).await;
    if result.is_ok() {
        agent_guard.disarm();
    }
    result
}

/// In-process equivalent of [`list_sessions_via`]: drive `session/list`
/// against a caller-provided agent over an in-memory duplex. Exercises the
/// **real** listing code path (the same one subprocess clients use) so tests
/// don't have to shortcut around it.
#[doc(hidden)]
pub async fn list_sessions_in_process_with<F>(
    cfg: SessionConfig,
    agent_runner: F,
) -> Result<Vec<SessionSummary>, BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process list agent terminated with error");
        }
    });

    let (read, write) = tokio::io::split(client_stream);
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

    list_sessions_via(transport, cfg).await
}

/// In-process equivalent of [`delete_session_via`].
#[doc(hidden)]
pub async fn delete_session_in_process_with<F>(
    cfg: SessionConfig,
    session_id: SessionId,
    agent_runner: F,
) -> Result<(), BridgeError>
where
    F: FnOnce(
            tokio::io::DuplexStream,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), BridgeError>> + Send>,
        > + Send
        + 'static,
{
    let (agent_stream, client_stream) = tokio::io::duplex(IN_PROCESS_DUPLEX_BUFFER);

    tokio::spawn(async move {
        if let Err(err) = agent_runner(agent_stream).await {
            tracing::warn!(error = %err, "in-process delete agent terminated with error");
        }
    });

    let (read, write) = tokio::io::split(client_stream);
    let transport = crate::guarded_transport::GuardedByteStreams::new(write, read);

    delete_session_via(transport, cfg, session_id).await
}

pub(crate) async fn spawn_session<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<AcpSessionHandle, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    spawn_session_with_work(connector, cfg, WorkAdmission::new()).await
}

pub(super) async fn spawn_session_with_work<T>(
    connector: T,
    cfg: SessionConfig,
    work_admission: Arc<WorkAdmission>,
) -> Result<AcpSessionHandle, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(COMMAND_BUFFER);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<SessionReady, BridgeError>>();
    let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
    let turn_queue = Arc::new(SessionTurnQueue::new(cfg.config.max_queued_turns));
    let unusable = Arc::new(AtomicBool::new(false));
    let init_state = Arc::new(Mutex::new(SessionInitState::default()));

    let handle_pending = pending_permissions.clone();
    let handle_turn_queue = turn_queue.clone();
    let handle_unusable = unusable.clone();
    let handle_init_state = init_state.clone();
    let event_buffer = cfg.config.event_buffer;
    let actor_state = SessionActorState {
        pending_permissions,
        turn_queue,
        unusable,
        init_state,
    };

    let mut actor_guard = AbortOnDrop::new(tokio::spawn(run_actor(
        connector,
        cfg,
        cmd_rx,
        ready_tx,
        actor_state,
        work_admission,
    )));

    match ready_rx.await {
        Ok(Ok(ready)) => {
            actor_guard.disarm();
            Ok(AcpSessionHandle::new(
                cmd_tx,
                handle_pending,
                handle_turn_queue,
                handle_unusable,
                ready.session_id,
                ready.supports_close,
                handle_init_state,
                event_buffer,
            ))
        }
        Ok(Err(err)) => Err(err),
        Err(_) => Err(BridgeError::SessionClosed),
    }
}

/// Keeps a newly spawned actor attached to its opener until the readiness
/// handshake succeeds. Dropping the opener future otherwise detaches the
/// actor, allowing a timed-out handshake to keep a connector/subprocess alive.
pub(super) struct AbortOnDrop<T> {
    pub(super) handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    pub(super) fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    pub(super) fn disarm(&mut self) {
        // Dropping a JoinHandle detaches the task. That is intentional only
        // after the actor has reported a successful ready handshake.
        self.handle.take();
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
