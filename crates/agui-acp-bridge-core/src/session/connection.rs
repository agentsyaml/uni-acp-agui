use super::*;
use std::ops::AsyncFnOnce;

pub(super) struct CallbackState {
    pub(super) init_state: Arc<Mutex<SessionInitState>>,
    pub(super) load_buffer: LoadBuffer,
    pub(super) event_slot_notification: EventSlot,
    pub(super) event_slot_permission: EventSlot,
    pub(super) mailbox_notification: EventMailboxTx,
    pub(super) mailbox_permission: EventMailboxTx,
    pub(super) spill: SpillBuffer,
    pub(super) pending_permissions: PendingPermissions,
    pub(super) policy: Arc<dyn PermissionPolicy>,
    pub(super) permission_timeout: Duration,
    pub(super) read_root: Arc<PathBuf>,
    pub(super) read_lock: Arc<AsyncMutex<()>>,
    pub(super) read_enabled: bool,
    pub(super) write_root: Arc<PathBuf>,
    pub(super) write_lock: Arc<AsyncMutex<()>>,
    pub(super) write_enabled: bool,
    pub(super) work: Arc<WorkAdmission>,
    pub(super) terminal_registry: Option<TerminalRegistry>,
    pub(super) terminal_enabled: bool,
}

pub(super) async fn connect<T>(
    connector: T,
    callbacks: CallbackState,
    foreground: impl AsyncFnOnce(ConnectionTo<Agent>) -> Result<(), agent_client_protocol::Error>,
) -> Result<(), agent_client_protocol::Error>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let callbacks = Arc::new(callbacks);
    Client
        .builder()
        .on_receive_notification(
            {
                let state = callbacks.clone();
                async move |notification: SessionNotification, _cx| {
                    notifications::session_notification(
                        notification,
                        state.init_state.clone(),
                        state.load_buffer.clone(),
                        state.event_slot_notification.clone(),
                        state.mailbox_notification.clone(),
                        state.spill.clone(),
                        state.pending_permissions.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: RequestPermissionRequest, responder, _cx| {
                    handle_permission_request(
                        req,
                        responder,
                        state.policy.clone(),
                        state.event_slot_permission.clone(),
                        state.mailbox_permission.clone(),
                        state.pending_permissions.clone(),
                        state.permission_timeout,
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: ReadTextFileRequest, responder, cx| {
                    filesystem::read_request(
                        req,
                        responder,
                        cx,
                        state.read_enabled,
                        state.read_root.clone(),
                        state.read_lock.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: WriteTextFileRequest, responder, cx| {
                    filesystem::write_request(
                        req,
                        responder,
                        cx,
                        state.write_enabled,
                        state.write_root.clone(),
                        state.write_lock.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: CreateTerminalRequest, responder, cx| {
                    terminal_requests::create(
                        req,
                        responder,
                        cx,
                        state.terminal_enabled,
                        state.terminal_registry.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: TerminalOutputRequest, responder, cx| {
                    terminal_requests::output(
                        req,
                        responder,
                        cx,
                        state.terminal_enabled,
                        state.terminal_registry.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: WaitForTerminalExitRequest, responder, cx| {
                    terminal_requests::wait(
                        req,
                        responder,
                        cx,
                        state.terminal_enabled,
                        state.terminal_registry.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: KillTerminalRequest, responder, cx| {
                    terminal_requests::kill(
                        req,
                        responder,
                        cx,
                        state.terminal_enabled,
                        state.terminal_registry.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = callbacks.clone();
                async move |req: ReleaseTerminalRequest, responder, cx| {
                    terminal_requests::release(
                        req,
                        responder,
                        cx,
                        state.terminal_enabled,
                        state.terminal_registry.clone(),
                        state.work.clone(),
                    )
                    .await
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, foreground)
        .await
}
