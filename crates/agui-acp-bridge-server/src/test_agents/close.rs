use super::*;

/// Close behavior used by the lifecycle test agent.
#[derive(Debug, Clone, Copy)]
pub enum CloseBehavior {
    Success,
    Error,
    Timeout,
}

/// ACP SessionIds received by a close-capable test agent.
pub type SharedCloseSessionIds = std::sync::Arc<Mutex<Vec<String>>>;

/// Coordination hooks for lifecycle claim race tests.
#[derive(Clone, Debug)]
pub struct LifecycleControl {
    close_started: std::sync::Arc<Notify>,
    allow_close: std::sync::Arc<Notify>,
    setting_started: std::sync::Arc<Notify>,
    setting_started_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    allow_setting: std::sync::Arc<Notify>,
}

impl LifecycleControl {
    #[must_use]
    pub fn new() -> Self {
        Self {
            close_started: std::sync::Arc::new(Notify::new()),
            allow_close: std::sync::Arc::new(Notify::new()),
            setting_started: std::sync::Arc::new(Notify::new()),
            setting_started_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            allow_setting: std::sync::Arc::new(Notify::new()),
        }
    }

    pub async fn wait_close_started(&self) {
        self.close_started.notified().await;
    }

    pub async fn wait_setting_started(&self) {
        self.setting_started.notified().await;
    }

    pub async fn wait_settings_started(&self, count: usize) {
        loop {
            let notified = self.setting_started.notified();
            if self
                .setting_started_count
                .load(std::sync::atomic::Ordering::Acquire)
                >= count
            {
                return;
            }
            notified.await;
        }
    }

    pub fn release_close(&self) {
        self.allow_close.notify_waiters();
    }

    pub fn release_setting(&self) {
        self.allow_setting.notify_waiters();
    }
}

impl Default for LifecycleControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Agent fixture that advertises `sessionCapabilities.close`, records the
/// typed ACP SessionId in each close request, and returns the selected close
/// outcome. The fixture also supports a normal prompt so endpoint admission
/// tests can create a cached session through the real route.
pub async fn run_close_agent(
    stream: DuplexStream,
    closed_ids: SharedCloseSessionIds,
    behavior: CloseBehavior,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CloseSessionRequest, CloseSessionResponse, SessionCapabilities, SessionCloseCapabilities,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-close-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().close(SessionCloseCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    "real-close-session-id",
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let closed_ids = closed_ids.clone();
                async move |req: CloseSessionRequest, responder, _cx| {
                    closed_ids
                        .lock()
                        .expect("close ids poisoned")
                        .push(req.session_id.0.to_string());
                    match behavior {
                        CloseBehavior::Success => responder.respond(CloseSessionResponse::new()),
                        CloseBehavior::Error => responder.respond_with_error(
                            agent_client_protocol::util::internal_error("close failed"),
                        ),
                        CloseBehavior::Timeout => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            responder.respond(CloseSessionResponse::new())
                        }
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("close agent prompt"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Close-capable fixture with independently gated close and setting RPCs.
/// Used to make lifecycle claim ordering deterministic in integration tests.
pub async fn run_close_setting_agent(
    stream: DuplexStream,
    closed_ids: SharedCloseSessionIds,
    control: LifecycleControl,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CloseSessionRequest, CloseSessionResponse, SessionCapabilities, SessionCloseCapabilities,
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    };

    fn options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "model-a",
                    vec![SessionConfigSelectOption::new("model-a", "Model A")],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-close-setting-race-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().close(SessionCloseCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from("real-close-session-id"))
                        .config_options(options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let closed_ids = closed_ids.clone();
                let control = control.clone();
                async move |req: CloseSessionRequest, responder, _cx| {
                    closed_ids
                        .lock()
                        .expect("close ids poisoned")
                        .push(req.session_id.0.to_string());
                    let allowed = control.allow_close.notified();
                    control.close_started.notify_waiters();
                    allowed.await;
                    responder.respond(CloseSessionResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |_req: SetSessionConfigOptionRequest, responder, _cx| {
                    let allowed = control.allow_setting.notified();
                    control
                        .setting_started_count
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                    control.setting_started.notify_waiters();
                    allowed.await;
                    responder.respond(SetSessionConfigOptionResponse::new(options("code")))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("close setting race prompt"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
