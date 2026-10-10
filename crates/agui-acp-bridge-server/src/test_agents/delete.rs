use super::*;

/// Delete behavior used by the session-delete lifecycle tests.
#[derive(Debug, Clone, Copy)]
pub enum DeleteBehavior {
    Success,
    Error,
    Timeout,
}

/// ACP SessionIds received by a delete-capable test agent.
pub type SharedDeleteSessionIds = std::sync::Arc<Mutex<Vec<String>>>;

/// Agent fixture that advertises `sessionCapabilities.delete`, records the
/// typed ACP SessionId in each delete request, and returns the selected
/// outcome. It deliberately does not advertise `sessionCapabilities.list` so
/// the bridge's delete capability check remains independent of listing.
pub async fn run_delete_agent(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    behavior: DeleteBehavior,
) -> Result<(), BridgeError> {
    run_delete_agent_with_session_id(stream, deleted_ids, behavior, "real-delete-session-id").await
}

/// Variant of [`run_delete_agent`] that lets a test model different cached
/// ACP SessionIds under different logical thread keys.
pub async fn run_delete_agent_with_session_id(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    behavior: DeleteBehavior,
    session_id: &'static str,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        DeleteSessionRequest, DeleteSessionResponse, SessionCapabilities, SessionDeleteCapabilities,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-delete-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().delete(SessionDeleteCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(session_id)))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let deleted_ids = deleted_ids.clone();
                async move |req: DeleteSessionRequest, responder, _cx| {
                    deleted_ids
                        .lock()
                        .expect("delete ids poisoned")
                        .push(req.session_id.0.to_string());
                    match behavior {
                        DeleteBehavior::Success => responder.respond(DeleteSessionResponse::new()),
                        DeleteBehavior::Error => responder.respond_with_error(
                            agent_client_protocol::util::internal_error("delete failed"),
                        ),
                        DeleteBehavior::Timeout => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            responder.respond(DeleteSessionResponse::new())
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
                        TextContent::new("delete agent prompt"),
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

/// Coordination hooks for delete admission and lifecycle-claim tests.
#[derive(Clone, Debug)]
pub struct DeleteLifecycleControl {
    prompt_started: std::sync::Arc<Notify>,
    allow_prompt: std::sync::Arc<Notify>,
    setting_started: std::sync::Arc<Notify>,
    allow_setting: std::sync::Arc<Notify>,
    delete_started: std::sync::Arc<Notify>,
    allow_delete: std::sync::Arc<Notify>,
}

impl DeleteLifecycleControl {
    #[must_use]
    pub fn new() -> Self {
        Self {
            prompt_started: std::sync::Arc::new(Notify::new()),
            allow_prompt: std::sync::Arc::new(Notify::new()),
            setting_started: std::sync::Arc::new(Notify::new()),
            allow_setting: std::sync::Arc::new(Notify::new()),
            delete_started: std::sync::Arc::new(Notify::new()),
            allow_delete: std::sync::Arc::new(Notify::new()),
        }
    }

    pub async fn wait_prompt_started(&self) {
        self.prompt_started.notified().await;
    }

    pub async fn wait_setting_started(&self) {
        self.setting_started.notified().await;
    }

    pub async fn wait_delete_started(&self) {
        self.delete_started.notified().await;
    }

    pub fn release_prompt(&self) {
        self.allow_prompt.notify_waiters();
    }

    pub fn release_setting(&self) {
        self.allow_setting.notify_waiters();
    }

    pub fn release_delete(&self) {
        self.allow_delete.notify_waiters();
    }
}

impl Default for DeleteLifecycleControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Delete-capable fixture with independently gated prompt, setting, and
/// delete requests.
pub async fn run_delete_lifecycle_agent(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    control: DeleteLifecycleControl,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        DeleteSessionRequest, DeleteSessionResponse, SessionCapabilities, SessionConfigKind,
        SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SessionDeleteCapabilities, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse,
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
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-delete-lifecycle-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().delete(SessionDeleteCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from("real-delete-session-id"))
                        .config_options(options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let deleted_ids = deleted_ids.clone();
                let control = control.clone();
                async move |req: DeleteSessionRequest, responder, _cx| {
                    deleted_ids
                        .lock()
                        .expect("delete ids poisoned")
                        .push(req.session_id.0.to_string());
                    control.delete_started.notify_waiters();
                    control.allow_delete.notified().await;
                    responder.respond(DeleteSessionResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |_: SetSessionConfigOptionRequest, responder, _cx| {
                    control.setting_started.notify_waiters();
                    control.allow_setting.notified().await;
                    responder.respond(SetSessionConfigOptionResponse::new(options("code")))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    control.prompt_started.notify_waiters();
                    control.allow_prompt.notified().await;
                    cx.send_notification(SessionNotification::new(
                        req.session_id,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("delete lifecycle prompt"),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                }
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
