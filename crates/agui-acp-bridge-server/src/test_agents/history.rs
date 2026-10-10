use super::*;

/// Shared session store for [`run_session_history_agent_with`]. Maps
/// `session_id -> (title, persisted cwd, history lines)`. Sharing one store across multiple
/// spawned agent instances models a real agent that persists sessions to
/// disk across separate ACP connections.
pub type SharedSessionStore = std::sync::Arc<
    Mutex<std::collections::HashMap<String, (Option<String>, std::path::PathBuf, Vec<String>)>>,
>;

/// Agent that supports session persistence: `session/new`, `session/list`,
/// `session/load`, and `session/prompt`. Used to exercise the bridge's
/// conversation-history surface (`GET /sessions`, resume-via-`session/load`).
///
/// Behaviour:
/// - `initialize` advertises `loadSession = true` and
///   `sessionCapabilities.list = {}`.
/// - `session/new` mints a fresh `SessionId`, records it with an empty
///   history and a title derived from the first prompt.
/// - `session/prompt` appends a user+assistant turn to the session's stored
///   history and streams the assistant reply (`"echo: <text>"`).
/// - `session/list` returns one `SessionInfo` per recorded session.
/// - `session/load` replays the stored history as `AgentMessageChunk`
///   notifications (prefixed `HISTORY:`) before responding, mirroring how a
///   real agent surfaces a resumed conversation.
///
/// Each spawned instance gets a private store; pass [`SharedSessionStore`] to
/// share one store across instances (modelling cross-connection persistence).
pub async fn run_session_history_agent_with(
    stream: DuplexStream,
    store: SharedSessionStore,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ListSessionsRequest, ListSessionsResponse, LoadSessionRequest, LoadSessionResponse,
        SessionCapabilities, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelect, SessionConfigSelectOption, SessionInfo, SessionListCapabilities,
    };

    fn history_config_options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "history-mode",
                "History mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![SessionConfigSelectOption::new("new", "New")],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let store_new = store.clone();
    let store_list = store.clone();
    let store_load = store.clone();
    let store_prompt = store.clone();

    Agent
        .builder()
        .name("agui-bridge-session-history-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new()
                            .load_session(true)
                            .session_capabilities(
                                SessionCapabilities::new().list(SessionListCapabilities::default()),
                            ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_new.clone();
                async move |req: NewSessionRequest, responder, _cx| {
                    let id = Uuid::new_v4().to_string();
                    store
                        .lock()
                        .expect("store poisoned")
                        .insert(id.clone(), (None, req.cwd, Vec::new()));
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(id))
                            .config_options(history_config_options("new")),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_list.clone();
                async move |_req: ListSessionsRequest, responder, _cx| {
                    let guard = store.lock().expect("store poisoned");
                    let sessions: Vec<SessionInfo> = guard
                        .iter()
                        .map(|(id, (title, cwd, _))| {
                            SessionInfo::new(SessionId::from(id.clone()), cwd.clone())
                                .title(title.clone())
                        })
                        .collect();
                    responder.respond(ListSessionsResponse::new(sessions))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_load.clone();
                async move |req: LoadSessionRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let sid = req.session_id.clone();
                    let history = {
                        let guard = store.lock().expect("store poisoned");
                        let Some((_, persisted_cwd, history)) = guard.get(&sid.0.to_string())
                        else {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error("unknown session id"),
                            );
                        };
                        if req.cwd != *persisted_cwd {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "session/load cwd does not match persisted cwd",
                                ),
                            );
                        }
                        history.clone()
                    };
                    // Replay the stored history as notifications before the
                    // load response (the contract `session/load` defines).
                    for line in history {
                        cx.send_notification(SessionNotification::new(
                            sid.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!("HISTORY:{line}"))),
                            )),
                        ))?;
                    }
                    responder.respond(
                        LoadSessionResponse::new().config_options(history_config_options("loaded")),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_prompt.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let text = req
                        .prompt
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text(t) => Some(t.text.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    let sid = req.session_id.clone();
                    let reply = format!("echo: {text}");

                    {
                        let mut guard = store.lock().expect("store poisoned");
                        let entry = guard
                            .entry(sid.0.to_string())
                            .or_insert_with(|| (None, std::path::PathBuf::new(), Vec::new()));
                        if entry.0.is_none() {
                            entry.0 = Some(text.clone());
                        }
                        entry.2.push(format!("user:{text}"));
                        entry.2.push(format!("assistant:{reply}"));
                    }

                    cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(reply),
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
