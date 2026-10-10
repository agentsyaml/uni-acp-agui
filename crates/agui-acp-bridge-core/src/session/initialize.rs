use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn initialize(
    cx: &ConnectionTo<Agent>,
    cwd: PathBuf,
    mcp_url: Option<String>,
    mcp_headers: Vec<HttpHeader>,
    load_session_id: Option<SessionId>,
    load_buffer: &LoadBuffer,
    init_state: &Mutex<SessionInitState>,
    filesystem_capabilities: FileSystemCapabilities,
    terminal_capability: bool,
) -> Result<(SessionId, SessionInitState, bool), BridgeError> {
    // Send the agent a conventional absolute cwd (no Windows `\\?\` verbatim
    // prefix) so its persisted session directory matches what other tools use
    // and directory-scoped `session/list` can find it later.
    let cwd = crate::file_ops::acp_cwd(&cwd);
    let init_response = cx
        .send_request(
            InitializeRequest::new(ProtocolVersion::V1).client_capabilities(
                ClientCapabilities::new()
                    .fs(filesystem_capabilities)
                    .terminal(terminal_capability)
                    .session(
                        ClientSessionCapabilities::new().config_options(
                            SessionConfigOptionsCapabilities::new()
                                .boolean(BooleanConfigOptionCapabilities::new()),
                        ),
                    ),
            ),
        )
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    // ACP negotiates a wire protocol version, not a schema/package version.
    // Do not issue any session request until the agent explicitly accepts v1.
    require_protocol_v1(init_response.protocol_version)?;
    let supports_close = init_response
        .agent_capabilities
        .session_capabilities
        .close
        .is_some();

    // Compose the optional MCP server entry once; both new and load requests
    // carry it so frontend tools work on resumed sessions too.
    let mcp_servers: Vec<McpServer> = match mcp_url {
        Some(url) if init_response.agent_capabilities.mcp_capabilities.http => {
            vec![mcp_http_server(url, mcp_headers)]
        }
        Some(_) => {
            tracing::warn!(
                "agent did not advertise mcpCapabilities.http=true; \
                 frontend tools will not be injected"
            );
            vec![]
        }
        None => vec![],
    };

    // Strict resume path: load an existing ACP session only when the caller
    // asked for it and the agent advertises `loadSession`. Unsupported load or
    // a failed `session/load` is returned to the caller; it never becomes a
    // fresh `session/new`. The agent replays history as `session/update`
    // notifications during the call; those are captured into `load_buffer`
    // (see the notification handler) so the handler can surface them on the
    // resume run's stream.
    if let Some(sid) = load_session_id {
        if !init_response.agent_capabilities.load_session {
            return Err(BridgeError::ResumeUnsupported(
                "agent does not advertise loadSession".into(),
            ));
        }
        if sid.0.trim().is_empty() {
            return Err(BridgeError::ResumeFailed(
                "session/load requires a non-empty session id".into(),
            ));
        }

        use agent_client_protocol::schema::v1::LoadSessionRequest;
        let session_id = sid;
        // Arm the buffer so history notifications are captured rather
        // than dropped ("no active prompt").
        load_buffer
            .lock()
            .expect("load buffer poisoned")
            .replace(LoadHistory::default());
        let mut req = LoadSessionRequest::new(session_id.clone(), cwd.clone());
        if !mcp_servers.is_empty() {
            req = req.mcp_servers(mcp_servers.clone());
        }
        let load = cx.send_request(req).block_task().await;
        match load {
            Ok(resp) => {
                let exceeded = load_buffer
                    .lock()
                    .expect("load buffer poisoned")
                    .as_ref()
                    .is_some_and(|history| history.exceeded);
                if exceeded {
                    load_buffer.lock().expect("load buffer poisoned").take();
                    return Err(BridgeError::ResumeFailed(
                        "session/load history exceeds the bridge event or byte limit".into(),
                    ));
                }
                let current = init_state.lock().expect("init_state poisoned").clone();
                let init = init_state_from_load(&resp, &current);
                return Ok((session_id, init, supports_close));
            }
            Err(error) => {
                // A strict resume never falls through to session/new. Clear
                // any replay notifications before surfacing the load error.
                let exceeded = load_buffer
                    .lock()
                    .expect("load buffer poisoned")
                    .as_ref()
                    .is_some_and(|history| history.exceeded);
                load_buffer.lock().expect("load buffer poisoned").take();
                if exceeded {
                    return Err(BridgeError::ResumeFailed(
                        "session/load history exceeds the bridge event or byte limit".into(),
                    ));
                }
                return Err(BridgeError::ResumeFailed(format!(
                    "session/load failed: {error}"
                )));
            }
        }
    }

    let mut new_session = NewSessionRequest::new(cwd);
    if !mcp_servers.is_empty() {
        new_session = new_session.mcp_servers(mcp_servers);
    }

    let session = cx
        .send_request(new_session)
        .block_task()
        .await
        .map_err(BridgeError::Acp)?;

    let init = extract_init_state(&session);
    Ok((session.session_id, init, supports_close))
}

pub(super) fn mcp_http_server(url: String, headers: Vec<HttpHeader>) -> McpServer {
    McpServer::Http(McpServerHttp::new(MCP_SERVER_NAME, url).headers(headers))
}

/// Merge the fields supplied by a `LoadSessionResponse` into state collected
/// while replaying the loaded session. Absent response fields do not clear the
/// replayed capability snapshot.
pub(super) fn init_state_from_load(
    resp: &agent_client_protocol::schema::v1::LoadSessionResponse,
    current: &SessionInitState,
) -> SessionInitState {
    let mut init = current.clone();
    if let Some(modes) = resp.modes.as_ref() {
        init.modes = Some(modes_from_state(modes));
    }
    if let Some(config_options) = resp.config_options.as_ref() {
        init.config_options = Some(config_options.clone());
        #[cfg(feature = "unstable_session_model")]
        {
            init.models = models_from_config_options(config_options);
        }
    }
    init
}

/// Convert the stable model config option into the bridge's legacy serializable
/// model mirror. Returns `None` when the agent does not advertise a model
/// select option.
pub(super) fn extract_init_state(resp: &NewSessionResponse) -> SessionInitState {
    SessionInitState {
        modes: resp.modes.as_ref().map(modes_from_state),
        #[cfg(feature = "unstable_session_model")]
        models: resp
            .config_options
            .as_deref()
            .and_then(models_from_config_options),
        #[cfg(not(feature = "unstable_session_model"))]
        models: None,
        config_options: resp.config_options.clone(),
    }
}

pub(super) fn require_protocol_v1(actual: ProtocolVersion) -> Result<(), BridgeError> {
    if actual == ProtocolVersion::V1 {
        Ok(())
    } else {
        Err(BridgeError::ProtocolVersionMismatch {
            expected: ProtocolVersion::V1,
            actual,
        })
    }
}

pub(super) fn modes_from_state(state: &SessionModeState) -> SessionModesInit {
    SessionModesInit {
        current_mode_id: state.current_mode_id.0.to_string(),
        available_modes: state
            .available_modes
            .iter()
            .map(|m: &SessionMode| ModeOffering {
                id: m.id.0.to_string(),
                name: m.name.clone(),
                description: m.description.clone(),
            })
            .collect(),
    }
}

#[cfg(feature = "unstable_session_model")]
pub(super) fn models_from_config_options(
    options: &[SessionConfigOption],
) -> Option<SessionModelsInit> {
    let option = options
        .iter()
        .find(|option| option.category.as_ref() == Some(&SessionConfigOptionCategory::Model))?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    let available_models = match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options
            .iter()
            .map(|model| ModelOffering {
                id: model.value.0.to_string(),
                name: model.name.clone(),
                description: model.description.clone(),
            })
            .collect(),
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .map(|model| ModelOffering {
                id: model.value.0.to_string(),
                name: model.name.clone(),
                description: model.description.clone(),
            })
            .collect(),
        _ => return None,
    };
    Some(SessionModelsInit {
        current_model_id: select.current_value.0.to_string(),
        available_models,
    })
}
