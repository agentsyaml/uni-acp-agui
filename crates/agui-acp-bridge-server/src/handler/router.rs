use super::{
    BridgeAppState, BridgeError, BridgeHandler, CancelSessionBody, CloseSessionBody,
    CloseSessionStatus, DEFAULT_BODY_LIMIT_BYTES, DeleteSessionBody, DeleteSessionStatus,
    ResolveOutcome, SetSessionConfigOptionBody, SetSessionStatus, agui_input_boundary,
    bridge_security_middleware, typed_config_option_value,
};

/// Build the AG-UI axum router for a given bridge state.
///
/// Mounts:
/// - `POST /` — AG-UI run endpoint (handled by [`BridgeHandler`])
/// - `GET /health` — anonymous liveness probe; returns `200 {"status":"ok"}`
/// - `POST /approval` — resolve a deferred permission request by interrupt id
///
/// A request body limit of 16 MiB is applied to every route. If your agents
/// receive significantly larger AG-UI inputs (e.g. very long conversation
/// histories), build the router yourself by composing
/// [`build_router_inner`] with your own `DefaultBodyLimit` layer.
pub fn build_router(state: BridgeAppState) -> axum::Router {
    build_router_inner_with_agui_body_limit(state, DEFAULT_BODY_LIMIT_BYTES).layer(
        axum::extract::DefaultBodyLimit::max(DEFAULT_BODY_LIMIT_BYTES),
    )
}

/// Same as [`build_router`] without the outer `DefaultBodyLimit` extractor
/// layer.
///
/// The direct AG-UI route is still size-bounded internally: its body-reading
/// boundary applies the same 16 MiB [`DEFAULT_BODY_LIMIT_BYTES`] cap, because
/// `agui-rs-server` reads that route with `to_bytes(..., usize::MAX)` and no
/// extractor layer can constrain it (see [`agui_input_boundary`]). There is
/// no public way to raise that AG-UI-route cap; a request over the limit is
/// rejected with HTTP 413.
pub fn build_router_inner(state: BridgeAppState) -> axum::Router {
    build_router_inner_with_agui_body_limit(state, DEFAULT_BODY_LIMIT_BYTES)
}

fn build_router_inner_with_agui_body_limit(
    state: BridgeAppState,
    agui_body_limit: usize,
) -> axum::Router {
    use axum::{Json, extract::State, routing::get, routing::post};

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ApprovalRequest {
        thread_id: String,
        interrupt_id: String,
        approved: bool,
        option_id: Option<String>,
    }

    async fn approval(
        State(state): State<BridgeAppState>,
        Json(body): Json<ApprovalRequest>,
    ) -> axum::http::StatusCode {
        use agent_client_protocol::schema::v1::PermissionOptionId;
        use agui_acp_bridge_core::PermissionDecision;
        let decision = if body.approved {
            // For an `approved` payload, the caller MUST supply the
            // `optionId` the user picked. We do not silently default to
            // "allow_once": that string is unlikely to be one of the
            // agent's advertised options, and validation would catch it
            // anyway — surfacing the 422 here gives a clearer error.
            let Some(option_id) = body.option_id else {
                return axum::http::StatusCode::BAD_REQUEST;
            };
            PermissionDecision::Allow {
                option_id: PermissionOptionId::new(option_id),
            }
        } else {
            PermissionDecision::Deny
        };
        match state.resolve_permission(&body.thread_id, &body.interrupt_id, decision) {
            ResolveOutcome::Resolved => axum::http::StatusCode::OK,
            ResolveOutcome::InvalidOption => axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            ResolveOutcome::NotFound => axum::http::StatusCode::NOT_FOUND,
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ToolResponseBody {
        thread_id: String,
        tool_call_id: String,
        #[serde(default)]
        content: String,
        #[serde(default)]
        is_error: bool,
    }

    async fn tool_response(
        State(state): State<BridgeAppState>,
        Json(body): Json<ToolResponseBody>,
    ) -> axum::http::StatusCode {
        use agui_acp_bridge_core::FrontendToolResponse;
        let span = tracing::info_span!(
            "frontend_tool_response",
            tool_call_id = %body.tool_call_id,
            is_error = body.is_error,
        );
        let _enter = span.enter();
        let resp = if body.is_error {
            FrontendToolResponse::error(body.content)
        } else {
            FrontendToolResponse::ok(body.content)
        };
        if state.resolve_frontend_tool(&body.thread_id, &body.tool_call_id, resp) {
            tracing::info!("resolved");
            axum::http::StatusCode::OK
        } else {
            tracing::warn!("no pending tool call for id");
            axum::http::StatusCode::NOT_FOUND
        }
    }

    async fn health() -> Json<serde_json::Value> {
        Json(serde_json::json!({ "status": "ok" }))
    }

    /// `GET /sessions` — list the agent's persisted conversations via ACP
    /// `session/list`. The bridge holds no history of its own; this is a
    /// pass-through. A client supplies an entry's `sessionId` in the private
    /// resume marker while choosing its own AG-UI `threadId`.
    ///
    /// | Status | Meaning                                                  |
    /// |--------|----------------------------------------------------------|
    /// | 200    | `{ "sessions": [ { sessionId, cwd, title?, updatedAt? } ] }` |
    /// | 501    | the agent does not support `session/list`.               |
    /// | 502    | the agent errored or the listing connection failed.      |
    async fn sessions(State(state): State<BridgeAppState>) -> axum::response::Response {
        use axum::response::IntoResponse;
        match state.list_sessions().await {
            Ok(list) => Json(serde_json::json!({ "sessions": list })).into_response(),
            Err(BridgeError::Unsupported(what)) => (
                axum::http::StatusCode::NOT_IMPLEMENTED,
                Json(serde_json::json!({ "error": format!("agent does not support {what}") })),
            )
                .into_response(),
            Err(e) => {
                tracing::warn!(error = %e, "session/list failed");
                (
                    axum::http::StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response()
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SessionInitQuery {
        thread_id: String,
    }

    /// `GET /session/init?threadId=...` — synchronous discovery of the
    /// session's mode / model offering. Returns 404 when no session is
    /// open for that thread (the frontend should issue a normal AG-UI
    /// run first to create one).
    async fn session_init(
        State(state): State<BridgeAppState>,
        axum::extract::Query(q): axum::extract::Query<SessionInitQuery>,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;
        match state.session_init_state(&q.thread_id) {
            Some(init) => {
                let body = serde_json::json!({
                    "modes": init.modes,
                    "models": init.models,
                    "configOptions": init.config_options,
                });
                Json(body).into_response()
            }
            None => axum::http::StatusCode::NOT_FOUND.into_response(),
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SetSessionModeBody {
        thread_id: String,
        mode_id: String,
    }

    /// `POST /session/set-mode` — switch the session's mode. When config
    /// options are advertised this maps to the discovered `mode` option;
    /// otherwise it uses the old SDK-compatible `session/set_mode` fallback.
    ///
    /// | Status | Meaning                                                       |
    /// |--------|---------------------------------------------------------------|
    /// | 200    | mode accepted; the agent has confirmed the switch.            |
    /// | 404    | no session for `threadId`.                                    |
    /// | 409    | a lifecycle close/eviction currently owns the thread.          |
    /// | 408    | agent did not respond within `set_session_timeout`.           |
    /// | 422    | agent rejected (likely `modeId` not in `availableModes`).     |
    /// | 503    | session actor was closed mid-flight; retry creates a new one. |
    async fn set_mode(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionModeBody>,
    ) -> axum::http::StatusCode {
        match state.set_session_mode(&body.thread_id, body.mode_id).await {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_mode rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => {
                tracing::warn!("set_mode timed out waiting for agent");
                axum::http::StatusCode::REQUEST_TIMEOUT
            }
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/set-config-option` — set a discovered select/value-id
    /// config option. The agent response replaces the full cached option list.
    async fn set_config_option(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionConfigOptionBody>,
    ) -> axum::http::StatusCode {
        let value = typed_config_option_value(body.value);
        match state
            .set_session_config_option_value(&body.thread_id, body.config_id, value)
            .await
        {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_config_option rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => axum::http::StatusCode::REQUEST_TIMEOUT,
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/cancel` — request cancellation of the current turn for
    /// a cached session. The actor continues receiving final updates and
    /// drains all pending permissions before applying the grace timeout.
    async fn cancel_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<CancelSessionBody>,
    ) -> axum::http::StatusCode {
        match state.cancel_session(&body.thread_id) {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "session cancel failed");
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
            Err(SetSessionStatus::Timeout) => axum::http::StatusCode::REQUEST_TIMEOUT,
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/close` — explicitly close a cached ACP session when the
    /// agent advertises `sessionCapabilities.close`.
    async fn close_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<CloseSessionBody>,
    ) -> axum::http::StatusCode {
        match state.close_session(&body.thread_id).await {
            Ok(()) => axum::http::StatusCode::NO_CONTENT,
            Err(CloseSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(CloseSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(CloseSessionStatus::Unsupported) => axum::http::StatusCode::NOT_IMPLEMENTED,
            Err(CloseSessionStatus::Timeout) => axum::http::StatusCode::GATEWAY_TIMEOUT,
            Err(CloseSessionStatus::Acp(message)) => {
                tracing::warn!(error = %message, "session/close rejected by agent");
                axum::http::StatusCode::BAD_GATEWAY
            }
            Err(CloseSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// `POST /session/delete` — delete the persisted ACP session mapped to an
    /// exact bridge thread. Cache misses are local 404s.
    async fn delete_session(
        State(state): State<BridgeAppState>,
        Json(body): Json<DeleteSessionBody>,
    ) -> axum::http::StatusCode {
        match state.delete_session(&body.thread_id).await {
            Ok(()) => axum::http::StatusCode::NO_CONTENT,
            Err(DeleteSessionStatus::InvalidInput) => axum::http::StatusCode::BAD_REQUEST,
            Err(DeleteSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(DeleteSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(DeleteSessionStatus::Unsupported) => axum::http::StatusCode::NOT_IMPLEMENTED,
            Err(DeleteSessionStatus::Timeout) => axum::http::StatusCode::GATEWAY_TIMEOUT,
            Err(DeleteSessionStatus::Acp(message)) => {
                tracing::warn!(error = %message, "session/delete rejected by agent");
                axum::http::StatusCode::BAD_GATEWAY
            }
        }
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SetSessionModelBody {
        thread_id: String,
        model_id: String,
    }

    /// `POST /session/set-model` — compatibility alias for the discovered
    /// model config option. It never sends ACP `session/set_model`.
    async fn set_model(
        State(state): State<BridgeAppState>,
        Json(body): Json<SetSessionModelBody>,
    ) -> axum::http::StatusCode {
        match state
            .set_session_model(&body.thread_id, body.model_id)
            .await
        {
            Ok(()) => axum::http::StatusCode::OK,
            Err(SetSessionStatus::NotFound) => axum::http::StatusCode::NOT_FOUND,
            Err(SetSessionStatus::Busy) => axum::http::StatusCode::CONFLICT,
            Err(SetSessionStatus::Acp(msg)) => {
                tracing::warn!(error = %msg, "set_model rejected by agent");
                axum::http::StatusCode::UNPROCESSABLE_ENTITY
            }
            Err(SetSessionStatus::Timeout) => {
                tracing::warn!("set_model timed out waiting for agent");
                axum::http::StatusCode::REQUEST_TIMEOUT
            }
            Err(SetSessionStatus::SessionClosed) => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    // The AG-UI router carries its own state (Arc<H>); our auxiliary routes
    // need `BridgeAppState`. Build them as a separate sub-router and `.merge()`.
    let aux: axum::Router = {
        let r = axum::Router::new()
            .route("/health", get(health))
            .route("/sessions", get(sessions))
            .route("/approval", post(approval))
            .route("/tool-response", post(tool_response))
            .route("/mcp/:thread", post(crate::mcp_endpoint::mcp_route))
            .route("/session/init", get(session_init))
            .route("/session/set-mode", post(set_mode))
            .route("/session/set-config-option", post(set_config_option))
            .route("/session/cancel", post(cancel_session))
            .route("/session/close", post(close_session))
            .route("/session/delete", post(delete_session))
            .route("/session/set-model", post(set_model));
        r.with_state(state.clone())
    };

    let allowed_origins = state.mcp_allowed_origins();
    let bearer_token = state.bearer_token();
    agui_rs_server::axum::agui_router(BridgeHandler::new(state))
        .layer(axum::middleware::from_fn(move |request, next| {
            agui_input_boundary(Some(agui_body_limit), request, next)
        }))
        .merge(aux)
        .layer(axum::middleware::from_fn(move |request, next| {
            let allowed_origins = allowed_origins.clone();
            let bearer_token = bearer_token.clone();
            async move {
                bridge_security_middleware(allowed_origins, bearer_token, request, next).await
            }
        }))
}
