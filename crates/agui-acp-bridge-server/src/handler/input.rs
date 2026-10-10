use super::*;

/// Keep the existing `agent:session_init` event shape and append the ACP
/// config-option snapshot without changing the general AG-UI translator.
pub(super) fn session_init_event_with_config(
    modes: Option<&agui_acp_bridge_core::SessionModesInit>,
    models: Option<&agui_acp_bridge_core::stream::SessionModelsInit>,
    config_options: Option<&[agui_acp_bridge_core::SessionConfigOption]>,
) -> Event {
    let mut event = session_init_event(modes, models);
    if let Event::Custom(custom) = &mut event
        && let serde_json::Value::Object(payload) = &mut custom.value
    {
        payload.insert(
            "configOptions".to_string(),
            config_options
                .and_then(|options| serde_json::to_value(options).ok())
                .unwrap_or(serde_json::Value::Null),
        );
    }
    event
}

pub(super) fn run_error_with_code(code: &'static str, message: impl Into<String>) -> Event {
    Event::RunError(agui_rs_core::events::RunErrorEvent {
        message: message.into(),
        code: Some(code.to_string()),
        base: agui_rs_core::events::BaseEventFields::default(),
    })
}

/// Map an ACP-origin `BridgeError` to a machine-readable RUN_ERROR code.
///
/// The cancel-grace expiry is the one timeout a client can act on (it
/// cancelled and the agent refused to stop within the grace window), so it
/// gets its own code via the dedicated [`BridgeError::CancelGraceExpired`]
/// variant. Every other ACP/connection failure collapses to `ACP_ERROR` with
/// the error string preserved in the message.
pub(super) fn acp_failure_run_error(error: &BridgeError) -> Event {
    let is_cancel_grace = matches!(error, BridgeError::CancelGraceExpired(_));
    if is_cancel_grace {
        run_error_with_code("ACP_CANCEL_GRACE_EXPIRED", error.to_string())
    } else {
        run_error_with_code("ACP_ERROR", error.to_string())
    }
}

tokio::task_local! {
    pub(super) static CAPACITY_REJECTED: Cell<bool>;
}

/// Map one ACP prompt stop reason to the bridge's single AG-UI terminal event.
///
/// ACP's enum is non-exhaustive, so a future stop reason fails closed as an
/// AG-UI run error rather than being reported as a successful turn.
pub(super) fn stop_reason_terminal_event(
    thread_id: String,
    run_id: String,
    stop_reason: StopReason,
) -> Event {
    match stop_reason {
        StopReason::EndTurn => factory::run_finished(thread_id, run_id),
        StopReason::Cancelled => run_error_with_code("ACP_CANCELLED", "ACP prompt was cancelled"),
        StopReason::MaxTokens => {
            run_error_with_code("ACP_MAX_TOKENS", "ACP prompt reached the token limit")
        }
        StopReason::MaxTurnRequests => run_error_with_code(
            "ACP_MAX_TURN_REQUESTS",
            "ACP prompt reached the turn-request limit",
        ),
        StopReason::Refusal => run_error_with_code("ACP_REFUSAL", "ACP agent refused the prompt"),
        _ => run_error_with_code("ACP_STOP_REASON", "ACP returned an unknown stop reason"),
    }
}

pub(super) fn guarded_event_stream(
    events: Vec<AgUiResult<Event>>,
    run_guard: RunAdmissionGuard,
) -> BoxStream<'static, AgUiResult<Event>> {
    stream::unfold((events.into_iter(), run_guard), |(mut events, run_guard)| async move {
        events
            .next()
            .map(|event| (event, (events, run_guard)))
    })
    .boxed()
}
/// Outcome of inspecting the **trailing** message of an AG-UI
/// `RunAgentInput.messages`. The bridge's contract: a run carries a
/// fresh user prompt only when the tail is a `User` text message.
#[derive(Debug)]
pub(super) enum TrailingUser {
    /// `messages[]` is empty.
    Empty,
    /// Tail is a non-user message (assistant / tool / activity / reasoning).
    /// This is what AG-UI runtimes (CopilotKit's `agentic_chat`, …) post
    /// when they auto-fire a follow-up run after a tool turn so the LLM
    /// sees the tool result. The agent has already handled the prior
    /// user turn; we MUST NOT re-prompt it.
    NonUserTail,
    /// Tail is a `User` message but the content is multi-part
    /// (images / files). We do not yet forward those to ACP `prompt()`,
    /// which is text-only in the bridge's current scope.
    NonText,
    /// Tail is a fresh `User` text message — forward to ACP.
    Text(String),
}

pub(super) fn acp_resume_session_id(
    forwarded_props: &serde_json::Value,
) -> Result<Option<SessionId>, &'static str> {
    let Some(marker) = forwarded_props
        .as_object()
        .and_then(|props| props.get("acpResume"))
    else {
        return Ok(None);
    };

    match marker {
        serde_json::Value::Object(value) => {
            let Some(session_id) = value.get("sessionId").and_then(serde_json::Value::as_str)
            else {
                return Err("forwardedProps.acpResume.sessionId must be a non-empty string");
            };
            if session_id.trim().is_empty() {
                return Err("forwardedProps.acpResume.sessionId must be a non-empty string");
            }
            Ok(Some(SessionId::from(session_id.to_owned())))
        }
        _ => Err("forwardedProps.acpResume.sessionId must be a non-empty string"),
    }
}

/// Inspect the trailing message of `messages[]` per the bridge's
/// "trailing-user-only" contract. See [`TrailingUser`] for outcomes.
///
/// Why "trailing only" rather than "last user found via reverse search"?
/// AG-UI runtimes that drive `agentic_chat` (CopilotKit, …) re-fire
/// `runAgent` after every tool turn so their LLM-facing state machine
/// can see the tool result. Those follow-up runs carry the same
/// historical user message somewhere in the array but the **tail** is
/// always an `assistant`/`tool` message. A reverse-find extractor would
/// re-prompt the ACP agent with the historical user text on each
/// follow-up — and because the ACP session already contains the prior
/// reply in its history, the agent thinks the user is repeating the
/// same question and replies again, ad infinitum. Empirically this
/// shows up as "every run loops the previous turn".
///
/// The trailing-only rule matches the protocol intent: in ACP each
/// `prompt()` corresponds to one user-driven turn. AG-UI's `messages[]`
/// is the conversation transcript; the tail tells us what kind of turn
/// the runtime is asking for.
pub(super) fn extract_trailing_user_text(messages: &[Message]) -> TrailingUser {
    let Some(last) = messages.last() else {
        return TrailingUser::Empty;
    };
    match last {
        Message::User(u) => match &u.content {
            UserMessageContent::Text(t) => TrailingUser::Text(t.clone()),
            UserMessageContent::Parts(_) => TrailingUser::NonText,
        },
        _ => TrailingUser::NonUserTail,
    }
}
pub(super) const DEFAULT_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// How often an idle SSE stream emits a keepalive frame.
///
/// The vendored `agui-rs-server` `sse_body` can only emit encoded AG-UI
/// `Event`s (`data: {json}\n\n`); a bare SSE `:comment` frame is not
/// expressible through it. The keepalive is therefore a protocol-legal
/// `CUSTOM` event (`agent:keepalive`) — the same mechanism the bridge already
/// uses for `agent:session_init`. It exists purely to defeat proxy/ALB idle
/// timeouts (~60s) while the agent sits inside a long tool call; consumers
/// should ignore its value.
pub(super) const SSE_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Test hook: the keepalive interval for streams created inside tests. Tests
/// shrink it so cadence assertions run in milliseconds instead of seconds;
/// production callers go through [`build_event_stream`]/[`build_history_stream`],
/// which default to [`SSE_KEEPALIVE_INTERVAL`].
#[cfg(test)]
pub(super) fn keepalive_interval_for_tests() -> std::time::Duration {
    std::time::Duration::from_millis(50)
}

pub(super) fn keepalive_event() -> Event {
    Event::Custom(agui_rs_core::events::CustomEvent {
        name: "agent:keepalive".to_string(),
        value: serde_json::Value::Null,
        base: agui_rs_core::events::BaseEventFields::default(),
    })
}

pub(super) fn invalid_agui_body(message: impl std::fmt::Display) -> Response {
    (
        StatusCode::BAD_REQUEST,
        format!("invalid request body: {message}"),
    )
        .into_response()
}

/// Read and validate the raw body before handing it to `agui-rs-server`.
///
/// `agui-rs-server` reads its route body with `to_bytes(..., usize::MAX)`, so
/// Axum's `DefaultBodyLimit` extractor layer does not constrain the direct
/// AG-UI route. This middleware is deliberately a body-reading boundary rather
/// than another extractor layer — it is the ONLY size enforcement on that
/// route. It ALWAYS parses and validates the
/// `RunAgentInput` (keeping invalid inputs out of session admission and
/// mapping them to HTTP 400) regardless of whether a body `limit` is
/// configured; only the size check itself is limit-gated.
///
/// It also enforces the JSON-in / SSE-out media-type boundary
/// (`docs/PROTOCOL_CONFORMANCE.md` §2 "Non-JSON/SSE AG-UI transport"): an
/// explicit protobuf `Accept` is rejected with HTTP 406 before any session
/// admission, and the outbound `Accept` is pinned to SSE so the upstream
/// encoder can never select protobuf for `*/*`.
pub(super) async fn agui_input_boundary(
    limit: Option<usize>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.method() != Method::POST || request.uri().path() != "/" {
        return next.run(request).await;
    }

    if let Some(reject) = reject_wrong_media_types(request.headers()) {
        return reject;
    }

    let content_length_over_limit = |request: &Request<Body>| {
        limit.is_some_and(|limit| {
            request
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > limit)
        })
    };
    if content_length_over_limit(&request) {
        let limit = limit.expect("checked above");
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("invalid request body: request body exceeds {limit} bytes"),
        )
            .into_response();
    }

    let (mut parts, body) = request.into_parts();
    // Pin the outbound Accept to SSE. `agui-rs-encoder` treats `*/*` (and an
    // absent Accept, which curl/browser defaults make common) as
    // protobuf-capable; this bridge only implements SSE, so downstream the
    // encoder must always see an explicit `text/event-stream`.
    parts.headers.insert(
        header::ACCEPT,
        HeaderValue::from_static(agui_rs_core::AGUI_MEDIA_TYPE_SSE),
    );
    let mut body_stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body_stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => return invalid_agui_body(error),
        };
        if let Some(limit) = limit
            && bytes.len().saturating_add(chunk.len()) > limit
        {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("invalid request body: request body exceeds {limit} bytes"),
            )
                .into_response();
        }
        bytes.extend_from_slice(&chunk);
    }

    let input = match serde_json::from_slice::<RunAgentInput>(&bytes) {
        Ok(input) => input,
        Err(error) => return invalid_agui_body(error),
    };
    if let Err(error) = input.validate() {
        return invalid_agui_body(error);
    }

    let (response, capacity_rejected) = CAPACITY_REJECTED
        .scope(Cell::new(false), async {
            let response = next
                .run(Request::from_parts(parts, Body::from(bytes)))
                .await;
            let rejected = CAPACITY_REJECTED.try_with(Cell::get).unwrap_or(false);
            (response, rejected)
        })
        .await;
    if capacity_rejected && response.status() == StatusCode::INTERNAL_SERVER_ERROR {
        let (mut parts, body) = response.into_parts();
        parts.status = StatusCode::SERVICE_UNAVAILABLE;
        Response::from_parts(parts, body)
    } else {
        response
    }
}

/// Enforce the bridge's JSON-in / SSE-out media-type boundary on the AG-UI
/// route (`docs/PROTOCOL_CONFORMANCE.md` §2 "Non-JSON/SSE AG-UI transport").
///
/// The upstream `agui-rs-server` route picks its encoder from `Accept`, and
/// `agui-rs-encoder` treats `*/*` as protobuf-capable — but this bridge only
/// implements the JSON/SSE path, so an explicit protobuf `Accept` is rejected
/// with HTTP 406 BEFORE any session admission or SSE stream starts. `*/*` and
/// a missing `Accept` are treated as SSE (the middleware later pins the
/// outbound `Accept` header to `text/event-stream`, so the upstream encoder
/// can never select protobuf for them). A non-JSON `Content-Type` is rejected
/// with 400; an absent `Content-Type` or one carrying charset/suffix
/// parameters (`application/json; charset=utf-8`,
/// `application/vnd.api+json`) is tolerated.
pub(super) fn reject_wrong_media_types(headers: &HeaderMap) -> Option<Response> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    let accepts_proto = accept.is_some_and(|accept| {
        accept.split(',').any(|part| {
            part.split(';').next().is_some_and(|media| {
                media
                    .trim()
                    .eq_ignore_ascii_case(agui_rs_core::AGUI_MEDIA_TYPE_PROTOBUF)
            })
        })
    });
    if accepts_proto {
        return Some(
            (
                StatusCode::NOT_ACCEPTABLE,
                "this bridge only implements the AG-UI JSON/SSE transport; \
                 the AG-UI protobuf encoding is not supported",
            )
                .into_response(),
        );
    }

    let json_content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|media| {
            let media = media.trim();
            media.eq_ignore_ascii_case("application/json")
                || (media.ends_with("+json") && !media.is_empty())
        });
    if json_content_type {
        None
    } else {
        Some(invalid_agui_body("content-type must be application/json"))
    }
}
