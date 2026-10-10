use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct ToolsCallParams {
    pub(super) name: String,
    #[serde(default)]
    pub(super) arguments: Value,
}

pub(super) async fn handle_tools_list(
    state: &BridgeAppState,
    thread_id: &str,
    id: Value,
    mode: ProtocolMode,
) -> Response {
    let Some(entry) = state.frontend_tools().get(thread_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(JsonRpcResponse::err(
                id,
                -32602,
                format!("unknown thread: {thread_id}"),
            )),
        )
            .into_response();
    };
    let tools: Vec<Value> = entry
        .tools()
        .into_iter()
        .map(tool_def_to_mcp_tool)
        .collect();
    let result = if mode == ProtocolMode::Modern {
        let mut result = json!({
            "resultType": "complete",
            "tools": tools,
            "_meta": modern_server_metadata(),
        });
        if let (Some(result), Some(cache_fields)) = (
            result.as_object_mut(),
            modern_cache_fields(MODERN_TOOLS_CACHE_SCOPE).as_object(),
        ) {
            result.extend(cache_fields.clone());
        }
        result
    } else {
        json!({ "tools": tools })
    };
    Json(JsonRpcResponse::ok(id, result)).into_response()
}

pub(super) fn tool_def_to_mcp_tool(def: FrontendToolDef) -> Value {
    json!({
        "name": def.name,
        "description": def.description,
        "inputSchema": def.parameters,
    })
}

pub(super) fn parse_tools_call_params(
    params: Value,
    mode: ProtocolMode,
) -> Result<ToolsCallParams, String> {
    let (arguments_present, arguments_are_object) = match params
        .as_object()
        .and_then(|object| object.get("arguments"))
    {
        Some(arguments) => (true, arguments.is_object()),
        None => (false, true),
    };
    let mut parsed =
        serde_json::from_value::<ToolsCallParams>(params).map_err(|error| error.to_string())?;
    if mode == ProtocolMode::Modern {
        if arguments_present && !arguments_are_object {
            return Err("arguments must be an object".into());
        }
        if !arguments_present {
            parsed.arguments = json!({});
        }
    }
    Ok(parsed)
}

pub(super) fn validate_modern_notification(
    state: &BridgeAppState,
    thread_id: &str,
    req: &JsonRpcRequest,
    id: Value,
) -> Result<(), Box<Response>> {
    match req.method.as_str() {
        "notifications/initialized" | "notifications/cancelled" => {
            if !req.params.is_null() && !req.params.is_object() {
                Err(Box::new(invalid_notification_params(
                    id,
                    "params must be an object",
                )))
            } else {
                Ok(())
            }
        }
        "server/discover" | "tools/list" => {
            if !req.params.is_null() && !req.params.is_object() {
                Err(Box::new(invalid_notification_params(
                    id,
                    "params must be an object",
                )))
            } else {
                Ok(())
            }
        }
        "tools/call" => {
            let parsed = parse_tools_call_params(req.params.clone(), ProtocolMode::Modern)
                .map_err(|error| {
                    Box::new(invalid_notification_params(
                        id.clone(),
                        format!("invalid params: {error}"),
                    ))
                })?;
            let Some(entry) = state.frontend_tools().get(thread_id) else {
                return Err(Box::new(invalid_notification_params(
                    id,
                    format!("unknown thread: {thread_id}"),
                )));
            };
            if !entry.tools().iter().any(|tool| tool.name == parsed.name) {
                return Err(Box::new(invalid_notification_params(
                    id,
                    format!("unknown tool: {}", parsed.name),
                )));
            }
            Ok(())
        }
        _ => Err(Box::new(method_not_found(id))),
    }
}

pub(super) async fn handle_tools_call(
    state: &BridgeAppState,
    thread_id: &str,
    id: Value,
    params: Value,
    mode: ProtocolMode,
) -> Response {
    let parsed = match parse_tools_call_params(params, mode) {
        Ok(p) => p,
        Err(e) => {
            return Json(JsonRpcResponse::err(
                id,
                -32602,
                format!("invalid params: {e}"),
            ))
            .into_response();
        }
    };

    let registry = state.frontend_tools();
    let Some(entry) = registry.get(thread_id) else {
        return Json(JsonRpcResponse::err(
            id,
            -32602,
            format!("unknown thread: {thread_id}"),
        ))
        .into_response();
    };

    // Validate the tool name against the current registry. We avoid
    // dispatching unknown names — they could only ever come from a
    // mis-configured agent or an attacker probing the surface.
    let known_names: std::collections::HashSet<String> =
        entry.tools().into_iter().map(|t| t.name).collect();
    if !known_names.contains(&parsed.name) {
        return Json(JsonRpcResponse::err(
            id.clone(),
            -32602,
            format!("unknown tool: {}", parsed.name),
        ))
        .into_response();
    }

    // Mint a tool_call_id; the frontend will echo it back. From here on
    // every log line for this dispatch carries the id in its span so
    // operators can correlate the MCP request, the complete AG-UI invocation
    // envelope, and the `/tool-response` POST.
    let tool_call_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!(
        "frontend_tool_call",
        tool_call_id = %tool_call_id,
        tool_name = %parsed.name,
        thread_id = %thread_id,
    );
    let _enter = span.enter();
    let Some((active_tx, rx)) = entry.register_pending_on_active_sender(tool_call_id.clone())
    else {
        // Either no prompt is in flight or the SSE stream just ended. The
        // atomic registration method guarantees no pending entry was created
        // while teardown owned the sender lock.
        return Json(JsonRpcResponse::ok(
            id.clone(),
            tool_result(
                mode,
                mcp_error_content("frontend tool call dropped: no active AG-UI prompt"),
            ),
        ))
        .into_response();
    };
    let mut pending_guard =
        entry.pending_call_guard_with_sender(tool_call_id.clone(), active_tx.clone());
    // The stream item includes START, optional complete ARGS, and END.
    // Disarm the legacy fallback before the first await: cancellation before
    // queue acceptance must not manufacture any SSE lifecycle events.
    pending_guard.complete();
    tracing::info!("dispatching frontend tool call");

    let dispatched = active_tx
        .send(BridgeStreamItem::FrontendToolCall {
            tool_call_id: tool_call_id.clone(),
            tool_name: parsed.name.clone(),
            arguments: parsed.arguments,
        })
        .await;
    if dispatched.is_err() {
        // The sender closed after atomic registration but before dispatch.
        // Clean up the pending entry to avoid a leak.
        tracing::warn!("SSE stream closed before dispatch; aborting tool call");
        entry.resolve_pending(
            &tool_call_id,
            agui_acp_bridge_core::frontend_tools::FrontendToolResponse::error(
                "frontend tool call dropped: SSE stream closed",
            ),
        );
        return Json(JsonRpcResponse::ok(
            id.clone(),
            tool_result(
                mode,
                mcp_error_content("frontend tool call dropped: SSE stream closed"),
            ),
        ))
        .into_response();
    }

    // Park the request until the frontend posts back, with a wall-clock
    // budget so misbehaving clients can't pin ACP turns indefinitely.
    let resolution = tokio::time::timeout(state.config().frontend_tool_timeout, rx).await;

    let result = match resolution {
        Ok(Ok(resp)) => {
            if resp.is_error {
                tracing::info!(is_error = true, "frontend tool returned error");
                tool_result(mode, mcp_error_content(resp.content))
            } else {
                tracing::info!(is_error = false, "frontend tool resolved");
                tool_result(mode, mcp_text_content(resp.content))
            }
        }
        Ok(Err(_)) => {
            // The Sender side dropped (registry drained); already cleaned up.
            tracing::warn!("frontend tool oneshot dropped before resolution");
            tool_result(
                mode,
                mcp_error_content("frontend tool aborted: registry closed"),
            )
        }
        Err(_) => {
            // Timeout: best-effort clean up the pending entry.
            tracing::warn!(
                timeout_secs = state.config().frontend_tool_timeout.as_secs(),
                "frontend tool timed out",
            );
            entry.resolve_pending(
                &tool_call_id,
                agui_acp_bridge_core::frontend_tools::FrontendToolResponse::error(
                    "frontend tool timed out",
                ),
            );
            tool_result(mode, mcp_error_content("frontend tool timed out"))
        }
    };

    Json(JsonRpcResponse::ok(id, result)).into_response()
}

pub(super) fn mcp_text_content(text: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": text.into() } ],
        "isError": false,
    })
}

pub(super) fn mcp_error_content(message: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": message.into() } ],
        "isError": true,
    })
}

pub(super) fn tool_result(mode: ProtocolMode, mut result: Value) -> Value {
    if mode == ProtocolMode::Modern
        && let Value::Object(object) = &mut result
    {
        object.insert("resultType".into(), Value::String("complete".into()));
    }
    result
}
