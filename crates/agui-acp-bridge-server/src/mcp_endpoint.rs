//! Minimal MCP "streamable HTTP" server for the bridge's frontend-tool
//! injection path.
//!
//! Mounted as `POST /mcp/{thread}` on the same axum router that hosts the
//! AG-UI run endpoint. Only the JSON-RPC subset MCP clients need is
//! implemented:
//!
//! - `initialize` → static reply advertising tools capability;
//! - `notifications/initialized` → no-op;
//! - `tools/list` → lookup by `thread`, emit the latest registry snapshot;
//! - `tools/call` → push `BridgeStreamItem::FrontendToolCall` into the
//!   live SSE stream for that thread, await the matching
//!   `/tool-response`, then return the result envelope to the agent.
//!
//! The transport is "request/response, no SSE": when an MCP client (e.g.
//! `opencode`) issues a request, we reply once with JSON. This is the
//! default streamable-HTTP behaviour for non-streaming tool calls and is
//! all opencode requires.
//!
//! Authentication is applied by the outer bridge router. The endpoint also
//! refuses `tools/call` without an active prompt sender and refuses
//! `tools/list` for unknown threads, but those checks are not a replacement
//! for the router's bearer middleware.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use agui_acp_bridge_core::frontend_tools::FrontendToolDef;
use agui_acp_bridge_core::stream::BridgeStreamItem;

use crate::handler::BridgeAppState;

/// JSON-RPC envelope: request shape we accept on `POST /mcp/{thread}`.
#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcRequest {
    /// JSON-RPC requires this to be the string "2.0". Non-string values are
    /// retained as `None` so the route can return a JSON-RPC invalid-request
    /// envelope instead of an axum deserialization rejection.
    #[serde(default, deserialize_with = "deserialize_jsonrpc_version")]
    jsonrpc: Option<String>,
    /// Methods we know about. Notifications omit `id`; we still parse them.
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    id: Option<Value>,
}

fn deserialize_jsonrpc_version<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?
        .as_str()
        .map(ToOwned::to_owned))
}

/// Minimal JSON-RPC reply. We always include `jsonrpc: "2.0"` so MCP
/// clients (and rmcp in particular) parse us cleanly.
#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn err(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }

    fn invalid_request(id: Value) -> Self {
        Self::err(id, -32600, "Invalid Request")
    }
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": { "listChanged": false }
        },
        "serverInfo": {
            "name": "agui-acp-bridge",
            "version": env!("CARGO_PKG_VERSION"),
        }
    })
}

/// Route handler. axum extracts the `{thread}` path parameter and the
/// shared `BridgeAppState`.
pub(crate) async fn mcp_route(
    Path(thread_id): Path<String>,
    State(state): State<BridgeAppState>,
    Json(req): Json<JsonRpcRequest>,
) -> Response {
    tracing::debug!(
        thread = %thread_id,
        method = %req.method,
        has_id = req.id.is_some(),
        "MCP request received"
    );
    if req.jsonrpc.as_deref() != Some("2.0") {
        return Json(JsonRpcResponse::invalid_request(
            req.id.unwrap_or(Value::Null),
        ))
        .into_response();
    }
    // Notifications: id is None and we MUST NOT reply with a JSON-RPC
    // envelope. Acknowledge with 202 Accepted (per MCP guidance).
    if req.id.is_none() {
        match req.method.as_str() {
            "notifications/initialized" | "notifications/cancelled" => {
                return StatusCode::ACCEPTED.into_response();
            }
            other => {
                tracing::debug!(method = %other, thread = %thread_id, "ignoring unknown MCP notification");
                return StatusCode::ACCEPTED.into_response();
            }
        }
    }
    let id = req.id.clone().unwrap_or(Value::Null);

    let response = match req.method.as_str() {
        "initialize" => Json(JsonRpcResponse::ok(id, initialize_result())).into_response(),
        "tools/list" => handle_tools_list(&state, &thread_id, id).await,
        "tools/call" => handle_tools_call(&state, &thread_id, id, req.params).await,
        // We only need to support the methods opencode (and any
        // well-behaved MCP client) actually issues. Surface anything
        // else as "method not found" rather than 500 so clients can
        // probe the surface gracefully.
        other => Json(JsonRpcResponse::err(
            id,
            -32601,
            format!("method not found: {other}"),
        ))
        .into_response(),
    };
    tracing::debug!(thread = %thread_id, method = %req.method, "MCP response sent");
    response
}

async fn handle_tools_list(state: &BridgeAppState, thread_id: &str, id: Value) -> Response {
    if !state.frontend_tools().has(thread_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(JsonRpcResponse::err(
                id,
                -32602,
                format!("unknown thread: {thread_id}"),
            )),
        )
            .into_response();
    }
    let entry = state.frontend_tools().entry(thread_id);
    let tools: Vec<Value> = entry
        .tools()
        .into_iter()
        .map(tool_def_to_mcp_tool)
        .collect();
    Json(JsonRpcResponse::ok(id, json!({ "tools": tools }))).into_response()
}

fn tool_def_to_mcp_tool(def: FrontendToolDef) -> Value {
    json!({
        "name": def.name,
        "description": def.description,
        "inputSchema": def.parameters,
    })
}

#[derive(Debug, Deserialize)]
struct ToolsCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

async fn handle_tools_call(
    state: &BridgeAppState,
    thread_id: &str,
    id: Value,
    params: Value,
) -> Response {
    let parsed = match serde_json::from_value::<ToolsCallParams>(params) {
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
    if !registry.has(thread_id) {
        return Json(JsonRpcResponse::err(
            id,
            -32602,
            format!("unknown thread: {thread_id}"),
        ))
        .into_response();
    }
    let entry = registry.entry(thread_id);

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
    // operators can correlate the MCP request, the AG-UI events, the
    // `/tool-response` POST, and the synthetic FrontendToolEnd.
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
            mcp_error_content("frontend tool call dropped: no active AG-UI prompt"),
        ))
        .into_response();
    };
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
            mcp_error_content("frontend tool call dropped: SSE stream closed"),
        ))
        .into_response();
    }

    // Park the request until the frontend posts back, with a wall-clock
    // budget so misbehaving clients can't pin ACP turns indefinitely.
    let resolution = tokio::time::timeout(state.config().frontend_tool_timeout, rx).await;

    // Whether we succeeded or timed out, dispatch a FrontendToolEnd so
    // the SSE-side translator closes the open TOOL_CALL_* envelope. We
    // do this via the same active_tx we used for the call — only valid
    // while the prompt is still alive; if it isn't we silently swallow
    // (the frontend can't be listening anyway).
    let _ = active_tx
        .send(BridgeStreamItem::FrontendToolEnd {
            tool_call_id: tool_call_id.clone(),
        })
        .await;

    let result = match resolution {
        Ok(Ok(resp)) => {
            if resp.is_error {
                tracing::info!(is_error = true, "frontend tool returned error");
                mcp_error_content(resp.content)
            } else {
                tracing::info!(is_error = false, "frontend tool resolved");
                mcp_text_content(resp.content)
            }
        }
        Ok(Err(_)) => {
            // The Sender side dropped (registry drained); already cleaned up.
            tracing::warn!("frontend tool oneshot dropped before resolution");
            mcp_error_content("frontend tool aborted: registry closed")
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
            mcp_error_content("frontend tool timed out")
        }
    };

    Json(JsonRpcResponse::ok(id, result)).into_response()
}

fn mcp_text_content(text: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": text.into() } ],
        "isError": false,
    })
}

fn mcp_error_content(message: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": message.into() } ],
        "isError": true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_def_to_mcp_tool_uses_input_schema_key() {
        let def = FrontendToolDef {
            name: "alert".into(),
            description: "show alert".into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}}}),
        };
        let tool = tool_def_to_mcp_tool(def);
        assert_eq!(tool["name"], "alert");
        assert_eq!(tool["description"], "show alert");
        assert!(tool["inputSchema"]["properties"]["text"].is_object());
    }

    #[test]
    fn initialize_does_not_advertise_unimplemented_list_changed_notifications() {
        let result = initialize_result();
        assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
    }

    #[test]
    fn jsonrpc_version_must_be_exactly_two_point_zero() {
        let valid: JsonRpcRequest = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .unwrap();
        assert_eq!(valid.jsonrpc.as_deref(), Some("2.0"));
        assert!(
            valid.id.is_none(),
            "valid notification must stay a notification"
        );

        for version in [json!("1.0"), json!(2.0), Value::Null] {
            let request: JsonRpcRequest = serde_json::from_value(json!({
                "jsonrpc": version,
                "id": 1,
                "method": "initialize"
            }))
            .unwrap();
            assert_ne!(request.jsonrpc.as_deref(), Some("2.0"));
        }

        let response = serde_json::to_value(JsonRpcResponse::invalid_request(json!(1))).unwrap();
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["error"]["code"], -32600);
        assert_eq!(response["error"]["message"], "Invalid Request");
    }

    #[test]
    fn jsonrpc_response_omits_optional_fields() {
        let r = JsonRpcResponse::ok(json!(1), json!({"x":1}));
        let s = serde_json::to_value(r).unwrap();
        assert!(s.get("error").is_none());
        let r = JsonRpcResponse::err(json!(2), -1, "boom");
        let s = serde_json::to_value(r).unwrap();
        assert!(s.get("result").is_none());
        assert_eq!(s["error"]["code"], -1);
    }

    #[test]
    fn mcp_text_content_shape_matches_spec() {
        let v = mcp_text_content("hi");
        assert_eq!(v["isError"], false);
        assert_eq!(v["content"][0]["type"], "text");
        assert_eq!(v["content"][0]["text"], "hi");
    }

    #[test]
    fn mcp_tool_failure_is_result_error_not_jsonrpc_error() {
        let result = mcp_error_content("frontend failed");
        let response = serde_json::to_value(JsonRpcResponse::ok(json!(1), result)).unwrap();

        assert!(response.get("error").is_none());
        assert_eq!(response["result"]["isError"], true);
        assert_eq!(response["result"]["content"][0]["type"], "text");
        assert_eq!(response["result"]["content"][0]["text"], "frontend failed");
    }
}
