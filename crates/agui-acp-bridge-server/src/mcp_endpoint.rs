//! MCP HTTP JSON-RPC endpoint for the bridge's frontend-tool injection path.
//!
//! Mounted as `POST /mcp/{thread}` on the same axum router that hosts the
//! AG-UI run endpoint. The modern endpoint is a stateless, single-message
//! POST contract. It implements:
//!
//! - `server/discover` → static server/capability discovery;
//! - `tools/list` → lookup by `thread`, emit the latest registry snapshot;
//! - `tools/call` → push `BridgeStreamItem::FrontendToolCall` into the live
//!   SSE stream for that thread, await the matching `/tool-response`, then
//!   return the result envelope to the agent.
//!
//! The old `2024-11-05` `initialize`/`tools/*` request-response subset remains
//! as a compatibility path. It is not the modern Streamable HTTP lifecycle:
//! neither path creates MCP sessions or serves GET/DELETE/SSE.
//!
//! Authentication is applied by the outer bridge router. The endpoint also
//! refuses `tools/call` without an active prompt sender and refuses
//! `tools/list` for unknown threads, but those checks are not a replacement
//! for the router's bearer middleware.

use axum::{
    Json,
    body::Bytes,
    extract::{FromRequestParts, Path, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use agui_acp_bridge_core::frontend_tools::FrontendToolDef;
use agui_acp_bridge_core::stream::BridgeStreamItem;

use crate::handler::BridgeAppState;

mod origin;
mod protocol;
mod tools;
mod wire;

use protocol::*;
use tools::*;
use wire::*;

pub(crate) use origin::{canonicalize_origin, is_mcp_path, origin_is_allowed};
pub(crate) use wire::JsonRpcRequest;

const LEGACY_PROTOCOL_VERSION: &str = "2024-11-05";
const MODERN_PROTOCOL_VERSION: &str = "2026-07-28";
const MCP_METHOD_HEADER: &str = "Mcp-Method";
const MCP_NAME_HEADER: &str = "Mcp-Name";
const MCP_SESSION_HEADER: &str = "Mcp-Session-Id";
#[cfg(test)]
const LAST_EVENT_ID_HEADER: &str = "Last-Event-ID";
const MODERN_PROTOCOL_METADATA_KEY: &str = "io.modelcontextprotocol/protocolVersion";
const MODERN_CAPABILITIES_METADATA_KEY: &str = "io.modelcontextprotocol/clientCapabilities";
const MODERN_SERVER_INFO_METADATA_KEY: &str = "io.modelcontextprotocol/serverInfo";
const MODERN_DISCOVER_CACHE_SCOPE: &str = "public";
const MODERN_TOOLS_CACHE_SCOPE: &str = "private";
const MODERN_CACHE_TTL_MS: u64 = 0;

fn initialize_result() -> Value {
    json!({
        "protocolVersion": LEGACY_PROTOCOL_VERSION,
        "capabilities": {
            "tools": { "listChanged": false }
        },
        "serverInfo": {
            "name": "agui-acp-bridge",
            "version": env!("CARGO_PKG_VERSION"),
        }
    })
}

fn modern_server_info() -> Value {
    json!({
        "name": "agui-acp-bridge",
        "version": env!("CARGO_PKG_VERSION"),
    })
}

fn modern_server_metadata() -> Value {
    let mut metadata = Map::new();
    metadata.insert(MODERN_SERVER_INFO_METADATA_KEY.into(), modern_server_info());
    Value::Object(metadata)
}

fn modern_cache_fields(scope: &str) -> Value {
    json!({
        "ttlMs": MODERN_CACHE_TTL_MS,
        "cacheScope": scope,
    })
}

fn discover_result() -> Value {
    let mut result = json!({
        "resultType": "complete",
        "supportedVersions": [MODERN_PROTOCOL_VERSION],
        "capabilities": {
            "tools": { "listChanged": false }
        },
        "_meta": modern_server_metadata(),
    });
    if let (Some(result), Some(cache_fields)) = (
        result.as_object_mut(),
        modern_cache_fields(MODERN_DISCOVER_CACHE_SCOPE).as_object(),
    ) {
        result.extend(cache_fields.clone());
    }
    result
}

/// Authenticate using request parts so rejected requests never poll the body.
pub(crate) struct AuthenticatedMcpThread(String);

#[async_trait::async_trait]
impl FromRequestParts<BridgeAppState> for AuthenticatedMcpThread {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &BridgeAppState,
    ) -> Result<Self, Self::Rejection> {
        let Path(thread_id) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        if !state.mcp_credential_valid(&thread_id, &parts.headers) {
            return Err(StatusCode::UNAUTHORIZED.into_response());
        }
        if !state.mcp_origin_allowed(&parts.headers) {
            return Err(StatusCode::FORBIDDEN.into_response());
        }
        Ok(Self(thread_id))
    }
}

pub(crate) async fn mcp_route(
    AuthenticatedMcpThread(thread_id): AuthenticatedMcpThread,
    State(state): State<BridgeAppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !is_json_content_type(&headers) {
        return transport_error(
            Value::Null,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        );
    }
    let body = match serde_json::from_slice::<Value>(&body) {
        Ok(body) => body,
        Err(error) => return parse_error(StatusCode::BAD_REQUEST, format!("Parse error: {error}")),
    };
    let request_id = request_id(&body);
    let req = match parse_jsonrpc_request(body.clone()) {
        Ok(request) => request,
        Err(response) => return (StatusCode::BAD_REQUEST, Json(*response)).into_response(),
    };
    let mode = match determine_protocol_mode(&req, &body, &headers) {
        Ok(mode) => mode,
        Err(ProtocolModeError::InvalidHeader) => {
            return mismatch_error(
                request_id.clone(),
                "MCP-Protocol-Version must be a valid header value",
                None,
            );
        }
        Err(ProtocolModeError::MissingHeader) => {
            return mismatch_error(
                request_id.clone(),
                "MCP-Protocol-Version is required for modern MCP requests",
                None,
            );
        }
        Err(ProtocolModeError::Unsupported(requested)) => {
            return unsupported_version(request_id.clone(), requested);
        }
        Err(ProtocolModeError::Mismatch {
            header_version,
            metadata_version,
        }) => {
            return mismatch_error(
                request_id.clone(),
                "MCP-Protocol-Version does not match request metadata",
                Some(json!({
                    "headerVersion": header_version,
                    "metadataVersion": metadata_version,
                })),
            );
        }
    };
    if let Err(response) = validate_transport(mode, &body, &req, &headers, request_id.clone()) {
        return *response;
    }

    tracing::debug!(
        thread = %thread_id,
        method = %req.method,
        jsonrpc = ?req.jsonrpc,
        has_id = req.id.is_some(),
        "MCP request received"
    );

    if mode == ProtocolMode::Modern && req.method == "tools/call" {
        let body_name = req
            .params
            .as_object()
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str);
        let header_name = match decode_mcp_name_header(&headers) {
            Ok(Some(name)) => name,
            _ => {
                return mismatch_error(
                    request_id.clone(),
                    "Mcp-Name is not a valid header value",
                    None,
                );
            }
        };
        if body_name != Some(header_name.as_str()) {
            return mismatch_error(
                request_id.clone(),
                "Mcp-Name must match params.name for tools/call",
                Some(json!({
                    "headerName": header_name,
                    "requestName": body_name,
                })),
            );
        }
    }

    if mode == ProtocolMode::Modern
        && req.id.is_none()
        && let Err(response) =
            validate_modern_notification(&state, &thread_id, &req, request_id.clone())
    {
        return *response;
    }

    // Notifications: id is None and we MUST NOT reply with a JSON-RPC
    // envelope. Acknowledge with 202 Accepted (per MCP guidance).
    if req.id.is_none() {
        tracing::debug!(method = %req.method, thread = %thread_id, "accepted MCP notification");
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request_id;

    if matches!(req.method.as_str(), "server/discover" | "tools/list")
        && !req.params.is_null()
        && !req.params.is_object()
    {
        return invalid_params(id, "params must be an object");
    }

    let response = match req.method.as_str() {
        "initialize" if mode == ProtocolMode::Legacy => {
            Json(JsonRpcResponse::ok(id, initialize_result())).into_response()
        }
        "server/discover" if mode == ProtocolMode::Modern => {
            Json(JsonRpcResponse::ok(id, discover_result())).into_response()
        }
        "tools/list" => handle_tools_list(&state, &thread_id, id, mode).await,
        "tools/call" => handle_tools_call(&state, &thread_id, id, req.params, mode).await,
        // We only need to support the methods opencode (and any
        // well-behaved MCP client) actually issues. Surface anything
        // else as "method not found" rather than 500 so clients can
        // probe the surface gracefully.
        _ => method_not_found(id),
    };
    tracing::debug!(thread = %thread_id, method = %req.method, "MCP response sent");
    response
}

#[cfg(test)]
#[path = "mcp_endpoint/tests/mod.rs"]
mod tests;
