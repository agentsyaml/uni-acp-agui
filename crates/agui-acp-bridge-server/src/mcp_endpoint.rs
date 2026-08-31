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

use std::collections::HashSet;

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use agui_acp_bridge_core::frontend_tools::FrontendToolDef;
use agui_acp_bridge_core::stream::BridgeStreamItem;

use crate::handler::BridgeAppState;

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
        Self::err_with_data(id, code, message, None)
    }

    fn err_with_data(
        id: Value,
        code: i32,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data,
            }),
        }
    }

    fn invalid_request(id: Value) -> Self {
        Self::err(id, -32600, "Invalid Request")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProtocolMode {
    Legacy,
    Modern,
}

fn request_id(value: &Value) -> Value {
    value
        .as_object()
        .and_then(|object| object.get("id"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// Canonicalize one serialized origin to its exact scheme/host/effective-port
/// tuple. Paths, wildcards, userinfo, and missing effective ports are not
/// origins that this HTTP endpoint can safely allowlist.
pub(crate) fn canonicalize_origin(value: &str) -> Result<String, String> {
    if value.is_empty()
        || value.trim() != value
        || value.contains(',')
        || value.eq_ignore_ascii_case("null")
    {
        return Err("origin must be one exact serialized origin".into());
    }

    let uri = value
        .parse::<Uri>()
        .map_err(|_| "origin must be an absolute URI".to_string())?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| "origin must include a scheme".to_string())?
        .to_ascii_lowercase();
    let authority = uri
        .authority()
        .ok_or_else(|| "origin must include a host".to_string())?;
    if authority.as_str().contains('@') {
        return Err("origin must not include userinfo".into());
    }
    let authority_start = value
        .find("://")
        .ok_or_else(|| "origin must include an authority".to_string())?
        + 3;
    if value[authority_start..]
        .bytes()
        .any(|byte| matches!(byte, b'/' | b'?' | b'#'))
    {
        return Err("origin must not include a path or query".into());
    }

    let host = authority.host();
    if host.is_empty() || host.bytes().any(|byte| matches!(byte, b'*' | b'%')) {
        return Err("origin must include one non-wildcard host".into());
    }

    let authority_suffix = &authority.as_str()[host.len()..];
    let port = match authority_suffix {
        "" => default_origin_port(&scheme)
            .ok_or_else(|| "origin must include an effective port".to_string())?,
        suffix if suffix.starts_with(':') => authority
            .port_u16()
            .ok_or_else(|| "origin port must be a valid u16".to_string())?,
        _ => return Err("origin authority is malformed".into()),
    };

    Ok(format!("{scheme}://{}:{port}", host.to_ascii_lowercase()))
}

fn default_origin_port(scheme: &str) -> Option<u16> {
    match scheme {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    }
}

/// Origin validation is deliberately independent from bearer authentication:
/// a present origin must be allowlisted even when the bearer is valid, while a
/// missing origin remains valid for non-browser MCP agents.
pub(crate) fn origin_is_allowed(headers: &HeaderMap, allowed: &HashSet<String>) -> bool {
    let mut values = headers.get_all(header::ORIGIN).iter();
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(value) = value.to_str() else {
        return false;
    };
    canonicalize_origin(value).is_ok_and(|origin| allowed.contains(&origin))
}

pub(crate) fn is_mcp_path(path: &str) -> bool {
    path == "/mcp" || path.starts_with("/mcp/")
}

fn parse_jsonrpc_request(value: Value) -> Result<JsonRpcRequest, Box<JsonRpcResponse>> {
    let id = request_id(&value);
    let Some(object) = value.as_object() else {
        return Err(Box::new(JsonRpcResponse::invalid_request(id)));
    };
    if let Some(request_id) = object.get("id")
        && (request_id.is_null() || !(request_id.is_string() || request_id.is_number()))
    {
        return Err(Box::new(JsonRpcResponse::invalid_request(Value::Null)));
    }
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || object.get("method").and_then(Value::as_str).is_none()
    {
        return Err(Box::new(JsonRpcResponse::invalid_request(id)));
    }
    serde_json::from_value(value).map_err(|_| Box::new(JsonRpcResponse::invalid_request(id)))
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    headers
        .get(name)
        .map(|value| value.to_str().map(str::trim).map_err(|_| ()))
        .transpose()
}

fn decode_mcp_name_header(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let Some(value) = headers.get(MCP_NAME_HEADER) else {
        return Ok(None);
    };
    decode_mcp_name_value(value.as_bytes()).map(Some)
}

fn decode_mcp_name_value(value: &[u8]) -> Result<String, ()> {
    if value.is_empty()
        || value.first().is_some_and(|byte| byte.is_ascii_whitespace())
        || value.last().is_some_and(|byte| byte.is_ascii_whitespace())
        || value
            .iter()
            .any(|byte| !byte.is_ascii() || byte.is_ascii_control())
    {
        return Err(());
    }
    let value = std::str::from_utf8(value).map_err(|_| ())?;
    let encoded = value.starts_with("=?base64?") && value.ends_with("?=");
    if !encoded {
        return Ok(value.to_owned());
    }
    if !value.starts_with("=?base64?") || !value.ends_with("?=") {
        return Err(());
    }
    let payload = &value[9..value.len() - 2];
    let decoded = decode_standard_base64(payload).ok_or(())?;
    String::from_utf8(decoded).map_err(|_| ())
}

fn decode_standard_base64(value: &str) -> Option<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 4 * 3);
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = index + 1 == bytes.len() / 4;
        let first = base64_value(chunk[0])?;
        let second = base64_value(chunk[1])?;
        decoded.push((first << 2) | (second >> 4));
        match chunk[2] {
            b'=' => {
                if !last || chunk[3] != b'=' || second & 0x0f != 0 {
                    return None;
                }
            }
            byte => {
                let third = base64_value(byte)?;
                decoded.push((second << 4) | (third >> 2));
                match chunk[3] {
                    b'=' => {
                        if !last || third & 0x03 != 0 {
                            return None;
                        }
                    }
                    byte => {
                        let fourth = base64_value(byte)?;
                        decoded.push((third << 6) | fourth);
                    }
                }
            }
        }
    }
    Some(decoded)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    header_text(headers, "content-type")
        .ok()
        .flatten()
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn accepts_media_type(headers: &HeaderMap, media_type: &str) -> Result<bool, ()> {
    let Some(value) = header_text(headers, "accept")? else {
        return Ok(false);
    };
    Ok(value.split(',').any(|part| {
        part.split(';').next().is_some_and(|media| {
            media.trim().eq_ignore_ascii_case(media_type) && accept_quality_is_positive(part)
        })
    }))
}

fn legacy_accept_is_supported(headers: &HeaderMap) -> Result<bool, ()> {
    let Some(value) = header_text(headers, "accept")? else {
        return Ok(true);
    };
    Ok(value.split(',').any(|part| {
        let media = part.split(';').next().map(str::trim).unwrap_or_default();
        (media.eq_ignore_ascii_case("application/json") || media == "*/*")
            && accept_quality_is_positive(part)
    }))
}

fn accept_quality_is_positive(part: &str) -> bool {
    let Some(quality) = part.split(';').skip(1).find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.trim()
            .eq_ignore_ascii_case("q")
            .then_some(value.trim())
    }) else {
        return true;
    };
    quality.parse::<f32>().is_ok_and(|value| value > 0.0)
}

fn request_metadata(value: &Value) -> Option<&Map<String, Value>> {
    value
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
}

fn modern_metadata_protocol(value: &Value) -> Option<&str> {
    request_metadata(value)?
        .get(MODERN_PROTOCOL_METADATA_KEY)
        .and_then(Value::as_str)
}

fn has_modern_metadata_marker(value: &Value) -> bool {
    if value.get("_meta").is_some() {
        return true;
    }
    let Some(params) = value.get("params").and_then(Value::as_object) else {
        return false;
    };
    let Some(meta_value) = params.get("_meta") else {
        return false;
    };
    let Some(meta) = meta_value.as_object() else {
        return true;
    };
    meta.keys().any(|key| {
        key == MODERN_PROTOCOL_METADATA_KEY
            || key == MODERN_CAPABILITIES_METADATA_KEY
            || key.starts_with("io.modelcontextprotocol/")
            || key.starts_with("io.modelcontextprotocol.")
    })
}

fn validate_modern_metadata(value: &Value) -> Result<(), &'static str> {
    if value.get("_meta").is_some() {
        return Err("modern MCP metadata must be under params._meta");
    }
    let Some(params) = value.get("params").and_then(Value::as_object) else {
        return Err("params._meta is required");
    };
    let Some(meta) = params.get("_meta").and_then(Value::as_object) else {
        return Err("params._meta is required");
    };
    if meta
        .get(MODERN_PROTOCOL_METADATA_KEY)
        .and_then(Value::as_str)
        .is_none()
    {
        return Err("params._meta protocolVersion is required");
    }
    if !meta
        .get(MODERN_CAPABILITIES_METADATA_KEY)
        .is_some_and(Value::is_object)
    {
        return Err("params._meta clientCapabilities is required");
    }
    Ok(())
}

fn modern_method_hint(req: &JsonRpcRequest, body: &Value, headers: &HeaderMap) -> bool {
    req.method == "server/discover"
        || has_modern_metadata_marker(body)
        || headers.contains_key(MCP_METHOD_HEADER)
        || headers.contains_key(MCP_NAME_HEADER)
}

#[derive(Debug)]
enum ProtocolModeError {
    InvalidHeader,
    MissingHeader,
    Unsupported(Option<String>),
    Mismatch {
        header_version: String,
        metadata_version: String,
    },
}

fn determine_protocol_mode(
    req: &JsonRpcRequest,
    body: &Value,
    headers: &HeaderMap,
) -> Result<ProtocolMode, ProtocolModeError> {
    let version = match header_text(headers, "MCP-Protocol-Version") {
        Ok(Some(version)) if version.is_empty() || !is_well_formed_protocol_version(version) => {
            return Err(ProtocolModeError::InvalidHeader);
        }
        Ok(version) => version,
        Err(_) => return Err(ProtocolModeError::InvalidHeader),
    };
    let metadata_version = modern_metadata_protocol(body).map(ToOwned::to_owned);
    match version {
        Some(MODERN_PROTOCOL_VERSION) => {
            if let Some(metadata_version) = metadata_version
                && metadata_version != MODERN_PROTOCOL_VERSION
            {
                return Err(ProtocolModeError::Mismatch {
                    header_version: MODERN_PROTOCOL_VERSION.to_string(),
                    metadata_version,
                });
            }
            Ok(ProtocolMode::Modern)
        }
        Some(LEGACY_PROTOCOL_VERSION) if !modern_method_hint(req, body, headers) => {
            Ok(ProtocolMode::Legacy)
        }
        Some(LEGACY_PROTOCOL_VERSION) => {
            if let Some(metadata_version) = metadata_version {
                return Err(ProtocolModeError::Mismatch {
                    header_version: LEGACY_PROTOCOL_VERSION.to_string(),
                    metadata_version,
                });
            }
            Err(ProtocolModeError::Unsupported(Some(
                LEGACY_PROTOCOL_VERSION.to_string(),
            )))
        }
        Some(version) => Err(ProtocolModeError::Unsupported(Some(version.to_owned()))),
        None if modern_method_hint(req, body, headers) => Err(ProtocolModeError::MissingHeader),
        None => Ok(ProtocolMode::Legacy),
    }
}

fn is_well_formed_protocol_version(version: &str) -> bool {
    let bytes = version.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

fn rpc_error(
    status: StatusCode,
    id: Value,
    code: i32,
    message: impl Into<String>,
    data: Option<Value>,
) -> Response {
    (
        status,
        Json(JsonRpcResponse::err_with_data(id, code, message, data)),
    )
        .into_response()
}

fn transport_error(id: Value, status: StatusCode, message: impl Into<String>) -> Response {
    rpc_error(status, id, -32600, message, None)
}

fn invalid_params(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::OK, id, -32602, message, None)
}

fn invalid_notification_params(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32602, message, None)
}

fn missing_metadata(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32602, message, None)
}

fn mismatch_error(id: Value, message: impl Into<String>, data: Option<Value>) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32020, message, data)
}

fn unsupported_version(id: Value, requested: Option<String>) -> Response {
    rpc_error(
        StatusCode::BAD_REQUEST,
        id,
        -32022,
        "Unsupported MCP protocol version",
        Some(json!({
            "supported": [MODERN_PROTOCOL_VERSION],
            "requested": requested,
        })),
    )
}

fn method_not_found(id: Value) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(JsonRpcResponse::err(id, -32601, "Method not found")),
    )
        .into_response()
}

fn parse_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(JsonRpcResponse::err(Value::Null, -32700, message)),
    )
        .into_response()
}

fn validate_transport(
    mode: ProtocolMode,
    body: &Value,
    req: &JsonRpcRequest,
    headers: &HeaderMap,
    id: Value,
) -> Result<(), Box<Response>> {
    if mode == ProtocolMode::Legacy && headers.contains_key(MCP_SESSION_HEADER) {
        return Err(Box::new(transport_error(
            id,
            StatusCode::BAD_REQUEST,
            "MCP sessions are not supported",
        )));
    }

    match mode {
        ProtocolMode::Legacy => {
            if !legacy_accept_is_supported(headers).unwrap_or(false) {
                return Err(Box::new(transport_error(
                    id,
                    StatusCode::NOT_ACCEPTABLE,
                    "legacy MCP accepts application/json only",
                )));
            }
        }
        ProtocolMode::Modern => {
            let accepts_json = accepts_media_type(headers, "application/json").unwrap_or(false);
            let accepts_sse = accepts_media_type(headers, "text/event-stream").unwrap_or(false);
            if !accepts_json || !accepts_sse {
                return Err(Box::new(transport_error(
                    id,
                    StatusCode::NOT_ACCEPTABLE,
                    "Accept must include application/json and text/event-stream",
                )));
            }
            if let Err(message) = validate_modern_metadata(body) {
                return Err(Box::new(missing_metadata(id, message)));
            }
            let Some(method_header) = header_text(headers, MCP_METHOD_HEADER)
                .ok()
                .flatten()
                .filter(|value| !value.is_empty())
            else {
                return Err(Box::new(mismatch_error(id, "Mcp-Method is required", None)));
            };
            if method_header != req.method {
                return Err(Box::new(mismatch_error(
                    id,
                    "Mcp-Method must match the JSON-RPC method",
                    Some(json!({
                        "headerMethod": method_header,
                        "requestMethod": req.method,
                    })),
                )));
            }
            let name_header = match decode_mcp_name_header(headers) {
                Ok(name) => name,
                Err(()) => {
                    return Err(Box::new(mismatch_error(
                        id,
                        "Mcp-Name is not a valid header value",
                        None,
                    )));
                }
            };
            if req.method == "tools/call" {
                if name_header.is_none() {
                    return Err(Box::new(mismatch_error(
                        id,
                        "Mcp-Name is required for tools/call",
                        None,
                    )));
                }
            } else if name_header.is_some() {
                return Err(Box::new(mismatch_error(
                    id,
                    "Mcp-Name is only valid for tools/call",
                    None,
                )));
            }
        }
    }
    Ok(())
}

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

/// Route handler. axum extracts the `{thread}` path parameter and the
/// shared `BridgeAppState`.
pub(crate) async fn mcp_route(
    Path(thread_id): Path<String>,
    State(state): State<BridgeAppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !state.mcp_origin_allowed(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
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

async fn handle_tools_list(
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

fn parse_tools_call_params(params: Value, mode: ProtocolMode) -> Result<ToolsCallParams, String> {
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

fn validate_modern_notification(
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

async fn handle_tools_call(
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
            tool_result(
                mode,
                mcp_error_content("frontend tool call dropped: no active AG-UI prompt"),
            ),
        ))
        .into_response();
    };
    let mut pending_guard =
        entry.pending_call_guard_with_sender(tool_call_id.clone(), active_tx.clone());
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
    pending_guard.complete();

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

fn tool_result(mode: ProtocolMode, mut result: Value) -> Value {
    if mode == ProtocolMode::Modern
        && let Value::Object(object) = &mut result
    {
        object.insert("resultType".into(), Value::String("complete".into()));
    }
    result
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
    fn mcp_name_header_accepts_plain_and_encoded_utf8_only() {
        assert_eq!(
            decode_mcp_name_value(b"missing-tool").unwrap(),
            "missing-tool"
        );
        assert_eq!(decode_mcp_name_value(b"=?base64?w6k=?=").unwrap(), "é");
        for value in [
            b"=?base64?%%%?=".as_slice(),
            b"=?base64?w6k?=".as_slice(),
            b" missing-tool".as_slice(),
            b"missing-tool ".as_slice(),
            b"m\xc3\xa9".as_slice(),
        ] {
            assert!(decode_mcp_name_value(value).is_err(), "{value:?}");
        }
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

    fn modern_request(
        method: &str,
        id: Option<i64>,
        params: Value,
        accept: Option<&str>,
    ) -> axum::http::Request<axum::body::Body> {
        let mut params = params;
        if let Value::Object(object) = &mut params {
            let mut metadata = Map::new();
            metadata.insert(
                MODERN_PROTOCOL_METADATA_KEY.into(),
                json!(MODERN_PROTOCOL_VERSION),
            );
            metadata.insert(MODERN_CAPABILITIES_METADATA_KEY.into(), json!({}));
            object.insert("_meta".into(), Value::Object(metadata));
        }
        let mut body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        if let Some(id) = id {
            body["id"] = json!(id);
        }
        let mut builder = axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri("/mcp/thread")
            .header("content-type", "application/json")
            .header("MCP-Protocol-Version", MODERN_PROTOCOL_VERSION)
            .header("Mcp-Method", method);
        if let Some(accept) = accept {
            builder = builder.header("accept", accept);
        }
        if method == "tools/call" {
            builder = builder.header("Mcp-Name", "missing-tool");
        }
        builder
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    }

    fn with_origin(
        mut request: axum::http::Request<axum::body::Body>,
        origin: &str,
    ) -> axum::http::Request<axum::body::Body> {
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        request
    }

    async fn response_json(response: Response) -> (StatusCode, Value) {
        use http_body_util::BodyExt;

        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        };
        (status, value)
    }

    #[tokio::test]
    async fn modern_headers_and_metadata_are_enforced() {
        use axum::body::Body;
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let app = crate::handler::build_router(state);

        let mut missing_content_type = modern_request("server/discover", Some(1), json!({}), None);
        missing_content_type.headers_mut().remove("content-type");
        let (status, _) =
            response_json(app.clone().oneshot(missing_content_type).await.unwrap()).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let missing_accept = modern_request("server/discover", Some(1), json!({}), None);
        let (status, body) =
            response_json(app.clone().oneshot(missing_accept).await.unwrap()).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(body["error"]["code"], -32600);

        let mut missing_metadata = modern_request("server/discover", Some(1), json!({}), None);
        *missing_metadata.body_mut() = Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {}
            })
            .to_string(),
        );
        missing_metadata.headers_mut().insert(
            "accept",
            "application/json, text/event-stream".parse().unwrap(),
        );
        let (status, body) =
            response_json(app.clone().oneshot(missing_metadata).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);

        let mut top_level_metadata = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        *top_level_metadata.body_mut() = Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {},
                "_meta": {"io.modelcontextprotocol/protocolVersion": MODERN_PROTOCOL_VERSION}
            })
            .to_string(),
        );
        let (status, body) =
            response_json(app.clone().oneshot(top_level_metadata).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);

        let mut dot_metadata = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        *dot_metadata.body_mut() = Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {"_meta": {
                    "io.modelcontextprotocol.protocolVersion": MODERN_PROTOCOL_VERSION,
                    "io.modelcontextprotocol.clientCapabilities": {}
                }}
            })
            .to_string(),
        );
        let (status, body) = response_json(app.clone().oneshot(dot_metadata).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);

        let mut missing_method = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        missing_method.headers_mut().remove(MCP_METHOD_HEADER);
        let (status, body) =
            response_json(app.clone().oneshot(missing_method).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut missing_name = modern_request(
            "tools/call",
            Some(1),
            json!({"name": "missing-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        missing_name.headers_mut().remove(MCP_NAME_HEADER);
        let (status, body) = response_json(app.clone().oneshot(missing_name).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut missing_version = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        missing_version.headers_mut().remove("MCP-Protocol-Version");
        let (status, body) =
            response_json(app.clone().oneshot(missing_version).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut malformed_version = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        malformed_version
            .headers_mut()
            .insert("MCP-Protocol-Version", "not-a-version".parse().unwrap());
        let (status, body) =
            response_json(app.clone().oneshot(malformed_version).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut unsupported_version = modern_request(
            "server/discover",
            Some(1),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        unsupported_version
            .headers_mut()
            .insert("MCP-Protocol-Version", "2025-06-18".parse().unwrap());
        let (status, body) = response_json(app.oneshot(unsupported_version).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32022);
        assert_eq!(body["error"]["data"]["requested"], "2025-06-18");
        assert_eq!(
            body["error"]["data"]["supported"][0],
            MODERN_PROTOCOL_VERSION
        );
    }

    #[tokio::test]
    async fn mcp_origin_allowlist_requires_exact_present_origins() {
        use tower::ServiceExt;

        let state = BridgeAppState::builder(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        )
        .with_mcp_allowed_origins(["HTTPS://Allowed.Example"])
        .unwrap()
        .build();
        let app = crate::handler::build_router(state);

        let allowed = with_origin(
            modern_request(
                "server/discover",
                Some(1),
                json!({}),
                Some("application/json, text/event-stream"),
            ),
            "https://allowed.example:443",
        );
        assert_eq!(
            app.clone().oneshot(allowed).await.unwrap().status(),
            StatusCode::OK
        );

        let missing = modern_request(
            "server/discover",
            Some(2),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        assert_eq!(
            app.clone().oneshot(missing).await.unwrap().status(),
            StatusCode::OK
        );

        for origin in [
            "https://evil.example",
            "null",
            "*",
            "https://*.allowed.example",
            "https://allowed.example/",
            "https://allowed.example/path",
            "https://allowed.example?query=1",
            "https://allowed.example:bad",
        ] {
            let request = with_origin(
                modern_request(
                    "server/discover",
                    Some(3),
                    json!({}),
                    Some("application/json, text/event-stream"),
                ),
                origin,
            );
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::FORBIDDEN,
                "origin should be rejected: {origin}"
            );
        }
    }

    #[tokio::test]
    async fn present_origin_is_rejected_without_configuration_before_json_dispatch() {
        use axum::body::Body;
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let app = crate::handler::build_router(state);
        let request = axum::http::Request::post("/mcp/thread")
            .header("origin", "https://any.example")
            .body(Body::from("not-json"))
            .unwrap();
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn invalid_origin_never_dispatches_a_frontend_tool_call() {
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry("thread");
        entry.set_tools(vec![FrontendToolDef {
            name: "alert".into(),
            description: "alert".into(),
            parameters: json!({"type": "object"}),
        }]);
        let (active_tx, mut events) = tokio::sync::mpsc::channel(1);
        entry.set_active_sender(Some(active_tx));
        let app = crate::handler::build_router(state.clone());
        let request = axum::http::Request::post("/mcp/thread")
            .header("content-type", "application/json")
            .header("origin", "https://evil.example")
            .body(axum::body::Body::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "alert", "arguments": {}}
                })
                .to_string(),
            ))
            .unwrap();

        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(state.frontend_tools().pending_len("thread"), 0);
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn mcp_origin_check_does_not_replace_bearer_authentication() {
        use tower::ServiceExt;

        let state = BridgeAppState::builder(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        )
        .with_mcp_allowed_origins(["https://allowed.example"])
        .unwrap()
        .with_bearer_token("origin-test-bearer-token")
        .unwrap()
        .build();
        let app = crate::handler::build_router(state);

        let missing_bearer = with_origin(
            modern_request(
                "server/discover",
                Some(1),
                json!({}),
                Some("application/json, text/event-stream"),
            ),
            "https://allowed.example",
        );
        assert_eq!(
            app.clone().oneshot(missing_bearer).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let invalid_without_bearer = with_origin(
            modern_request(
                "server/discover",
                Some(2),
                json!({}),
                Some("application/json, text/event-stream"),
            ),
            "https://evil.example",
        );
        assert_eq!(
            app.clone()
                .oneshot(invalid_without_bearer)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );

        let mut authenticated_missing_origin = modern_request(
            "server/discover",
            Some(3),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        authenticated_missing_origin.headers_mut().insert(
            axum::http::header::AUTHORIZATION,
            "Bearer origin-test-bearer-token".parse().unwrap(),
        );
        assert_eq!(
            app.clone()
                .oneshot(authenticated_missing_origin)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let mut invalid_origin = with_origin(
            modern_request(
                "server/discover",
                Some(4),
                json!({}),
                Some("application/json, text/event-stream"),
            ),
            "https://evil.example",
        );
        invalid_origin.headers_mut().insert(
            axum::http::header::AUTHORIZATION,
            "Bearer origin-test-bearer-token".parse().unwrap(),
        );
        assert_eq!(
            app.oneshot(invalid_origin).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn modern_tools_use_the_bound_thread_registry() {
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry("thread");
        entry.set_tools(vec![FrontendToolDef {
            name: "missing-tool".into(),
            description: "modern".into(),
            parameters: json!({"type": "object"}),
        }]);
        let app = crate::handler::build_router(state);

        let list = modern_request(
            "tools/list",
            Some(10),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(list).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["resultType"], "complete");
        assert_eq!(body["result"]["cacheScope"], MODERN_TOOLS_CACHE_SCOPE);
        assert_eq!(body["result"]["ttlMs"], MODERN_CACHE_TTL_MS);
        assert_eq!(
            body["result"]["_meta"][MODERN_SERVER_INFO_METADATA_KEY]["name"],
            "agui-acp-bridge"
        );
        assert_eq!(body["result"]["tools"][0]["name"], "missing-tool");

        let call = modern_request(
            "tools/call",
            Some(11),
            json!({"name": "missing-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.oneshot(call).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("error").is_none());
        assert_eq!(body["result"]["resultType"], "complete");
        assert_eq!(body["result"]["isError"], true);
    }

    #[tokio::test]
    async fn modern_discover_is_stateless_and_returns_capabilities() {
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let app = crate::handler::build_router(state);
        let request = modern_request(
            "server/discover",
            Some(7),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(MCP_SESSION_HEADER).is_none());
        let (_, body) = response_json(response).await;
        assert_eq!(body["result"]["resultType"], "complete");
        assert_eq!(
            body["result"]["supportedVersions"][0],
            MODERN_PROTOCOL_VERSION
        );
        assert_eq!(
            body["result"]["_meta"][MODERN_SERVER_INFO_METADATA_KEY]["name"],
            "agui-acp-bridge"
        );
        assert_eq!(body["result"]["cacheScope"], MODERN_DISCOVER_CACHE_SCOPE);
        assert_eq!(
            body["result"]["capabilities"]["tools"]["listChanged"],
            false
        );
    }

    #[tokio::test]
    async fn modern_method_errors_and_notifications_use_http_semantics() {
        use axum::body::Body;
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let app = crate::handler::build_router(state);

        let unknown = modern_request(
            "does/not-exist",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(unknown).await.unwrap()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], -32601);

        let unknown_notification = modern_request(
            "does/not-exist",
            None,
            json!({}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) =
            response_json(app.clone().oneshot(unknown_notification).await.unwrap()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], -32601);
        assert_eq!(body["id"], Value::Null);

        let mut method_mismatch = modern_request(
            "server/discover",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        method_mismatch
            .headers_mut()
            .insert(MCP_METHOD_HEADER, "tools/list".parse().unwrap());
        let (status, body) =
            response_json(app.clone().oneshot(method_mismatch).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut name_on_list = modern_request(
            "tools/list",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        name_on_list
            .headers_mut()
            .insert(MCP_NAME_HEADER, "unexpected".parse().unwrap());
        let (status, body) = response_json(app.clone().oneshot(name_on_list).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let name_mismatch = modern_request(
            "tools/call",
            Some(8),
            json!({"name": "other-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(name_mismatch).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let mut encoded_name = modern_request(
            "tools/call",
            Some(8),
            json!({"name": "é", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        encoded_name.headers_mut().insert(
            MCP_NAME_HEADER,
            axum::http::HeaderValue::from_static("=?base64?w6k=?="),
        );
        let (status, body) = response_json(app.clone().oneshot(encoded_name).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], -32602);

        for plain_name in ["=?foo", "foo?="] {
            let mut plain_name_request = modern_request(
                "tools/call",
                Some(8),
                json!({"name": plain_name, "arguments": {}}),
                Some("application/json, text/event-stream"),
            );
            plain_name_request.headers_mut().insert(
                MCP_NAME_HEADER,
                axum::http::HeaderValue::from_static(plain_name),
            );
            let (status, body) =
                response_json(app.clone().oneshot(plain_name_request).await.unwrap()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["error"]["code"], -32602);
        }

        let mut malformed_name = modern_request(
            "tools/call",
            Some(8),
            json!({"name": "missing-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        malformed_name.headers_mut().insert(
            MCP_NAME_HEADER,
            axum::http::HeaderValue::from_static("=?base64?%%%?="),
        );
        let (status, body) =
            response_json(app.clone().oneshot(malformed_name).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);

        let notification_name_mismatch = modern_request(
            "tools/call",
            None,
            json!({"name": "other-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(
            app.clone()
                .oneshot(notification_name_mismatch)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32020);
        assert_eq!(body["id"], Value::Null);

        let unknown_tool_notification = modern_request(
            "tools/call",
            None,
            json!({"name": "missing-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(
            app.clone()
                .oneshot(unknown_tool_notification)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
        assert_eq!(body["id"], Value::Null);

        let mut replay = modern_request(
            "server/discover",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        replay
            .headers_mut()
            .insert(LAST_EVENT_ID_HEADER, "event-1".parse().unwrap());
        let (status, body) = response_json(app.clone().oneshot(replay).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["resultType"], "complete");

        let mut session_id = modern_request(
            "server/discover",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        session_id
            .headers_mut()
            .insert(MCP_SESSION_HEADER, "session-1".parse().unwrap());
        let (status, body) = response_json(app.clone().oneshot(session_id).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["resultType"], "complete");

        let mut batch = modern_request(
            "server/discover",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        *batch.body_mut() =
            Body::from("[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server/discover\"}]");
        let (status, body) = response_json(app.clone().oneshot(batch).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32600);

        let invalid = modern_request(
            "tools/call",
            Some(9),
            json!({"name": "missing-tool", "arguments": {}}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(invalid).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["error"]["code"], -32602);

        let mut null_id = modern_request(
            "server/discover",
            Some(8),
            json!({}),
            Some("application/json, text/event-stream"),
        );
        *null_id.body_mut() = Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": null,
                "method": "server/discover",
                "params": {"_meta": {
                    "io.modelcontextprotocol/protocolVersion": MODERN_PROTOCOL_VERSION,
                    "io.modelcontextprotocol/clientCapabilities": {}
                }}
            })
            .to_string(),
        );
        let (status, body) = response_json(app.clone().oneshot(null_id).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32600);
        assert_eq!(body["id"], Value::Null);

        let notification = modern_request(
            "notifications/initialized",
            None,
            json!({}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.clone().oneshot(notification).await.unwrap()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, Value::Null);

        for method in [axum::http::Method::GET, axum::http::Method::DELETE] {
            let request = axum::http::Request::builder()
                .method(method)
                .uri("/mcp/thread")
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }

    #[tokio::test]
    async fn modern_tools_call_arguments_must_be_objects_before_dispatch() {
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry("thread");
        entry.set_tools(vec![FrontendToolDef {
            name: "missing-tool".into(),
            description: "modern".into(),
            parameters: json!({"type": "object"}),
        }]);
        let (active_tx, mut events) = tokio::sync::mpsc::channel(8);
        entry.set_active_sender(Some(active_tx));
        let app = crate::handler::build_router(state.clone());

        for (label, arguments) in [
            ("array", json!([])),
            ("string", json!("text")),
            ("number", json!(42)),
            ("boolean", json!(true)),
            ("null", Value::Null),
        ] {
            let call = modern_request(
                "tools/call",
                Some(10),
                json!({"name": "missing-tool", "arguments": arguments}),
                Some("application/json, text/event-stream"),
            );
            let (status, body) = response_json(app.clone().oneshot(call).await.unwrap()).await;
            assert_eq!(status, StatusCode::OK, "invalid {label} arguments");
            assert_eq!(body["error"]["code"], -32602, "invalid {label} arguments");
            assert_eq!(
                body["error"]["message"], "invalid params: arguments must be an object",
                "invalid {label} arguments"
            );
            assert_eq!(state.frontend_tools().pending_len("thread"), 0);
            assert!(events.try_recv().is_err(), "invalid {label} dispatched");
        }

        let parsed =
            parse_tools_call_params(json!({"name": "missing-tool"}), ProtocolMode::Modern).unwrap();
        assert_eq!(parsed.arguments, json!({}));

        let invalid_notification = modern_request(
            "tools/call",
            None,
            json!({"name": "missing-tool", "arguments": []}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) =
            response_json(app.clone().oneshot(invalid_notification).await.unwrap()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], -32602);
        assert_eq!(
            body["error"]["message"],
            "invalid params: arguments must be an object"
        );
        assert_eq!(body["id"], Value::Null);

        let notification = modern_request(
            "tools/call",
            None,
            json!({"name": "missing-tool"}),
            Some("application/json, text/event-stream"),
        );
        let (status, body) = response_json(app.oneshot(notification).await.unwrap()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, Value::Null);
        assert_eq!(state.frontend_tools().pending_len("thread"), 0);
        assert!(events.try_recv().is_err(), "valid notification dispatched");
    }

    #[tokio::test]
    async fn legacy_initialize_and_tools_list_remain_request_response_compatible() {
        use tower::ServiceExt;

        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry("thread");
        entry.set_tools(vec![FrontendToolDef {
            name: "legacy_tool".into(),
            description: "legacy".into(),
            parameters: json!({"type": "object"}),
        }]);
        let app = crate::handler::build_router(state);

        let legacy_session = axum::http::Request::post("/mcp/thread")
            .header("content-type", "application/json")
            .header(MCP_SESSION_HEADER, "legacy-session")
            .body(axum::body::Body::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
                })
                .to_string(),
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(legacy_session).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        let legacy_replay = axum::http::Request::post("/mcp/thread")
            .header("content-type", "application/json")
            .header(LAST_EVENT_ID_HEADER, "legacy-event")
            .body(axum::body::Body::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
                })
                .to_string(),
            ))
            .unwrap();
        let (status, body) = response_json(app.clone().oneshot(legacy_replay).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["protocolVersion"], LEGACY_PROTOCOL_VERSION);

        let initialize = axum::http::Request::post("/mcp/thread")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": LEGACY_PROTOCOL_VERSION, "capabilities": {}}
                })
                .to_string(),
            ))
            .unwrap();
        let (status, body) = response_json(app.clone().oneshot(initialize).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["protocolVersion"], LEGACY_PROTOCOL_VERSION);

        let list = axum::http::Request::post("/mcp/thread")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
                    .to_string(),
            ))
            .unwrap();
        let (status, body) = response_json(app.oneshot(list).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"]["tools"][0]["name"], "legacy_tool");
    }

    #[tokio::test]
    async fn cancelled_tools_call_drops_pending_entry_promptly() {
        let state = BridgeAppState::new(
            std::sync::Arc::new(agui_acp_bridge_core::InProcessAcpClient::new()),
            std::path::PathBuf::from("/"),
        );
        let entry = state.frontend_tools().entry("cancel-thread");
        entry.set_tools(vec![FrontendToolDef {
            name: "alert".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }]);
        let (active_tx, mut events) = tokio::sync::mpsc::channel(1);
        entry.set_active_sender(Some(active_tx));

        let call_state = state.clone();
        let task = tokio::spawn(async move {
            handle_tools_call(
                &call_state,
                "cancel-thread",
                json!(1),
                json!({"name":"alert","arguments":{}}),
                ProtocolMode::Legacy,
            )
            .await
        });

        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("MCP call dispatches before waiting");
        assert_eq!(state.frontend_tools().pending_len("cancel-thread"), 1);

        task.abort();
        let _ = task.await;
        assert_eq!(state.frontend_tools().pending_len("cancel-thread"), 0);
        let end = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("cancelled MCP call closes its frontend lifecycle")
            .expect("frontend lifecycle end item");
        assert!(matches!(
            end,
            BridgeStreamItem::FrontendToolEnd { ref tool_call_id } if !tool_call_id.is_empty()
        ));
    }
}
