use super::*;

/// JSON-RPC envelope: request shape we accept on `POST /mcp/{thread}`.
#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcRequest {
    /// JSON-RPC requires this to be the string "2.0". Non-string values are
    /// retained as `None` so the route can return a JSON-RPC invalid-request
    /// envelope instead of an axum deserialization rejection.
    #[serde(default, deserialize_with = "deserialize_jsonrpc_version")]
    pub(super) jsonrpc: Option<String>,
    /// Methods we know about. Notifications omit `id`; we still parse them.
    pub(super) method: String,
    #[serde(default)]
    pub(super) params: Value,
    #[serde(default)]
    pub(super) id: Option<Value>,
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
pub(super) struct JsonRpcResponse {
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
    pub(super) fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(super) fn err(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self::err_with_data(id, code, message, None)
    }

    pub(super) fn err_with_data(
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

    pub(super) fn invalid_request(id: Value) -> Self {
        Self::err(id, -32600, "Invalid Request")
    }
}

pub(super) fn request_id(value: &Value) -> Value {
    value
        .as_object()
        .and_then(|object| object.get("id"))
        .cloned()
        .unwrap_or(Value::Null)
}

pub(super) fn parse_jsonrpc_request(value: Value) -> Result<JsonRpcRequest, Box<JsonRpcResponse>> {
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

pub(super) fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, ()> {
    headers
        .get(name)
        .map(|value| value.to_str().map(str::trim).map_err(|_| ()))
        .transpose()
}

pub(super) fn decode_mcp_name_header(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let Some(value) = headers.get(MCP_NAME_HEADER) else {
        return Ok(None);
    };
    decode_mcp_name_value(value.as_bytes()).map(Some)
}

pub(super) fn decode_mcp_name_value(value: &[u8]) -> Result<String, ()> {
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
    // Strict standard decoding: padding required, canonical alphabet, no
    // whitespace — matching the hand-rolled decoder's acceptance exactly.
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|_| ())?;
    String::from_utf8(decoded).map_err(|_| ())
}

pub(super) fn is_json_content_type(headers: &HeaderMap) -> bool {
    header_text(headers, "content-type")
        .ok()
        .flatten()
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

pub(super) fn accepts_media_type(headers: &HeaderMap, media_type: &str) -> Result<bool, ()> {
    let Some(value) = header_text(headers, "accept")? else {
        return Ok(false);
    };
    Ok(value.split(',').any(|part| {
        part.split(';').next().is_some_and(|media| {
            media.trim().eq_ignore_ascii_case(media_type) && accept_quality_is_positive(part)
        })
    }))
}

pub(super) fn legacy_accept_is_supported(headers: &HeaderMap) -> Result<bool, ()> {
    let Some(value) = header_text(headers, "accept")? else {
        return Ok(true);
    };
    Ok(value.split(',').any(|part| {
        let media = part.split(';').next().map(str::trim).unwrap_or_default();
        (media.eq_ignore_ascii_case("application/json") || media == "*/*")
            && accept_quality_is_positive(part)
    }))
}

pub(super) fn accept_quality_is_positive(part: &str) -> bool {
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

pub(super) fn rpc_error(
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

pub(super) fn transport_error(
    id: Value,
    status: StatusCode,
    message: impl Into<String>,
) -> Response {
    rpc_error(status, id, -32600, message, None)
}

pub(super) fn invalid_params(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::OK, id, -32602, message, None)
}

pub(super) fn invalid_notification_params(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32602, message, None)
}

pub(super) fn missing_metadata(id: Value, message: impl Into<String>) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32602, message, None)
}

pub(super) fn mismatch_error(
    id: Value,
    message: impl Into<String>,
    data: Option<Value>,
) -> Response {
    rpc_error(StatusCode::BAD_REQUEST, id, -32020, message, data)
}

pub(super) fn unsupported_version(id: Value, requested: Option<String>) -> Response {
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

pub(super) fn method_not_found(id: Value) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(JsonRpcResponse::err(id, -32601, "Method not found")),
    )
        .into_response()
}

pub(super) fn parse_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(JsonRpcResponse::err(Value::Null, -32700, message)),
    )
        .into_response()
}
