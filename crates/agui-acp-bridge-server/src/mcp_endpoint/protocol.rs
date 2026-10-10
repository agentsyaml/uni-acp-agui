use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProtocolMode {
    Legacy,
    Modern,
}

#[derive(Debug)]
pub(super) enum ProtocolModeError {
    InvalidHeader,
    MissingHeader,
    Unsupported(Option<String>),
    Mismatch {
        header_version: String,
        metadata_version: String,
    },
}

pub(super) fn request_metadata(value: &Value) -> Option<&Map<String, Value>> {
    value
        .get("params")
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
}

pub(super) fn modern_metadata_protocol(value: &Value) -> Option<&str> {
    request_metadata(value)?
        .get(MODERN_PROTOCOL_METADATA_KEY)
        .and_then(Value::as_str)
}

pub(super) fn has_modern_metadata_marker(value: &Value) -> bool {
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

pub(super) fn validate_modern_metadata(value: &Value) -> Result<(), &'static str> {
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

pub(super) fn modern_method_hint(req: &JsonRpcRequest, body: &Value, headers: &HeaderMap) -> bool {
    req.method == "server/discover"
        || has_modern_metadata_marker(body)
        || headers.contains_key(MCP_METHOD_HEADER)
        || headers.contains_key(MCP_NAME_HEADER)
}

pub(super) fn determine_protocol_mode(
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

pub(super) fn is_well_formed_protocol_version(version: &str) -> bool {
    let bytes = version.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

pub(super) fn validate_transport(
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
