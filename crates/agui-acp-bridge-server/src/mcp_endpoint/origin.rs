use super::*;
use std::collections::HashSet;

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
