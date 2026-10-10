use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

const MIN_BEARER_TOKEN_LEN: usize = 16;

pub(super) fn validate_bearer_token(token: &str) -> Result<(), String> {
    if token.is_empty() {
        return Err("AGUI_ACP_BRIDGE_TOKEN must not be empty".into());
    }
    if token.len() < MIN_BEARER_TOKEN_LEN {
        return Err(format!(
            "bearer token must be at least {MIN_BEARER_TOKEN_LEN} bytes"
        ));
    }
    if !token.is_ascii() || token.chars().any(char::is_whitespace) {
        return Err("bearer token must contain only non-whitespace ASCII characters".into());
    }
    if token.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("bearer token must not contain control characters".into());
    }
    Ok(())
}

fn constant_time_eq(expected: &[u8], provided: &[u8]) -> bool {
    let mut difference = (expected.len() ^ provided.len()) as u64;
    for index in 0..expected.len().max(provided.len()) {
        difference |= u64::from(
            expected.get(index).copied().unwrap_or_default()
                ^ provided.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

pub(super) fn has_valid_bearer(headers: &HeaderMap, expected: &[u8]) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some((scheme, credentials)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("Bearer")
        || credentials.is_empty()
        || credentials.chars().any(char::is_whitespace)
    {
        return false;
    }
    constant_time_eq(expected, credentials.as_bytes())
}

fn is_anonymous_health_probe(request: &Request<Body>) -> bool {
    matches!(request.method(), &Method::GET | &Method::HEAD)
        && request
            .uri()
            .path_and_query()
            .is_some_and(|value| value.as_str() == "/health")
}

fn unauthorized() -> Response {
    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

pub(super) async fn bridge_security_middleware(
    allowed_origins: Arc<HashSet<String>>,
    bearer_token: Option<Arc<str>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if crate::mcp_endpoint::is_mcp_path(request.uri().path())
        && !crate::mcp_endpoint::origin_is_allowed(request.headers(), &allowed_origins)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let scoped_mcp_post = request.method() == Method::POST
        && request
            .uri()
            .path()
            .strip_prefix("/mcp/")
            .is_some_and(|tail| !tail.is_empty() && !tail.contains('/'));
    if scoped_mcp_post
        || bearer_token.is_none()
        || is_anonymous_health_probe(&request)
        || has_valid_bearer(
            request.headers(),
            bearer_token.as_deref().unwrap_or_default().as_bytes(),
        )
    {
        next.run(request).await
    } else {
        unauthorized()
    }
}
