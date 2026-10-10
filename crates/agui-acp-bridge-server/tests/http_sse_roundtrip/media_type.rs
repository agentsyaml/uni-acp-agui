use super::*;

use agui_acp_bridge_server::{build_router, build_router_inner};
use axum::body::Body;
use axum::http::{Request as HttpRequest, header};
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn post_with_headers(
    state: BridgeAppState,
    headers: &[(&'static str, &'static str)],
    body: Vec<u8>,
) -> (StatusCode, String, Option<String>) {
    let app = build_router(state);
    let mut builder = HttpRequest::post("/");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app.oneshot(builder.body(Body::from(body)).unwrap()),
    )
    .await
    .expect("router deadlocked")
    .expect("router error");
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        response.into_body().collect(),
    )
    .await
    .expect("body collect deadlocked")
    .expect("body collect failed")
    .to_bytes();
    (
        status,
        String::from_utf8_lossy(&bytes).into_owned(),
        content_type,
    )
}

#[tokio::test]
async fn protobuf_accept_is_rejected_with_406_before_any_sse() {
    let state = fresh_state();
    let input = user_input("thread-proto", "run-proto", "hello");
    let body = serde_json::to_vec(&input).unwrap();

    let (status, body_text, content_type) = post_with_headers(
        state.clone(),
        &[
            ("Content-Type", "application/json"),
            ("Accept", "application/vnd.ag-ui.event+proto"),
        ],
        body,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "body: {body_text}");
    assert!(
        !body_text.contains("RUN_STARTED"),
        "406 must precede any AG-UI events: {body_text}"
    );
    assert!(
        content_type
            .as_ref()
            .is_none_or(|ct| !ct.contains("text/event-stream")),
        "406 must not start an SSE stream, got content-type {content_type:?}"
    );
    assert_eq!(
        state.session_count(),
        0,
        "proto accept must not open a session"
    );

    // A protobuf Accept listed alongside other types is still an explicit
    // proto request from the encoder's perspective — reject it too.
    let (status, _, _) = post_with_headers(
        state,
        &[
            ("Content-Type", "application/json"),
            (
                "Accept",
                "application/vnd.ag-ui.event+proto, text/event-stream;q=0.5",
            ),
        ],
        serde_json::to_vec(&user_input("thread-proto2", "run-proto2", "hi")).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn wildcard_and_missing_accept_run_normally_as_sse() {
    // `*/*` (bare curl) must NOT produce protobuf bytes: the middleware pins
    // the outbound Accept to text/event-stream, so the run completes as SSE.
    let state = fresh_state();
    let (status, body, content_type) = post_with_headers(
        state.clone(),
        &[("Content-Type", "application/json"), ("Accept", "*/*")],
        serde_json::to_vec(&user_input("thread-star", "run-star", "hello")).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        content_type.as_deref(),
        Some("text/event-stream"),
        "`*/*` must be served as SSE, got {content_type:?}"
    );
    assert!(body.contains("\"type\":\"RUN_FINISHED\""), "body: {body}");

    // Missing Accept entirely behaves the same.
    let (status, body, content_type) = post_with_headers(
        state,
        &[("Content-Type", "application/json")],
        serde_json::to_vec(&user_input("thread-noaccept", "run-noaccept", "hello")).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(content_type.as_deref(), Some("text/event-stream"));
    assert!(body.contains("\"type\":\"RUN_FINISHED\""), "body: {body}");
}

#[tokio::test]
async fn non_json_content_type_is_rejected_with_400() {
    let state = fresh_state();
    let input = user_input("thread-ct", "run-ct", "hello");
    let body = serde_json::to_vec(&input).unwrap();

    for content_type in ["text/plain", "application/x-www-form-urlencoded"] {
        let (status, body_text, _) = post_with_headers(
            state.clone(),
            &[("Content-Type", content_type)],
            body.clone(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{content_type} must be rejected, body: {body_text}"
        );
        assert!(
            !body_text.contains("RUN_STARTED"),
            "400 must precede any AG-UI events: {body_text}"
        );
    }
    assert_eq!(
        state.session_count(),
        0,
        "bad content-type must not open a session"
    );
}

#[tokio::test]
async fn build_router_inner_without_limit_still_validates_input() {
    // The guarantee must not silently vanish on the no-limit path: an invalid
    // RunAgentInput is rejected with 400 even when the size check is off.
    let state = fresh_state();
    let app = build_router_inner(state);
    let mut invalid = user_input("thread-nolimit", "run-nolimit", "dup");
    invalid.messages.push(Message::User(UserMessage {
        id: "msg-1".into(), // duplicate of the id user_input() generates
        content: UserMessageContent::Text("second".into()),
        name: None,
        encrypted_value: None,
    }));
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app.oneshot(
            HttpRequest::post("/")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&invalid).unwrap()))
                .unwrap(),
        ),
    )
    .await
    .expect("router deadlocked")
    .expect("router error");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
