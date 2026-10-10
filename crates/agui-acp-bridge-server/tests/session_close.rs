//! Lifecycle coverage for the explicit ACP `session/close` endpoint.

#[path = "session_close/lifecycle.rs"]
mod lifecycle;
mod support;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, BridgeConfig, acp::CustomAgentInProcessClient, test_agents,
};
use axum::body::Body;
use axum::http::{Request, StatusCode, header::CONTENT_TYPE};
use tower::ServiceExt;

use support::{collect_sse_body, state_with_client, user_input};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

fn close_state(
    closed_ids: test_agents::SharedCloseSessionIds,
    behavior: test_agents::CloseBehavior,
    timeout: Duration,
) -> BridgeAppState {
    let client = client_for(move |stream| {
        test_agents::run_close_agent(stream, closed_ids.clone(), behavior)
    });
    BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: timeout,
            ..BridgeConfig::default()
        })
        .build()
}

fn close_setting_state(
    closed_ids: test_agents::SharedCloseSessionIds,
    control: test_agents::LifecycleControl,
) -> BridgeAppState {
    let client = client_for(move |stream| {
        test_agents::run_close_setting_agent(stream, closed_ids.clone(), control.clone())
    });
    BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: Duration::from_secs(1),
            ..BridgeConfig::default()
        })
        .build()
}

async fn close_request(state: BridgeAppState, thread_id: &str) -> StatusCode {
    json_post(
        state,
        "/session/close",
        serde_json::json!({ "threadId": thread_id }),
    )
    .await
}

async fn json_post(state: BridgeAppState, path: &str, body: serde_json::Value) -> StatusCode {
    let body = serde_json::to_vec(&body).expect("request body serializes");
    let response = agui_acp_bridge_server::build_router(state)
        .oneshot(
            Request::post(path)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .expect("request builds"),
        )
        .await
        .expect("route responds");
    response.status()
}

async fn config_option_request(state: BridgeAppState, thread_id: &str) -> StatusCode {
    json_post(
        state,
        "/session/set-config-option",
        serde_json::json!({
            "threadId": thread_id,
            "configId": "mode",
            "value": "code",
        }),
    )
    .await
}

async fn mode_request(state: BridgeAppState, thread_id: &str) -> StatusCode {
    json_post(
        state,
        "/session/set-mode",
        serde_json::json!({ "threadId": thread_id, "modeId": "code" }),
    )
    .await
}

async fn model_request(state: BridgeAppState, thread_id: &str) -> StatusCode {
    json_post(
        state,
        "/session/set-model",
        serde_json::json!({ "threadId": thread_id, "modelId": "model-a" }),
    )
    .await
}

#[tokio::test]
async fn validates_close_uses_real_session_id_and_removes_cached_state() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let state = close_state(
        closed_ids.clone(),
        test_agents::CloseBehavior::Success,
        Duration::from_secs(1),
    );

    let (status, body) =
        collect_sse_body(state.clone(), user_input("thread-close", "run-1", "hello")).await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(state.session_count(), 1);

    assert_eq!(
        close_request(state.clone(), "thread-close").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 0, "close must evict the cache entry");
    assert_eq!(
        closed_ids.lock().expect("close ids poisoned").as_slice(),
        ["real-close-session-id"]
    );
}

#[tokio::test]
async fn validates_unsupported_close_preserves_a_reusable_session() {
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-no-close", "run-1", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");

    assert_eq!(
        close_request(state.clone(), "thread-no-close").await,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(state.session_count(), 1);

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-no-close", "run-2", "again"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "unsupported close must preserve actor: {body}"
    );
}

#[tokio::test]
async fn validates_close_error_and_timeout_remove_uncertain_sessions() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let state = close_state(
        closed_ids.clone(),
        test_agents::CloseBehavior::Error,
        Duration::from_secs(1),
    );
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-close-error", "run-1", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(
        close_request(state.clone(), "thread-close-error").await,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(state.session_count(), 0);

    let timeout_ids = Arc::new(Mutex::new(Vec::new()));
    let state = close_state(
        timeout_ids,
        test_agents::CloseBehavior::Timeout,
        Duration::from_millis(20),
    );
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-close-timeout", "run-1", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(
        close_request(state.clone(), "thread-close-timeout").await,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(state.session_count(), 0);
}

#[tokio::test]
async fn validates_close_rejects_active_and_pending_work() {
    let state = state_with_client(client_for(|stream| {
        test_agents::run_slow_prompt_agent(stream, 250)
    }));
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-close-busy", "run-0", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");
    let run_state = state.clone();
    let prompt = tokio::spawn(async move {
        collect_sse_body(run_state, user_input("thread-close-busy", "run-1", "hello")).await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        close_request(state.clone(), "thread-close-busy").await,
        StatusCode::CONFLICT
    );
    let _ = prompt.await.expect("prompt task joins");

    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let state = close_state(
        closed_ids,
        test_agents::CloseBehavior::Success,
        Duration::from_secs(1),
    );
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-close-pending", "run-1", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");

    let entry = state.frontend_tools().entry("thread-close-pending");
    let receiver = entry.register_pending("call-pending".into());
    assert_eq!(
        state.frontend_tools().pending_len("thread-close-pending"),
        1
    );
    assert_eq!(
        close_request(state.clone(), "thread-close-pending").await,
        StatusCode::CONFLICT
    );
    state.frontend_tools().drop_thread("thread-close-pending");
    let response = receiver.await.expect("pending call is drained");
    assert!(response.is_error);
}

#[tokio::test]
async fn validates_close_missing_session_is_not_found() {
    let state = close_state(
        Arc::new(Mutex::new(Vec::new())),
        test_agents::CloseBehavior::Success,
        Duration::from_secs(1),
    );
    assert_eq!(
        close_request(state, "never-opened").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn validates_setting_before_close_returns_busy_without_session_closed() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let state = close_setting_state(closed_ids, control.clone());
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-setting-first", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let setting_state = state.clone();
    let setting =
        tokio::spawn(
            async move { config_option_request(setting_state, "thread-setting-first").await },
        );
    tokio::time::timeout(Duration::from_secs(2), control.wait_setting_started())
        .await
        .expect("setting must reach the agent before close");

    assert_eq!(
        close_request(state.clone(), "thread-setting-first").await,
        StatusCode::CONFLICT
    );
    control.release_setting();
    assert_eq!(setting.await.expect("setting task joins"), StatusCode::OK);

    let (status, body) = collect_sse_body(
        state,
        user_input("thread-setting-first", "run-after-setting", "still usable"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "setting must not be SessionClosed: {body}"
    );
}

#[tokio::test]
async fn validates_close_before_setting_returns_busy_and_releases_for_new_run() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let state = close_setting_state(closed_ids, control.clone());
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-close-first", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let close_state = state.clone();
    let close = tokio::spawn(async move { close_request(close_state, "thread-close-first").await });
    tokio::time::timeout(Duration::from_secs(2), control.wait_close_started())
        .await
        .expect("close must reach the agent before setting");

    assert_eq!(
        config_option_request(state.clone(), "thread-close-first").await,
        StatusCode::CONFLICT
    );
    control.release_close();
    assert_eq!(
        close.await.expect("close task joins"),
        StatusCode::NO_CONTENT
    );

    let (status, body) = collect_sse_body(
        state,
        user_input("thread-close-first", "run-after-close", "fresh"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "claim release must admit a new run: {body}"
    );
}

#[tokio::test]
async fn validates_two_concurrent_config_settings_release_busy_state() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let state = close_setting_state(closed_ids, control.clone());
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-two-settings", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let first_state = state.clone();
    let second_state = state.clone();
    let first =
        tokio::spawn(
            async move { config_option_request(first_state, "thread-two-settings").await },
        );
    let second =
        tokio::spawn(
            async move { config_option_request(second_state, "thread-two-settings").await },
        );
    tokio::time::timeout(Duration::from_secs(2), control.wait_settings_started(1))
        .await
        .expect("first setting must reach the agent");

    tokio::time::sleep(Duration::from_millis(10)).await;
    control.release_setting();
    tokio::time::timeout(Duration::from_secs(2), control.wait_settings_started(2))
        .await
        .expect("second setting must reach the agent");
    control.release_setting();
    assert_eq!(first.await.expect("first setting joins"), StatusCode::OK);
    assert_eq!(second.await.expect("second setting joins"), StatusCode::OK);
    let close_state = state.clone();
    let close =
        tokio::spawn(async move { close_request(close_state, "thread-two-settings").await });
    tokio::time::timeout(Duration::from_secs(2), control.wait_close_started())
        .await
        .expect("close must reach the agent after settings finish");
    control.release_close();
    assert_eq!(
        close.await.expect("close task joins"),
        StatusCode::NO_CONTENT,
        "concurrent setting guards must not leave a busy marker"
    );
}

#[tokio::test]
async fn validates_two_concurrent_discovered_settings_release_busy_state() {
    let closed_ids = Arc::new(Mutex::new(Vec::new()));
    let control = test_agents::LifecycleControl::new();
    let state = close_setting_state(closed_ids, control.clone());
    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("thread-two-discovered-settings", "run-1", "warm"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let first_state = state.clone();
    let second_state = state.clone();
    let first =
        tokio::spawn(
            async move { mode_request(first_state, "thread-two-discovered-settings").await },
        );
    let second =
        tokio::spawn(
            async move { model_request(second_state, "thread-two-discovered-settings").await },
        );
    tokio::time::timeout(Duration::from_secs(2), control.wait_settings_started(1))
        .await
        .expect("first discovered setting must reach the agent");

    tokio::time::sleep(Duration::from_millis(10)).await;
    control.release_setting();
    tokio::time::timeout(Duration::from_secs(2), control.wait_settings_started(2))
        .await
        .expect("second discovered setting must reach the agent");
    control.release_setting();
    assert_eq!(first.await.expect("mode setting joins"), StatusCode::OK);
    assert_eq!(second.await.expect("model setting joins"), StatusCode::OK);
    let close_state = state.clone();
    let close =
        tokio::spawn(
            async move { close_request(close_state, "thread-two-discovered-settings").await },
        );
    tokio::time::timeout(Duration::from_secs(2), control.wait_close_started())
        .await
        .expect("close must reach the agent after discovered settings finish");
    control.release_close();
    assert_eq!(
        close.await.expect("close task joins"),
        StatusCode::NO_CONTENT,
        "concurrent discovered settings must not leave a busy marker"
    );
}
