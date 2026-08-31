//! Lifecycle coverage for the explicit ACP `session/delete` endpoint.

mod support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, BridgeConfig, CustomAgentInProcessClient, build_router, test_agents,
};
use axum::body::Body;
use axum::http::{Request, StatusCode, header::AUTHORIZATION, header::CONTENT_TYPE};
use tower::ServiceExt;

use support::{collect_sse_body, state_with_client, user_input};
use test_agents::{DeleteBehavior, DeleteLifecycleControl, SharedDeleteSessionIds};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

fn delete_state(
    deleted_ids: SharedDeleteSessionIds,
    behavior: DeleteBehavior,
    timeout: Duration,
) -> BridgeAppState {
    let client = client_for(move |stream| {
        test_agents::run_delete_agent(stream, deleted_ids.clone(), behavior)
    });
    BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: timeout,
            ..BridgeConfig::default()
        })
        .build()
}

fn lifecycle_state(
    deleted_ids: SharedDeleteSessionIds,
    control: DeleteLifecycleControl,
) -> BridgeAppState {
    let client = client_for(move |stream| {
        test_agents::run_delete_lifecycle_agent(stream, deleted_ids.clone(), control.clone())
    });
    BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: Duration::from_secs(1),
            ..BridgeConfig::default()
        })
        .build()
}

async fn json_post(state: BridgeAppState, path: &str, body: serde_json::Value) -> StatusCode {
    let body = serde_json::to_vec(&body).expect("request body serializes");
    let response = build_router(state)
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

async fn delete_request(state: BridgeAppState, thread_id: &str) -> StatusCode {
    json_post(
        state,
        "/session/delete",
        serde_json::json!({ "threadId": thread_id }),
    )
    .await
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

#[tokio::test]
async fn forwards_real_session_id_and_cleans_cached_state() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let state = delete_state(
        deleted_ids.clone(),
        DeleteBehavior::Success,
        Duration::from_secs(1),
    );
    let (status, body) =
        collect_sse_body(state.clone(), user_input("logical-thread", "run-1", "hi")).await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(state.session_count(), 1);

    assert_eq!(
        delete_request(state.clone(), "logical-thread").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 0);
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-delete-session-id"]
    );
}

#[tokio::test]
async fn cache_miss_returns_not_found_without_wire_delete() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let state = delete_state(
        deleted_ids.clone(),
        DeleteBehavior::Success,
        Duration::from_secs(1),
    );

    assert_eq!(
        delete_request(state.clone(), "real-delete-session-id").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        delete_request(state, "persisted-session").await,
        StatusCode::NOT_FOUND
    );
    assert!(deleted_ids.lock().expect("delete ids poisoned").is_empty());
}

#[tokio::test]
async fn deleting_one_thread_removes_only_its_exact_mapping() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let state = delete_state(
        deleted_ids.clone(),
        DeleteBehavior::Success,
        Duration::from_secs(1),
    );
    for (thread_id, run_id) in [("alias-a", "run-a"), ("alias-b", "run-b")] {
        let (status, body) =
            collect_sse_body(state.clone(), user_input(thread_id, run_id, "hi")).await;
        assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    }
    assert_eq!(state.session_count(), 2);
    assert_eq!(state.frontend_tools().thread_count(), 2);

    assert_eq!(
        delete_request(state.clone(), "alias-a").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 1);
    assert_eq!(state.frontend_tools().thread_count(), 1);
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-delete-session-id"]
    );
    assert_eq!(
        delete_request(state.clone(), "alias-b").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 0);
    assert_eq!(state.frontend_tools().thread_count(), 0);
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-delete-session-id", "real-delete-session-id"]
    );
}

#[tokio::test]
async fn exact_thread_key_wins_over_another_cached_acp_id() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let next_id = Arc::new(AtomicUsize::new(0));
    let client = client_for({
        let deleted_ids = deleted_ids.clone();
        let next_id = next_id.clone();
        move |stream| {
            let session_id = match next_id.fetch_add(1, Ordering::Relaxed) {
                0 => "real-a",
                _ => "real-b",
            };
            test_agents::run_delete_agent_with_session_id(
                stream,
                deleted_ids.clone(),
                DeleteBehavior::Success,
                session_id,
            )
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            set_session_timeout: Duration::from_secs(1),
            ..BridgeConfig::default()
        })
        .build();

    for (thread_id, run_id) in [("same-key", "run-a"), ("real-a", "run-b")] {
        let (status, body) =
            collect_sse_body(state.clone(), user_input(thread_id, run_id, "hi")).await;
        assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    }

    assert_eq!(
        delete_request(state.clone(), "real-a").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 1);
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-b"]
    );
    assert_eq!(
        delete_request(state, "same-key").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-b", "real-a"]
    );
}

#[tokio::test]
async fn unsupported_delete_does_not_evict_or_break_reuse() {
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (status, body) =
        collect_sse_body(state.clone(), user_input("no-delete", "run-1", "hi")).await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");

    assert_eq!(
        delete_request(state.clone(), "no-delete").await,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(state.session_count(), 1);
    let (status, body) = collect_sse_body(state, user_input("no-delete", "run-2", "again")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "unsupported delete broke reuse: {body}"
    );
}

#[tokio::test]
async fn error_and_timeout_remove_uncertain_cached_sessions() {
    let error_state = delete_state(
        Arc::new(Mutex::new(Vec::new())),
        DeleteBehavior::Error,
        Duration::from_secs(1),
    );
    let (status, body) = collect_sse_body(
        error_state.clone(),
        user_input("delete-error", "run-1", "hi"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(
        delete_request(error_state.clone(), "delete-error").await,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(error_state.session_count(), 0);

    let timeout_state = delete_state(
        Arc::new(Mutex::new(Vec::new())),
        DeleteBehavior::Timeout,
        Duration::from_millis(20),
    );
    let (status, body) = collect_sse_body(
        timeout_state.clone(),
        user_input("delete-timeout", "run-1", "hi"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prompt failed: {body}");
    assert_eq!(
        delete_request(timeout_state.clone(), "delete-timeout").await,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(timeout_state.session_count(), 0);
}

#[tokio::test]
async fn active_setting_and_frontend_work_return_busy_without_wire_delete() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let control = DeleteLifecycleControl::new();
    let state = lifecycle_state(deleted_ids.clone(), control.clone());

    let warm_state = state.clone();
    let warm = tokio::spawn(async move {
        collect_sse_body(warm_state, user_input("delete-busy", "run-warm", "warm")).await
    });
    tokio::time::timeout(Duration::from_secs(2), control.wait_prompt_started())
        .await
        .expect("prompt must start");
    control.release_prompt();
    assert_eq!(warm.await.expect("warm task joins").0, StatusCode::OK);

    let setting_state = state.clone();
    let setting =
        tokio::spawn(async move { config_option_request(setting_state, "delete-busy").await });
    tokio::time::timeout(Duration::from_secs(2), control.wait_setting_started())
        .await
        .expect("setting must start");
    assert_eq!(
        delete_request(state.clone(), "delete-busy").await,
        StatusCode::CONFLICT
    );
    assert!(deleted_ids.lock().expect("delete ids poisoned").is_empty());
    control.release_setting();
    assert_eq!(setting.await.expect("setting task joins"), StatusCode::OK);

    let entry = state.frontend_tools().entry("delete-busy");
    let pending = entry.register_pending("pending-delete".into());
    assert_eq!(
        delete_request(state.clone(), "delete-busy").await,
        StatusCode::CONFLICT
    );
    assert!(deleted_ids.lock().expect("delete ids poisoned").is_empty());
    state.frontend_tools().drop_thread("delete-busy");
    assert!(pending.await.expect("pending call is drained").is_error);
}

#[tokio::test]
async fn lifecycle_claim_blocks_replacement_until_delete_finishes() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let control = DeleteLifecycleControl::new();
    let state = lifecycle_state(deleted_ids.clone(), control.clone());
    let warm_state = state.clone();
    let warm = tokio::spawn(async move {
        collect_sse_body(warm_state, user_input("delete-claim", "run-warm", "warm")).await
    });
    tokio::time::timeout(Duration::from_secs(2), control.wait_prompt_started())
        .await
        .expect("prompt must start");
    control.release_prompt();
    assert_eq!(warm.await.expect("warm task joins").0, StatusCode::OK);

    let delete_state = state.clone();
    let delete = tokio::spawn(async move { delete_request(delete_state, "delete-claim").await });
    tokio::time::timeout(Duration::from_secs(2), control.wait_delete_started())
        .await
        .expect("delete must reach the agent");

    let (status, body) = collect_sse_body(
        state.clone(),
        user_input("delete-claim", "replacement", "must be rejected"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("CONCURRENT_RUN"),
        "replacement was admitted: {body}"
    );

    control.release_delete();
    assert_eq!(
        delete.await.expect("delete task joins"),
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 0);
}

#[tokio::test]
async fn delete_claim_is_exact_thread_only() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let control = DeleteLifecycleControl::new();
    let state = lifecycle_state(deleted_ids.clone(), control.clone());
    let warm_state = state.clone();
    let warm = tokio::spawn(async move {
        collect_sse_body(warm_state, user_input("logical", "run-warm", "warm")).await
    });
    tokio::time::timeout(Duration::from_secs(2), control.wait_prompt_started())
        .await
        .expect("prompt must start");
    control.release_prompt();
    let (status, body) = warm.await.expect("warm task joins");
    assert_eq!(status, StatusCode::OK, "warm-up prompt failed: {body}");

    let delete_state = state.clone();
    let delete = tokio::spawn(async move { delete_request(delete_state, "logical").await });
    tokio::time::timeout(Duration::from_secs(2), control.wait_delete_started())
        .await
        .expect("delete must reach the agent");

    let mut resume = user_input("real-delete-session-id", "run-resume", "must not alias");
    resume.messages.clear();
    resume.forwarded_props = serde_json::json!({
        "acpResume": {"sessionId": "real-delete-session-id"}
    });
    let (status, body) = collect_sse_body(state.clone(), resume).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.contains("ACP_RESUME_UNSUPPORTED"),
        "ACP-looking thread must not claim the logical thread: {body}"
    );
    assert_eq!(
        state.session_count(),
        1,
        "resume must not insert an ACP-id alias"
    );
    assert_eq!(
        deleted_ids.lock().expect("delete ids poisoned").as_slice(),
        ["real-delete-session-id"]
    );

    control.release_delete();
    assert_eq!(
        delete.await.expect("delete task joins"),
        StatusCode::NO_CONTENT
    );
    assert_eq!(state.session_count(), 0);
}

#[tokio::test]
async fn empty_input_is_bad_request_and_unauthorized_delete_sends_no_wire() {
    let deleted_ids = Arc::new(Mutex::new(Vec::new()));
    let state = delete_state(
        deleted_ids.clone(),
        DeleteBehavior::Success,
        Duration::from_secs(1),
    );
    assert_eq!(
        json_post(
            state.clone(),
            "/session/delete",
            serde_json::json!({ "threadId": "" }),
        )
        .await,
        StatusCode::BAD_REQUEST
    );

    let auth_state = BridgeAppState::builder(
        client_for({
            let deleted_ids = deleted_ids.clone();
            move |stream| {
                test_agents::run_delete_agent(stream, deleted_ids.clone(), DeleteBehavior::Success)
            }
        }),
        PathBuf::from("/"),
    )
    .with_bearer_token("delete-test-bearer-token")
    .expect("token is valid")
    .build();
    let body = serde_json::to_vec(&serde_json::json!({
        "threadId": "wire-must-not-run"
    }))
    .expect("request body serializes");
    let response = build_router(auth_state)
        .oneshot(
            Request::post("/session/delete")
                .header(CONTENT_TYPE, "application/json")
                .header(AUTHORIZATION, "Bearer wrong-token-1234")
                .body(Body::from(body))
                .expect("request builds"),
        )
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(deleted_ids.lock().expect("delete ids poisoned").is_empty());
}
