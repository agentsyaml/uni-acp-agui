//! Deep mock-agent integration tests for the AG-UI ↔ ACP bridge.
//!
//! Every test starts with `validates_*` since the previous "proves_bug_*"
//! suite has been folded back into positive assertions after the underlying
//! issues were fixed.

#[path = "bridge_mock_agent/approvals.rs"]
mod approvals;
#[path = "bridge_mock_agent/basics.rs"]
mod basics;
#[path = "bridge_mock_agent/cancellation.rs"]
mod cancellation;
#[path = "bridge_mock_agent/concurrency.rs"]
mod concurrency;
#[path = "bridge_mock_agent/config_modes.rs"]
mod config_modes;
#[path = "bridge_mock_agent/config_timeouts.rs"]
mod config_timeouts;
#[path = "bridge_mock_agent/config_validation.rs"]
mod config_validation;
#[path = "bridge_mock_agent/deferred_approval.rs"]
mod deferred_approval;
#[path = "bridge_mock_agent/permissions.rs"]
mod permissions;
mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agui_acp_bridge_policy::{AutoAllow, InterruptViaAgUiEvent};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, BridgeConfig, SessionConfig, acp::CustomAgentInProcessClient,
    test_agents,
};
use axum::http::StatusCode;

use support::{
    AllowAlwaysFirstOption, PolicySpy, collect_sse_body, count_events, extract_event_types,
    state_with_client, state_with_policy, user_input,
};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

fn session_config() -> SessionConfig {
    SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    }
}

// --- helpers used only by validates_defer_policy_emits_state_snapshot_and_resolves_via_approval ---

async fn raw_post(
    addr: std::net::SocketAddr,
    path: &str,
    body: Vec<u8>,
    extra_headers: &str,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n{extra_headers}Content-Length: {len}\r\nConnection: close\r\n\r\n",
        len = body.len(),
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

async fn read_status_code(stream: &mut tokio::net::TcpStream) -> StatusCode {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 256];
    let n = stream.read(&mut buf).await.unwrap_or(0);
    let head = String::from_utf8_lossy(&buf[..n]);
    let line = head.lines().next().unwrap_or("");
    let code: u16 = line
        .split_whitespace()
        .nth(1)
        .unwrap_or("500")
        .parse()
        .unwrap_or(500);
    StatusCode::from_u16(code).unwrap()
}

async fn wait_for_state_snapshot(
    stream: &mut tokio::net::TcpStream,
) -> Option<(serde_json::Value, String)> {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 8192];
    let mut accumulated = String::new();
    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => n,
        };
        accumulated.push_str(&String::from_utf8_lossy(&buf[..n]));
        for line in accumulated.lines() {
            if let Some(payload) = line.strip_prefix("data:") {
                let payload = payload.trim();
                if payload.contains("\"type\":\"STATE_SNAPSHOT\"")
                    && let Ok(v) = serde_json::from_str::<serde_json::Value>(payload)
                {
                    return Some((v, accumulated));
                }
            }
        }
    }
}

async fn drain_to_end(mut stream: tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut acc = String::new();
    let mut buf = vec![0u8; 8192];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => acc.push_str(&String::from_utf8_lossy(&buf[..n])),
        }
    }
    acc
}

// --- Mode / Model / config-option surface ---

// --- helpers used by the mode/model tests ---

#[cfg(feature = "unstable_session_model")]
async fn raw_get(addr: std::net::SocketAddr, path: &str) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

#[cfg(feature = "unstable_session_model")]
async fn read_status_and_body(stream: &mut tokio::net::TcpStream) -> (StatusCode, String) {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut buf),
    )
    .await
    .unwrap_or(Ok(0));
    let raw = String::from_utf8_lossy(&buf);
    let line = raw.lines().next().unwrap_or("");
    let code: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // body starts after the empty line separating headers and body
    let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (
        StatusCode::from_u16(code).unwrap_or(StatusCode::IM_A_TEAPOT),
        body,
    )
}

async fn drain_to_end_local(stream: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    // 5s budget covers the agent's single chunk + RUN_FINISHED.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_end(&mut buf),
    )
    .await;
    String::from_utf8_lossy(&buf).into_owned()
}
