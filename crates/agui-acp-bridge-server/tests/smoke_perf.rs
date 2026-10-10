//! Smoke + performance / stability suite for the AG-UI ↔ ACP bridge.
//!
//! These tests complement the behavioural coverage in `bridge_mock_agent.rs`
//! and `http_sse_roundtrip.rs`. They focus on the stability properties that
//! matter most in production:
//!
//!  * **Startup latency** — `open_session` (the ACP handshake) must complete
//!    quickly for the in-process path, and `open_session_timeout` must bound
//!    a slow handshake instead of hanging.
//!  * **Session lifecycle** — distinct `thread_id`s create distinct sessions;
//!    reuse keeps the count flat; the idle reaper drains everything back to
//!    zero; a dead session is evicted and transparently rebuilt.
//!  * **Streaming integrity** — under a high-volume single turn the bridge
//!    preserves chunk ordering and drops none, and always frames the run with
//!    exactly one `RUN_STARTED` … `RUN_FINISHED` pair.
//!  * **Throughput / no-leak under load** — many sequential and concurrent
//!    runs complete cleanly and leave the session map bounded.
//!
//! All timing assertions use generous upper bounds so the suite stays robust
//! on slow CI runners; they are sanity ceilings, not micro-benchmarks.

#[path = "smoke_perf/capacity.rs"]
mod capacity;
#[path = "smoke_perf/realsocket.rs"]
mod realsocket;
#[path = "smoke_perf/startup_lifecycle.rs"]
mod startup_lifecycle;
#[path = "smoke_perf/streaming_load.rs"]
mod streaming_load;
mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agui_acp_bridge_core::{BridgeConfig, SessionConfig};
use agui_acp_bridge_policy::AutoAllow;
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, CustomAgentInProcessClient, InProcessAcpClient, build_router,
    test_agents,
};
use axum::http::StatusCode;

use support::{collect_sse_body, count_events, extract_event_types, state_with_client, user_input};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

struct DropFlag(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn test_session_config() -> SessionConfig {
    SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    }
}

// ---------------------------------------------------------------------------
// Startup latency
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Streaming integrity
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Throughput / no-leak under load
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Real-socket (TCP + SSE) performance & stability
//
// The helpers above use `tower::oneshot`, which buffers the whole body and
// never exercises socket-level streaming or backpressure. The tests below
// bind a real `127.0.0.1` listener and drive it with `reqwest` so we cover
// the production transport: time-to-first-event, incremental delivery, and
// graceful completion.
// ---------------------------------------------------------------------------

use tokio::net::TcpListener;

/// Spawn the bridge on an ephemeral `127.0.0.1` port. Returns the bound
/// address and the server task handle (abort it to shut down).
async fn spawn_bridge(
    state: BridgeAppState,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, server)
}

// ---------------------------------------------------------------------------
// Session-cap & registry-cleanup regressions
//
// These pin the two production bugs reported for the multi-session case:
//  1. The idle reaper must also drop the matching frontend-tools registry
//     entry, or those entries leak for the life of the process.
//  2. `max_sessions` must bound the cached-session count by evicting the LRU
//     idle session, so a browser that mints a fresh thread on every refresh
//     can't accumulate unbounded agent subprocesses.
// ---------------------------------------------------------------------------
