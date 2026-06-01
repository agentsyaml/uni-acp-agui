//! Example 2 — In-Process Dev Mode
//!
//! Runs the bridge with an embedded echo agent. No external ACP binary is
//! required. Every prompt is echoed back verbatim as a streamed text response,
//! making this ideal for testing AG-UI frontends locally without a real agent.
//!
//! # Usage
//!
//! ```text
//! cargo run -p agui-acp-bridge-server --example 02_in_process_dev
//! ```
//!
//! Override the bind port via the `PORT` env var (default: `8080`).
//!
//! Then send a request:
//!
//! ```text
//! curl -N -X POST http://localhost:8080/ \
//!   -H 'Content-Type: application/json' \
//!   -H 'Accept: text/event-stream' \
//!   -d '{
//!     "threadId":       "thread-1",
//!     "runId":          "run-1",
//!     "messages":       [{"role":"user","id":"m1","content":"ping"}],
//!     "tools":          [],
//!     "context":        [],
//!     "forwardedProps": {},
//!     "state":          {}
//!   }'
//! ```
//!
//! All seven fields above (`threadId`, `runId`, `messages`, `tools`, `context`,
//! `forwardedProps`, `state`) are required by the AG-UI `RunAgentInput` schema
//! and use **camelCase**. The `Accept: text/event-stream` header is required —
//! protobuf encoding is not yet implemented.
//!
//! Expected SSE output:
//!
//! ```text
//! data: {"type":"RUN_STARTED",         ...}
//! data: {"type":"TEXT_MESSAGE_START",  ...}
//! data: {"type":"TEXT_MESSAGE_CONTENT","delta":"Echo: "}
//! data: {"type":"TEXT_MESSAGE_CONTENT","delta":"ping "}
//! data: {"type":"TEXT_MESSAGE_END",    ...}
//! data: {"type":"RUN_FINISHED",        ...}
//! ```
//!
//! # Session reuse
//!
//! Sending multiple requests with the same `threadId` reuses the same in-process
//! echo session. Different `threadId` values get independent sessions.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use agui_acp_bridge_server::{AcpClient, BridgeAppState, InProcessAcpClient, build_router};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let client: Arc<dyn AcpClient> = Arc::new(InProcessAcpClient::new());
    let state = BridgeAppState::new(client, PathBuf::from("."));

    let port = port_from_env();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind {addr} failed: {e}");
            return ExitCode::from(1);
        }
    };

    tracing::info!("In-process echo bridge on http://{addr}  (no agent binary needed)");

    if let Err(e) = axum::serve(listener, build_router(state)).await {
        eprintln!("server error: {e}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn port_from_env() -> u16 {
    std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(8080)
}
