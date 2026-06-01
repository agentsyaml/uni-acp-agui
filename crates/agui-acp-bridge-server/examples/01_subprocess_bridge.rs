//! Example 1 — Subprocess Bridge
//!
//! Wraps a real ACP agent binary as an AG-UI HTTP/SSE endpoint. Each unique
//! `threadId` in incoming requests maps to one long-lived ACP session. The
//! session is created lazily on first use and reused across subsequent turns.
//!
//! # Usage
//!
//! ```text
//! cargo run -p agui-acp-bridge-server --example 01_subprocess_bridge \
//!     -- ./path/to/my_agent [arg1 arg2 ...]
//! ```
//!
//! Override the bind port via the `PORT` env var (default: `8080`).
//!
//! Then from another terminal:
//!
//! ```text
//! curl -N -X POST http://localhost:8080/ \
//!   -H 'Content-Type: application/json' \
//!   -H 'Accept: text/event-stream' \
//!   -d '{
//!     "threadId":       "thread-1",
//!     "runId":          "run-1",
//!     "messages":       [{"role":"user","id":"m1","content":"hello"}],
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
//! The response is an SSE stream:
//!
//! ```text
//! data: {"type":"RUN_STARTED",   ...}
//! data: {"type":"TEXT_MESSAGE_START", ...}
//! data: {"type":"TEXT_MESSAGE_CONTENT","delta":"Hello!"}
//! data: {"type":"TEXT_MESSAGE_END",   ...}
//! data: {"type":"RUN_FINISHED",  ...}
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use agui_acp_bridge_server::{AcpClient, BridgeAppState, ProcessAcpClient, build_router};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut argv = std::env::args().skip(1);
    let Some(command) = argv.next() else {
        eprintln!("usage: 01_subprocess_bridge <agent-binary> [extra-args...]");
        eprintln!();
        eprintln!("  Wraps an ACP agent binary as an AG-UI HTTP/SSE endpoint.");
        eprintln!("  Override bind port via the PORT env var (default: 8080).");
        return ExitCode::from(2);
    };
    let extra: Vec<String> = argv.collect();

    let client: Arc<dyn AcpClient> = if extra.is_empty() {
        Arc::new(ProcessAcpClient::new(command))
    } else {
        Arc::new(ProcessAcpClient::new(command).with_args(extra))
    };

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

    tracing::info!("AG-UI bridge listening on http://{addr}");
    tracing::info!("POST /  with RunAgentInput JSON  →  SSE stream of AG-UI events");

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
