//! Example 3 — Custom Permission Policy
//!
//! Demonstrates `BridgeAppState::builder` with a non-default permission policy.
//! An ACP agent can request permission before executing a tool call; the
//! bridge delegates that decision to whatever `PermissionPolicy` is in effect.
//!
//! This example uses `Allowlist`, which approves only tool calls whose `title`
//! appears in a pre-configured set and denies all others.
//!
//! # Available policies (from `agui-acp-bridge-policy`)
//!
//! | Policy                | Behaviour                                             |
//! |-----------------------|-------------------------------------------------------|
//! | `AutoAllow` (default) | Approves every permission request automatically       |
//! | `AutoDeny`            | Rejects every permission request                      |
//! | `Allowlist`           | Approves only tool calls matching a title set         |
//! | `InterruptViaAgUiEvent` | Defers to the frontend via `acp.permission_request` custom event |
//!
//! # Usage
//!
//! ```text
//! cargo run -p agui-acp-bridge-server --example 03_custom_policy \
//!     -- ./path/to/my_agent
//! ```
//!
//! Override the bind port via the `PORT` env var (default: `8080`).
//!
//! Then send a request (the request shape is identical to examples 01 and 02):
//!
//! ```text
//! curl -N -X POST http://localhost:8080/ \
//!   -H 'Content-Type: application/json' \
//!   -H 'Accept: text/event-stream' \
//!   -d '{
//!     "threadId":       "thread-1",
//!     "runId":          "run-1",
//!     "messages":       [{"role":"user","id":"m1","content":"please read foo.txt"}],
//!     "tools":          [],
//!     "context":        [],
//!     "forwardedProps": {},
//!     "state":          {}
//!   }'
//! ```
//!
//! All seven fields above are required (camelCase). The `Accept:
//! text/event-stream` header is required — protobuf encoding is not yet
//! implemented.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use agui_acp_bridge_policy::Allowlist;
use agui_acp_bridge_server::{AcpClient, BridgeAppState, ProcessAcpClient, build_router};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let Some(command) = std::env::args().nth(1) else {
        eprintln!("usage: 03_custom_policy <agent-binary>");
        eprintln!();
        eprintln!("  Wraps an ACP agent binary as an AG-UI HTTP/SSE endpoint with");
        eprintln!("  an Allowlist permission policy (only \"Read file\" and");
        eprintln!("  \"List directory\" tool calls are approved).");
        eprintln!("  Override bind port via the PORT env var (default: 8080).");
        return ExitCode::from(2);
    };

    let client: Arc<dyn AcpClient> = Arc::new(ProcessAcpClient::new(command));

    let policy = Allowlist::new(["Read file", "List directory"]);

    let state = BridgeAppState::builder(client, PathBuf::from("."))
        .with_policy(Arc::new(policy))
        .build();

    let port = port_from_env();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind {addr} failed: {e}");
            return ExitCode::from(1);
        }
    };

    tracing::info!("Bridge with Allowlist policy on http://{addr}");
    tracing::info!("Approved tool titles: \"Read file\", \"List directory\"");

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
