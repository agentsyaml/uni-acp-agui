# agui-acp-bridge

A Rust bridge for a supported subset of [ACP v1](https://agentclientprotocol.com/), exposing an [AG-UI](https://docs.copilotkit.ai/ag-ui) HTTP/SSE and frontend-tool gateway. It targets the ACP SDK 2.0.0 stable v1 schema; it is not a complete implementation of every ACP method or capability.

- **ACP v1** — the JSON-RPC session/update subset used by the bridge and its adapters
- **AG-UI** — the HTTP/SSE event protocol used by CopilotKit and similar frontends

The bridge translates supported ACP turns and updates into AG-UI events, with optional permission and frontend-tool plumbing. Agents may expose capabilities outside this subset without the bridge implementing them.

中文版：[README.zh.md](./README.zh.md)

## Quick start

Spin up an in-process echo gateway (no external agent binary required):

```bash
cargo run -p agui-acp-bridge-cli -- --in-process
```

In another terminal, send a request:

```bash
curl -N -X POST http://127.0.0.1:8080/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"threadId":"t1","runId":"r1","messages":[{"role":"user","id":"m1","content":"hello"}],"tools":[],"context":[],"forwardedProps":{},"state":{}}'
```

All seven fields are required, camelCase. `Accept: text/event-stream` is mandatory (protobuf encoding is not yet implemented). The response is an SSE stream of `RUN_STARTED → TEXT_MESSAGE_START → TEXT_MESSAGE_CONTENT* → TEXT_MESSAGE_END → RUN_FINISHED`.

To wrap a real agent binary, swap `--in-process` for the executable path:

```bash
cargo run -p agui-acp-bridge-cli -- ./target/debug/examples/my_agent
```

## How it works

```
AG-UI client ──POST /──► BridgeHandler ──prompt()──► AcpSessionHandle
     ▲                       │                              │
     │                  Translator                  ACP subprocess / in-process
     └─────── SSE ───────────┘
```

The bridge implements an explicit mapping where `thread_id` is the AG-UI
conversation key and maps 1:1 to an
`AcpSessionHandle`; the handle's real ACP `SessionId` is a separate identity.
Turns sharing the same thread reuse the same session. The agent streams
`SessionUpdate` notifications over stdio, `Translator` converts each into
AG-UI events, and they are written to the SSE response in order. The stream
terminates with `RUN_FINISHED` or `RUN_ERROR`.

## CLI

```text
agui-acp-bridge [OPTIONS] [-- AGENT_COMMAND...]

  --in-process                       Use the embedded echo agent (mutually exclusive with AGENT_COMMAND)
  --host <IP>            127.0.0.1   Bind address
  --port, -p <PORT>      8080        Bind port
  --cwd, -w <DIR>        .           Working directory passed to each ACP session
  --policy <KIND>        auto-deny   auto-allow | auto-deny | allowlist | interrupt
  --allow <TITLE>                    Tool titles approved by allowlist (repeatable / comma-separated)
  --permission-timeout <SEC>   300   Fallback timeout for deferred permission requests
  --idle-timeout <SEC>          120  Reap threshold for idle sessions (in-flight prompts are never reaped)
  --open-session-timeout <SEC> 30    ACP handshake timeout
  --event-buffer <N>           64    Per-prompt event channel capacity
```

Run `agui-acp-bridge --help` for the full list. Logs go to stderr; tune verbosity via `RUST_LOG`. Ctrl-C / SIGTERM trigger graceful shutdown.

### HTTP endpoints

| Path        | Method | Purpose                                                                    |
| ----------- | ------ | -------------------------------------------------------------------------- |
| `/`         | POST   | AG-UI `RunAgentInput` → AG-UI event SSE stream (16 MiB body limit by default) |
| `/health`   | GET    | `200 {"status":"ok"}`                                                       |
| `/sessions` | GET    | List the agent's persisted conversations via ACP `session/list` (`501` if unsupported) |
| `/approval` | POST   | Resolve a permission request deferred by `--policy interrupt`              |
| `/session/cancel` | POST | Cancel the current turn for a cached session (`404` if absent) |
| `/session/close` | POST | Gracefully close a cached ACP session when the agent advertises `sessionCapabilities.close` |
| `/session/delete` | POST | Remove a persisted ACP session from `session/list` when the agent advertises `sessionCapabilities.delete` |

`POST /session/close` accepts `{ "threadId": "..." }` and uses the real ACP
`SessionId` returned during initialization. It returns `204` after a successful
close, `404` when no cached session exists, `409` while the session is active,
queued, has an active setting, or has pending frontend/permission work, `501` when close is not
advertised, `502` for an ACP close error, and `504` for a bounded close timeout.
Successful, failed, and timed-out closes remove the local session and pending
frontend state; an unsupported close leaves the session reusable. Idle reaping
and LRU capacity eviction use the same graceful-close path before dropping an
idle entry. The session setting routes return `409` while a lifecycle close or
eviction owns the thread; ordinary settings may still queue behind a prompt.

`POST /session/delete` accepts `{ "threadId": "..." }` and returns `204` after
the agent accepts the ACP `session/delete` request. It returns `409` for active
or pending local work, `501` when delete is not advertised, `502` for an ACP
error, and `504` for a bounded timeout. Delete removes the session from the
agent's `session/list`; ACP does not promise a hard-delete of every underlying
artifact. It resolves only the exact bridge-owned `threadId` mapping: a cache
miss, including an ACP-looking thread ID, returns `404` without a wire
request. The exact mapping and local frontend state are removed after a
terminal delete outcome, while unsupported delete leaves the cached session
reusable.

ACP v1 `MessageId` values from content updates are forwarded to the matching
AG-UI text/reasoning message lifecycle. They remain distinct from AG-UI
`runId`, bridge turn IDs, and MCP `toolCallId`; synthetic bridge events use
bridge-managed fallback IDs.

### Conversation history (`/sessions` + resume)

When the ACP agent advertises the `session/list` and `loadSession` capabilities,
the bridge surfaces them statelessly — it stores no history of its own:

- `GET /sessions` → `{"sessions":[{"sessionId","cwd","title?","updatedAt?"}]}`.
  `sessionId` is the ACP identity; it never doubles as an AG-UI `threadId`.
- To **resume** a conversation, choose an AG-UI `threadId` and POST a run
  whose `forwardedProps` contains
  `{"acpResume":{"sessionId":"<ACP sessionId>"}}`. This typed marker is
  the only private resume form; boolean or malformed markers return
  `ACP_RESUME_SESSION_ID_REQUIRED` and open no actor. On a cache miss the
  bridge issues `session/load` with only the supplied ACP `sessionId`, and the
  agent's replayed history streams back before the new turn. A cached-thread
  mapping must match the supplied ACP ID; otherwise the run returns
  `ACP_RESUME_FAILED`. Missing `loadSession` or a failed `session/load` returns
  the existing non-success AG-UI resume error and never falls back to
  `session/new`. There is no ACP-ID alias or fallback cleanup path.


`/approval` request body and status codes:

```json
{ "interruptId": "<uuid from STATE_SNAPSHOT>", "approved": true, "optionId": "allow_once" }
```

| Status                     | Trigger                                                                        |
| -------------------------- | ------------------------------------------------------------------------------ |
| `200 OK`                   | Decision delivered to the session actor                                        |
| `400 Bad Request`          | `approved=true` but `optionId` is missing                                      |
| `404 Not Found`            | `interruptId` is unknown (already answered, timed out, or never existed)       |
| `422 Unprocessable Entity` | `optionId` is not one the agent advertised (the pending request is preserved for retry) |

## Permission policies

ACP agents emit `requestPermission` before executing tool calls. The bridge delegates each decision to a pluggable `PermissionPolicy`:

| Policy                  | Behaviour                                                                  |
| ----------------------- | -------------------------------------------------------------------------- |
| `AutoDeny` (default)    | Reject every `requestPermission`                                           |
| `AutoAllow`             | Approve every `requestPermission`                                          |
| `Allowlist`             | Approve only tool calls whose `title` is in the configured set             |
| `InterruptViaAgUiEvent` | Defer to the frontend via a `STATE_SNAPSHOT` event; the frontend resolves it through `POST /approval` |

Custom policy:

```rust
use agent_client_protocol::schema::v1::RequestPermissionRequest;
use agui_acp_bridge_core::{PermissionDecision, PermissionPolicy};
use async_trait::async_trait;

#[derive(Debug)]
struct MyPolicy;

#[async_trait]
impl PermissionPolicy for MyPolicy {
    async fn decide(&self, req: &RequestPermissionRequest) -> PermissionDecision {
        // inspect req.tool_call, req.options, etc.
        PermissionDecision::Deny
    }
}
```

## Use as a library

Minimal integration (bring your own listener):

```rust
use std::{path::PathBuf, sync::Arc};
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, ProcessAcpClient, build_router,
};

let client: Arc<dyn AcpClient> = Arc::new(ProcessAcpClient::new("./my_agent"));
let state = BridgeAppState::new(client, PathBuf::from("."));
let router = build_router(state);
// axum::serve(listener, router).await?;
```

`ProcessAcpClient` passes `command`, each `with_args(...)` value, and
`with_env(...)` overrides to ACP 2.0's structured subprocess configuration;
argv values containing spaces are preserved and environment overrides are
forwarded to the child. `SessionConfig.cwd` remains the ACP session working
directory, not a process-launch configuration value.

For dev mode, swap `ProcessAcpClient::new(...)` for `InProcessAcpClient::new()`. To customize policy or timeouts, use the builder:

```rust
BridgeAppState::builder(client, PathBuf::from("."))
    .with_policy(Arc::new(Allowlist::new(["Read file", "List directory"])))
    .with_config(BridgeConfig {
        idle_timeout: std::time::Duration::from_secs(600),
        ..BridgeConfig::default()
    })
    .build();
```

If 16 MiB is the wrong default body limit for you, call `build_router_inner` and add your own `DefaultBodyLimit` layer.

Workspace layout:

| Crate                    | Responsibility                                                                                  |
| ------------------------ | ----------------------------------------------------------------------------------------------- |
| `agui-acp-bridge-core`   | `AcpClient` trait, `ProcessAcpClient` / `InProcessAcpClient`, `Translator`, `BridgeConfig`      |
| `agui-acp-bridge-policy` | `PermissionPolicy` implementations: `AutoAllow` / `AutoDeny` / `Allowlist` / `InterruptViaAgUiEvent` |
| `agui-acp-bridge-server` | `BridgeHandler`, `BridgeAppState`, `build_router`, AG-UI SSE/session routes, auth, and frontend-tool MCP |
| `agui-acp-bridge-cli`    | The `agui-acp-bridge` binary                                                                    |

End-to-end demos in `crates/agui-acp-bridge-server/examples/`:

```bash
cargo run -p agui-acp-bridge-server --example 02_in_process_dev      # no agent needed
cargo run -p agui-acp-bridge-server --example 01_subprocess_bridge -- ./my_agent
cargo run -p agui-acp-bridge-server --example 03_custom_policy     -- ./my_agent
```

## Testing & development

Rust checks used locally and in CI:

```bash
cargo test --workspace --all-targets --locked
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --all-targets --no-default-features --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Frontend checks:

```bash
cd examples/copilotkit-acp-demo
bun install --frozen-lockfile
bun run type-check
bun run lint
bun run build
```

GitHub Actions runs the Rust test matrix on Rust `1.88.0` and `stable` for
default, all-features, and no-default-features builds, plus stable fmt/Clippy,
the Bun frontend checks/build, and dependency audits. High frontend advisories
are reported non-blocking; critical advisories fail the security job. Optional
subprocess smoke tests use a sibling `acp-rust` checkout and skip when it is
absent; set `ACP_RUST_PATH` to use another checkout.

## Dependency note

The bridge depends on the published [`agui-rs`](https://crates.io/crates/agui-rs) crates from crates.io: `agui-rs-core`, `agui-rs-server`, and `agui-rs-encoder` (all `0.1`).

## License

MIT OR Apache-2.0
