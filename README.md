# agui-acp-bridge

A Rust gateway that puts an [AG-UI](https://docs.copilotkit.ai/ag-ui) frontend in front of any [ACP](https://agentcommunicationprotocol.dev/) agent.

- **ACP** — the JSON-RPC protocol used by `acp-rust`, Claude Code, Kiro CLI, and other agent runtimes
- **AG-UI** — the HTTP/SSE event protocol used by CopilotKit and similar frontends

Drop this layer in front of any ACP-compliant agent and AG-UI frontends can talk to it directly. `cargo test --workspace` is currently 77 passing / 0 failing; CI runs `build / clippy / fmt / test`.

中文版：[README.zh.md](./README.zh.md)

## Quick start

Spin up an in-process echo gateway (no external agent binary required):

```bash
cargo run -p agui-acp-bridge-cli -- --in-process
```

In another terminal, send a request:

```bash
curl -N -X POST http://localhost:8080/ \
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

`thread_id` maps 1:1 to an `AcpSessionHandle`; turns sharing the same thread reuse the same session. The agent streams `SessionUpdate` notifications over stdio, `Translator` converts each into AG-UI events, and they are written to the SSE response in order. The stream terminates with `RUN_FINISHED` or `RUN_ERROR`.

## CLI

```text
agui-acp-bridge [OPTIONS] [-- AGENT_COMMAND...]

  --in-process                       Use the embedded echo agent (mutually exclusive with AGENT_COMMAND)
  --host <IP>            0.0.0.0     Bind address
  --port, -p <PORT>      8080        Bind port
  --cwd, -w <DIR>        .           Working directory passed to each ACP session
  --policy <KIND>        auto-allow  auto-allow | auto-deny | allowlist | interrupt
  --allow <TITLE>                    Tool titles approved by allowlist (repeatable / comma-separated)
  --permission-timeout <SEC>   300   Fallback timeout for deferred permission requests
  --idle-timeout <SEC>         1800  Reap threshold for idle sessions (in-flight prompts are never reaped)
  --open-session-timeout <SEC> 30    ACP handshake timeout
  --event-buffer <N>           64    Per-prompt event channel capacity
```

Run `agui-acp-bridge --help` for the full list. Logs go to stderr; tune verbosity via `RUST_LOG`. Ctrl-C / SIGTERM trigger graceful shutdown.

### HTTP endpoints

| Path        | Method | Purpose                                                                    |
| ----------- | ------ | -------------------------------------------------------------------------- |
| `/`         | POST   | AG-UI `RunAgentInput` → AG-UI event SSE stream (16 MiB body limit by default) |
| `/health`   | GET    | `200 {"status":"ok","sessions":N}`                                         |
| `/sessions` | GET    | List the agent's persisted conversations via ACP `session/list` (`501` if unsupported) |
| `/approval` | POST   | Resolve a permission request deferred by `--policy interrupt`              |

### Conversation history (`/sessions` + resume)

When the ACP agent advertises the `session/list` and `loadSession` capabilities,
the bridge surfaces them statelessly — it stores no history of its own:

- `GET /sessions` → `{"sessions":[{"sessionId","cwd","title?","updatedAt?"}]}`.
  Each `sessionId` doubles as the AG-UI `threadId` used to resume.
- To **resume** a conversation, POST a run whose `threadId` is that `sessionId`
  and whose `forwardedProps` contains `{"acpResume": true}`. On a cache-miss the
  bridge issues `session/load`, and the agent's replayed history streams back as
  AG-UI events before the new turn. If the agent lacks `loadSession`, the bridge
  transparently falls back to a fresh session.


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
| `AutoAllow` (default)   | Approve every `requestPermission`                                          |
| `AutoDeny`              | Reject every `requestPermission`                                           |
| `Allowlist`             | Approve only tool calls whose `title` is in the configured set             |
| `InterruptViaAgUiEvent` | Defer to the frontend via a `STATE_SNAPSHOT` event; the frontend resolves it through `POST /approval` |

Custom policy:

```rust
use agent_client_protocol::schema::RequestPermissionRequest;
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
| `agui-acp-bridge-server` | `BridgeHandler`, `BridgeAppState`, `build_router` (axum router + `/health` + `/approval`)       |
| `agui-acp-bridge-cli`    | The `agui-acp-bridge` binary                                                                    |

End-to-end demos in `crates/agui-acp-bridge-server/examples/`:

```bash
cargo run -p agui-acp-bridge-server --example 02_in_process_dev      # no agent needed
cargo run -p agui-acp-bridge-server --example 01_subprocess_bridge -- ./my_agent
cargo run -p agui-acp-bridge-server --example 03_custom_policy     -- ./my_agent
```

## Testing & development

```bash
just ci      # build + clippy + fmt + test (must all pass)
just test    # tests only
just fmt     # format
```

The subprocess integration tests expect a sibling `acp-rust` checkout; they skip silently when it's missing. Set `ACP_RUST_PATH` to point elsewhere. Design decisions and per-iteration refactor notes live in [`REFACTOR_PROGRESS.md`](./REFACTOR_PROGRESS.md).

## Dependency note

The bridge depends on the published [`agui-rs`](https://crates.io/crates/agui-rs) crates from crates.io: `agui-rs-core`, `agui-rs-server`, and `agui-rs-encoder` (all `0.1`).

## License

MIT OR Apache-2.0
