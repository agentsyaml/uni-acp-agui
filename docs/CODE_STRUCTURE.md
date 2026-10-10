# Code structure

Root modules remain the facades. The focused files below organize their
implementation; public API paths and wire behavior are unchanged.

## Core

- `src/acp.rs` owns ACP handle/turn APIs; `src/acp/handle.rs` and
  `src/acp/turns.rs` hold the focused implementations.
- `src/session.rs` coordinates the session actor. Admission, mailbox,
  connection, initialization, prompt, notifications, history, permissions,
  settings, filesystem, terminal requests, and worker/lifecycle responsibilities
  live under `src/session/`.
- `src/file_ops.rs` exposes sandboxed file operations, with I/O and Linux-secure
  writes under `src/file_ops/`.
- `src/terminal.rs` exposes the session-local terminal registry; process groups,
  cwd validation, requests, supervision, runtime, and cleanup live under
  `src/terminal/`.
- `src/translation.rs` remains the `Translator` facade; text, tools, plans, and
  event helpers live under `src/translation/`.

## Server

- `src/handler.rs` remains the bridge facade and owns shared state. Its
  `src/handler/` modules separate state construction, admission/cache/capacity,
  opening and lifecycle, settings, frontend credentials, AG-UI input/run,
  security, routing, and stream delivery.
- `src/mcp_endpoint.rs` is the MCP endpoint facade; origin checks, protocol
  dispatch, tools, and JSON-RPC wire handling live under
  `src/mcp_endpoint/`.
- `src/test_agents.rs` is the test-fixture facade and explicitly re-exports
  fixtures from its focused modules. Handler unit tests remain under the same
  module test target in `src/handler/tests/`; existing Cargo test targets and
  commands are unchanged.

See [Protocol conformance](PROTOCOL_CONFORMANCE.md) for behavior and evidence.
