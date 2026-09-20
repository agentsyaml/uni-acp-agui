# Protocol conformance

Gate 0 remediation artifact for the requested full protocol work. This is a
wire contract and ownership record, not a claim that the current bridge is
complete. Status values below are exactly: `implemented`, `partial`,
`explicitly rejected`, `pending implementation`, and `N/A`.

## 1. Target manifest

### Protocols and versions

- **ACP:** stable wire Protocol v1. The Rust SDK dependency
  `agent-client-protocol 2.0.0` supplies the v1 schema; SDK version `2.0.0`
  is **not** wire Protocol v2. ACP v2 is a draft (2026-07-20) and out of
  scope; this bridge targets stable v1. Note that v2 removes client
  filesystem/terminal methods and merges load into resume, which will require
  redesign if/when targeted
  (<https://agentclientprotocol.com/protocol/v2/migration>).
- Official ACP v1 sources:
  - <https://agentclientprotocol.com/protocol/v1/overview>
  - <https://agentclientprotocol.com/protocol/v1/schema>
  - <https://agentclientprotocol.com/protocol/v1/session-setup>
  - <https://agentclientprotocol.com/protocol/v1/prompt-turn>
  - <https://github.com/agentclientprotocol/agent-client-protocol>
- **AG-UI:** current core JSON input and JSON events over SSE. The repository
  pins `@ag-ui/client 0.0.53` in
  `examples/copilotkit-acp-demo/package.json`; the Rust `agui-rs` core/server
  dependencies are `0.1` in the workspace `Cargo.toml`. The bridge does not
  implement AG-UI 1.0 (npm `@ag-ui/client` 1.0.0, shipped 2026-09-17); 1.0
  adds native interrupt outcomes (`RUN_FINISHED.outcome` with
  `type: "interrupt"` plus `RunAgentInput.resume[]`) and native usage on
  `RUN_FINISHED`/`RUN_ERROR`. The target now includes those 1.0 native
  surfaces; the current implementation remains private-extension-based, and
  upgrading to AG-UI 1.0 is a pending protocol-position decision.
- AG-UI references for this target:
  <https://docs.ag-ui.com/sdk/js/core/events>,
  <https://docs.ag-ui.com/sdk/js/core/types>,
  <https://docs.ag-ui.com/spec/1.0/events>,
  <https://docs.ag-ui.com/concepts/interrupts>, and
  <https://docs.ag-ui.com/spec/1.0/basic/run-input>.
- **MCP:** an optional, private frontend-tool extension. It is not ACP core
  and is not AG-UI core. Its presence must not increase either protocol's
  conformance claim.
  - Target references: <https://modelcontextprotocol.io/specification/2026-07-28/basic>,
    <https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http>,
    <https://modelcontextprotocol.io/specification/2026-07-28/server/discover>,
    <https://modelcontextprotocol.io/specification/2026-07-28/server/tools>.

### Explicit transport boundary

| Direction | Wire | Current target |
| --- | --- | --- |
| AG-UI in | HTTP `POST /`, JSON `RunAgentInput` | JSON only |
| AG-UI out | `text/event-stream`, one JSON AG-UI `Event` per SSE data item | JSON/SSE |
| ACP in/out | ACP JSON-RPC requests, responses, notifications over the SDK byte-stream transport | ACP v1 |
| MCP extension in | Authenticated agent `POST /mcp/:thread` HTTP JSON-RPC request/response | Optional, request/response only |
| MCP extension out | JSON-RPC response to the agent; browser `POST /tool-response` resolves the private call | Optional |
| Not in this target | AG-UI protobuf or other non-JSON/SSE encodings, ACP v2 draft, MCP SSE transport | No claim |

The AG-UI core target does not include the private `STATE_SNAPSHOT` approval
payload, `forwardedProps.acpResume`, or the MCP frontend-tool path as native
protocol features.

### MCP frontend-tool transport boundary

The modern bridge contract is stateless and single-message: one JSON-RPC
request per authenticated `POST /mcp/:thread`, with
`MCP-Protocol-Version: 2026-07-28`, `Accept: application/json, text/event-stream`,
`Mcp-Method`, and `params._meta` containing the exact
`io.modelcontextprotocol/protocolVersion` and
`io.modelcontextprotocol/clientCapabilities` keys. Top-level `_meta` and dot
namespace keys are not accepted. `Mcp-Name` is required for `tools/call` and
must match the tool name; method/name mismatches are `-32020`.

The supported modern methods are `server/discover`, `tools/list`, and
`tools/call`; successful results are JSON and include `resultType: "complete"`.
Discovery returns `supportedVersions`, capabilities, and
`result._meta["io.modelcontextprotocol/serverInfo"]`; `tools/list` also returns
cache fields and server info, while `tools/call` has no cache fields. Tool
failures are result-level `isError: true`, and notifications receive HTTP `202`
without a JSON-RPC body.
The bridge does not create MCP sessions, return session IDs, serve GET/DELETE or
SSE, accept batches, or replay messages.

MCP Origin security is an independent HTTP boundary. An explicitly configured
allowlist compares every present `Origin` by canonical scheme, host, and
effective port; `null`, `*`, paths, trailing slashes, malformed values, and
unlisted origins receive HTTP `403`. Missing `Origin` remains allowed for
non-browser agents, while an empty allowlist rejects every present origin.

The legacy `2024-11-05` `initialize`/`tools/list`/`tools/call`
request-response subset remains for existing agents. It is compatibility-only
and is not a claim of legacy Streamable HTTP support.

## 2. Current-versus-target conformance matrix

Paths are repository-relative. A pending or rejected row states the required
wire behavior; it must not be treated as implemented merely because a helper,
raw event, or HTTP route exists.

For compactness below, `core/` means
`crates/agui-acp-bridge-core/`, and `server/` means
`crates/agui-acp-bridge-server/`.

### ACP methods, capabilities, content, and updates

| Surface | Current | Status | Target or unsupported wire behavior | Evidence / test path |
| --- | --- | --- | --- | --- |
| `initialize` / Protocol v1 negotiation | Sends `ProtocolVersion::V1`; rejects another negotiated version. | implemented | Keep the v1 check before session requests. | `crates/agui-acp-bridge-core/src/session.rs`; `src/acp.rs` |
| `session/new` and `session/prompt` | Creates a real ACP session and sends a prompt per turn. | implemented | Preserve the ACP `sessionId` and prompt response. | `core/src/session.rs`; `server/tests/http_sse_roundtrip.rs` |
| `session/load` | Strict load path, gated by `loadSession`; replayed updates are buffered and streamed. | implemented | Keep load distinct from resume and never fall back to `session/new` after a requested load fails. | `core/src/session.rs`; `server/tests/session_history.rs` |
| `session/resume` | No ACP `session/resume` request is sent; the private bridge path is only strict `session/load`. | explicitly rejected | Do not advertise or imply ACP resume. The bridge keeps ACP `session/resume` separate from its typed private load marker and never aliases or falls back between them. | `core/src/acp.rs`, `core/src/session.rs`; `server/tests/session_history.rs` |
| `session/list` | Capability-gated pass-through with pagination. | implemented | Unsupported capability remains an explicit bridge `Unsupported` result, not an empty list. | `core/src/session.rs`; `server/tests/session_history.rs` |
| `session/close` / `session/delete` | Capability-gated, uses the real ACP `SessionId`, and cleans local state on terminal outcomes. | implemented | Keep lifecycle claims and capability checks; do not substitute `threadId` for a known ACP ID. | `core/src/session.rs`, `server/src/handler.rs`; `server/tests/session_close.rs`, `session_delete.rs` |
| `session/cancel` | Sends ACP cancel and waits through the configured grace period. | implemented | Preserve cancel notification semantics and final `StopReason`; a timeout makes the session unusable. | `core/src/acp.rs`, `core/src/session.rs`; `server/tests/bridge_mock_agent.rs` |
| Permission request | Policy supports allow, deny, and deferred private approval. | partial | Preserve ACP request/response choices; private approval is not a replacement for any other ACP interaction. | `core/src/session.rs`, `core/src/policy.rs`; `server/tests/bridge_mock_agent.rs` |
| Modes and config | `session/set_mode` fallback and `session/set_config_option` select/value subset; snapshots are cached. | partial | Forward every supported ACP config kind and complete `SessionInfo`/config mapping before claiming full coverage. | `core/src/acp.rs`, `core/src/session.rs`; `server/tests/bridge_mock_agent.rs`, `session_history.rs` |
| Capability forwarding | Reads v1 agent capabilities, `loadSession`, session list/close/delete, and `mcpCapabilities.http`; forwards policy-enabled filesystem and terminal capabilities, both disabled by default. | partial | Advertise only implemented client capabilities; expose each implemented agent capability without inventing support. An unsupported capability must produce method-not-found or an explicit unsupported result at its wire boundary. | `core/src/session.rs`; `core/src/policy.rs`, `core/src/error.rs` |
| Filesystem requests | Read/write handlers are capability-gated; a policy may opt into text-file read and/or write, with the default policy advertising neither. | partial | Keep the bounded sandbox and cancellation/error mapping; expand only with matching capability and boundary tests. | `core/src/session.rs`, `core/src/file_ops.rs`, `core/src/policy.rs`; `core/src/session.rs` filesystem probes |
| Terminal requests | Create/output/wait/kill/release are capability-gated; a policy may opt into the session-local terminal registry, with the default policy advertising none. | partial | Keep terminal process/output bounds, cleanup, and cancellation behavior; do not advertise terminal unless the policy enables it. | `core/src/session.rs`, `core/src/terminal.rs`, `core/src/policy.rs`; `core/tests/terminal.rs` |
| Authentication and elicitation | No ACP lane is registered. HTTP bearer middleware is bridge security, not ACP authentication. | explicitly rejected | Do not advertise either capability. Requests remain explicit unsupported/method-not-found outcomes; add protocol tests before enabling them. | `server/src/handler.rs`, `core/src/session.rs`; no current dedicated test |
| Prompt `ContentBlock` input | The actor constructs one `ContentBlock::Text`; non-text AG-UI input is rejected before session creation. | partial | Normalize ordered input into the complete ACP `ContentBlock` set without dropping blocks; retain text as the compatibility path. Add per-variant round-trip tests. | `core/src/session.rs`, `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs` |
| Text and thought updates | Text `AgentMessageChunk`, `UserMessageChunk`, and `AgentThoughtChunk` become text/reasoning lifecycles. | implemented | Preserve ACP message identity and ordering. | `core/src/translation.rs`; its unit tests and `server/tests/http_sse_roundtrip.rs` |
| Tool updates | `ToolCall` and `ToolCallUpdate` become `TOOL_CALL_*`; raw input/output are cached as replacement snapshots. | implemented | Keep one logical lifecycle and emit result only after end; frontend MCP echoes stay suppressed. | `core/src/translation.rs`; `server/tests/frontend_tools.rs`, `frontend_tool_lifecycle.rs` |
| `CurrentModeUpdate` / `AvailableCommandsUpdate` | Forwarded as bridge CUSTOM events. | partial | Retain fields losslessly while a native AG-UI mapping is pending; do not label CUSTOM as a core event. | `core/src/translation.rs` |
| `ConfigOptionUpdate` / `SessionInfoUpdate` | Config is cached, but updates are otherwise RAW and the public summary is a subset. | pending implementation | Preserve the complete ACP snapshots and map them to the agreed native AG-UI state; unsupported fields must not disappear. | `core/src/session.rs`, `core/src/stream.rs`, `server/src/handler.rs`; `server/tests/session_history.rs` |
| `UsageUpdate` | Feature-gated and emitted as `agent:usage_update` CUSTOM. | partial | Map usage fields to the selected AG-UI core representation or preserve a lossless raw form; feature flags must not alter ACP wire version. | `core/src/translation.rs`, `core/Cargo.toml` |
| `Plan` snapshot | Limited step start/finish edges plus RAW snapshot. | partial | Keep snapshot replacement semantics and ordering; do not infer identity that ACP does not provide. | `core/src/translation.rs` unit tests |
| `PlanUpdate` / `PlanRemoved` | No native handling. | pending implementation | Preserve the ACP update variant losslessly and emit no fabricated step lifecycle. Add capability/update tests before claiming native mapping. | `core/src/translation.rs`; no current dedicated integration test |
| Unknown update | Active-stream opaque/unknown variants become serialized AG-UI RAW events with source `acp`; out-of-turn notifications are spilled to a bounded buffer drained by the next run. | partial | Never silently drop. Preserve a lossless raw event while possible; otherwise terminate the affected stream/connection with an explicit protocol error. | `core/src/translation.rs`, `core/src/session.rs`; `server/tests/bridge_mock_agent.rs` spill tests |
| Frontend MCP extension | `mcp_servers` is added only when the agent advertises `mcpCapabilities.http`; modern stateless `server/discover`, `tools/list`, and `tools/call` are implemented, with the legacy `2024-11-05` request-response subset retained. | N/A | Keep this separately labeled as MCP extension behavior, with MCP JSON-RPC errors; it is not ACP/AG-UI core conformance. No MCP session/SSE lifecycle is claimed. | `server/src/mcp_endpoint.rs`; its focused tests, `server/tests/frontend_tools.rs`, `frontend_tool_lifecycle.rs` |

### AG-UI input, events, lifecycle, state, and transport

| Surface | Current | Status | Target or unsupported wire behavior | Evidence / test path |
| --- | --- | --- | --- | --- |
| JSON `RunAgentInput` text tail | A trailing user text message becomes one ACP prompt; `state` and `messages` remain request context, with no generic `STATE_SNAPSHOT` or `MESSAGES_SNAPSHOT`; empty/non-user tails are clean no-op runs. | implemented | Keep one ACP turn per fresh trailing user turn without emitting generic state/message snapshots. | `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs`, `bridge_mock_agent.rs` |
| Multimodal / multipart user input | `UserMessageContent::Parts` is rejected before ACP session creation. | explicitly rejected | Emit AG-UI `RUN_ERROR` with `UNSUPPORTED_INPUT`; do not open ACP. Until full mapping lands, keep this rejection rather than silently converting to text. | `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs` |
| Core run lifecycle | `RUN_STARTED`, text/reasoning/tool events, then one `RUN_FINISHED` or `RUN_ERROR`; normal, no-op, and error runs emit no `MESSAGES_SNAPSHOT`. | implemented | `RUN_STARTED` first; exactly one terminal event last; no events after it or generic message snapshot. | `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs`, `bridge_mock_agent.rs` |
| Text, reasoning, and tool core events | Text/thought/tool subsets are emitted with ordering guards and ACP IDs. | implemented | Complete field and variant coverage remains bounded by the ACP rows above. | `core/src/translation.rs`; `server/tests/frontend_tools.rs` |
| Limited plan events | `STEP_STARTED`/`STEP_FINISHED` are derived only from safe `Plan` edges; snapshots may be RAW. | partial | Add native plan update/removal mapping without inventing entry IDs. | `core/src/translation.rs` |
| AG-UI `resume[]` | Any `input.resume` is rejected. | explicitly rejected | Emit machine-readable `RUN_ERROR` (`AGUI_RESUME_UNSUPPORTED` today); do not open a session or turn it into a no-op. Native resume remains explicitly rejected until a complete interop path exists. | `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs` |
| Private `forwardedProps.acpResume` | Typed object `{ "sessionId": "<ACP SessionId>" }` selects strict ACP `session/load` and one-shot history replay. | implemented | Keep it explicitly private; boolean or malformed markers return `ACP_RESUME_SESSION_ID_REQUIRED`, and it must never be presented as AG-UI `resume[]` or ACP `session/resume`. | `server/src/handler.rs`; `server/tests/session_history.rs`, `session_identity.rs`, `examples/copilotkit-acp-demo/src/hooks/use-acp-resume.ts` |
| Approval interrupt | Only deferred permission emits the private approval `STATE_SNAPSHOT` object plus `POST /approval`; input state is not emitted as a generic snapshot. | implemented | Keep as a documented private bridge extension only. | `server/src/handler.rs`; `server/tests/bridge_mock_agent.rs` |
| Canonical interrupt/continuation | No native AG-UI interrupt outcome or continuation is implemented. AG-UI 1.0 defines a native one (`RUN_FINISHED.outcome` `type: "interrupt"` plus `RunAgentInput.resume[]`); this bridge pins 0.x and does not emit it. | explicitly rejected | Do not claim canonical interrupt. Until implemented, native requests remain an explicit unsupported run error; private approval must not substitute for it. Migrating to the 1.0 interrupt/resume surface is a pending protocol-position decision. | `server/src/handler.rs`; `server/tests/bridge_mock_agent.rs`, `http_sse_roundtrip.rs`; <https://docs.ag-ui.com/spec/1.0/events>, <https://docs.ag-ui.com/concepts/interrupts> |
| Generic state delta and activity | `RunAgentInput.state` is request context and emits no generic `STATE_SNAPSHOT`; only private approval uses `STATE_SNAPSHOT`. No `STATE_DELTA` or `ACTIVITY_*` lane exists. | explicitly rejected | Do not emit generic state/activity snapshots or deltas without an ACP/source contract; approval remains private and stateful AG-UI semantics are not claimed. | `server/src/handler.rs`; `server/tests/http_sse_roundtrip.rs`, `bridge_mock_agent.rs` |
| Generic raw event | Opaque/unknown ACP updates use AG-UI RAW with serialized payload and source `acp`; known bridge extensions and usage remain CUSTOM. | implemented | Preserve the serialized ACP payload and source on the RAW event; this lane does not implement STATE_DELTA, ACTIVITY, MESSAGES_SNAPSHOT, or native resume/interrupt. | `core/src/translation.rs`; `server/tests/http_sse_roundtrip.rs`, `bridge_mock_agent.rs` |
| Config/session info and usage events | Available through CUSTOM session-init and `agent:usage_update` usage extensions plus RAW update snapshots. AG-UI 1.0 adds native usage on `RUN_FINISHED`/`RUN_ERROR`; this bridge pins 0.x, so usage remains CUSTOM here. | partial | Keep usage as CUSTOM while pinned to 0.x; on an AG-UI 1.0 upgrade (a pending protocol-position decision) migrate to the native usage fields without dropping data; preserve other config/session data losslessly with truthful capabilities. | `server/src/handler.rs`, `core/src/stream.rs`; <https://docs.ag-ui.com/spec/1.0/events> |
| Non-JSON/SSE AG-UI transport | The route requires the JSON/SSE path; other encodings are not implemented. | explicitly rejected | Reject unsupported media/`Accept` at the HTTP boundary without opening ACP; HTTP rejection is not an AG-UI wire event. | `server/src/handler.rs`, `examples/copilotkit-acp-demo/src/lib/agui-bridge.ts` |

## 3. Frozen internal interfaces

- **`BridgeStreamItem`:** the ordered actor-to-stream vocabulary. `Update`
  carries the ACP `SessionUpdate` without lossy reordering; `SessionInit` is a
  snapshot before turn updates; `Interrupt`, frontend tool items, `RunError`,
  and `Finished` are distinct lifecycle items. Add variants only additively.
- **`SessionInitState`:** one atomic snapshot of modes, models, and the complete
  `config_options` returned by `session/new` or `session/load`, then replaced by
  successful setting responses or relevant updates. It is not a history store
  and must not be conflated with `SessionInfo`.
- **`PromptStream`:** `events` is a bounded, ordered per-turn channel;
  `finished` resolves exactly once with `Ok(StopReason)` or `BridgeError`.
  Closing either side cancels the same turn; a later run cannot consume its
  events.
- **`StopReason`:** retain the exact ACP value until terminal translation.
  `EndTurn` is the only current success mapping. `Cancelled`, limits,
  refusal, and unknown/future values are machine-readable AG-UI run errors,
  never successful completion.
- **Input `ContentBlock`:** normalize AG-UI input into an ordered ACP block
  sequence. The current text-only prompt is a compatibility subset, not the
  frozen full contract. No non-text block, empty placeholder, or conversion
  failure may be silently dropped.
- **Feature/capability forwarding:** ACP initialization always negotiates v1;
  this crate's own core features such as `unstable_session_model` and
  `unstable_session_usage` cannot change that wire version. They are distinct
  from SDK unstable features such as `unstable_auth_methods`,
  `unstable_elicitation`, `unstable_end_turn_token_usage`,
  `unstable_mcp_over_acp`, `unstable_session_fork`, and
  `unstable_protocol_v2`, which are not enabled here. Advertise only
  implemented client capabilities, gate agent methods on the advertised
  capability, and forward capability/config snapshots without fabricating
  support.

Ordering is fixed: `RUN_STARTED` → `SessionInit`/updates and translated
events → translator flush → exactly one `RUN_FINISHED` or `RUN_ERROR` → end
of-stream. Text/reasoning closes precede a tool start; tool end precedes its
result; loaded history precedes a new prompt. No event is emitted after the
terminal event. Updates arriving outside an active turn are spilled to a
bounded buffer drained by the next run, never silently dropped; if that buffer
is at capacity, the update is dropped with a logged warning and the run
continues. If the session actor terminates while the spill buffer is still
non-empty, the buffered updates are discarded but logged as a warning. Unknown updates inside a turn are lossless
RAW (with known bridge extensions remaining CUSTOM) or an explicit error.
A disconnected client is an explicit cancellation,
not a successful run.

## 4. ID and lifecycle mapping

| ID | Meaning and mapping | Rule |
| --- | --- | --- |
| AG-UI `threadId` | Always a bridge/application conversation key. It is distinct from ACP `SessionId` and never aliases one, including private history. | Reuse is allowed only for later runs of the same conversation; resolve the ACP session only through the explicit bridge mapping. |
| AG-UI `runId` | One HTTP/SSE run and one admission claim. | Must be unique per run and is never an ACP or MCP ID. |
| AG-UI `messageId` | Message lifecycle identity. ACP content `MessageId` is forwarded when present; fallback IDs are bridge-generated. | Never reuse it as a run, session, request, or tool ID. |
| AG-UI `toolCallId` | AG-UI tool lifecycle identity. Frontend MCP calls receive bridge-minted UUIDs. | Never reuse; agent-side ACP tool echoes and frontend calls must not create duplicate lifecycles. |
| ACP `sessionId` | Real ID returned by ACP `session/new` or `session/load`; used by prompt, cancel, close, and delete. | Store it behind the explicit bridge mapping; never replace it with `threadId`, and never reuse after terminal close/delete. |
| ACP request ID | JSON-RPC correlation for one outstanding ACP request. | Preserve SDK correlation; never derive it from AG-UI or MCP IDs. |
| ACP tool/message IDs | ACP `ToolCallId` and content `MessageId` from updates. | Preserve within ACP scope; map only through the documented AG-UI lifecycle, without cross-scope reuse. |
| MCP request ID | Agent-to-bridge JSON-RPC request correlation; the endpoint echoes it in its response. | It remains an MCP ID and never becomes `runId` or `toolCallId`. |
| MCP tool ID / private call ID | MCP tool name selects the tool; the bridge's private UUID correlates `tools/call`, AG-UI events, and `/tool-response`. | Keep MCP request ID and private call ID distinct and non-reusable. |

The bridge implements and owns an explicit `threadId → ACP sessionId` mapping. A normal cache miss
uses `session/new`; an explicit typed `forwardedProps.acpResume.sessionId` cache
miss uses `session/load` with only that supplied ACP ID. The real ACP ID is stored
separately and used for every ACP request. Close/delete remove only the exact
thread mapping after their lifecycle outcome is settled; an ACP ID supplied where
a bridge key is required is not silently accepted as an alias, and a load failure
never falls back to `session/new`. This mapping is also the collision boundary
for repeated runs and resumed conversations.

These flows are not aliases:

1. ACP `session/load` replays history in the current bridge and is the private
   bootstrap path.
2. ACP `session/resume` is a separate target method and must not be implemented
   by renaming or assuming `session/load` semantics.
3. AG-UI `resume[]` is a native AG-UI input contract; it is currently rejected.
4. Private `forwardedProps.acpResume` is only the typed
   `{ "sessionId": "<ACP SessionId>" }` marker for the bridge's `session/load`
   path. It resolves the explicit thread-to-session mapping and never changes
   either ID; boolean markers, ACP-ID aliases, and load fallbacks are rejected.

## 5. Error boundary

- **ACP:** ACP JSON-RPC errors stay ACP errors at the ACP boundary. Capability
  absence is represented by truthful initialization plus method-not-found or
  the protocol's unsupported response when an unadvertised request is probed.
  `BridgeError::Acp`, `Unsupported`, `ResumeUnsupported`, and `ResumeFailed`
  must not be turned into a false ACP success.
- **AG-UI:** an accepted AG-UI run reports failures as machine-readable
  `RUN_ERROR` events. Existing codes include `UNSUPPORTED_INPUT`,
  `AGUI_RESUME_UNSUPPORTED`, `ACP_RESUME_SESSION_ID_REQUIRED`,
  `ACP_RESUME_UNSUPPORTED`, `ACP_RESUME_FAILED`,
  `ACP_CANCELLED`, `ACP_MAX_TOKENS`, `ACP_MAX_TURN_REQUESTS`, `ACP_REFUSAL`,
  `CONCURRENT_RUN`, and `ACP_QUEUE_CAPACITY`. A run error and a run finish are
  mutually exclusive.
- **MCP:** JSON-RPC protocol failures use the MCP JSON-RPC envelope. The
  endpoint uses `-32600` for malformed requests/transport metadata, `-32020`
  with HTTP `400` for header/body mismatches, `-32022` with HTTP `400` and
  supported/requested version data for unsupported protocol versions, and
  `-32602` for missing request metadata, invalid params, or unknown tools.
  Unknown methods are HTTP `404` with `-32601`. A tool execution failure is a
  successful JSON-RPC response whose result has `isError: true`; it is not a
  JSON-RPC transport error. Modern notifications return `202` without a
  response body. Request IDs are non-null strings/numbers; notifications omit
  `id`.
- **HTTP is not a wire error:** `401`, `404`, `409`, `501`, `502`, `504`, body
  limits, `403` Origin rejection, and timeout responses on bridge routes are
  HTTP boundary behavior.
  They must not be described as ACP or AG-UI protocol errors. The AG-UI run
  route normally carries an error inside the SSE stream.
- **Unsupported capability rule:** do not advertise what is not implemented;
  do not silently fall back across semantic operations; return an explicit
  unsupported/method-not-found result at the relevant boundary and add a test.

## 6. Ownership and phases

| Phase | Scope | Main files and tests |
| --- | --- | --- |
| P1-A | Core session, prompt, content, ACP ordering, terminal response | `crates/agui-acp-bridge-core/src/acp.rs`, `session.rs`, `stream.rs`; `crates/agui-acp-bridge-core/tests/process_echo.rs`; server `bridge_mock_agent.rs` |
| P1-B | Capability lanes: permission, config, filesystem, terminal, auth, elicitation, ACP update variants | Core `session.rs`, `policy.rs`, `file_ops.rs`, `config.rs`; `server/tests/bridge_mock_agent.rs`; core `session.rs` unsupported-method test. Any new internal update shape requires P2 review, but P1 does not own AG-UI serialization. |
| P1-C | ACP `session/load` versus `session/resume`, list, IDs, lifecycle | Core `acp.rs`, `session.rs`; server `handler.rs`; `server/tests/session_history.rs`, `session_delete.rs`, `session_close.rs` |
| P2 | AG-UI translation, native lifecycle, interrupt/resume, state/activity/raw | Core `translation.rs`, `stream.rs`; server `handler.rs`; `server/tests/http_sse_roundtrip.rs`, `bridge_mock_agent.rs`, `smoke_perf.rs`; client `examples/copilotkit-acp-demo/src/lib/agui-bridge.ts` |
| P3 | MCP extension, HTTP/Next proxies, body/timeouts/cancel/disconnect/backpressure/resource limits | Server `mcp_endpoint.rs`, `handler.rs`; core `frontend_tools.rs`, `config.rs`; Next `src/app/api/bridge/**`, `src/app/api/copilotkit/route.ts`, `src/hooks/use-acp-frontend-tool.ts` |
| P4 | Interop fixtures, docs, release and test evidence | `docs/PROTOCOL_CONFORMANCE.md`, `server/src/test_agents.rs`, server integration tests, `examples/copilotkit-acp-demo/package.json`, `.github/workflows/ci.yml` |

Shared-state review is mandatory for every P2/P3 change:

- `FrontendToolRegistry` owns per-thread tools, the active sender, pending
  calls, and the reverse `tool_call_id` index. Update registration, resolve,
  drain, drop, and exact per-thread cleanup together.
- The active sender is installed by `build_event_stream`. Registration of a
  pending call and sender lookup must remain atomic under the sender lock.
- `ClearOnDrop` may clear only through
  `clear_active_sender_if_same`. If it owns the slot, it aborts pending calls;
  if a newer sender owns the slot, it must not clear or abort that newer run.
- Review `active_runs`, lifecycle claims, session pointer-identity removal,
  and `ClearOnDrop` together. No teardown may remove a replacement session,
  sender, or thread-to-session mapping.
- Resource-limit changes must cover `event_buffer`,
  `slow_consumer_timeout`, `frontend_tool_timeout`, `cancel_grace_timeout`,
  `open_session_timeout`, body limits, `max_sessions`, and
  `max_queued_turns`; backpressure must cancel rather than silently drop.

## 7. Minimal acceptance checklist

- [x] **Load versus resume:** `server/tests/session_history.rs` proves
  list/load/private replay, typed session-ID validation, strict no-fallback
  errors, and one-shot history; `http_sse_roundtrip.rs` proves AG-UI
  `resume[]` rejection. ACP `session/resume` is not sent or aliased to the
  private `session/load` path.
- [ ] **All content:** existing image-output RAW and multipart rejection are
  in `server/tests/http_sse_roundtrip.rs`. Add every input/output
  `ContentBlock` variant, ordering, and lossless failure tests.
- [ ] **Order and terminal exactly once:** retain checks in
  `http_sse_roundtrip.rs`, `bridge_mock_agent.rs`, and `smoke_perf.rs`; add
  disconnect, ACP error, unknown update, and cancel cases asserting one final
  terminal event and no later event.
- [ ] **Native resume/interrupt:** existing private approval coverage is in
  `bridge_mock_agent.rs`; add canonical interrupt outcome/continuation and
  native resume interop tests.
- [ ] **Snapshot versus delta:** existing plan snapshot tests are in
  `core/src/translation.rs`; add state snapshot/delta and
  `PlanUpdate`/`PlanRemoved` tests that reject fabricated deltas.
- [ ] **Unsupported errors:** retain the core capability-gated
  filesystem/terminal tests, `session_history.rs` capability test, and
  `mcp_endpoint.rs` JSON-RPC tests; add auth, elicitation, every unsupported
  capability, and exact ACP/AG-UI/MCP error assertions.
- [x] **ID non-reuse:** existing message-ID and frontend lifecycle tests are in
  `core/src/translation.rs`, `core/src/frontend_tools.rs`, and
  `server/tests/frontend_tool_lifecycle.rs`; P1-C identity, close/delete,
  load/resume, and repeated-run mapping tests are in
  `server/tests/session_identity.rs`, `session_close.rs`, and
  `session_delete.rs`.
- [x] **Thread/session identity isolation:** `server/tests/session_identity.rs`
  proves that a bridge `threadId` is never an ACP ID alias across private load,
  normal repeated runs, and resumed conversations; `session_close.rs` and
  `session_delete.rs` assert real-ID lifecycle requests and exact mapping
  cleanup.
- [ ] **Limits and teardown:** add body-limit, open/setting/frontend timeout,
  ACP cancel grace, client disconnect while idle, bounded-channel backpressure,
  and queue/session-capacity tests. Existing timeout/cancel coverage is in
  `server/tests/bridge_mock_agent.rs` and `frontend_tool_lifecycle.rs`.
- [ ] **Boundary tests from the current fix wave:** evidence required for the
  out-of-turn/no-silent-drop contract (§3) and truthful error codes — media-type
  / `Accept` HTTP 406 rejection at the AG-UI route, truthful limit-error codes
  (`ACP_MAX_TOKENS`, `ACP_MAX_TURN_REQUESTS`, `ACP_QUEUE_CAPACITY`) asserted
  exactly, a dedicated cancel-grace timeout code distinct from generic errors,
  and the bounded-spill/overflow (warn-and-drop) path for post-terminal updates.

## 8. Gate status

**Gate 0: needs adjustment.** This document is the remediation artifact. No
protocol implementation may be claimed complete until every pending row is
either `implemented` or `explicitly rejected` with the stated wire behavior
and tests, and the acceptance checklist has evidence for the boundary cases.
