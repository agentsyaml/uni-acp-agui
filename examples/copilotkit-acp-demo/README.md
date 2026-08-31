# copilotkit-acp-demo

A Next.js + CopilotKit demo that connects to the Rust [`agui-acp-bridge`](../../) sitting in front of a local ACP agent — by default, [`opencode acp`](https://opencode.ai/).

```
Browser
  └─ <CopilotKit runtimeUrl="/api/copilotkit">
       └─ <CopilotChat /> · <CopilotSidebar /> · useAgent() …
              │ HTTP/SSE
              ▼
  Next.js  app/api/copilotkit/route.ts  (CopilotRuntime)
              │ HttpAgent → http://127.0.0.1:8080/
              ▼
  Rust  agui-acp-bridge-cli           (this repo)
              │ JSON-RPC stdio
              ▼
         opencode acp                  (local ACP agent)
```

The browser never talks to `127.0.0.1:8080` directly — `CopilotRuntime` proxies
each run, which means CORS, auth headers, and middleware all stay server-side.

## Prerequisites

- Rust 1.88+ (already required by the workspace)
- [Bun](https://bun.sh/) 1.3.14 (CI uses this version; compatible Bun 1.3+ also works)
- [opencode](https://opencode.ai/) CLI installed and authenticated:
  ```bash
  bun install -g opencode-ai
  opencode auth login          # configure at least one model provider
  ```

## Run it

Open three terminals.

### 1. Start the bridge

Pick one:

```bash
# Wrap opencode acp (real LLM)
bun run dev:bridge

# Or echo agent (no LLM, deterministic, good for protocol smoke tests)
bun run dev:bridge:echo

# Or HITL mode for the /approval demo
bun run dev:bridge:hitl
```

Each script just shells out to `cargo run -p agui-acp-bridge-cli -- ...` so you
can also run those commands by hand from the workspace root.

The bridge listens on `127.0.0.1:8080`. Health probe:

```bash
curl http://127.0.0.1:8080/health
# → {"status":"ok"}
```

### 2. Start the Next.js app

```bash
bun install --frozen-lockfile
bun run dev
```

Run the frontend checks/build when validating a change:

```bash
bun run type-check
bun run lint
bun run build
```

Open <http://localhost:3000>.

### 3. (Optional) Verify the path with curl

```bash
curl -N -X POST http://127.0.0.1:8080/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: text/event-stream' \
  -d '{"threadId":"t1","runId":"r1","messages":[{"role":"user","id":"m1","content":"hi"}],"tools":[],"context":[],"forwardedProps":{},"state":{}}'
```

You should see SSE frames flowing.

## What's in this demo

| Page | What it shows | Hooks / components |
| --- | --- | --- |
| `/` | Landing + setup notes | – |
| `/chat` | Streaming chat against the bridge | `<CopilotChat>` (v2) |
| `/sidebar` | Floating sidebar variant | `<CopilotSidebar>` (v2) |
| `/tool-rendering` | Renders ACP `ToolCall` updates as React cards | `useDefaultRenderTool` |
| `/raw-events` | Live AG-UI event log via `agent.subscribe()` | `useAgent` |
| `/approval` | HITL demo for `--policy interrupt`; uses `STATE_SNAPSHOT` + `POST /approval` | `useAgent` + `/api/bridge/approval` |
| `/health` | `GET /api/bridge/health` proxy | – |

API routes:
- `POST /api/copilotkit` — `CopilotRuntime` mounting one `HttpAgent` named `default` against `http://127.0.0.1:8080/`.
- `GET /api/bridge/health` — server-side proxy to the bridge `/health`.
- `POST /api/bridge/approval` — server-side proxy to the bridge `/approval`.
- `POST /api/bridge/session/close` — server-side proxy to the bridge
  `/session/close` lifecycle endpoint.
- `POST /api/bridge/session/delete` — server-side proxy to the bridge
  `/session/delete` persistence endpoint.

### Mutating-route CSRF boundary

The mutating bridge routes (`POST`/`DELETE`, including `/api/copilotkit`) reject
`Sec-Fetch-Site: cross-site` before reading or forwarding a request body. When a
request has an `Origin`, it must be an exact trusted app origin; malformed
origins return `400` and other origin failures return `403`. Requests without
`Origin` remain available to server-side proxy calls and non-browser CLI tools.

Local development defaults to the fixed single-user origins
`http://localhost:3000`, `http://127.0.0.1:3000`, and `http://[::1]:3000`, with an
additional exact match against the request URL. For a non-default port or a
reverse proxy, set a comma-separated explicit allowlist, for example:

```bash
AGUI_APP_ORIGINS=https://my-app.example.com
```

Do not use `*`. The route does not trust `X-Forwarded-Host` or
`X-Forwarded-Proto`; configure `AGUI_APP_ORIGINS` when the proxy's public origin
cannot be reliably reconstructed by Next.

## Where the wiring lives

- `src/lib/agui-bridge.ts` — single source of truth for the bridge URL + the
  `HttpAgent` factory.
- `src/app/api/copilotkit/route.ts` — registers the bridge agent under the
  name `default` so v2 components pick it up automatically.
- `src/app/layout.tsx` — `<CopilotKit runtimeUrl="/api/copilotkit">` provider
  with v2 styles.

## Caveats

- The `default` agent registration assumes one bridge per app. To target
  multiple bridges, register them under distinct keys and pass `agentId` to
  `<CopilotChat>` / `useAgent`.
- `opencode acp` requires a configured model provider. Without one, the SSE
  stream will end with a `RUN_ERROR`.
- The bridge caps cached sessions at `--max-sessions` (default 128) and reaps
  idle ones after `--idle-timeout` (default 120s). The demo persists its
  AG-UI `threadId`; selecting saved history keeps the ACP `sessionId` separate
  and sends it through the typed private resume marker.

## License

MIT OR Apache-2.0 (matches the workspace).
