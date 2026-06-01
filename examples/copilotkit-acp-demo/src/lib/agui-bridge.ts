import { HttpAgent } from "@ag-ui/client";

/**
 * URL of the Rust `agui-acp-bridge` HTTP/SSE endpoint.
 *
 * The bridge wraps an ACP-compliant agent (e.g. `opencode acp`) and exposes
 * it as an AG-UI HTTP endpoint. Default: http://127.0.0.1:8080/.
 *
 * Override via `AGUI_BRIDGE_URL` (server) or `NEXT_PUBLIC_AGUI_BRIDGE_URL`
 * (only if you want to talk to the bridge directly from the browser without
 * going through the CopilotRuntime proxy — not recommended).
 */
export const BRIDGE_URL =
  process.env.AGUI_BRIDGE_URL ??
  process.env.NEXT_PUBLIC_AGUI_BRIDGE_URL ??
  "http://127.0.0.1:8080/";

/**
 * Build an AG-UI {@link HttpAgent} pointing at the Rust bridge.
 *
 * Each call returns a fresh instance — the `CopilotRuntime` clones agents per
 * thread internally, so a single instance per registration is fine.
 */
export function createBridgeAgent(): HttpAgent {
  return new HttpAgent({
    url: BRIDGE_URL,
    headers: {
      // The bridge requires SSE; HttpAgent already sets this, but being
      // explicit keeps middleware (if any) from rewriting it away.
      Accept: "text/event-stream",
    },
  });
}
