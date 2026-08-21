import "server-only";
import { HttpAgent } from "@ag-ui/client";

/**
 * URL of the Rust `agui-acp-bridge` HTTP/SSE endpoint.
 *
 * The bridge wraps an ACP-compliant agent (e.g. `opencode acp`) and exposes
 * it as an AG-UI HTTP endpoint. Default: http://127.0.0.1:8080/.
 *
 * Override via the server-only `AGUI_BRIDGE_URL` environment variable.
 */
export const BRIDGE_URL =
  process.env.AGUI_BRIDGE_URL ?? "http://127.0.0.1:8080/";

/** Headers for server-side requests to the protected bridge. */
export function bridgeHeaders(): Record<string, string> {
  const token = process.env.AGUI_BRIDGE_TOKEN;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

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
      ...bridgeHeaders(),
      // The bridge requires SSE; HttpAgent already sets this, but being
      // explicit keeps middleware (if any) from rewriting it away.
      Accept: "text/event-stream",
    },
  });
}
