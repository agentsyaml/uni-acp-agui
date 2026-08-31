import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `GET /sessions` endpoint, which lists
 * the ACP agent's persisted conversations (via `session/list`). The bridge
 * holds no history itself — this is a pass-through of what the agent reports.
 *
 * Status codes mirror the bridge:
 * - 200 → `{ sessions: [ { sessionId, cwd, title?, updatedAt? } ] }`
 * - 501 → the agent does not support `session/list` (frontend hides history)
 * - 502 → the agent errored / listing connection failed
 */
export async function GET(req: NextRequest) {
  return proxyBridgeRequest(req, "/sessions");
}
