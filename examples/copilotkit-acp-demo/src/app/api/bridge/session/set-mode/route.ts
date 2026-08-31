import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `POST /session/set-mode`.
 *
 * The bridge issues an ACP `session/set_mode` request to the underlying
 * agent. Status codes are forwarded verbatim:
 *
 * - 200 → mode accepted by the agent;
 * - 404 → no session for `threadId`;
 * - 422 → agent rejected (likely `modeId` not in `availableModes`);
 * - 503 → session actor closed mid-flight.
 */
export async function POST(req: NextRequest) {
  return proxyBridgeRequest(req, "/session/set-mode");
}
