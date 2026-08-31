import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `POST /session/set-model`.
 *
 * The bridge issues an ACP `session/set_model` request to the underlying
 * agent. Status-code semantics match `/session/set-mode`. The bridge only
 * mounts this route when the `unstable_session_model` feature is enabled
 * (default-on); a 404 here may also mean the deployment was built without
 * model-switching support.
 */
export async function POST(req: NextRequest) {
  return proxyBridgeRequest(req, "/session/set-model");
}
