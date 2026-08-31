import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `/tool-response` endpoint.
 *
 * The browser cannot POST directly to `127.0.0.1:8080` from a `localhost:3000`
 * page without CORS, so the frontend hook (`useAcpFrontendTool`) routes
 * through here instead. We pass the body through verbatim.
 */
export async function POST(req: NextRequest) {
  return proxyBridgeRequest(req, "/tool-response");
}
