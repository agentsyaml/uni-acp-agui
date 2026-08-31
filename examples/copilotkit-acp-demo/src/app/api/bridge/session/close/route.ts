import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `POST /session/close` endpoint.
 */
export async function POST(req: NextRequest) {
  return proxyBridgeRequest(req, "/session/close");
}
