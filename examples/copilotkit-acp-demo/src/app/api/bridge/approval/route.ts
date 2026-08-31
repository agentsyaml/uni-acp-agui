import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `/approval` endpoint.
 *
 * Browsers can't POST to `127.0.0.1:8080` from a `localhost:3000` page
 * without the bridge advertising CORS, so we forward through Next.js.
 */
export async function POST(req: NextRequest) {
  return proxyBridgeRequest(req, "/approval");
}
