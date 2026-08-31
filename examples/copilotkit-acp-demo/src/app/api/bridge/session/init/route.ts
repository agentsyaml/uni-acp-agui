import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL, proxyBridgeRequest } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `GET /session/init?threadId=...`
 * endpoint. Surfaces the cached `SessionModeState` / `SessionModelState`
 * the agent advertised on `session/new` (plus the current selections).
 *
 * Returns 404 when no session has been opened for the supplied thread id;
 * the frontend should issue a normal AG-UI run first.
 */
export async function GET(req: NextRequest) {
  const threadId = req.nextUrl.searchParams.get("threadId");
  if (!threadId) {
    return NextResponse.json(
      { error: "missing threadId query parameter" },
      { status: 400 },
    );
  }
  const target = new URL("/session/init", BRIDGE_URL);
  target.searchParams.set("threadId", threadId);
  return proxyBridgeRequest(req, target);
}
