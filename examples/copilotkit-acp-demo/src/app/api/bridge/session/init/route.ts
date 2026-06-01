import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL } from "@/lib/agui-bridge";

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
  try {
    const target = new URL("/session/init", BRIDGE_URL);
    target.searchParams.set("threadId", threadId);
    const upstream = await fetch(target, { cache: "no-store" });
    const text = await upstream.text();
    return new NextResponse(text || null, {
      status: upstream.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return NextResponse.json(
      { status: "unreachable", error: String(err) },
      { status: 502 },
    );
  }
}
