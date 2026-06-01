import { NextResponse } from "next/server";
import { BRIDGE_URL } from "@/lib/agui-bridge";

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
export async function GET() {
  try {
    const target = new URL("/sessions", BRIDGE_URL);
    const upstream = await fetch(target, { cache: "no-store" });
    const text = await upstream.text();
    return new NextResponse(text || null, {
      status: upstream.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return NextResponse.json(
      { error: `bridge unreachable: ${String(err)}` },
      { status: 502 },
    );
  }
}
