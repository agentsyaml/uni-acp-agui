import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL, bridgeHeaders } from "@/lib/agui-bridge";

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
  const body = await req.text();
  try {
    const target = new URL("/session/set-mode", BRIDGE_URL);
    const upstream = await fetch(target, {
      method: "POST",
      headers: {
        ...bridgeHeaders(),
        "Content-Type": "application/json",
      },
      body,
    });
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
