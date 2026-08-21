import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL, bridgeHeaders } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `POST /session/close` endpoint.
 */
export async function POST(req: NextRequest) {
  const body = await req.text();
  try {
    const target = new URL("/session/close", BRIDGE_URL);
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
