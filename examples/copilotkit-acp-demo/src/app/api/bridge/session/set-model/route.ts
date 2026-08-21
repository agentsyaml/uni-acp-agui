import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL, bridgeHeaders } from "@/lib/agui-bridge";

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
  const body = await req.text();
  try {
    const target = new URL("/session/set-model", BRIDGE_URL);
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
