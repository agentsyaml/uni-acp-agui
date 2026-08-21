import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL, bridgeHeaders } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `/tool-response` endpoint.
 *
 * The browser cannot POST directly to `127.0.0.1:8080` from a `localhost:3000`
 * page without CORS, so the frontend hook (`useAcpFrontendTool`) routes
 * through here instead. We pass the body through verbatim.
 */
export async function POST(req: NextRequest) {
  const body = await req.text();
  const target = new URL("/tool-response", BRIDGE_URL);
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
}
