import { NextResponse, type NextRequest } from "next/server";
import { BRIDGE_URL } from "@/lib/agui-bridge";

/**
 * Server-side proxy for the bridge's `/approval` endpoint.
 *
 * Browsers can't POST to `127.0.0.1:8080` from a `localhost:3000` page
 * without the bridge advertising CORS, so we forward through Next.js.
 */
export async function POST(req: NextRequest) {
  const body = await req.text();
  const target = new URL("/approval", BRIDGE_URL);
  const upstream = await fetch(target, {
    method: "POST",
    headers: {
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
