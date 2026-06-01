import { NextResponse } from "next/server";
import { BRIDGE_URL } from "@/lib/agui-bridge";

export async function GET() {
  try {
    const target = new URL("/health", BRIDGE_URL);
    const upstream = await fetch(target, { cache: "no-store" });
    const text = await upstream.text();
    return new NextResponse(text, {
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
