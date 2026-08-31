import type { NextRequest } from "next/server";
import { proxyBridgeRequest } from "@/lib/agui-bridge";

export async function GET(req: NextRequest) {
  return proxyBridgeRequest(req, "/health");
}
