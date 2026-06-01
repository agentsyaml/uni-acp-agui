import {
  CopilotRuntime,
  ExperimentalEmptyAdapter,
  copilotRuntimeNextJSAppRouterEndpoint,
} from "@copilotkit/runtime";
import type { NextRequest } from "next/server";
import { createBridgeAgent } from "@/lib/agui-bridge";

/**
 * CopilotKit runtime route.
 *
 * Registers the Rust `agui-acp-bridge` (which wraps an ACP agent like
 * `opencode acp`) as the `default` agent. Because the agent is registered
 * under the name `default`, CopilotKit's prebuilt UI components
 * (`<CopilotChat>`, `<CopilotSidebar>`, `<CopilotPopup>`) pick it up
 * automatically with no extra `agentId` plumbing on the frontend.
 *
 * The runtime needs *some* service adapter to satisfy the v1 contract; we
 * use `ExperimentalEmptyAdapter` because all real LLM work happens inside
 * the registered agent (the bridge → ACP → opencode).
 */
const serviceAdapter = new ExperimentalEmptyAdapter();

const runtime = new CopilotRuntime({
  agents: {
    default: createBridgeAgent(),
  },
});

export const POST = async (req: NextRequest) => {
  const { handleRequest } = copilotRuntimeNextJSAppRouterEndpoint({
    runtime,
    serviceAdapter,
    endpoint: "/api/copilotkit",
  });
  return handleRequest(req);
};

// Long-running SSE — opt out of the default 10s edge response cap.
export const maxDuration = 300;
