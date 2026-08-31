import {
  CopilotRuntime,
  ExperimentalEmptyAdapter,
  copilotRuntimeNextJSAppRouterEndpoint,
} from "@copilotkit/runtime";
import type { NextRequest } from "next/server";
import {
  BRIDGE_AUXILIARY_TIMEOUT_MS,
  BridgeRequestBodyTooLargeError,
  bridgeErrorResponse,
  bridgeRequestCsrfResponse,
  createBridgeAgent,
  readBoundedRequestBody,
} from "@/lib/agui-bridge";

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
  const csrfResponse = bridgeRequestCsrfResponse(req);
  if (csrfResponse) return csrfResponse;

  const bodyAdmissionTimeoutSignal = AbortSignal.timeout(
    BRIDGE_AUXILIARY_TIMEOUT_MS,
  );
  const bodyAdmissionSignal = AbortSignal.any([
    req.signal,
    bodyAdmissionTimeoutSignal,
  ]);
  let body: string;

  try {
    body = await readBoundedRequestBody(req, bodyAdmissionSignal);
  } catch (error) {
    if (error instanceof BridgeRequestBodyTooLargeError) {
      return bridgeErrorResponse(413, "request body too large");
    }
    if (req.signal.aborted) throw error;
    if (bodyAdmissionTimeoutSignal.aborted) {
      return bridgeErrorResponse(504, "bridge request timed out");
    }
    throw error;
  }

  // This 30s deadline is only for bounded request-body admission. Do not pass
  // it into handleRequest: the primary response is intentionally long-lived
  // SSE, so the runtime keeps the original signal for disconnect cancellation.
  const runtimeRequest = new Request(req.url, {
    method: req.method,
    headers: new Headers(req.headers),
    body,
    signal: req.signal,
  });
  const { handleRequest } = copilotRuntimeNextJSAppRouterEndpoint({
    runtime,
    serviceAdapter,
    endpoint: "/api/copilotkit",
  });
  return handleRequest(runtimeRequest);
};

// Primary AG-UI SSE lifetime is bounded by Next's maxDuration and the
// incoming request signal, not the 30s auxiliary deadline above.
export const maxDuration = 300;
