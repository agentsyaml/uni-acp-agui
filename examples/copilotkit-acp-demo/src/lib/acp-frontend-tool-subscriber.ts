import type { AbstractAgent, AgentSubscriber } from "@ag-ui/client";

type Agent = Pick<AbstractAgent, "threadId" | "subscribe">;

type SubscriberOptions<Args extends Record<string, unknown>> = {
  matchNames: () => Set<string>;
  handler: (args: Args) => Promise<unknown> | unknown;
  log: (...args: unknown[]) => void;
};

export function frontendToolDeclaration<T>(
  tool: { name: string; description: string; parameters?: T },
) {
  return {
    name: tool.name,
    description: tool.description,
    parameters: tool.parameters,
    followUp: false as const,
  };
}

export function subscribeAcpFrontendTool<Args extends Record<string, unknown>>(
  agent: Agent,
  options: SubscriberOptions<Args>,
  send: typeof fetch = fetch,
): () => void {
  type ActiveCall = { threadId: string; argsDelta: string };
  const activeCalls = new Map<string, ActiveCall>();

  const subscriber: AgentSubscriber = {
    onRunStartedEvent: () => activeCalls.clear(),
    onRunErrorEvent: () => activeCalls.clear(),
    onRunFinishedEvent: () => activeCalls.clear(),
    onToolCallStartEvent: ({ event }) => {
      const threadId = agent.threadId;
      if (
        event.toolCallId &&
        threadId &&
        options.matchNames().has(event.toolCallName)
      ) {
        activeCalls.set(event.toolCallId, { threadId, argsDelta: "" });
      }
    },
    onToolCallArgsEvent: ({ event }) => {
      const call = activeCalls.get(event.toolCallId);
      if (call) call.argsDelta += event.delta;
    },
    onToolCallEndEvent: ({ event }) => {
      const call = activeCalls.get(event.toolCallId);
      if (!call) return;
      activeCalls.delete(event.toolCallId);

      let args: Args;
      try {
        const parsed: unknown = call.argsDelta ? JSON.parse(call.argsDelta) : {};
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          throw new Error("arguments must be a JSON object");
        }
        args = parsed as Args;
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        void postResponse(
          event.toolCallId,
          call.threadId,
          `Invalid tool-call arguments: ${detail}`,
          true,
        );
        return;
      }

      void runHandler(event.toolCallId, call.threadId, args);
    },
  };

  const subscription = agent.subscribe(subscriber);

  async function runHandler(toolCallId: string, threadId: string, args: Args) {
    let isError = false;
    let content: string;
    try {
      const result = await Promise.resolve(options.handler(args));
      content = typeof result === "string" ? result : JSON.stringify(result);
      options.log("[useAcpFrontendTool] resolved", { toolCallId, content });
    } catch (error) {
      isError = true;
      content = error instanceof Error ? error.message : String(error);
      options.log("[useAcpFrontendTool] handler errored", { toolCallId, error });
    }
    await postResponse(toolCallId, threadId, content, isError);
  }

  async function postResponse(
    toolCallId: string,
    threadId: string,
    content: string,
    isError: boolean,
  ) {
    try {
      const response = await send("/api/bridge/tool-response", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ threadId, toolCallId, content, isError }),
      });
      if (!response.ok) {
        options.log(
          "[useAcpFrontendTool] /tool-response non-ok",
          response.status,
          await response.text(),
        );
      }
    } catch (error) {
      options.log("[useAcpFrontendTool] /tool-response failed", error);
    }
  }

  return () => {
    subscription.unsubscribe();
    activeCalls.clear();
  };
}
