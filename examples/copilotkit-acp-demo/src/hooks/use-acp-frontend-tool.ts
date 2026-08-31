"use client";

import { useAgent, useFrontendTool } from "@copilotkit/react-core/v2";
import { useEffect, useRef } from "react";
import type { StandardSchemaV1 } from "@standard-schema/spec";

/**
 * Single-hook API for AG-UI frontend tools driven through the
 * agui-acp-bridge.
 *
 * Internally this:
 *
 * 1. Calls CopilotKit's `useFrontendTool` so the tool ends up in the
 *    `RunAgentInput.tools` payload the bridge sees on every run. The
 *    bridge then registers it on its in-process MCP server scoped to
 *    the current thread, and the agent picks it up through ACP's
 *    standard `NewSessionRequest.mcp_servers` mechanism.
 *
 *    **No handler is registered with CopilotKit**, and `followUp: false`
 *    is forced. This is critical: when the bridge dispatches the tool
 *    via MCP, the agent emits a `toolCall` on its assistant message but
 *    there is no matching `tool` message in the same `newMessages` set
 *    (because the result returned via MCP, not via the AG-UI stream).
 *    CopilotKit's `processAgentResult` would therefore invoke any
 *    registered handler a SECOND time — duplicating side effects (UI
 *    state, HITL dialogs, …) and triggering a follow-up `runAgent`
 *    round-trip. Suppressing the handler + follow-up keeps execution
 *    on a single canonical path: the bridge's MCP route.
 *
 * 2. Subscribes to AG-UI tool-call events on the same agent. When the
 *    bridge dispatches a `TOOL_CALL_START` whose name matches `name`
 *    (or its MCP-prefixed variant `agui-acp-bridge_<name>`), the hook
 *    waits for the args to arrive, runs the user-supplied handler, and
 *    POSTs the result to `/api/bridge/tool-response`. That endpoint is
 *    a thin Next.js proxy to the bridge's `/tool-response`.
 *
 * Parallel tool calls are tracked in a `Map` keyed by `toolCallId`, so
 * the agent issuing two calls in the same turn (`Estimate A100` *and*
 * `Estimate B200`) fires the handler for both — instead of the second
 * `TOOL_CALL_START` overwriting the first and stranding the first call
 * until the bridge's `frontend_tool_timeout` (default 10 min) fires.
 *
 * @example
 * ```tsx
 * import { z } from "zod";
 * useAcpFrontendTool({
 *   name: "say_hello",
 *   description: "Greet someone by name.",
 *   parameters: z.object({ name: z.string() }),
 *   handler: async ({ name }) => `hi ${name}`,
 * });
 * ```
 */
export function useAcpFrontendTool<Args extends Record<string, unknown>>(opts: {
  /** Tool name advertised to the agent. */
  name: string;
  /** Human-readable description surfaced to the LLM. */
  description?: string;
  /** Standard Schema for the tool's parameters (Zod, Valibot, etc.). */
  parameters?: StandardSchemaV1<unknown, Args>;
  /**
   * Async handler. Return a JSON-serialisable value or a string. Errors
   * are caught and forwarded to the bridge as MCP `isError: true`
   * envelopes so the agent's LLM can react.
   */
  handler: (args: Args) => Promise<unknown> | unknown;
  /** Optional logger; defaults to no-op. */
  log?: (...args: unknown[]) => void;
  /**
   * MCP server name the bridge advertises when injecting tools. Most
   * agents (opencode, …) prefix MCP-sourced tool names with this when
   * they surface them on the session/update channel. The default
   * matches the bridge's hard-coded server name.
   */
  mcpServerName?: string;
}) {
  const { agent } = useAgent();

  // Register only the *declaration* with CopilotKit so it ends up in
  // `RunAgentInput.tools`. We deliberately omit `handler` and force
  // `followUp: false` — see the class doc for why.
  useFrontendTool({
    name: opts.name,
    description: opts.description ?? "",
    parameters: opts.parameters as
      | StandardSchemaV1<unknown, Record<string, unknown>>
      | undefined,
    followUp: false,
  });

  // Refs let us update the user-supplied handler without re-subscribing.
  const handlerRef = useRef(opts.handler);
  const noopLog = (..._args: unknown[]) => {
    void _args;
  };
  const logRef = useRef<(...args: unknown[]) => void>(opts.log ?? noopLog);
  const matchNamesRef = useRef<Set<string>>(
    computeMatchNames(opts.name, opts.mcpServerName),
  );
  handlerRef.current = opts.handler;
  logRef.current = opts.log ?? noopLog;
  matchNamesRef.current = computeMatchNames(opts.name, opts.mcpServerName);

  useEffect(() => {
    if (!agent) return;

    type ActiveCall = {
      id: string;
      threadId: string;
      argsDelta: string;
      resolved: boolean;
    };
    // Tracking *every* in-flight call (not just the most recent) is
    // required for parallel tool calls. The agent legitimately emits
    // `Estimate the cost of 3 units of A100 and 2 of B200` as two
    // independent TOOL_CALL_START events; a single-slot tracker would
    // overwrite the first and strand it.
    const activeCalls = new Map<string, ActiveCall>();

    const subscription = agent.subscribe({
      onToolCallStartEvent: ({ event }) => {
        const e = event as {
          toolCallId?: string;
          toolCallName?: string;
        };
        const id = e.toolCallId ?? "";
        const name = e.toolCallName ?? "";
        const threadId = agent.threadId;
        if (!id || !threadId || !matchNamesRef.current.has(name)) {
          return;
        }
        activeCalls.set(id, { id, threadId, argsDelta: "", resolved: false });
      },
      onToolCallArgsEvent: ({ event }) => {
        const e = event as { toolCallId?: string; delta?: string };
        const id = e.toolCallId ?? "";
        const call = activeCalls.get(id);
        if (!call || call.resolved) return;
        call.argsDelta += e.delta ?? "";
        // The bridge sends raw_input as a single delta in practice,
        // but we tolerate streaming. As soon as the accumulated
        // delta parses as JSON we fire the handler.
        let parsed: unknown;
        try {
          parsed = JSON.parse(call.argsDelta);
        } catch {
          return;
        }
        call.resolved = true;
        const args = parsed as Args;
        void runHandler(id, call.threadId, args);
      },
      onToolCallEndEvent: ({ event }) => {
        const e = event as { toolCallId?: string };
        const id = e.toolCallId ?? "";
        const call = activeCalls.get(id);
        if (!call) return;
        // No-arg tool path: the agent ended the call without ever
        // streaming args. Fire the handler with `{}` so the bridge's
        // MCP request resolves instead of timing out.
        if (!call.resolved) {
          call.resolved = true;
          void runHandler(id, call.threadId, {} as Args);
        }
        activeCalls.delete(id);
      },
    });

    return () => {
      subscription.unsubscribe();
      activeCalls.clear();
    };

    async function runHandler(toolCallId: string, threadId: string, args: Args) {
      let isError = false;
      let content: string;
      try {
        const result = await Promise.resolve(handlerRef.current(args));
        content = typeof result === "string" ? result : JSON.stringify(result);
        logRef.current("[useAcpFrontendTool] resolved", {
          toolCallId,
          content,
        });
      } catch (err) {
        isError = true;
        content = err instanceof Error ? err.message : String(err);
        logRef.current("[useAcpFrontendTool] handler errored", {
          toolCallId,
          err,
        });
      }
      try {
        const res = await fetch("/api/bridge/tool-response", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ threadId, toolCallId, content, isError }),
        });
        if (!res.ok) {
          logRef.current(
            "[useAcpFrontendTool] /tool-response non-ok",
            res.status,
            await res.text(),
          );
        }
      } catch (postErr) {
        logRef.current("[useAcpFrontendTool] /tool-response failed", postErr);
      }
    }
  }, [agent]);
}

const DEFAULT_MCP_SERVER_NAME = "agui-acp-bridge";

/**
 * Names the agent might surface for our tool: the canonical short name,
 * and the variant prefixed with the bridge's MCP server name (which is
 * what opencode and many other ACP agents use when listing MCP tools to
 * their LLM).
 */
function computeMatchNames(name: string, mcpServerName?: string): Set<string> {
  const prefix = mcpServerName ?? DEFAULT_MCP_SERVER_NAME;
  return new Set([name, `${prefix}_${name}`]);
}
