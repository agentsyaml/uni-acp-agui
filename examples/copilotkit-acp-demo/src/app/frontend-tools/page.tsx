"use client";

import { CopilotChat } from "@copilotkit/react-core/v2";
import { useState } from "react";
import { z } from "zod";
import { useAcpFrontendTool } from "@/hooks/use-acp-frontend-tool";

/**
 * `useFrontendTool` end-to-end demo.
 *
 * The flow:
 *
 *  1. `useAcpFrontendTool` registers each tool with CopilotKit
 *     internally, so it ends up in `RunAgentInput.tools`.
 *  2. The bridge sees that list, mounts an in-process MCP HTTP endpoint
 *     scoped to the thread, and points the ACP session at it via
 *     `NewSessionRequest.mcp_servers`.
 *  3. opencode (or any ACP agent advertising `mcpCapabilities.http`)
 *     opens an MCP connection back to the bridge. Its LLM sees the
 *     tool and can call it.
 *  4. When the agent calls the tool, the bridge translates the MCP
 *     request into AG-UI `TOOL_CALL_*` events on the live SSE stream
 *     AND parks the MCP request on a oneshot.
 *  5. `useAcpFrontendTool` observes the tool-call events, runs our
 *     local handler, and POSTs the result via
 *     `/api/bridge/tool-response`. The bridge resolves the oneshot →
 *     returns the value to the agent's MCP call → agent's LLM
 *     continues generating.
 */
export default function FrontendToolsPage() {
  const [calls, setCalls] = useState<
    Array<{ ts: string; name: string; arg: string; result: string }>
  >([]);

  const log = (entry: { name: string; arg: string; result: string }) =>
    setCalls((prev) =>
      [
        {
          ts: new Date().toLocaleTimeString(),
          ...entry,
        },
        ...prev,
      ].slice(0, 50),
    );

  useAcpFrontendTool({
    name: "say_hello",
    description: "Greet someone by name. Returns a friendly text greeting.",
    parameters: z.object({
      name: z.string().describe("name of the person to greet"),
    }),
    handler: ({ name }) => {
      const greeting = `hi ${name}, this greeting was produced inside your browser`;
      log({
        name: "say_hello",
        arg: JSON.stringify({ name }),
        result: greeting,
      });
      return greeting;
    },
  });

  useAcpFrontendTool({
    name: "current_time",
    description: "Returns the user's local wall-clock time.",
    parameters: z.object({}),
    handler: () => {
      const now = new Date().toLocaleString();
      log({ name: "current_time", arg: "{}", result: now });
      return now;
    },
  });

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Frontend Tools (useFrontendTool)
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          The full AG-UI frontend-tool round-trip: the browser declares the
          tool → the bridge injects it via the standard ACP{" "}
          <code>NewSessionRequest.mcp_servers</code> → opencode&rsquo;s LLM
          decides to call it → the bridge surfaces the call as AG-UI events
          here → your handler runs → the result flows back to opencode so the
          LLM can keep talking.
        </p>
        <p className="text-muted text-sm leading-relaxed">
          Try: <em>&ldquo;Call say_hello with name=alex&rdquo;</em>, or{" "}
          <em>&ldquo;Use current_time to tell me what time it is&rdquo;</em>.
        </p>
      </header>

      <div className="grid lg:grid-cols-2 gap-4">
        <div className="demo-card h-[70vh] flex flex-col p-2">
          <CopilotChat className="flex-1" />
        </div>

        <div className="demo-card h-[70vh] flex flex-col">
          <h2 className="font-semibold mb-3 flex items-center justify-between">
            <span>Local tool-call log</span>
            <span className="badge badge-accent">{calls.length}</span>
          </h2>
          <div className="flex-1 overflow-auto space-y-2 text-xs">
            {calls.length === 0 && (
              <div className="h-full flex items-center justify-center">
                <p className="text-muted text-center">
                  No calls yet.<br />
                  Ask the agent to invoke <code>say_hello</code>.
                </p>
              </div>
            )}
            {calls.map((c, i) => (
              <div
                key={i}
                className="rounded-lg border border-[var(--border)] bg-[var(--background)] p-2.5"
              >
                <div className="flex items-center justify-between mb-1.5">
                  <span className="badge badge-success font-semibold">
                    {c.name}
                  </span>
                  <span className="text-muted font-mono">{c.ts}</span>
                </div>
                <div className="space-y-1">
                  <div className="flex gap-1.5">
                    <span className="text-muted shrink-0 w-12">arg:</span>
                    <span className="font-mono break-all">{c.arg}</span>
                  </div>
                  <div className="flex gap-1.5">
                    <span className="text-muted shrink-0 w-12">result:</span>
                    <span className="font-mono break-all">{c.result}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
