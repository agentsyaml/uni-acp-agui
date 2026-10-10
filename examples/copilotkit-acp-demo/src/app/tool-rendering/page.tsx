"use client";

import { useDefaultRenderTool } from "@copilotkit/react-core/v2";
import { ChatSurface } from "@/components/persistent-chat";

/**
 * Tool-call rendering demo.
 *
 * The Rust bridge translates each ACP `ToolCall` / `ToolCallUpdate` into AG-UI
 * `TOOL_CALL_START` / `TOOL_CALL_ARGS` / `TOOL_CALL_END` events. CopilotKit
 * v2 routes any tool call without a name-specific renderer into the wildcard
 * `useDefaultRenderTool`, which lets us render *any* tool call as a custom
 * React component without registering each tool name up-front.
 */
export default function ToolRenderingPage() {
  useDefaultRenderTool({
    render: ({ name, parameters, status, result }) => (
      <ToolCallCard
        name={name}
        parameters={parameters}
        status={status}
        result={result}
      />
    ),
  });

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          ACP Tool Calls → Generative UI
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          opencode often invokes its own tools while running (
          <code>read</code> / <code>write</code> / <code>bash</code> /{" "}
          <code>glob</code> / etc.). The bridge translates each call into
          AG-UI <code>TOOL_CALL_*</code> events, and CopilotKit hands them
          off to <code>useDefaultRenderTool</code> for rendering. Try asking:
        </p>
        <ul className="text-sm leading-relaxed list-disc pl-6 text-muted space-y-1">
          <li>&ldquo;List the files in the current directory.&rdquo;</li>
          <li>&ldquo;What does README.md say?&rdquo;</li>
          <li>
            &ldquo;Run <code>echo hello</code> with bash.&rdquo;
          </li>
        </ul>
      </header>

      <div className="demo-card h-[70vh] flex flex-col p-2">
        <ChatSurface className="flex-1" />
      </div>
    </div>
  );
}

function ToolCallCard({
  name,
  parameters,
  status,
  result,
}: {
  name: string;
  parameters: unknown;
  status: "inProgress" | "executing" | "complete";
  result?: string;
}) {
  const statusConfig = {
    inProgress: {
      borderColor: "var(--accent)",
      label: "in progress",
      badgeClass: "badge-accent",
    },
    executing: {
      borderColor: "var(--warning)",
      label: "executing",
      badgeClass: "badge-warning",
    },
    complete: {
      borderColor: "var(--success)",
      label: "complete",
      badgeClass: "badge-success",
    },
  } as const;

  const cfg = statusConfig[status];

  return (
    <div
      className="my-2 rounded-lg bg-[var(--card)] p-3 border-l-4"
      style={{ borderLeftColor: cfg.borderColor }}
    >
      <div className="flex items-center gap-2 text-sm">
        <span className="font-mono font-semibold">{name}</span>
        <span className={`badge ${cfg.badgeClass}`}>{cfg.label}</span>
      </div>
      <details className="mt-2">
        <summary className="cursor-pointer text-xs text-muted select-none hover:text-[var(--foreground)] transition-colors">
          parameters
        </summary>
        <pre className="demo-pre mt-1 text-xs">
          {safeStringify(parameters)}
        </pre>
      </details>
      {result !== undefined && (
        <details className="mt-2" open>
          <summary className="cursor-pointer text-xs text-muted select-none hover:text-[var(--foreground)] transition-colors">
            result
          </summary>
          <pre className="demo-pre mt-1 text-xs">{safeStringify(result)}</pre>
        </details>
      )}
    </div>
  );
}

function safeStringify(value: unknown): string {
  try {
    if (typeof value === "string") return value;
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}
