import Link from "next/link";

const demos: Array<{
  href: string;
  title: string;
  blurb: string;
  agUi: string[];
}> = [
  {
    href: "/chat",
    title: "Streaming Chat",
    blurb:
      "Plain CopilotChat — sends user messages through the bridge and renders the ACP agent's streamed reply directly.",
    agUi: ["RUN_STARTED", "TEXT_MESSAGE_*", "RUN_FINISHED"],
  },
  {
    href: "/sidebar",
    title: "Sidebar",
    blurb: "Built-in sidebar component you can mount alongside any page.",
    agUi: ["Same as Chat"],
  },
  {
    href: "/frontend-tools",
    title: "Frontend Tools (useFrontendTool) ★",
    blurb:
      "Standard AG-UI frontend-tool round-trip: browser declares the tool → bridge injects it via NewSessionRequest.mcp_servers → agent decides to call it → browser handler executes → result flows back to the LLM.",
    agUi: ["TOOL_CALL_START/ARGS/END", "POST /tool-response"],
  },
  {
    href: "/generative-tool",
    title: "Generative UI Tool",
    blurb:
      "A frontend tool that returns structured data: the browser renders it as a card and feeds the same payload back to the LLM, which keeps referring to it in later turns.",
    agUi: ["TOOL_CALL_*", "Structured JSON result"],
  },
  {
    href: "/hitl-tool",
    title: "Human-in-the-Loop Tool",
    blurb:
      "Agent invokes request_user_confirmation, which opens a modal in the browser. Approve or decline before the agent's MCP request unblocks. Per-tool HITL — independent from the global --policy interrupt flow.",
    agUi: ["TOOL_CALL_*", "Promise paused → respond/reject"],
  },
  {
    href: "/tool-rendering",
    title: "Tool-Call Rendering (Generative UI)",
    blurb:
      "Use useDefaultRenderTool to render tool calls the ACP agent makes itself (read/write/bash/...) as custom React cards.",
    agUi: ["TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END"],
  },
  {
    href: "/raw-events",
    title: "Raw AG-UI Event Stream",
    blurb:
      "useAgent + subscribe() — print every AG-UI event as it arrives so you can inspect what the bridge produces.",
    agUi: ["All event types"],
  },
  {
    href: "/approval",
    title: "Human-in-the-Loop (Bridge-Native)",
    blurb:
      "Run the bridge with --policy interrupt; every tool call the agent makes is gated. This page receives STATE_SNAPSHOT events and POSTs the user's decision to /approval.",
    agUi: ["STATE_SNAPSHOT", "POST /approval"],
  },
  {
    href: "/health",
    title: "Bridge Health Check",
    blurb:
      "GET http://127.0.0.1:8080/health to see the live count of active ACP sessions.",
    agUi: ["/health"],
  },
];

export default function HomePage() {
  return (
    <div className="space-y-10">
      <header className="space-y-4">
        <div>
          <span className="badge badge-accent mb-3">DEMO</span>
          <h1 className="text-4xl font-bold tracking-tight">
            CopilotKit ⇄ ACP Bridge
          </h1>
          <p className="text-muted mt-3 leading-relaxed max-w-3xl">
            This example shows how the Rust <code>agui-acp-bridge</code> in this
            repo can act as an AG-UI endpoint that fronts any ACP-compatible
            agent. The default backend is a local <code>opencode acp</code>{" "}
            process; swap in Claude Code, Kiro CLI, or the example agent
            shipped with acp-rust just as easily.
          </p>
        </div>

        <div className="grid md:grid-cols-2 gap-4">
          <div className="demo-card space-y-3">
            <div className="flex items-center gap-2">
              <span className="badge badge-accent">PREREQ</span>
              <h2 className="font-semibold">Start the bridge</h2>
            </div>
            <p className="text-sm text-muted leading-relaxed">
              In a separate terminal at the repo root, run one of:
            </p>
            <pre className="demo-pre">
{`# opencode as the ACP agent
cargo run -p agui-acp-bridge-cli -- opencode acp

# echo mode (no LLM / no network needed)
cargo run -p agui-acp-bridge-cli -- --in-process

# HITL demo (gates every tool call)
cargo run -p agui-acp-bridge-cli -- --policy interrupt -- opencode acp`}
            </pre>
            <p className="text-xs text-muted leading-relaxed">
              The bridge listens on <code>0.0.0.0:8080</code>. This Next.js app
              proxies through <code>/api/copilotkit</code>; the browser never
              talks to port 8080 directly. Before running{" "}
              <code>opencode acp</code> for the first time, run{" "}
              <code>opencode auth login</code> and configure at least one model
              provider.
            </p>
          </div>

          <div className="demo-card space-y-3">
            <div className="flex items-center gap-2">
              <span className="badge badge-accent">HOW</span>
              <h2 className="font-semibold">Implementation</h2>
            </div>
            <p className="text-sm text-muted leading-relaxed">
              <code>useFrontendTool</code> works through the standard ACP path:
              the bridge gives the agent a session-scoped, in-process MCP HTTP
              endpoint via <code>NewSessionRequest.mcp_servers</code>, and the
              agent discovers tools through plain MCP.
            </p>
            <p className="text-sm text-muted leading-relaxed">
              <strong className="text-[var(--foreground)]">
                No global MCP config is touched
              </strong>{" "}
              and nothing depends on opencode-specific behavior — any ACP
              agent advertising <code>mcpCapabilities.http=true</code> works.
            </p>
          </div>
        </div>
      </header>

      <section>
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-lg font-semibold">Demos</h2>
          <span className="text-xs text-muted">{demos.length} pages</span>
        </div>
        <div className="grid sm:grid-cols-2 gap-4">
          {demos.map((d) => (
            <Link key={d.href} href={d.href} className="demo-card-link">
              <div className="flex items-start justify-between gap-2 mb-2">
                <h3 className="font-semibold leading-tight">{d.title}</h3>
                <svg
                  className="text-muted shrink-0 mt-0.5"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M5 12h14M12 5l7 7-7 7" />
                </svg>
              </div>
              <p className="text-sm text-muted leading-relaxed mb-3">
                {d.blurb}
              </p>
              <div className="text-xs text-muted font-mono pt-3 border-t border-[var(--border)]">
                AG-UI: {d.agUi.join(" · ")}
              </div>
            </Link>
          ))}
        </div>
      </section>
    </div>
  );
}
