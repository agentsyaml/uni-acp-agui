"use client";

import { CopilotSidebar } from "@copilotkit/react-core/v2";

export default function SidebarPage() {
  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Sidebar Mode</h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          The built-in <code>CopilotSidebar</code> component is meant to live
          next to your main app — it does not replace page content and
          automatically connects to the <code>default</code> agent (our ACP
          bridge).
        </p>
      </header>

      <article className="demo-card space-y-3">
        <h2 className="text-lg font-semibold">Placeholder content</h2>
        <p className="text-sm leading-relaxed text-muted">
          Click the floating button in the bottom-right corner (or the
          toolbar icon on the right) to open the Copilot sidebar. Try asking:
        </p>
        <ul className="text-sm leading-relaxed text-muted list-disc pl-6 space-y-1">
          <li>&ldquo;Summarize the directory structure of this project.&rdquo;</li>
          <li>&ldquo;Which tools does opencode currently support?&rdquo;</li>
          <li>&ldquo;Write me a TypeScript debounce implementation.&rdquo;</li>
        </ul>
        <p className="text-sm leading-relaxed text-muted">
          Replies stream in. Note that the ACP agent itself may invoke file
          read, shell, and similar tools — the bridge will auto-approve,
          deny, or hand off for review depending on its <code>--policy</code>{" "}
          flag (see the README).
        </p>
      </article>

      <CopilotSidebar defaultOpen={false} />
    </div>
  );
}
