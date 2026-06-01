"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { AgentPicker } from "./agent-picker";

const tabs = [
  { href: "/", label: "Home" },
  { href: "/chat", label: "Chat" },
  { href: "/sidebar", label: "Sidebar" },
  { href: "/frontend-tools", label: "Frontend Tools" },
  { href: "/generative-tool", label: "Generative UI" },
  { href: "/hitl-tool", label: "HITL Tool" },
  { href: "/tool-rendering", label: "Tool Rendering" },
  { href: "/raw-events", label: "AG-UI Events" },
  { href: "/approval", label: "Global Approval" },
  { href: "/health", label: "Health" },
];

export function SiteNav() {
  const pathname = usePathname();
  return (
    <nav className="site-nav px-6 py-3">
      <div className="max-w-6xl mx-auto flex flex-wrap gap-1.5 items-center">
        <Link
          href="/"
          className="font-semibold text-[var(--foreground)] mr-3 flex items-center gap-2"
        >
          <span className="inline-block w-2 h-2 rounded-full bg-[var(--accent)]" />
          CopilotKit ⇄ ACP
        </Link>
        {tabs.map((t) => {
          const active =
            t.href === "/" ? pathname === "/" : pathname?.startsWith(t.href);
          return (
            <Link
              key={t.href}
              href={t.href}
              className={`nav-tab ${active ? "active" : ""}`}
            >
              {t.label}
            </Link>
          );
        })}
        {/* Right-aligned ACP mode/model picker; sourced from
            `agent:session_init` CUSTOM events emitted by the bridge. */}
        <div className="ml-auto">
          <AgentPicker />
        </div>
      </div>
    </nav>
  );
}
