import type { Metadata } from "next";
import "@copilotkit/react-ui/v2/styles.css";
import "./globals.css";
import { SiteNav } from "@/components/site-nav";
import { AcpSessionProvider } from "@/components/acp-session-context";
import { CopilotProvider } from "@/components/copilot-provider";

export const metadata: Metadata = {
  title: "CopilotKit ⇄ ACP Bridge Demo",
  description:
    "Demonstrates the agui-acp-bridge connecting AG-UI / CopilotKit to a local ACP agent (opencode acp).",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        {/*
          The CopilotRuntime route at /api/copilotkit registers our
          AG-UI HttpAgent (pointing at the Rust bridge) under the name
          "default", so v2 components like <CopilotChat>, <CopilotSidebar>
          and the useAgent() hook pick it up automatically.

          AcpSessionProvider sits *inside* CopilotKit (so it can call
          useAgent()) but *outside* every page (so the picker, mode/model
          state, and the set-mode/set-model RPC helpers are shared
          globally — the active mode/model is one-per-thread, and
          changing tab should not reset it).
        */}
        <CopilotProvider>
          <AcpSessionProvider>
            <div className="min-h-screen flex flex-col">
              <SiteNav />
              <main className="flex-1 max-w-6xl w-full mx-auto px-6 py-10">
                {children}
              </main>
              <footer className="border-t border-[var(--border)] py-5 px-6 text-xs text-center text-muted">
                <span>
                  AG-UI ⇄ ACP — frontend talks to{" "}
                  <code>/api/copilotkit</code> →{" "}
                  <code>CopilotRuntime</code> →{" "}
                  <code>HttpAgent</code> →{" "}
                  <code>agui-acp-bridge</code> →{" "}
                  <code>opencode acp</code>
                </span>
              </footer>
            </div>
          </AcpSessionProvider>
        </CopilotProvider>
      </body>
    </html>
  );
}
