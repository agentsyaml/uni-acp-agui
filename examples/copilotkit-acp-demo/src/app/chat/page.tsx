"use client";

import { useAgent } from "@copilotkit/react-core/v2";
import { useEffect, useState } from "react";
import { ConversationHistory } from "@/components/conversation-history";
import { useConversations } from "@/components/copilot-provider";
import { ChatSurface } from "@/components/persistent-chat";
import { friendlyRunErrorMessage } from "@/lib/agui-run-errors";

export default function ChatPage() {
  // The active AG-UI thread comes from the conversation switcher. It remains
  // distinct from the ACP SessionId used by an explicit resume.
  const { resumeError, dismissResumeError } = useConversations();
  const { agent } = useAgent();
  const [runError, setRunError] = useState<string | null>(null);

  useEffect(() => {
    if (!agent) return;
    const subscription = agent.subscribe({
      onRunErrorEvent: ({ event }) => {
        // Map known codes to friendly text; unknown codes fall back to the
        // bridge's own message.
        setRunError(
          friendlyRunErrorMessage(event.code) ||
            event.message?.trim() ||
            "The run failed",
        );
      },
      onRunStartedEvent: () => setRunError(null),
    });
    return () => subscription.unsubscribe();
  }, [agent]);

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Streaming Chat</h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          Multi-conversation chat. The sidebar lists conversations the ACP
          agent has persisted (via <code>GET /sessions</code> →{" "}
          <code>session/list</code>). Selecting one resumes it through{" "}
          <code>session/load</code>; the bridge keeps no history of its own.
        </p>
      </header>

      {runError && (
        <div
          role="alert"
          className="demo-card border border-red-500/40 px-4 py-3 text-sm text-red-500"
        >
          {runError}
        </div>
      )}

      {resumeError && (
        <div
          role="alert"
          className="demo-card flex items-center justify-between gap-3 border border-red-500/40 px-4 py-3 text-sm text-red-500"
        >
          <span>Could not resume conversation: {resumeError}</span>
          <button
            type="button"
            onClick={dismissResumeError}
            className="text-xs px-2 py-1 rounded border border-red-500/40 hover:bg-red-500/10 shrink-0"
          >
            Dismiss
          </button>
        </div>
      )}

      <div className="demo-card h-[70vh] flex overflow-hidden p-0">
        <ConversationHistory />
        <div className="flex-1 flex flex-col p-2">
          <ChatSurface
            className="flex-1"
            welcomeMessageText="Hi, I'm opencode wired through the agui-acp-bridge. Ask me anything."
          />
        </div>
      </div>
    </div>
  );
}
