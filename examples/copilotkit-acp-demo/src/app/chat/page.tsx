"use client";

import { CopilotChat } from "@copilotkit/react-core/v2";
import { ConversationHistory } from "@/components/conversation-history";
import { useConversations } from "@/components/copilot-provider";
import { useAcpResume } from "@/hooks/use-acp-resume";

export default function ChatPage() {
  // The active thread comes from the conversation switcher. Passing it as an
  // explicit `threadId` makes CopilotChat treat the thread as caller-managed.
  const { threadId } = useConversations();

  // Drive an explicit `session/load` resume run when a past conversation is
  // opened. CopilotKit's own connect path does not reach a self-hosted
  // bridge, so we trigger the resume ourselves (see the hook docs).
  useAcpResume();

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

      <div className="demo-card h-[70vh] flex overflow-hidden p-0">
        <ConversationHistory />
        <div className="flex-1 flex flex-col p-2">
          <CopilotChat
            threadId={threadId}
            className="flex-1"
            labels={{
              welcomeMessageText:
                "Hi, I'm opencode wired through the agui-acp-bridge. Ask me anything.",
            }}
          />
        </div>
      </div>
    </div>
  );
}
