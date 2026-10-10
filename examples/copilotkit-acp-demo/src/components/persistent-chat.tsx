"use client";

import {
  CopilotChat,
  CopilotChatView,
  CopilotSidebarView,
  type CopilotChatProps,
  type CopilotChatViewProps,
} from "@copilotkit/react-core/v2";
import { createPortal } from "react-dom";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useConversations } from "@/components/copilot-provider";
import { useAcpResume } from "@/hooks/use-acp-resume";

type Surface = {
  container: HTMLDivElement;
  className?: string;
  labels?: CopilotChatProps["labels"];
  sidebar: boolean;
};
type SurfaceContextValue = {
  surface: Surface | null;
  attach: (surface: Surface) => () => void;
};
const SurfaceContext = createContext<SurfaceContextValue | null>(null);

export function ChatSurface({
  className,
  welcomeMessageText,
  sidebar = false,
}: {
  className?: string;
  welcomeMessageText?: string;
  sidebar?: boolean;
}) {
  const context = useContext(SurfaceContext);
  if (!context) throw new Error("ChatSurface must be under CopilotProvider");
  const [container, setContainer] = useState<HTMLDivElement | null>(null);
  const attach = context.attach;
  const labels = useMemo(
    () => (welcomeMessageText ? { welcomeMessageText } : undefined),
    [welcomeMessageText],
  );
  useEffect(() => {
    if (!container) return;
    return attach({ container, className, labels, sidebar });
  }, [attach, container, className, labels, sidebar]);
  return <div ref={setContainer} style={{ display: "contents" }} />;
}

function ChatViewSlot(props: CopilotChatViewProps) {
  const surface = useContext(SurfaceContext)?.surface;
  if (!surface) return null;
  const view = surface.sidebar ? (
    <CopilotSidebarView {...props} className={surface.className} defaultOpen={false} />
  ) : (
    <CopilotChatView {...props} className={surface.className} />
  );
  return createPortal(view, surface.container);
}

const PersistentChatView = Object.assign(ChatViewSlot, CopilotChatView);

function ChatController() {
  const { threadId } = useConversations();
  const surface = useContext(SurfaceContext)?.surface;
  useAcpResume();
  return (
    <CopilotChat
      threadId={threadId}
      chatView={PersistentChatView}
      labels={surface?.labels}
    />
  );
}

export function PersistentChatHost({ children }: { children: ReactNode }) {
  const [surface, setSurface] = useState<Surface | null>(null);
  const attach = useCallback((next: Surface) => {
    setSurface(next);
    return () => {
      setSurface((current) => (current === next ? null : current));
    };
  }, []);
  const value = { surface, attach };
  return (
    <SurfaceContext.Provider value={value}>
      {children}
      <ChatController key="persistent-chat-controller" />
    </SurfaceContext.Provider>
  );
}
