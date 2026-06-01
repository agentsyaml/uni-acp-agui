"use client";

import { CopilotKit } from "@copilotkit/react-core/v2";
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";

const THREAD_STORAGE_KEY = "agui-acp-demo.threadId";

function freshId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `t-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

/** Read (or lazily create) the persisted active thread id. Browser-only. */
function loadOrCreateThreadId(): string {
  try {
    const existing = window.localStorage.getItem(THREAD_STORAGE_KEY);
    if (existing) return existing;
    const created = freshId();
    window.localStorage.setItem(THREAD_STORAGE_KEY, created);
    return created;
  } catch {
    return freshId();
  }
}

function persistThreadId(id: string) {
  try {
    window.localStorage.setItem(THREAD_STORAGE_KEY, id);
  } catch {
    /* ignore */
  }
}

interface ConversationsContextValue {
  /** The active AG-UI threadId (== ACP SessionId for resumed conversations). */
  threadId: string;
  /**
   * Increments every time the user explicitly opens a past conversation.
   * The chat surface watches this to drive an explicit resume run (see
   * `useAcpResume`) — CopilotKit's own `connect` path does not reach a
   * self-hosted bridge, so we trigger the `session/load` ourselves.
   */
  resumeToken: number;
  /** Start a brand-new conversation (fresh threadId). */
  newConversation: () => void;
  /** Switch to an existing conversation by its ACP SessionId (triggers resume). */
  openConversation: (sessionId: string) => void;
}

const ConversationsContext = createContext<ConversationsContextValue | null>(
  null,
);

/** Access conversation switching from anywhere under {@link CopilotProvider}. */
export function useConversations(): ConversationsContextValue {
  const ctx = useContext(ConversationsContext);
  if (!ctx) {
    throw new Error("useConversations must be used within <CopilotProvider>");
  }
  return ctx;
}

/**
 * Pins a **stable, switchable** `threadId` on the CopilotKit provider and
 * exposes conversation-management actions to descendants.
 *
 * Why the stable threadId matters: `@ag-ui/client`'s `HttpAgent` defaults
 * `threadId` to a fresh `randomUUID()` when none is supplied, and CopilotKit
 * does not persist it. Without pinning, every reload starts a new ACP session
 * on the bridge → a new agent subprocess that lingers until the idle reaper.
 *
 * Resume: opening a past conversation sets the active threadId to that
 * conversation's ACP SessionId. When CopilotKit then connects (a bootstrap
 * with no user prompt), the bridge sees a thread it has no live session for
 * and issues `session/load`, replaying the conversation history. Detection is
 * purely protocol-shape based on the bridge side, so no client flag is needed.
 */
export function CopilotProvider({ children }: { children: ReactNode }) {
  // `useState` initializer runs once per mount. On the server it returns a
  // placeholder; the first client render re-runs it (reading localStorage).
  // CopilotKit tolerates the brief difference — no run fires during first paint.
  const [threadId, setThreadId] = useState<string>(() => {
    if (typeof window === "undefined") return "ssr-placeholder";
    return loadOrCreateThreadId();
  });
  const [resumeToken, setResumeToken] = useState(0);

  const newConversation = useCallback(() => {
    const id = freshId();
    persistThreadId(id);
    setThreadId(id);
  }, []);

  const openConversation = useCallback((sessionId: string) => {
    persistThreadId(sessionId);
    setThreadId(sessionId);
    // Bump the resume token so the chat surface re-runs the explicit resume
    // even if the same conversation is re-opened.
    setResumeToken((n) => n + 1);
  }, []);

  const ctx = useMemo<ConversationsContextValue>(
    () => ({ threadId, resumeToken, newConversation, openConversation }),
    [threadId, resumeToken, newConversation, openConversation],
  );

  return (
    <CopilotKit runtimeUrl="/api/copilotkit" threadId={threadId}>
      <ConversationsContext.Provider value={ctx}>
        {children}
      </ConversationsContext.Provider>
    </CopilotKit>
  );
}
