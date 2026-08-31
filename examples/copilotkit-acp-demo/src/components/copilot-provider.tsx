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
const RESUME_SESSION_STORAGE_KEY = "agui-acp-demo.resumeSessionId";

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

function loadResumeSessionId(): string | null {
  try {
    return window.localStorage.getItem(RESUME_SESSION_STORAGE_KEY);
  } catch {
    return null;
  }
}

function persistResumeSessionId(id: string | null) {
  try {
    if (id) window.localStorage.setItem(RESUME_SESSION_STORAGE_KEY, id);
    else window.localStorage.removeItem(RESUME_SESSION_STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

interface ConversationsContextValue {
  /** The active AG-UI threadId, distinct from any ACP SessionId. */
  threadId: string;
  /** ACP SessionId to load for the current explicit resume, if any. */
  resumeSessionId: string | null;
  /**
   * Increments every time the user explicitly opens a past conversation.
   * The chat surface watches this to drive an explicit resume run (see
   * `useAcpResume`) — CopilotKit's own `connect` path does not reach a
   * self-hosted bridge, so `useAcpResume` triggers an explicit private
   * `session/load` run itself.
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
 * Resume: opening a past conversation creates a fresh AG-UI threadId and
 * keeps the selected ACP SessionId separate. `useAcpResume` sends the typed
 * sessionId marker; ordinary runs never infer ACP identity from threadId.
 */
export function CopilotProvider({ children }: { children: ReactNode }) {
  // `useState` initializer runs once per mount. On the server it returns a
  // placeholder; the first client render re-runs it (reading localStorage).
  // CopilotKit tolerates the brief difference — no run fires during first paint.
  const [threadId, setThreadId] = useState<string>(() => {
    if (typeof window === "undefined") return "ssr-placeholder";
    return loadOrCreateThreadId();
  });
  const [resumeSessionId, setResumeSessionId] = useState<string | null>(() => {
    if (typeof window === "undefined") return null;
    return loadResumeSessionId();
  });
  const [resumeToken, setResumeToken] = useState(() =>
    resumeSessionId ? 1 : 0,
  );

  const newConversation = useCallback(() => {
    const id = freshId();
    persistThreadId(id);
    persistResumeSessionId(null);
    setThreadId(id);
    setResumeSessionId(null);
  }, []);

  const openConversation = useCallback((sessionId: string) => {
    const id = freshId();
    persistThreadId(id);
    persistResumeSessionId(sessionId);
    setThreadId(id);
    setResumeSessionId(sessionId);
    // Bump the resume token so the chat surface re-runs the explicit resume
    // even if the same conversation is re-opened.
    setResumeToken((n) => n + 1);
  }, []);

  const ctx = useMemo<ConversationsContextValue>(
    () => ({
      threadId,
      resumeSessionId,
      resumeToken,
      newConversation,
      openConversation,
    }),
    [
      threadId,
      resumeSessionId,
      resumeToken,
      newConversation,
      openConversation,
    ],
  );

  return (
    <CopilotKit runtimeUrl="/api/copilotkit" threadId={threadId}>
      <ConversationsContext.Provider value={ctx}>
        {children}
      </ConversationsContext.Provider>
    </CopilotKit>
  );
}
