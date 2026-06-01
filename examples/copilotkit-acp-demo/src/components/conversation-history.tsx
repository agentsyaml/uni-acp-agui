"use client";

import { useCallback, useEffect, useState } from "react";
import { useConversations } from "@/components/copilot-provider";

interface SessionSummary {
  sessionId: string;
  cwd: string;
  title?: string;
  updatedAt?: string;
}

type Fetch =
  | { kind: "loading" }
  | { kind: "ok"; sessions: SessionSummary[] }
  | { kind: "unsupported" }
  | { kind: "error"; message: string };

/**
 * Conversation-history sidebar backed by the bridge's `GET /sessions`
 * (ACP `session/list`). Selecting an entry resumes it via `session/load`;
 * "New chat" starts a fresh conversation.
 *
 * The bridge stores no history — this list reflects exactly what the ACP
 * agent persists, so it survives reloads and is shared across clients of the
 * same agent.
 */
export function ConversationHistory() {
  const { threadId, newConversation, openConversation } = useConversations();
  const [state, setState] = useState<Fetch>({ kind: "loading" });
  const [reloadKey, setReloadKey] = useState(0);

  /** Trigger a re-fetch (used by the Refresh button). */
  const refresh = useCallback(() => setReloadKey((k) => k + 1), []);

  // Fetch the session list on mount, whenever the active thread changes (a
  // new turn may have created/renamed a session), and on manual refresh.
  // State is only set from the async continuation / abort callback, never
  // synchronously in the effect body.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch("/api/bridge/sessions", { cache: "no-store" });
        if (cancelled) return;
        if (res.status === 501) {
          setState({ kind: "unsupported" });
          return;
        }
        if (!res.ok) {
          setState({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const json = (await res.json()) as { sessions?: SessionSummary[] };
        if (cancelled) return;
        setState({ kind: "ok", sessions: json.sessions ?? [] });
      } catch (err) {
        if (!cancelled) setState({ kind: "error", message: String(err) });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [threadId, reloadKey]);

  return (
    <aside className="w-64 shrink-0 border-r border-[var(--border)] flex flex-col">
      <div className="p-3 border-b border-[var(--border)] flex items-center justify-between gap-2">
        <span className="text-sm font-semibold">Conversations</span>
        <button
          type="button"
          onClick={newConversation}
          className="text-xs px-2 py-1 rounded bg-[var(--accent)] text-white hover:opacity-90"
        >
          + New
        </button>
      </div>

      <div className="flex-1 overflow-y-auto">
        {state.kind === "loading" && (
          <p className="p-3 text-xs text-muted">Loading…</p>
        )}

        {state.kind === "unsupported" && (
          <p className="p-3 text-xs text-muted leading-relaxed">
            This agent doesn&apos;t support <code>session/list</code>, so
            history isn&apos;t available. New chats still work.
          </p>
        )}

        {state.kind === "error" && (
          <p className="p-3 text-xs text-red-500">
            Failed to load history: {state.message}
          </p>
        )}

        {state.kind === "ok" && state.sessions.length === 0 && (
          <p className="p-3 text-xs text-muted">
            No saved conversations yet. Send a message to start one.
          </p>
        )}

        {state.kind === "ok" &&
          state.sessions.map((s) => {
            const active = s.sessionId === threadId;
            const label = s.title?.trim() || s.sessionId.slice(0, 8);
            return (
              <button
                key={s.sessionId}
                type="button"
                onClick={() => openConversation(s.sessionId)}
                className={`w-full text-left px-3 py-2 text-sm border-b border-[var(--border)] hover:bg-[var(--accent)]/10 ${
                  active ? "bg-[var(--accent)]/15 font-medium" : ""
                }`}
                title={s.sessionId}
              >
                <div className="truncate">{label}</div>
                {s.updatedAt && (
                  <div className="text-[10px] text-muted truncate">
                    {s.updatedAt}
                  </div>
                )}
              </button>
            );
          })}
      </div>

      <div className="p-2 border-t border-[var(--border)]">
        <button
          type="button"
          onClick={refresh}
          className="w-full text-xs px-2 py-1 rounded border border-[var(--border)] hover:bg-[var(--accent)]/10"
        >
          Refresh
        </button>
      </div>
    </aside>
  );
}
