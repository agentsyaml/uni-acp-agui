"use client";

import { useAgent } from "@copilotkit/react-core/v2";
import { useEffect, useRef } from "react";
import { useConversations } from "@/components/copilot-provider";

type ResumeAgent = {
  runAgent: (input: { forwardedProps: { acpResume: { sessionId: string } } }) => Promise<unknown>;
};

export function scheduleAcpResume(
  agent: ResumeAgent | null | undefined,
  isReady: boolean,
  resumeToken: number,
  resumeSessionId: string | null,
  resumeError: string | null,
  lastHandledToken: { current: number },
  reportResumeFailure: (message: string) => void,
  timer = { setTimeout, clearTimeout },
) {
  if (
    !isReady || !agent || resumeToken === 0 ||
    resumeToken === lastHandledToken.current || !resumeSessionId || resumeError
  ) return;

  const id = timer.setTimeout(() => {
    lastHandledToken.current = resumeToken;
    void agent
      .runAgent({ forwardedProps: { acpResume: { sessionId: resumeSessionId } } })
      .catch((err: unknown) => {
        console.error("[useAcpResume] resume run failed", err);
        reportResumeFailure(err instanceof Error ? err.message : String(err));
      });
  }, 0);
  return () => timer.clearTimeout(id);
}

/**
 * Drives an **explicit resume run** when the user opens a past conversation.
 *
 * Why this exists: CopilotKit's built-in thread restore goes through
 * `agent.connect()`, which for a self-hosted runtime-proxied agent never
 * reaches our bridge (it throws `AGUIConnectNotImplementedError`, swallowed
 * by CopilotKit, or targets cloud-only `/threads` endpoints — hence the
 * harmless `GET /api/copilotkit/threads 404`). So a click produced no network
 * call to the bridge and the panel stayed blank.
 *
 * Instead we trigger the resume ourselves: keep the selected ACP SessionId
 * separate from the AG-UI threadId and call `agent.runAgent()` with no new
 * user message plus the typed `acpResume` forwarded prop. That is a real
 * `POST /` the bridge turns into a private `session/load` run → the
 * agent replays the conversation history as AG-UI `TEXT_MESSAGE_*` events,
 * which `runAgent` applies to `agent.messages`, and `<CopilotChat>` renders.
 *
 * Mounted with the persistent CopilotChat controller so route changes do
 * not drop the resume listener. Keyed on `resumeToken` so reopening a
 * conversation re-runs the explicit resume.
 */
export function useAcpResume() {
  const { agent, isReady } = useAgent();
  const {
    resumeSessionId,
    threadId,
    resumeToken,
    resumeError,
    reportResumeFailure,
  } = useConversations();
  const lastHandledToken = useRef<number>(0);

  useEffect(() => {
    // The persistent `<CopilotChat threadId={threadId}>` binds the new agent
    // thread independently of route surfaces. Defer the resume run until
    // after that thread binding and message reset have committed. The bridge
    // only issues session/load when this marker is present.
    //
    // We do NOT mutate the agent object directly (threadId/messages) — the
    // React Compiler treats it as immutable, and CopilotChat owns that state.
    // Don't retry automatically after a failure: the stale session id has
    // already been cleared, so refiring would just error again on reload.
    return scheduleAcpResume(
      agent, isReady, resumeToken, resumeSessionId, resumeError,
      lastHandledToken, reportResumeFailure,
    );
  }, [
    agent,
    isReady,
    resumeSessionId,
    threadId,
    resumeToken,
    resumeError,
    reportResumeFailure,
  ]);
}
