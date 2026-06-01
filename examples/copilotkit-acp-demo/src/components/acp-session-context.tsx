"use client";

/**
 * ACP session-mode / model context.
 *
 * The bridge surfaces the agent's `SessionModeState` and (when the
 * `unstable_session_model` feature is on) `SessionModelState` via:
 *
 * 1. A `CUSTOM` event named `agent:session_init` emitted at the start of
 *    every prompt's SSE stream. Payload shape:
 *    `{ modes?: SessionModesInit, models?: SessionModelsInit }`.
 * 2. A synchronous `GET /session/init?threadId=...` discovery endpoint
 *    that returns the same shape (404 when no session has been opened
 *    for that thread).
 *
 * This provider listens to (1) so picker UIs in any descendant
 * component automatically reflect the agent's offering. It does *not*
 * call (2) automatically — discovery on first render races against
 * session creation and would usually 404. Users that need to render a
 * picker before the first prompt can call `refresh()` after an explicit
 * "open session" action.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useAgent } from "@copilotkit/react-core/v2";
import type { BaseEvent } from "@ag-ui/client";

export interface ModeOffering {
  id: string;
  name: string;
  description?: string;
}

export interface ModelOffering {
  id: string;
  name: string;
  description?: string;
}

export interface SessionModesInit {
  currentModeId: string;
  availableModes: ModeOffering[];
}

export interface SessionModelsInit {
  currentModelId: string;
  availableModels: ModelOffering[];
}

export interface AcpSessionState {
  /** Current ACP `SessionModeState` (or `null` when the agent does not advertise modes). */
  modes: SessionModesInit | null;
  /** Current ACP `SessionModelState` (or `null` when not advertised / unstable feature off). */
  models: SessionModelsInit | null;
  /** Last error from a set-mode / set-model attempt (cleared on next success). */
  error: string | null;
  /** Indicates an in-flight set-mode / set-model request. */
  pending: boolean;
  /** Refresh from `GET /session/init`. Returns `true` on 200, `false` otherwise. */
  refresh: () => Promise<boolean>;
  /** Issue ACP `session/set_mode` via the bridge. */
  setMode: (modeId: string) => Promise<boolean>;
  /** Issue ACP `session/set_model` via the bridge. Only meaningful when `models` is non-null. */
  setModel: (modelId: string) => Promise<boolean>;
}

const Ctx = createContext<AcpSessionState | null>(null);

/**
 * Client-side timeout for set-mode / set-model / refresh requests. The
 * bridge enforces its own timeout (`set_session_timeout`, default 30s) so
 * keep this slightly larger to let the bridge respond first; 35s strikes
 * the balance.
 */
const FETCH_TIMEOUT_MS = 35_000;

interface SessionInitPayload {
  modes?: SessionModesInit | null;
  models?: SessionModelsInit | null;
}

export function AcpSessionProvider({ children }: { children: ReactNode }) {
  const { agent } = useAgent();
  const [modes, setModes] = useState<SessionModesInit | null>(null);
  const [models, setModels] = useState<SessionModelsInit | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  // We track threadId in a ref because subscribe()'s callbacks fire over
  // the lifetime of the agent and we want to read the latest value
  // without re-subscribing on every render.
  const agentRef = useRef(agent);
  useEffect(() => {
    agentRef.current = agent;
  }, [agent]);

  // Listen for agent:session_init custom events.
  useEffect(() => {
    if (!agent) return;
    const subscription = agent.subscribe({
      onEvent: ({ event }: { event: BaseEvent }) => {
        const e = event as {
          type?: string;
          name?: string;
          value?: SessionInitPayload;
        };
        if (e.type !== "CUSTOM" || e.name !== "agent:session_init") return;
        const payload = e.value ?? {};
        setModes(payload.modes ?? null);
        setModels(payload.models ?? null);
        setError(null);
      },
    });
    return () => subscription.unsubscribe();
  }, [agent]);

  const refresh = useCallback(async (): Promise<boolean> => {
    const threadId = agentRef.current?.threadId;
    if (!threadId) return false;
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch(
        `/api/bridge/session/init?threadId=${encodeURIComponent(threadId)}`,
        { cache: "no-store", signal: ctl.signal },
      );
      if (!res.ok) return false;
      const json = (await res.json()) as SessionInitPayload;
      setModes(json.modes ?? null);
      setModels(json.models ?? null);
      setError(null);
      return true;
    } catch (err) {
      setError(String(err));
      return false;
    } finally {
      clearTimeout(timer);
    }
  }, []);

  const setMode = useCallback(async (modeId: string): Promise<boolean> => {
    const threadId = agentRef.current?.threadId;
    if (!threadId) {
      setError("no active agent thread; send a message first");
      return false;
    }
    setPending(true);
    setError(null);
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch("/api/bridge/session/set-mode", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ threadId, modeId }),
        signal: ctl.signal,
      });
      if (!res.ok) {
        const body = await res.text();
        setError(`set-mode HTTP ${res.status}: ${body || "(no body)"}`);
        return false;
      }
      setModes((prev) =>
        prev ? { ...prev, currentModeId: modeId } : prev,
      );
      return true;
    } catch (err) {
      setError(String(err));
      return false;
    } finally {
      clearTimeout(timer);
      setPending(false);
    }
  }, []);

  const setModel = useCallback(async (modelId: string): Promise<boolean> => {
    const threadId = agentRef.current?.threadId;
    if (!threadId) {
      setError("no active agent thread; send a message first");
      return false;
    }
    setPending(true);
    setError(null);
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
    try {
      const res = await fetch("/api/bridge/session/set-model", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ threadId, modelId }),
        signal: ctl.signal,
      });
      if (!res.ok) {
        const body = await res.text();
        setError(`set-model HTTP ${res.status}: ${body || "(no body)"}`);
        return false;
      }
      setModels((prev) =>
        prev ? { ...prev, currentModelId: modelId } : prev,
      );
      return true;
    } catch (err) {
      setError(String(err));
      return false;
    } finally {
      clearTimeout(timer);
      setPending(false);
    }
  }, []);

  const value = useMemo<AcpSessionState>(
    () => ({ modes, models, error, pending, refresh, setMode, setModel }),
    [modes, models, error, pending, refresh, setMode, setModel],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAcpSession(): AcpSessionState {
  const value = useContext(Ctx);
  if (!value) {
    throw new Error(
      "useAcpSession must be used inside <AcpSessionProvider>; ensure layout.tsx wraps children in it",
    );
  }
  return value;
}
