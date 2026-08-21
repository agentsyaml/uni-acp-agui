"use client";

/**
 * ACP session configuration context.
 *
 * The bridge surfaces the agent's stable ACP 2.0 `configOptions`, plus the
 * legacy mode/model mirrors, via:
 *
 * 1. A `CUSTOM` event named `agent:session_init` emitted at the start of
 *    every prompt's SSE stream. Payload shape:
 *    `{ modes?: SessionModesInit, models?: SessionModelsInit,
 *       configOptions?: SessionConfigOption[] }`.
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

export interface SessionConfigSelectOption {
  value: string;
  name: string;
  description?: string;
}

export interface SessionConfigSelectGroup {
  group: string;
  name: string;
  options: SessionConfigSelectOption[];
}

export type SessionConfigSelectOptions =
  | SessionConfigSelectOption[]
  | SessionConfigSelectGroup[];

/** JSON shape of an ACP `SessionConfigOption` after its flattened kind. */
export interface SessionConfigOption {
  id: string;
  name: string;
  description?: string;
  category?: string;
  type: string;
  currentValue: string | boolean;
  options?: SessionConfigSelectOptions;
}

export interface AcpSessionState {
  /** Current ACP `SessionModeState` (or `null` when the agent does not advertise modes). */
  modes: SessionModesInit | null;
  /** Current ACP `SessionModelState` (or `null` when not advertised / unstable feature off). */
  models: SessionModelsInit | null;
  /** Stable ACP 2.0 config options advertised by the agent. */
  configOptions: SessionConfigOption[] | null;
  /** Last error from a setting attempt (cleared on next success). */
  error: string | null;
  /** Indicates an in-flight setting request. */
  pending: boolean;
  /** Refresh from `GET /session/init`. Returns `true` on 200, `false` otherwise. */
  refresh: () => Promise<boolean>;
  /** Issue ACP `session/set_mode` via the bridge. */
  setMode: (modeId: string) => Promise<boolean>;
  /** Issue ACP `session/set_model` via the bridge. Only meaningful when `models` is non-null. */
  setModel: (modelId: string) => Promise<boolean>;
  /** Issue ACP `session/set_config_option` via the bridge. */
  setConfigOption: (configId: string, value: string) => Promise<boolean>;
}

const Ctx = createContext<AcpSessionState | null>(null);

/**
 * Client-side timeout for setting / refresh requests. The
 * bridge enforces its own timeout (`set_session_timeout`, default 30s) so
 * keep this slightly larger to let the bridge respond first; 35s strikes
 * the balance.
 */
const FETCH_TIMEOUT_MS = 35_000;

interface SessionInitPayload {
  modes?: SessionModesInit | null;
  models?: SessionModelsInit | null;
  configOptions?: SessionConfigOption[] | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isSelectOption(value: unknown): value is SessionConfigSelectOption {
  return (
    isRecord(value) &&
    typeof value.value === "string" &&
    typeof value.name === "string" &&
    (value.description === undefined || typeof value.description === "string")
  );
}

function isSelectGroup(value: unknown): value is SessionConfigSelectGroup {
  return (
    isRecord(value) &&
    typeof value.group === "string" &&
    typeof value.name === "string" &&
    Array.isArray(value.options) &&
    value.options.every(isSelectOption)
  );
}

function isSelectOptions(value: unknown): value is SessionConfigSelectOptions {
  return (
    Array.isArray(value) &&
    value.every((item) =>
      isRecord(item) && "options" in item
        ? isSelectGroup(item)
        : isSelectOption(item),
    )
  );
}

function isSessionConfigOption(value: unknown): value is SessionConfigOption {
  if (
    !isRecord(value) ||
    typeof value.id !== "string" ||
    typeof value.name !== "string" ||
    typeof value.type !== "string" ||
    (value.description !== undefined && typeof value.description !== "string") ||
    (value.category !== undefined && typeof value.category !== "string")
  ) {
    return false;
  }
  if (value.type === "select") {
    return typeof value.currentValue === "string" && isSelectOptions(value.options);
  }
  return value.type === "boolean" && typeof value.currentValue === "boolean";
}

function normalizeConfigOptions(value: unknown): SessionConfigOption[] | null {
  if (!Array.isArray(value)) return null;
  return value.filter(isSessionConfigOption);
}

export function AcpSessionProvider({ children }: { children: ReactNode }) {
  const { agent } = useAgent();
  const [modes, setModes] = useState<SessionModesInit | null>(null);
  const [models, setModels] = useState<SessionModelsInit | null>(null);
  const [configOptions, setConfigOptions] = useState<SessionConfigOption[] | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  const applySessionInit = useCallback((payload: SessionInitPayload) => {
    setModes(payload.modes ?? null);
    setModels(payload.models ?? null);
    setConfigOptions(normalizeConfigOptions(payload.configOptions));
    setError(null);
  }, []);

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
          value?: unknown;
        };
        if (e.type !== "CUSTOM") return;
        if (e.name === "agent:session_init") {
          applySessionInit(
            (isRecord(e.value) ? e.value : {}) as SessionInitPayload,
          );
          return;
        }
        if (e.name !== "acp.session_update" || !isRecord(e.value)) return;
        if (
          e.value.sessionUpdate !== "config_option_update" ||
          !Array.isArray(e.value.configOptions)
        ) {
          return;
        }
        const next = normalizeConfigOptions(e.value.configOptions);
        if (next) {
          setConfigOptions(next);
          setError(null);
        }
      },
    });
    return () => subscription.unsubscribe();
  }, [agent, applySessionInit]);

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
      if (!res.ok) {
        const body = await res.text();
        setError(`session init HTTP ${res.status}: ${body || "(no body)"}`);
        return false;
      }
      const json = (await res.json()) as SessionInitPayload;
      applySessionInit(json);
      return true;
    } catch (err) {
      setError(String(err));
      return false;
    } finally {
      clearTimeout(timer);
    }
  }, [applySessionInit]);

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

  const setConfigOption = useCallback(
    async (configId: string, value: string): Promise<boolean> => {
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
        const res = await fetch("/api/bridge/session/set-config-option", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ threadId, configId, value }),
          signal: ctl.signal,
        });
        if (!res.ok) {
          const body = await res.text();
          setError(
            `set-config-option HTTP ${res.status}: ${body || "(no body)"}`,
          );
          return false;
        }
        // The route intentionally forwards only the bridge status. Re-read the
        // bridge snapshot because an option change may also change other choices.
        return await refresh();
      } catch (err) {
        setError(String(err));
        return false;
      } finally {
        clearTimeout(timer);
        setPending(false);
      }
    },
    [refresh],
  );

  const value = useMemo<AcpSessionState>(
    () => ({
      modes,
      models,
      configOptions,
      error,
      pending,
      refresh,
      setMode,
      setModel,
      setConfigOption,
    }),
    [
      modes,
      models,
      configOptions,
      error,
      pending,
      refresh,
      setMode,
      setModel,
      setConfigOption,
    ],
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
