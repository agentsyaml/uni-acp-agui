"use client";

/**
 * Top-bar picker UI for the ACP session's mode and (optionally) model.
 *
 * Visibility rules:
 *
 * - Mode dropdown: shown iff the agent advertised a `SessionModeState` on
 *   `session/new` (so the user actually has options).
 * - Model dropdown: shown iff the agent advertised a `SessionModelState`.
 *   This currently requires the bridge built with the `unstable_session_model`
 *   feature (default-on) AND an agent that returns model info.
 * - When neither is advertised the picker renders a small inert hint so
 *   operators can tell the bridge is connected but the agent is silent on
 *   modes/models.
 */

import { useAcpSession } from "./acp-session-context";

export function AgentPicker() {
  const { modes, models, error, pending, setMode, setModel } = useAcpSession();
  const hasAny = !!modes || !!models;

  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      {modes ? (
        <label className="flex items-center gap-1.5">
          <span className="text-muted font-medium">Mode</span>
          <select
            disabled={pending}
            value={modes.currentModeId}
            onChange={(e) => void setMode(e.target.value)}
            className="form-select"
          >
            {modes.availableModes.map((m) => (
              <option key={m.id} value={m.id} title={m.description ?? ""}>
                {m.name}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      {models ? (
        <label className="flex items-center gap-1.5">
          <span className="text-muted font-medium">Model</span>
          <select
            disabled={pending}
            value={models.currentModelId}
            onChange={(e) => void setModel(e.target.value)}
            className="form-select"
          >
            {models.availableModels.map((m) => (
              <option key={m.id} value={m.id} title={m.description ?? ""}>
                {m.name}
              </option>
            ))}
          </select>
        </label>
      ) : null}

      {!hasAny ? (
        <span
          className="text-muted opacity-60"
          title="Agent did not advertise any modes/models on session/new"
        >
          No mode / model
        </span>
      ) : null}

      {pending ? (
        <span className="text-muted flex items-center gap-1">
          <span className="inline-block w-1.5 h-1.5 rounded-full bg-[var(--accent)] animate-pulse" />
          Switching…
        </span>
      ) : null}

      {error ? (
        <span className="badge badge-danger" title={error}>
          ⚠ {error.length > 60 ? error.slice(0, 60) + "…" : error}
        </span>
      ) : null}
    </div>
  );
}
