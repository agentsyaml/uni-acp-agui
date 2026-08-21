"use client";

/**
 * Top-bar picker UI for the ACP session's mode, model, and other select
 * configuration options.
 *
 * Visibility rules:
 *
 * - Stable ACP `configOptions` are preferred. Their `mode` and `model`
 *   categories replace the legacy mirrors, and every other select option is
 *   shown after them.
 * - Legacy mode/model mirrors remain as fallbacks for older bridges.
 * - When neither is advertised the picker renders a small inert hint so
 *   operators can tell the bridge is connected but the agent is silent on
 *   modes/models.
 */

import {
  useAcpSession,
  type SessionConfigOption,
  type SessionConfigSelectGroup,
  type SessionConfigSelectOption,
} from "./acp-session-context";

type SelectableConfigOption = SessionConfigOption & {
  type: "select";
  currentValue: string;
  options: (SessionConfigSelectOption | SessionConfigSelectGroup)[];
};

function isConfigGroup(
  option: SessionConfigSelectOption | SessionConfigSelectGroup,
): option is SessionConfigSelectGroup {
  return "options" in option;
}

function isSelectableConfigOption(
  option: SessionConfigOption,
): option is SelectableConfigOption {
  return (
    option.type === "select" &&
    typeof option.currentValue === "string" &&
    Array.isArray(option.options)
  );
}

function configLabel(option: SessionConfigOption): string {
  if (option.category === "mode") return "Mode";
  if (option.category === "model") return "Model";
  return option.name;
}

function ConfigSelect({
  option,
  pending,
  onChange,
}: {
  option: SelectableConfigOption;
  pending: boolean;
  onChange: (configId: string, value: string) => void;
}) {
  return (
    <label className="flex items-center gap-1.5">
      <span
        className="text-muted font-medium"
        title={option.description ?? option.name}
      >
        {configLabel(option)}
      </span>
      <select
        disabled={pending}
        value={option.currentValue}
        onChange={(e) => onChange(option.id, e.target.value)}
        className="form-select"
        aria-label={configLabel(option)}
      >
        {option.options.map((entry) =>
          isConfigGroup(entry) ? (
            <optgroup key={entry.group} label={entry.name}>
              {entry.options.map((value) => (
                <option
                  key={`${entry.group}:${value.value}`}
                  value={value.value}
                  title={value.description ?? ""}
                >
                  {value.name}
                </option>
              ))}
            </optgroup>
          ) : (
            <option
              key={entry.value}
              value={entry.value}
              title={entry.description ?? ""}
            >
              {entry.name}
            </option>
          ),
        )}
      </select>
    </label>
  );
}

export function AgentPicker() {
  const {
    modes,
    models,
    configOptions,
    error,
    pending,
    setMode,
    setModel,
    setConfigOption,
  } = useAcpSession();
  const stableSelects = (configOptions ?? []).filter(isSelectableConfigOption);
  const stableMode = stableSelects.find(
    (option) => option.category === "mode",
  );
  const stableModel = stableSelects.find(
    (option) => option.category === "model",
  );
  const otherConfigOptions = stableSelects.filter(
    (option) => option !== stableMode && option !== stableModel,
  );
  const hasAny = stableSelects.length > 0 || !!modes || !!models;

  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      {stableMode ? (
        <ConfigSelect
          option={stableMode}
          pending={pending}
          onChange={(configId, value) => void setConfigOption(configId, value)}
        />
      ) : modes ? (
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

      {stableModel ? (
        <ConfigSelect
          option={stableModel}
          pending={pending}
          onChange={(configId, value) => void setConfigOption(configId, value)}
        />
      ) : models ? (
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

      {otherConfigOptions.map((option) => (
        <ConfigSelect
          key={option.id}
          option={option}
          pending={pending}
          onChange={(configId, value) => void setConfigOption(configId, value)}
        />
      ))}

      {!hasAny ? (
        <span
          className="text-muted opacity-60"
          title="Agent did not advertise any selectable session config"
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
