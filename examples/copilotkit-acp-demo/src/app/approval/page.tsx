"use client";

import { useAgent, CopilotChat } from "@copilotkit/react-core/v2";
import { useEffect, useState } from "react";

type PermissionOption = {
  optionId?: string;
  option_id?: string;
  name?: string;
  kind?: string;
};

type ApprovalRequest = {
  pending: true;
  interruptId: string;
  toolName?: string;
  options: PermissionOption[];
};

type ApprovalSnapshot = {
  approval?: ApprovalRequest;
};

export default function ApprovalPage() {
  const { agent } = useAgent();
  const [pending, setPending] = useState<ApprovalRequest | null>(null);
  const [history, setHistory] = useState<
    Array<{ ts: string; line: string }>
  >([]);

  useEffect(() => {
    if (!agent) return;
    const subscription = agent.subscribe({
      onStateSnapshotEvent: ({ event }) => {
        const snap = (event as { snapshot?: ApprovalSnapshot }).snapshot;
        if (snap?.approval?.pending) {
          setPending(snap.approval);
          appendHistory(
            setHistory,
            `Permission request received: ${snap.approval.toolName ?? "?"} (${snap.approval.interruptId.slice(0, 8)}…)`,
          );
        }
      },
    });
    return () => subscription.unsubscribe();
  }, [agent]);

  const respond = async (
    approved: boolean,
    optionId: string | undefined,
    summary: string,
  ) => {
    if (!pending) return;
    appendHistory(
      setHistory,
      `→ ${summary} (${pending.interruptId.slice(0, 8)}…)`,
    );
    setPending(null);
    try {
      const res = await fetch("/api/bridge/approval", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          interruptId: pending.interruptId,
          approved,
          optionId,
        }),
      });
      const text = await res.text();
      appendHistory(setHistory, `← ${res.status} ${text || "(empty)"}`);
    } catch (err) {
      appendHistory(setHistory, `Request failed: ${String(err)}`);
    }
  };

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Human-in-the-Loop (Bridge-Native)
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          For this page to do anything, launch the bridge with{" "}
          <code>--policy interrupt</code>:
        </p>
        <pre className="demo-pre">
          cargo run -p agui-acp-bridge-cli -- --policy interrupt -- opencode acp
        </pre>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          Every tool call the ACP agent attempts will then be paused; the
          bridge pushes a <code>STATE_SNAPSHOT</code> event here, the dialog
          below appears, and your decision is forwarded to{" "}
          <code>/api/bridge/approval</code> →{" "}
          <code>/approval</code> on the bridge.
        </p>
      </header>

      {pending && (
        <div
          className="demo-card space-y-4"
          style={{
            borderColor: "var(--warning)",
            borderWidth: "2px",
            background:
              "color-mix(in srgb, var(--warning) 6%, var(--card))",
          }}
        >
          <div className="flex items-start gap-3">
            <div
              className="shrink-0 w-10 h-10 rounded-full flex items-center justify-center text-xl"
              style={{
                background: "color-mix(in srgb, var(--warning) 20%, transparent)",
              }}
            >
              ⚠
            </div>
            <div className="flex-1 space-y-1.5">
              <p className="font-semibold text-[var(--warning)] text-sm uppercase tracking-wide">
                Permission requested
              </p>
              <p className="text-base">
                Tool:{" "}
                <code>{pending.toolName ?? "(untitled)"}</code>
              </p>
              <p className="text-xs text-muted font-mono break-all">
                interruptId: {pending.interruptId}
              </p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {pending.options.length === 0 && (
              <p className="text-xs text-muted italic">
                Agent provided no options — only deny is available.
              </p>
            )}
            {pending.options.map((opt, idx) => {
              const id = opt.optionId ?? opt.option_id ?? "";
              const label = opt.name ?? id ?? `option ${idx + 1}`;
              return (
                <button
                  key={`${id}-${idx}`}
                  type="button"
                  className="btn btn-success"
                  onClick={() => respond(true, id, `Approved (optionId=${id})`)}
                >
                  ✓ {label}
                </button>
              );
            })}
            <button
              type="button"
              className="btn btn-danger"
              onClick={() => respond(false, undefined, "Denied")}
            >
              ✗ Deny
            </button>
          </div>
          <details>
            <summary className="cursor-pointer text-xs text-muted select-none">
              Raw options
            </summary>
            <pre className="demo-pre text-xs mt-1">
              {JSON.stringify(pending.options, null, 2)}
            </pre>
          </details>
        </div>
      )}

      <div className="grid lg:grid-cols-2 gap-4">
        <div className="demo-card h-[60vh] flex flex-col p-2">
          <CopilotChat className="flex-1" />
        </div>

        <div className="demo-card h-[60vh] flex flex-col">
          <h2 className="font-semibold mb-3 flex items-center justify-between">
            <span>Approval history</span>
            <span className="badge badge-accent">{history.length}</span>
          </h2>
          <div className="flex-1 overflow-auto space-y-1 text-xs font-mono">
            {history.length === 0 && (
              <div className="h-full flex items-center justify-center">
                <p className="text-muted text-center">No history yet.</p>
              </div>
            )}
            {history.map((h, i) => (
              <p
                key={i}
                className="rounded border border-[var(--border)] bg-[var(--background)] p-1.5"
              >
                <span className="text-muted">{h.ts}</span> {h.line}
              </p>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function appendHistory(
  setHistory: React.Dispatch<
    React.SetStateAction<Array<{ ts: string; line: string }>>
  >,
  line: string,
) {
  setHistory((prev) =>
    [{ ts: new Date().toLocaleTimeString(), line }, ...prev].slice(0, 100),
  );
}
