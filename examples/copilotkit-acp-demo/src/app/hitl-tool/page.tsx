"use client";

import { useState } from "react";
import { z } from "zod";
import { useAcpHumanInTheLoop } from "@/hooks/use-acp-human-in-the-loop";
import { ChatSurface } from "@/components/persistent-chat";

/**
 * Human-in-the-loop tool demo.
 *
 * The agent calls `request_user_confirmation`. The bridge dispatches the
 * call into the browser; this page parks it in a confirmation modal until
 * the user clicks Approve / Decline. Only then does the result flow back
 * to the agent's MCP request and unblock its turn.
 *
 * This is the "stop and ask" pattern: distinct from the bridge's native
 * `--policy interrupt` HITL, which gates *any* tool call the agent makes.
 * This one is opt-in per tool, declared by the page using
 * `useAcpHumanInTheLoop`.
 */
export default function HitlToolPage() {
  const [history, setHistory] = useState<
    Array<{ ts: string; question: string; outcome: string; approved: boolean }>
  >([]);

  const confirm = useAcpHumanInTheLoop({
    name: "request_user_confirmation",
    description:
      "Ask the user to approve a potentially destructive action. Returns " +
      'the string "approved" if the user clicked approve, or rejects with ' +
      "the user-supplied reason otherwise. Use sparingly — the agent's run " +
      "is paused while the dialog is open.",
    parameters: z.object({
      question: z.string().describe("the question to show the user"),
      detail: z
        .string()
        .optional()
        .describe("optional secondary line shown beneath the question"),
    }),
  });

  const onApprove = () => {
    if (!confirm.pending) return;
    const q = (confirm.pending.args as { question: string }).question;
    setHistory((prev) =>
      [
        {
          ts: new Date().toLocaleTimeString(),
          question: q,
          outcome: "Approved",
          approved: true,
        },
        ...prev,
      ].slice(0, 30),
    );
    confirm.respond("approved");
  };

  const onDecline = () => {
    if (!confirm.pending) return;
    const q = (confirm.pending.args as { question: string }).question;
    setHistory((prev) =>
      [
        {
          ts: new Date().toLocaleTimeString(),
          question: q,
          outcome: "Declined",
          approved: false,
        },
        ...prev,
      ].slice(0, 30),
    );
    confirm.reject("user declined");
  };

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Human-in-the-Loop Tool
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          When the agent calls <code>request_user_confirmation</code>, the
          bridge pauses its MCP request, the browser shows a confirmation
          dialog, and the result is delivered back to the agent only after the
          user clicks. This is per-tool HITL — distinct from the bridge&rsquo;s
          global <code>--policy interrupt</code> mode.
        </p>
        <p className="text-muted text-sm leading-relaxed">
          Try:{" "}
          <em>
            &ldquo;Use request_user_confirmation to ask whether to delete
            node_modules.&rdquo;
          </em>
        </p>
      </header>

      {confirm.pending && (
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
            <div className="shrink-0 w-10 h-10 rounded-full flex items-center justify-center text-xl"
              style={{
                background: "color-mix(in srgb, var(--warning) 20%, transparent)",
              }}
            >
              ⚠
            </div>
            <div className="flex-1 space-y-1.5">
              <p className="font-semibold text-[var(--warning)] text-sm uppercase tracking-wide">
                Agent confirmation request
              </p>
              <p className="text-base font-medium leading-relaxed">
                {(confirm.pending.args as { question: string }).question}
              </p>
              {(confirm.pending.args as { detail?: string }).detail && (
                <p className="text-sm text-muted leading-relaxed">
                  {(confirm.pending.args as { detail?: string }).detail}
                </p>
              )}
            </div>
          </div>
          <div className="flex gap-2 pt-1">
            <button
              type="button"
              className="btn btn-success"
              onClick={onApprove}
            >
              ✓ Approve
            </button>
            <button
              type="button"
              className="btn btn-danger"
              onClick={onDecline}
            >
              ✗ Decline
            </button>
          </div>
        </div>
      )}

      <div className="grid lg:grid-cols-2 gap-4">
        <div className="demo-card h-[60vh] flex flex-col p-2">
          <ChatSurface className="flex-1" />
        </div>

        <div className="demo-card h-[60vh] flex flex-col">
          <h2 className="font-semibold mb-3 flex items-center justify-between">
            <span>Decisions</span>
            <span className="badge badge-accent">{history.length}</span>
          </h2>
          <div className="flex-1 overflow-auto space-y-2 text-xs">
            {history.length === 0 && (
              <div className="h-full flex items-center justify-center">
                <p className="text-muted text-center">
                  No decisions yet.<br />
                  Ask the agent to call{" "}
                  <code>request_user_confirmation</code>.
                </p>
              </div>
            )}
            {history.map((h, i) => (
              <div
                key={i}
                className="rounded-lg border border-[var(--border)] bg-[var(--background)] p-2.5"
              >
                <div className="flex items-center justify-between mb-1.5">
                  <span
                    className={`badge ${h.approved ? "badge-success" : "badge-danger"}`}
                  >
                    {h.approved ? "✓" : "✗"} {h.outcome}
                  </span>
                  <span className="text-muted font-mono">{h.ts}</span>
                </div>
                <p className="text-muted leading-relaxed">{h.question}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
