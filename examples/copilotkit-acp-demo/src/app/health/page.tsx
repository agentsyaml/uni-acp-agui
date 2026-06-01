"use client";

import { useEffect, useState } from "react";

type HealthState =
  | { kind: "loading" }
  | { kind: "ok"; sessions: number; raw: unknown }
  | { kind: "error"; error: string };

export default function HealthPage() {
  const [state, setState] = useState<HealthState>({ kind: "loading" });
  const [tick, setTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch("/api/bridge/health", { cache: "no-store" });
        const json = (await res.json()) as { sessions?: number };
        if (cancelled) return;
        if (res.ok) {
          setState({
            kind: "ok",
            sessions: json.sessions ?? 0,
            raw: json,
          });
        } else {
          setState({
            kind: "error",
            error: `HTTP ${res.status}: ${JSON.stringify(json)}`,
          });
        }
      } catch (err) {
        if (cancelled) return;
        setState({ kind: "error", error: String(err) });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [tick]);

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Bridge Health Check
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          Frontend → <code>/api/bridge/health</code> (Next API) →{" "}
          <code>GET http://127.0.0.1:8080/health</code>. The result tells you
          how many ACP sessions the bridge currently caches (one per{" "}
          <code>thread_id</code>, reused across runs).
        </p>
      </header>

      <div className="demo-card space-y-4">
        <div className="flex items-center justify-between">
          <button
            type="button"
            onClick={() => setTick((t) => t + 1)}
            className="btn btn-primary"
          >
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M21 12a9 9 0 11-3-6.7L21 8" />
              <path d="M21 3v5h-5" />
            </svg>
            Refresh
          </button>
          {state.kind === "ok" && (
            <span className="badge badge-success">
              ● Bridge online
            </span>
          )}
          {state.kind === "error" && (
            <span className="badge badge-danger">● Bridge unreachable</span>
          )}
          {state.kind === "loading" && (
            <span className="badge badge-accent">● Loading…</span>
          )}
        </div>

        <div className="text-sm">
          {state.kind === "loading" && (
            <p className="text-muted">Requesting…</p>
          )}
          {state.kind === "ok" && (
            <div className="space-y-3">
              <div className="rounded-lg border border-[var(--border)] bg-[var(--background)] p-4">
                <div className="text-xs text-muted uppercase tracking-wide mb-1">
                  Active sessions
                </div>
                <div className="text-3xl font-bold font-mono">
                  {state.sessions}
                </div>
              </div>
              <details>
                <summary className="cursor-pointer text-xs text-muted select-none">
                  Raw response
                </summary>
                <pre className="demo-pre mt-1">
                  {JSON.stringify(state.raw, null, 2)}
                </pre>
              </details>
            </div>
          )}
          {state.kind === "error" && (
            <div className="space-y-3">
              <p className="leading-relaxed">
                The bridge is not reachable. Make sure you have started it in
                another terminal:
              </p>
              <pre className="demo-pre">
                cargo run -p agui-acp-bridge-cli -- opencode acp
              </pre>
              <details>
                <summary className="cursor-pointer text-xs text-muted select-none">
                  Error details
                </summary>
                <pre className="demo-pre mt-1">{state.error}</pre>
              </details>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
