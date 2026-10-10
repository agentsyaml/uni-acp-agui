"use client";

import { useAgent } from "@copilotkit/react-core/v2";
import type { BaseEvent } from "@ag-ui/client";
import { useEffect, useState } from "react";
import { ChatSurface } from "@/components/persistent-chat";

type LogEntry = {
  ts: string;
  type: string;
  payload: BaseEvent;
};

export default function RawEventsPage() {
  const { agent } = useAgent();
  const [events, setEvents] = useState<LogEntry[]>([]);
  const [count, setCount] = useState(0);

  useEffect(() => {
    if (!agent) return;
    const subscription = agent.subscribe({
      onEvent: ({ event }) => {
        setCount((c) => c + 1);
        setEvents((prev) =>
          [
            {
              ts: new Date().toLocaleTimeString(),
              type: (event as { type?: string }).type ?? "<unknown>",
              payload: event,
            },
            ...prev,
          ].slice(0, 200),
        );
      },
    });
    return () => subscription.unsubscribe();
  }, [agent]);

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Raw AG-UI Event Stream
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          We subscribe to every event type via{" "}
          <code>useAgent().agent.subscribe()</code>; each translation the
          bridge emits from ACP shows up below (newest first). The chat box
          and the event list share the same <code>HttpAgent</code> instance.
        </p>
      </header>

      <div className="grid lg:grid-cols-2 gap-4">
        <div className="demo-card h-[70vh] flex flex-col p-2">
          <ChatSurface className="flex-1" />
        </div>

        <div className="demo-card h-[70vh] flex flex-col">
          <div className="flex items-center justify-between mb-3">
            <h2 className="font-semibold flex items-center gap-2">
              <span>Event log</span>
              <span className="badge badge-accent">{count}</span>
            </h2>
            <button
              type="button"
              className="btn btn-ghost text-xs py-1 px-2"
              onClick={() => {
                setCount(0);
                setEvents([]);
              }}
            >
              Clear
            </button>
          </div>
          <div className="flex-1 overflow-auto space-y-1.5 text-xs">
            {events.length === 0 && (
              <div className="h-full flex items-center justify-center">
                <p className="text-muted text-center">
                  No events yet.<br />
                  Send a message on the left to start.
                </p>
              </div>
            )}
            {events.map((e, idx) => (
              <details
                key={`${e.ts}-${idx}`}
                className="rounded-lg border border-[var(--border)] bg-[var(--background)] p-2 group"
              >
                <summary className="cursor-pointer flex gap-2 items-center select-none">
                  <span className="text-muted font-mono shrink-0">
                    {e.ts}
                  </span>
                  <span className="badge badge-accent">{e.type}</span>
                </summary>
                <pre className="demo-pre mt-2 text-[11px]">
                  {JSON.stringify(e.payload, null, 2)}
                </pre>
              </details>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
