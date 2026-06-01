#!/usr/bin/env node
/**
 * Diagnostic for the conversation-history surface (`GET /sessions` →
 * ACP `session/list`).
 *
 * It:
 *   1. GET /sessions (before)  — shows what the agent currently persists.
 *   2. Drives two short prompts on two distinct threads to create sessions.
 *   3. GET /sessions (after)   — shows whether the new conversations appear.
 *
 * Run the bridge first (e.g. `bun run dev:bridge` in the demo), then:
 *   node scripts/diagnose-sessions.mjs [bridge-url]
 *   # default bridge-url is http://127.0.0.1:8080
 *
 * Watch the BRIDGE's stdout too: it logs `session/list complete total=N`
 * (and per-page counts at debug level) so you can see the raw agent reply.
 */

const baseUrl = process.argv[2] ?? "http://127.0.0.1:8080";

async function getSessions(label) {
  const res = await fetch(new URL("/sessions", baseUrl), {
    headers: { Accept: "application/json" },
  });
  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = text;
  }
  console.log(`\n--- GET /sessions (${label}) → HTTP ${res.status} ---`);
  if (res.status === 501) {
    console.log("Agent does NOT advertise session/list. History is unavailable.");
  } else if (Array.isArray(parsed?.sessions)) {
    console.log(`sessions: ${parsed.sessions.length}`);
    for (const s of parsed.sessions) {
      console.log(
        `  - ${s.sessionId}  title=${JSON.stringify(s.title)}  cwd=${s.cwd}  updatedAt=${s.updatedAt ?? "-"}`,
      );
    }
  } else {
    console.log(parsed);
  }
  return { status: res.status, parsed };
}

async function drivePrompt(threadId, text) {
  const body = {
    threadId,
    runId: `run-${Date.now()}`,
    messages: [{ role: "user", id: `m-${Date.now()}`, content: text }],
    tools: [],
    context: [],
    forwardedProps: {},
    state: {},
  };
  const res = await fetch(new URL("/", baseUrl), {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`prompt run failed: HTTP ${res.status} ${await res.text()}`);
  }
  // Drain the SSE stream to completion so the turn finishes (and the agent
  // persists the session) before we list.
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let acc = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    acc += decoder.decode(value, { stream: true });
    if (acc.includes('"type":"RUN_FINISHED"') || acc.includes('"type":"RUN_ERROR"')) {
      break;
    }
  }
  reader.cancel().catch(() => {});
  console.log(`  drove prompt on thread ${threadId} (${text.length} chars)`);
}

console.log(`Bridge: ${baseUrl}`);
await getSessions("before");

console.log("\nDriving two prompts to create conversations…");
await drivePrompt(`diag-a-${Date.now()}`, "Say hello in one short sentence.");
await drivePrompt(`diag-b-${Date.now()}`, "Say goodbye in one short sentence.");

// Give the agent a beat to persist.
await new Promise((r) => setTimeout(r, 500));

const after = await getSessions("after");

console.log("\n========");
if (after.status === 501) {
  console.log("RESULT: agent lacks session/list — frontend history can't work.");
} else if (Array.isArray(after.parsed?.sessions) && after.parsed.sessions.length > 0) {
  console.log("RESULT: sessions are listed. The history UI should populate.");
} else {
  console.log(
    "RESULT: session/list returned EMPTY even after creating conversations.\n" +
      "This means the agent persists sessions in a scope the bridge's listing\n" +
      "connection can't see (e.g. directory-scoped, or a different storage root).\n" +
      "Check the bridge log line `session/list complete total=…` and the agent's\n" +
      "storage dir (opencode: ~/.local/share/opencode/storage/session/).",
  );
}
