import assert from "node:assert/strict";
import { test } from "bun:test";
import { scheduleAcpResume } from "../src/hooks/use-acp-resume.ts";

function harness(runAgent = async () => {}) {
  const jobs = new Map();
  let nextId = 0;
  const timer = {
    setTimeout(fn) { const id = ++nextId; jobs.set(id, fn); return id; },
    clearTimeout(id) { jobs.delete(id); },
  };
  const advance = () => {
    const pending = [...jobs.values()];
    jobs.clear();
    pending.forEach(fn => fn());
  };
  const calls = [];
  const messages = [{ id: "existing" }];
  const agent = { messages, runAgent: input => { calls.push(input); return runAgent(input); } };
  const errors = [];
  const handled = { current: 0 };
  const schedule = (ready, token, session = `session-${token}`) =>
    scheduleAcpResume(agent, ready, token, session, null, handled, e => errors.push(e), timer);
  return { advance, calls, messages, errors, handled, schedule };
}

test("StrictMode cleanup and provisional agent do not consume the resume token", () => {
  const h = harness();
  assert.equal(h.schedule(false, 4), undefined);
  const cleanup = h.schedule(true, 4);
  cleanup();
  h.schedule(true, 4);
  h.advance();
  assert.equal(h.calls.length, 1);
  assert.deepEqual(h.calls[0], { forwardedProps: { acpResume: { sessionId: "session-4" } } });
  assert.deepEqual(h.messages, [{ id: "existing" }]);
  assert.equal(h.handled.current, 4);
});

test("only the latest scheduled resume runs and a canceled token stays available", () => {
  const h = harness();
  const canceled = h.schedule(true, 1, "old-session");
  canceled();
  h.schedule(true, 2, "new-session");
  h.advance();
  assert.deepEqual(h.calls, [{ forwardedProps: { acpResume: { sessionId: "new-session" } } }]);
  assert.equal(h.handled.current, 2);
});

test("a started failure is reported once; only a new token retries", async () => {
  const h = harness(async () => { throw new Error("load failed"); });
  h.schedule(true, 7);
  h.advance();
  await Promise.resolve();
  assert.deepEqual(h.errors, ["load failed"]);
  h.schedule(true, 7);
  h.advance();
  h.schedule(true, 8);
  h.advance();
  await Promise.resolve();
  assert.deepEqual(h.errors, ["load failed", "load failed"]);
  assert.equal(h.calls.length, 2);
});
