import assert from "node:assert/strict";
import { test } from "bun:test";
import {
  frontendToolDeclaration,
  subscribeAcpFrontendTool,
} from "../src/lib/acp-frontend-tool-subscriber.ts";

function harness(handler) {
  const posted = [];
  let subscriber;
  let unsubscribed = false;
  const agent = {
    threadId: "thread-a",
    subscribe(value) {
      subscriber = value;
      return { unsubscribe: () => (unsubscribed = true) };
    },
  };
  const state = {
    posted,
    agent,
    get subscriber() { return subscriber; },
  };
  const send = async (url, init) => {
    posted.push({ url, ...JSON.parse(init.body) });
    state.notify?.();
    return new Response(null, { status: 200 });
  };
  state.cleanup = subscribeAcpFrontendTool(
    agent,
    {
      matchNames: () => new Set(["say_hello", "agui-acp-bridge_say_hello"]),
      handler,
      log() {},
    },
    send,
  );
  state.unsubscribed = () => unsubscribed;
  state.waitForPosts = async count => {
    while (posted.length < count) {
      await new Promise(resolve => {
        state.notify = resolve;
      });
    }
    state.notify = null;
  };
  return state;
}

const event = (subscriber, name, payload) => subscriber[name]({ event: payload });
const start = (h, id, toolCallName = "say_hello") =>
  event(h.subscriber, "onToolCallStartEvent", { toolCallId: id, toolCallName });
const args = (h, id, delta) => event(h.subscriber, "onToolCallArgsEvent", { toolCallId: id, delta });
const end = (h, id) => event(h.subscriber, "onToolCallEndEvent", { toolCallId: id });

test("registers only the declaration and waits for TOOL_CALL_END", async () => {
  const calls = [];
  const h = harness((value) => {
    calls.push(value);
    return "done";
  });
  const declaration = frontendToolDeclaration({
    name: "say_hello",
    description: "greet",
    parameters: {},
    handler: () => calls.push("unexpected"),
  });
  assert.equal(declaration.followUp, false);
  assert.equal("handler" in declaration, false);

  start(h, "prefix");
  args(h, "prefix", "{}");
  assert.deepEqual(calls, []);
  assert.equal(h.posted.length, 0);
  end(h, "prefix");
  await h.waitForPosts(1);
  assert.deepEqual(calls, [{}]);
  assert.equal(h.posted[0].isError, false);
  assert.equal(h.posted[0].content, "done");

  start(h, "no-args");
  end(h, "no-args");
  await h.waitForPosts(2);
  assert.deepEqual(calls, [{}, {}]);
  h.cleanup();
});

test("joins fragmented Unicode arguments before invoking the handler", async () => {
  const calls = [];
  const h = harness((value) => {
    calls.push(value);
    return { greeting: `\u4F60\u597D ${value.name} \u{1F30D}` };
  });
  start(h, "fragmented");
  args(h, "fragmented", '{"name":"\u5C71');
  args(h, "fragmented", '\u7530\uD83D\uDE42"}');
  assert.equal(h.posted.length, 0);
  end(h, "fragmented");
  await h.waitForPosts(1);
  assert.deepEqual(calls, [{ name: "\u5C71\u7530\uD83D\uDE42" }]);
  assert.equal(
    h.posted[0].content,
    JSON.stringify({ greeting: "\u4F60\u597D \u5C71\u7530\uD83D\uDE42 \u{1F30D}" }),
  );
  assert.equal(h.posted[0].isError, false);
  h.cleanup();
});

test("handles interleaved IDs once and posts to each captured thread", async () => {
  const resolvers = {};
  const h = harness(
    ({ name }) => new Promise((resolve) => { resolvers[name] = resolve; }),
  );
  for (const id of ["one", "two"]) {
    start(h, id);
    args(h, id, JSON.stringify({ name: id }));
  }
  end(h, "one");
  end(h, "one");
  end(h, "two");
  h.agent.threadId = "thread-b";
  resolvers.one("finished one");
  resolvers.two({ result: "\u96EA" });
  await h.waitForPosts(2);
  assert.equal(h.posted.length, 2);
  assert.deepEqual(h.posted.map(x => [x.toolCallId, x.threadId]), [
    ["one", "thread-a"], ["two", "thread-a"],
  ]);
  assert.equal(h.posted[0].content, "finished one");
  assert.equal(h.posted[1].content, JSON.stringify({ result: "\u96EA" }));
  h.cleanup();
});

test("discards partial calls on error, finish, next start, and cleanup", async () => {
  const calls = [];
  const h = harness(value => calls.push(value));
  start(h, "error"); args(h, "error", '{"x":');
  event(h.subscriber, "onRunErrorEvent", { type: "RUN_ERROR" }); end(h, "error");
  start(h, "finished"); args(h, "finished", '{"x":');
  event(h.subscriber, "onRunFinishedEvent", { event: { type: "RUN_FINISHED" } }); end(h, "finished");
  start(h, "next"); args(h, "next", '{"x":');
  event(h.subscriber, "onRunStartedEvent", { event: { type: "RUN_STARTED" } }); end(h, "next");
  start(h, "cleanup"); args(h, "cleanup", '{"x":');
  h.cleanup(); end(h, "cleanup");
  await Promise.resolve();
  assert.deepEqual(calls, []);
  assert.equal(h.posted.length, 0);
  assert.equal(h.unsubscribed(), true);
});

test("rejects malformed arguments without invoking the handler", async () => {
  const calls = [];
  const h = harness(value => calls.push(value));
  start(h, "bad"); args(h, "bad", '{"name":'); end(h, "bad");
  await h.waitForPosts(1);
  assert.deepEqual(calls, []);
  assert.equal(h.posted[0].isError, true);
  assert.match(h.posted[0].content, /^Invalid tool-call arguments:/);

  start(h, "bad"); args(h, "bad", '{"name":"valid"}'); end(h, "bad");
  await h.waitForPosts(2);
  assert.deepEqual(calls, [{ name: "valid" }]);
  h.cleanup();
});

test("forwards handler exceptions as error tool responses", async () => {
  const h = harness(() => {
    throw new Error("handler failed");
  });
  start(h, "throws"); end(h, "throws");
  await h.waitForPosts(1);
  assert.equal(h.posted[0].isError, true);
  assert.equal(h.posted[0].content, "handler failed");
  h.cleanup();
});
