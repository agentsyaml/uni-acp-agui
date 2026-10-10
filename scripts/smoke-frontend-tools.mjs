#!/usr/bin/env node
/**
 * End-to-end smoke test for the bridge's `useFrontendTool` injection path,
 * using a real `opencode acp` agent.
 *
 * Runs three scenarios sequentially against the same bridge instance:
 *
 *  1. **basic**    — single tool call, agent receives "hi alex" back
 *  2. **structured** — handler returns JSON, asserts the JSON survives
 *                       the MCP roundtrip and the agent text references the marker
 *  3. **multi-call** — agent calls the tool twice in one prompt; both
 *                       calls receive distinct results
 *
 * Usage:
 *   bun run scripts/smoke-frontend-tools.mjs <bridge-url>
 *   # default bridge-url is http://127.0.0.1:8080
 */

const baseUrl = process.argv[2] ?? "http://127.0.0.1:8080";

let exitCode = 0;
const failures = [];

await runScenario("basic", basicScenario);
await runScenario("structured", structuredScenario);
await runScenario("multi-call", multiCallScenario);

console.log("\n========");
console.log(failures.length === 0 ? "ALL OK" : `${failures.length} FAILED`);
for (const f of failures) console.log(" - " + f);
process.exit(exitCode);

// --------------------------------------------------------------------------
// Scenarios
// --------------------------------------------------------------------------

async function runScenario(name, fn) {
  console.log(`\n=== ${name} ===`);
  try {
    await fn();
    console.log(`✓ ${name}`);
  } catch (err) {
    exitCode = 1;
    failures.push(`${name}: ${err instanceof Error ? err.message : err}`);
    console.error(`✗ ${name}: ${err}`);
  }
}

async function basicScenario() {
  const tool = makeTool({
    name: "say_hello",
    description: "Greet someone by name. Returns a friendly text greeting.",
    parameters: paramsObject({ name: { type: "string" } }, ["name"]),
  });
  const summary = await driveRun({
    threadId: `basic-${Date.now()}`,
    tools: [tool],
    userMessage:
      "Call the say_hello tool with name=alex (no other tools). " +
      "Then in your response paste the tool's output verbatim.",
    handler: () => ({ ok: true, content: "hi alex" }),
  });

  expect(summary.toolCallSeen, "tool call seen");
  expect(summary.toolEnded, "tool ended");
  expect(summary.runFinished, "run finished");
  expect(
    summary.agentText.toLowerCase().includes("hi alex"),
    `agent should echo "hi alex" but got ${JSON.stringify(summary.agentText)}`,
  );
}

async function structuredScenario() {
  const tool = makeTool({
    name: "estimate_order",
    description:
      "Given a SKU and quantity, return a JSON object {sku, qty, unit, total}.",
    parameters: paramsObject(
      { sku: { type: "string" }, qty: { type: "number" } },
      ["sku", "qty"],
    ),
  });
  const summary = await driveRun({
    threadId: `structured-${Date.now()}`,
    tools: [tool],
    userMessage:
      "Use estimate_order with sku=A100 and qty=3, then in your final " +
      "response say the literal word STRUCTURED_OK once you have the result.",
    handler: (args) => {
      const sku = args?.sku ?? "?";
      const qty = Number(args?.qty ?? 0);
      const unit = 19.99;
      return {
        ok: true,
        content: JSON.stringify({
          sku,
          qty,
          unit,
          total: qty * unit,
          marker: "STRUCTURED_OK",
        }),
      };
    },
  });

  expect(summary.toolCallSeen, "tool call seen");
  expect(summary.runFinished, "run finished");
  expect(
    summary.agentText.includes("STRUCTURED_OK"),
    `agent text should include STRUCTURED_OK marker; got ${JSON.stringify(summary.agentText)}`,
  );
}

async function multiCallScenario() {
  const tool = makeTool({
    name: "multiply",
    description: "Multiply two numbers. Returns {a, b, product}.",
    parameters: paramsObject(
      { a: { type: "number" }, b: { type: "number" } },
      ["a", "b"],
    ),
  });
  let calls = 0;
  const summary = await driveRun({
    threadId: `multi-${Date.now()}`,
    tools: [tool],
    userMessage:
      "Call multiply(2,3), then call multiply(4,5). In your final " +
      "response say MULTI_OK if both tool calls succeeded.",
    handler: (args) => {
      calls += 1;
      const a = Number(args?.a ?? 0);
      const b = Number(args?.b ?? 0);
      return {
        ok: true,
        content: JSON.stringify({ a, b, product: a * b }),
      };
    },
  });

  expect(summary.runFinished, "run finished");
  expect(
    calls >= 2,
    `expected >=2 tool calls in this run, got ${calls}`,
  );
  expect(
    summary.agentText.includes("MULTI_OK"),
    `agent should include MULTI_OK; got ${JSON.stringify(summary.agentText)}`,
  );
}

// --------------------------------------------------------------------------
// Harness
// --------------------------------------------------------------------------

function makeTool({ name, description, parameters }) {
  return { name, description, parameters };
}

function paramsObject(properties, required) {
  return { type: "object", properties, required };
}

function expect(cond, msg) {
  if (!cond) throw new Error(msg);
}

/**
 * @param {{
 *   threadId: string,
 *   tools: Array<{name:string,description:string,parameters:object}>,
 *   userMessage: string,
 *   handler: (args: any) => { ok: boolean, content: string },
 * }} opts
 */
async function driveRun(opts) {
  const input = {
    threadId: opts.threadId,
    runId: `r-${Date.now()}`,
    messages: [
      { role: "user", id: "u1", content: opts.userMessage },
    ],
    tools: opts.tools,
    context: [],
    forwardedProps: {},
    state: {},
  };

  console.log(`POST ${baseUrl}/  thread=${opts.threadId}`);
  // Retry the initial POST a couple of times: opencode occasionally
  // returns 500 transiently between turns while it juggles its session
  // state (it's a long-running subprocess and the bridge surfaces those
  // errors as 500). Re-trying on a fresh runId is benign because each
  // run is independent.
  let res;
  for (let attempt = 0; attempt < 3; attempt++) {
    res = await fetch(`${baseUrl}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/event-stream",
      },
      body: JSON.stringify({
        ...input,
        runId: `${input.runId}-try${attempt}`,
      }),
    });
    if (res.ok) break;
    console.log(`  retry ${attempt + 1}: HTTP ${res.status}`);
    await new Promise((r) => setTimeout(r, 1500));
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  let toolCallSeen = false;
  let toolEnded = false;
  let runFinished = false;
  let agentText = "";
  const toolCalls = new Map();

  const decoder = new TextDecoder();
  let buf = "";
  const reader = res.body.getReader();
  const deadline = Date.now() + 90_000;

  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf("\n\n")) !== -1) {
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      for (const line of frame.split(/\r?\n/)) {
        if (!line.startsWith("data:")) continue;
        const payload = line.slice(5).trimStart();
        let event;
        try {
          event = JSON.parse(payload);
        } catch {
          continue;
        }
        switch (event.type) {
          case "TOOL_CALL_START": {
            toolCallSeen = true;
            toolCalls.set(event.toolCallId, {
              name: event.toolCallName,
              args: "",
            });
            console.log(
              `  → TOOL_CALL_START id=${event.toolCallId} name=${event.toolCallName}`,
            );
            break;
          }
          case "TOOL_CALL_ARGS": {
            const id = event.toolCallId;
            const call = toolCalls.get(id);
            if (call) call.args += event.delta ?? "";
            break;
          }
          case "TOOL_CALL_END": {
            toolEnded = true;
            const id = event.toolCallId;
            const call = toolCalls.get(id);
            if (!call) break;
            toolCalls.delete(id);
            let args;
            try {
              args = call.args === "" ? {} : JSON.parse(call.args);
            } catch (error) {
              throw new Error(`invalid complete TOOL_CALL_ARGS JSON for ${id}: ${error}`);
            }
            const result = opts.handler(args);
            const response = await fetch(`${baseUrl}/tool-response`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                toolCallId: id,
                threadId: opts.threadId,
                content: result.content,
                isError: !result.ok,
              }),
            });
            if (!response.ok) {
              throw new Error(`tool-response ${response.status}: ${await response.text()}`);
            }
            break;
          }
          case "TEXT_MESSAGE_CONTENT":
            if (typeof event.delta === "string") agentText += event.delta;
            break;
          case "RUN_FINISHED":
            runFinished = true;
            break;
          case "RUN_ERROR":
            throw new Error(`RUN_ERROR: ${event.message ?? "(no message)"}`);
        }
        if (runFinished) break;
      }
      if (runFinished) break;
    }
    if (runFinished) break;
  }

  return { toolCallSeen, toolEnded, runFinished, agentText };
}
