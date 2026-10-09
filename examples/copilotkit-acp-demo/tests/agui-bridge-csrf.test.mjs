import assert from "node:assert/strict";
import { mock, test } from "bun:test";
import { approvalRequestBody } from "../src/lib/approval.ts";

// `server-only` is a Next build-time marker and is intentionally not a runtime
// dependency of this demo. Mock it so this pure bridge contract test can run in Bun.
mock.module("server-only", () => ({}));

const {
  bridgeRequestCsrfResponse,
  proxyBridgeRequest,
  validateBridgeRequestOrigin,
} = await import("../src/lib/agui-bridge.ts");

function request(url, headers = {}, method = "POST") {
  return new Request(url, { method, headers });
}

test("rejects cross-site writes before origin evaluation", async () => {
  const response = bridgeRequestCsrfResponse(
    request("http://localhost:3000/api/bridge/approval", {
      Origin: "http://localhost:3000",
      "Sec-Fetch-Site": "cross-site",
    }),
  );

  assert.equal(response.status, 403);
  assert.deepEqual(await response.json(), { error: "forbidden" });
});

test("proxy rejects before consuming a mutating request body", async () => {
  let bodyRead = false;
  const body = {
    getReader() {
      bodyRead = true;
      throw new Error("body was read");
    },
  };
  const response = await proxyBridgeRequest(
    {
      method: "POST",
      url: "http://localhost:3000/api/bridge/approval",
      headers: new Headers({ Origin: "https://attacker.example" }),
      signal: new AbortController().signal,
      body,
    },
    "/approval",
  );

  assert.equal(response.status, 403);
  assert.equal(bodyRead, false);
});

test("requires an exact local request origin by default", () => {
  assert.equal(
    validateBridgeRequestOrigin(
      request("http://localhost:3000/api/bridge/approval", {
        Origin: "http://127.0.0.1:3000",
      }),
    )?.status,
    403,
  );
  assert.equal(
    validateBridgeRequestOrigin(
      request("http://localhost:3000/api/bridge/approval", {
        Origin: "http://localhost:3000",
      }),
    ),
    null,
  );
});

test("rejects malformed and untrusted origins without details", async () => {
  const malformed = bridgeRequestCsrfResponse(
    request("http://localhost:3000/api/copilotkit", {
      Origin: "null",
    }),
  );
  const untrusted = bridgeRequestCsrfResponse(
    request("http://localhost:3000/api/copilotkit", {
      Origin: "https://attacker.example",
    }),
  );

  assert.equal(malformed.status, 400);
  assert.deepEqual(await malformed.json(), { error: "invalid request origin" });
  assert.equal(untrusted.status, 403);
  assert.deepEqual(await untrusted.json(), { error: "forbidden" });
});

test("an explicit allowlist supports a public origin behind a proxy", () => {
  const decision = validateBridgeRequestOrigin(
    request("http://127.0.0.1:3000/api/copilotkit", {
      Origin: "https://app.example.com",
    }),
    {
      trustedOrigins: ["https://app.example.com"],
      requireRequestOriginMatch: false,
    },
  );

  assert.equal(decision, null);
});

test("keeps origin-less CLI and GET requests available", () => {
  assert.equal(
    bridgeRequestCsrfResponse(
      request("http://localhost:3000/api/bridge/approval", {}, "POST"),
    ),
    null,
  );
  assert.equal(
    bridgeRequestCsrfResponse(
      request("http://attacker.example/api/bridge/health", {
        Origin: "https://attacker.example",
      }, "GET"),
    ),
    null,
  );
});

// The bridge's `POST /approval` is thread-scoped: its `ApprovalRequest` has
// `threadId` as a REQUIRED field, so a body without it is rejected with HTTP
// 422 (plain text) by the JSON extractor before any handler code runs.
test("approval body always carries the required threadId", () => {
  const body = approvalRequestBody({
    threadId: "thread-1",
    interruptId: "interrupt-1",
    approved: true,
    optionId: "allow_once",
  });

  assert.deepEqual(body, {
    threadId: "thread-1",
    interruptId: "interrupt-1",
    approved: true,
    optionId: "allow_once",
  });
});

test("approval body is refused instead of posting thread-less (would 422)", () => {
  assert.equal(
    approvalRequestBody({
      threadId: null,
      interruptId: "interrupt-1",
      approved: false,
    }),
    null,
  );
  assert.equal(
    approvalRequestBody({
      threadId: "",
      interruptId: "interrupt-1",
      approved: false,
    }),
    null,
  );
});

test("deny request omits optionId from the wire body", () => {
  const body = approvalRequestBody({
    threadId: "thread-1",
    interruptId: "interrupt-1",
    approved: false,
  });

  assert.equal(body.optionId, undefined);
  assert.equal(JSON.stringify(body).includes("optionId"), false);
});
