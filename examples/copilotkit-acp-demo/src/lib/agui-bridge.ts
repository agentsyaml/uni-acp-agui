import "server-only";
import { HttpAgent } from "@ag-ui/client";

/**
 * URL of the Rust `agui-acp-bridge` HTTP/SSE endpoint.
 *
 * The bridge wraps an ACP-compliant agent (e.g. `opencode acp`) and exposes
 * it as an AG-UI HTTP endpoint. Default: http://127.0.0.1:8080/.
 *
 * Override via the server-only `AGUI_BRIDGE_URL` environment variable.
 *
 * Auth pairing: the Rust server reads `AGUI_ACP_BRIDGE_TOKEN`; this demo
 * proxy reads `AGUI_BRIDGE_TOKEN` — set both in production (same value).
 */
export const BRIDGE_URL =
  process.env.AGUI_BRIDGE_URL ?? "http://127.0.0.1:8080/";

export const BRIDGE_MAX_REQUEST_BODY_BYTES = 1024 * 1024;
/** Applies to auxiliary bridge proxy fetches and request-body admission only. */
export const BRIDGE_AUXILIARY_TIMEOUT_MS = 30_000;
export const BRIDGE_APP_ORIGINS_ENV = "AGUI_APP_ORIGINS";

const DEFAULT_LOCAL_APP_ORIGINS = [
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "http://[::1]:3000",
] as const;
const BRIDGE_WRITE_METHODS = new Set(["POST", "DELETE"]);

export type BridgeOriginRejection = {
  status: 400 | 403;
  error: "invalid request origin" | "forbidden";
};

export type BridgeOriginPolicy = {
  trustedOrigins: readonly string[];
  requireRequestOriginMatch: boolean;
};

/** Normalize an Origin value without accepting paths, credentials, or wildcards. */
export function normalizeBridgeOrigin(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed || trimmed === "null") return null;

  try {
    const origin = new URL(trimmed);
    if (
      (origin.protocol !== "http:" && origin.protocol !== "https:") ||
      origin.username ||
      origin.password ||
      origin.pathname !== "/" ||
      origin.search ||
      origin.hash
    ) {
      return null;
    }
    return origin.origin;
  } catch {
    return null;
  }
}

function configuredBridgeOriginPolicy(): BridgeOriginPolicy {
  const configuredOrigins = process.env[BRIDGE_APP_ORIGINS_ENV];
  if (configuredOrigins !== undefined) {
    const trustedOrigins = configuredOrigins
      .split(",")
      .map(normalizeBridgeOrigin);

    // An invalid allowlist fails closed. In particular, "*" is never special.
    return {
      trustedOrigins: trustedOrigins.every(
        (origin): origin is string => origin !== null,
      )
        ? trustedOrigins
        : [],
      // A configured public-origin allowlist also covers deployments where the
      // framework sees an internal origin behind a validated reverse proxy.
      requireRequestOriginMatch: false,
    };
  }

  return {
    trustedOrigins: DEFAULT_LOCAL_APP_ORIGINS,
    // Without an explicit proxy configuration, compare against the actual
    // request URL and only permit the fixed local single-user origins above.
    requireRequestOriginMatch: true,
  };
}

function requestUrlOrigin(requestUrl: string): string | null {
  try {
    return normalizeBridgeOrigin(new URL(requestUrl).origin);
  } catch {
    return null;
  }
}

/**
 * Pure origin decision used by every mutating bridge route.
 *
 * Missing Origin remains valid for server-side requests and non-browser CLI
 * clients. Browser cross-site requests are rejected before their body is read.
 */
export function validateBridgeRequestOrigin(
  request: Pick<Request, "headers" | "url">,
  policy: BridgeOriginPolicy = configuredBridgeOriginPolicy(),
): BridgeOriginRejection | null {
  if (request.headers.get("sec-fetch-site")?.trim().toLowerCase() === "cross-site") {
    return { status: 403, error: "forbidden" };
  }

  const originHeader = request.headers.get("origin");
  if (originHeader === null) return null;

  const origin = normalizeBridgeOrigin(originHeader);
  if (!origin) return { status: 400, error: "invalid request origin" };

  const trustedOrigins = new Set(
    policy.trustedOrigins
      .map(normalizeBridgeOrigin)
      .filter((trusted): trusted is string => trusted !== null),
  );
  if (!trustedOrigins.has(origin)) return { status: 403, error: "forbidden" };

  if (policy.requireRequestOriginMatch && requestUrlOrigin(request.url) !== origin) {
    return { status: 403, error: "forbidden" };
  }

  return null;
}

/** Return the generic CSRF response for a mutating bridge request, if needed. */
export function bridgeRequestCsrfResponse(
  request: Pick<Request, "method" | "headers" | "url">,
): Response | null {
  if (!BRIDGE_WRITE_METHODS.has(request.method.toUpperCase())) return null;

  const rejection = validateBridgeRequestOrigin(request);
  return rejection
    ? bridgeErrorResponse(rejection.status, rejection.error)
    : null;
}

const STRIPPED_RESPONSE_HEADERS = new Set([
  "connection",
  "content-encoding",
  "content-length",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

const SENSITIVE_RESPONSE_HEADER_NAMES = new Set([
  "authorization",
  "cookie",
  "set-cookie",
  "set-cookie2",
  "x-api-key",
]);

export class BridgeRequestBodyTooLargeError extends Error {}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("request aborted", "AbortError");
}

async function readRequestChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  signal: AbortSignal,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  if (signal.aborted) throw abortReason(signal);

  let removeAbortListener: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    const onAbort = () => {
      void (async () => {
        try {
          await reader.cancel(signal.reason);
        } catch {
          // The abort reason still determines the response if cancellation races.
        }
        reject(abortReason(signal));
      })();
    };
    signal.addEventListener("abort", onAbort, { once: true });
    removeAbortListener = () => signal.removeEventListener("abort", onAbort);
  });

  try {
    return await Promise.race([reader.read(), aborted]);
  } finally {
    removeAbortListener?.();
  }
}

export async function readBoundedRequestBody(
  request: Request,
  signal: AbortSignal = request.signal,
): Promise<string> {
  const contentLength = request.headers.get("content-length");
  const declaredLength = contentLength === null ? null : Number(contentLength);
  if (
    declaredLength !== null &&
    Number.isFinite(declaredLength) &&
    declaredLength > BRIDGE_MAX_REQUEST_BODY_BYTES
  ) {
    throw new BridgeRequestBodyTooLargeError();
  }

  if (!request.body) {
    if (signal.aborted) throw abortReason(signal);
    return "";
  }

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  try {
    while (true) {
      const { done, value } = await readRequestChunk(reader, signal);
      if (done) break;

      totalBytes += value.byteLength;
      if (totalBytes > BRIDGE_MAX_REQUEST_BODY_BYTES) {
        try {
          await reader.cancel();
        } catch {
          // The size limit still determines the response if cancellation races.
        }
        throw new BridgeRequestBodyTooLargeError();
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  if (signal.aborted) throw abortReason(signal);

  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(body);
}

function bridgeResponseHeaders(upstream: Response): Headers {
  const headers = new Headers();
  for (const [name, value] of upstream.headers) {
    const lowerName = name.toLowerCase();
    if (
      STRIPPED_RESPONSE_HEADERS.has(lowerName) ||
      SENSITIVE_RESPONSE_HEADER_NAMES.has(lowerName) ||
      lowerName.includes("auth") ||
      lowerName.includes("cookie") ||
      lowerName.includes("token")
    ) {
      continue;
    }
    headers.append(name, value);
  }
  return headers;
}

export function bridgeErrorResponse(
  status: 400 | 403 | 413 | 502 | 504,
  error: string,
): Response {
  return Response.json({ error }, { status });
}

/**
 * Forward an auxiliary bridge request without buffering its response body.
 * The 30s auxiliary deadline covers POST body admission and the fetch.
 */
export async function proxyBridgeRequest(
  request: Request,
  endpoint: string | URL,
): Promise<Response> {
  const csrfResponse = bridgeRequestCsrfResponse(request);
  if (csrfResponse) return csrfResponse;

  const auxiliaryTimeoutSignal = AbortSignal.timeout(
    BRIDGE_AUXILIARY_TIMEOUT_MS,
  );
  const auxiliarySignal = AbortSignal.any([
    request.signal,
    auxiliaryTimeoutSignal,
  ]);
  let body: string | undefined;
  if (request.method.toUpperCase() === "POST") {
    try {
      body = await readBoundedRequestBody(request, auxiliarySignal);
    } catch (error) {
      if (error instanceof BridgeRequestBodyTooLargeError)
        return bridgeErrorResponse(413, "request body too large");
      if (request.signal.aborted) throw error;
      if (auxiliaryTimeoutSignal.aborted)
        return bridgeErrorResponse(504, "bridge request timed out");
      throw error;
    }
  }

  const target =
    typeof endpoint === "string" ? new URL(endpoint, BRIDGE_URL) : endpoint;
  const headers = bridgeHeaders();
  if (body !== undefined) headers["Content-Type"] = "application/json";

  const init: RequestInit = {
    method: request.method,
    cache: "no-store",
    headers,
    signal: auxiliarySignal,
  };
  if (body !== undefined) init.body = body;

  try {
    const upstream = await fetch(target, init);
    return new Response(upstream.body, {
      status: upstream.status,
      headers: bridgeResponseHeaders(upstream),
    });
  } catch (error) {
    if (request.signal.aborted) throw error;
    if (auxiliaryTimeoutSignal.aborted) {
      return bridgeErrorResponse(504, "bridge request timed out");
    }
    return bridgeErrorResponse(502, "bridge unreachable");
  }
}

/** Headers for server-side requests to the protected bridge. */
export function bridgeHeaders(): Record<string, string> {
  // Auth pairing: the Rust server reads `AGUI_ACP_BRIDGE_TOKEN`; this demo
  // proxy reads `AGUI_BRIDGE_TOKEN` — set both in production (same value).
  const token = process.env.AGUI_BRIDGE_TOKEN;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Build an AG-UI {@link HttpAgent} pointing at the Rust bridge.
 *
 * The primary `/api/copilotkit` stream intentionally has no 30s auxiliary
 * timeout; that route is long-lived SSE and owns its maxDuration/cancellation.
 *
 * Each call returns a fresh instance — the `CopilotRuntime` clones agents per
 * thread internally, so a single instance per registration is fine.
 */
export function createBridgeAgent(): HttpAgent {
  return new HttpAgent({
    url: BRIDGE_URL,
    headers: {
      ...bridgeHeaders(),
      // The bridge requires SSE; HttpAgent already sets this, but being
      // explicit keeps middleware (if any) from rewriting it away.
      Accept: "text/event-stream",
    },
  });
}
