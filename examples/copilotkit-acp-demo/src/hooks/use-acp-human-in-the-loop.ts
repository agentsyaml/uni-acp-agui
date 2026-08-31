"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { StandardSchemaV1 } from "@standard-schema/spec";
import { useAcpFrontendTool } from "./use-acp-frontend-tool";

/**
 * Human-in-the-loop variant of {@link useAcpFrontendTool}.
 *
 * Same dispatch path (CopilotKit `useFrontendTool` → bridge MCP →
 * `/tool-response`), but the React handler **does not return immediately**.
 * Instead it parks an in-flight call in component state and exposes
 * `respond()` / `reject()` so the UI can resolve it after the user clicks.
 *
 * Returns:
 * - `pending`: the in-flight call's args + tool_call_id (or `null`).
 * - `respond(value)`: resolve the call with a payload, unparking the agent.
 * - `reject(message)`: resolve as an MCP-side error so the agent's LLM
 *   sees the failure mode and can react.
 *
 * Only one call can be pending per hook instance at a time. If the agent
 * starts a second call while the first is unresolved, the second one
 * supersedes the first (the first one's promise resolves with an error).
 *
 * @example
 * ```tsx
 * const confirm = useAcpHumanInTheLoop({
 *   name: "delete_file",
 *   parameters: z.object({ path: z.string() }),
 * });
 *
 * return (
 *   <>
 *     <CopilotChat />
 *     {confirm.pending && (
 *       <Modal>
 *         <p>Delete {confirm.pending.args.path}?</p>
 *         <button onClick={() => confirm.respond("ok")}>Yes</button>
 *         <button onClick={() => confirm.reject("user declined")}>No</button>
 *       </Modal>
 *     )}
 *   </>
 * );
 * ```
 */
export function useAcpHumanInTheLoop<Args extends Record<string, unknown>>(opts: {
  name: string;
  description?: string;
  parameters?: StandardSchemaV1<unknown, Args>;
  mcpServerName?: string;
}): {
  pending: { toolCallId: string; args: Args } | null;
  respond: (value: unknown) => void;
  reject: (message: string) => void;
} {
  const [pending, setPending] = useState<{
    toolCallId: string;
    args: Args;
  } | null>(null);

  // The in-flight call's resolver. We always have at most one — see
  // class doc.
  const resolverRef = useRef<{
    resolve: (value: unknown) => void;
    reject: (err: Error) => void;
  } | null>(null);

  useEffect(() => {
    return () => {
      const resolver = resolverRef.current;
      resolverRef.current = null;
      resolver?.reject(
        new Error("human-in-the-loop request cancelled: component unmounted"),
      );
    };
  }, []);

  // Adopt a new in-flight call. If a prior call was unresolved, fail it.
  const adopt = useCallback(
    (toolCallId: string, args: Args) => {
      const prior = resolverRef.current;
      if (prior) {
        prior.reject(new Error("superseded by a newer tool call"));
      }
      return new Promise<unknown>((resolve, reject) => {
        resolverRef.current = { resolve, reject };
        setPending({ toolCallId, args });
      });
    },
    [setPending],
  );

  const respond = useCallback((value: unknown) => {
    const r = resolverRef.current;
    resolverRef.current = null;
    setPending(null);
    if (r) r.resolve(value);
  }, []);

  const reject = useCallback((message: string) => {
    const r = resolverRef.current;
    resolverRef.current = null;
    setPending(null);
    if (r) r.reject(new Error(message));
  }, []);

  useAcpFrontendTool<Args>({
    name: opts.name,
    description: opts.description,
    parameters: opts.parameters,
    mcpServerName: opts.mcpServerName,
    handler: async (args) => {
      // Generate a synthetic id local to the hook — the real one is
      // owned by the bridge, but we expose this so the UI can render
      // distinct dialogs for distinct calls (UUID is good enough for
      // a key prop).
      const localId =
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID()
          : `hil-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      return await adopt(localId, args);
    },
  });

  return { pending, respond, reject };
}
