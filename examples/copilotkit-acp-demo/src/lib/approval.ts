export type ApprovalDecision = {
  /** Owning thread of the interrupt; the bridge requires it (422 otherwise). */
  threadId?: string | null;
  interruptId: string;
  approved: boolean;
  optionId?: string;
};

/**
 * Body for the bridge's thread-scoped `POST /approval` endpoint.
 *
 * Returns `null` when `threadId` is missing: the bridge's `ApprovalRequest`
 * has `threadId` as a REQUIRED field, so a thread-less body is rejected by
 * the JSON extractor with HTTP 422 before any handler runs. Callers must
 * disable their approve/deny controls in that state instead of posting.
 */
export function approvalRequestBody(decision: ApprovalDecision): {
  threadId: string;
  interruptId: string;
  approved: boolean;
  optionId?: string;
} | null {
  if (!decision.threadId) return null;
  return {
    threadId: decision.threadId,
    interruptId: decision.interruptId,
    approved: decision.approved,
    // Dropped by JSON.stringify when undefined.
    optionId: decision.optionId,
  };
}
