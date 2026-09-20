/**
 * Map an AG-UI `RUN_ERROR` code to a friendly message.
 *
 * Client-safe (no `server-only` import) so run-event subscribers in any
 * component can use it. Unknown codes return `null` so callers fall back to
 * the generic rendering (the bridge's own error message).
 */
export function friendlyRunErrorMessage(code?: string): string | null {
  if (code === "CONCURRENT_RUN") {
    return "A run is already in progress for this conversation — wait for it to finish";
  }
  return null;
}
