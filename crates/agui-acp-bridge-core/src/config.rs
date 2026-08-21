use std::time::Duration;

/// Runtime tuning knobs for the bridge.
///
/// All values are wired:
/// - `permission_timeout` — applied by the session actor when a `Defer`
///   policy decision awaits external resolution; on timeout the bridge
///   responds `Cancelled` to the agent.
/// - `idle_timeout` — applied by the bridge's idle-session reaper (see
///   `BridgeAppState::spawn_reaper` in `agui-acp-bridge-server`), which
///   gracefully closes and then drops sessions unused for longer than this
///   duration. Sessions with active/queued prompts, settings, permissions, or
///   frontend work are skipped regardless of `last_used` so live operations
///   are not killed mid-flight.
/// - `event_buffer` — sets the bound of the per-prompt
///   [`BridgeStreamItem`](crate::stream::BridgeStreamItem) mpsc channel
///   (and the SSE-side translated-event channel). Larger values absorb
///   chatty agents at the cost of more memory; smaller values back-pressure
///   the agent task.
/// - `slow_consumer_timeout` — maximum time each SSE event send waits for the
///   downstream consumer. When this expires the current prompt is cancelled
///   instead of leaving the stream task and session pinned. `0` explicitly
///   disables this protection and restores an unbounded send wait.
/// - `open_session_timeout` — applied to the lazy session creation path
///   (`AcpClient::open_session`). If the agent does not complete the ACP
///   handshake within this budget, the bridge returns an error rather than
///   hanging the request indefinitely.
/// - `frontend_tool_timeout` — applied by the in-process MCP endpoint when
///   awaiting the browser's response to a tool call (`useFrontendTool` /
///   `useAcpFrontendTool`). On timeout the bridge returns an MCP-side
///   `isError: true` envelope to the agent so the LLM can react instead
///   of hanging on a parked request. Independent from
///   `permission_timeout`, which applies only to ACP `requestPermission`.
/// - `set_session_timeout` — applied to ACP `session/set_mode`,
///   `session/set_config_option`, and capability-aware `session/close`
///   requests issued from the bridge's HTTP API or lifecycle workers. If the
///   agent does not respond within this budget the bridge returns an error to
///   the HTTP caller rather than hanging the request indefinitely. Tunable
///   independently from `open_session_timeout` because some agents take
///   longer to apply a session operation than to open a session.
/// - `cancel_grace_timeout` — the maximum time the session actor waits for an
///   ACP `session/prompt` response after sending `session/cancel`. If the
///   agent does not finish within this grace window, the session is marked
///   unusable and its connection is closed instead of leaving AG-UI waiting
///   forever.
/// - `max_sessions` — hard cap on the number of concurrently cached ACP
///   sessions. `0` means **unlimited** as an explicit development mode. When
///   non-zero, creating a session that would exceed the cap first evicts the
///   least-recently-used **idle** session (no prompt/queue/permission/setting
///   or pending frontend work). If every cached session is busy, creation is
///   rejected before the ACP actor or subprocess is opened.
/// - `max_queued_turns` — hard cap on each session's active-plus-queued prompt
///   turns. `0` means **unlimited** as an explicit development mode. A full
///   queue rejects the new turn before sending an actor command; existing
///   active and queued turns remain untouched.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub permission_timeout: Duration,
    pub idle_timeout: Duration,
    pub event_buffer: usize,
    pub slow_consumer_timeout: Duration,
    pub open_session_timeout: Duration,
    pub frontend_tool_timeout: Duration,
    pub set_session_timeout: Duration,
    pub cancel_grace_timeout: Duration,
    pub max_sessions: usize,
    pub max_queued_turns: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            permission_timeout: Duration::from_secs(5 * 60),
            // Default 2 minutes. Sessions hold an agent subprocess open, so a
            // short idle window keeps resource usage bounded when clients
            // churn through threads. Long-running prompts are never reaped
            // (the reaper skips entries with `active_prompts > 0`), so this
            // only affects genuinely idle sessions. Bump it for deployments
            // where re-establishing a session is expensive.
            idle_timeout: Duration::from_secs(2 * 60),
            event_buffer: 64,
            // Bound each downstream SSE send so a stalled browser cannot pin
            // a prompt forever. Set to zero only for explicit legacy mode.
            slow_consumer_timeout: Duration::from_secs(30),
            open_session_timeout: Duration::from_secs(30),
            // Default 2 minutes: long enough for human-in-the-loop tools
            // (think confirmation dialogs), short enough to fail fast on
            // an unresponsive frontend rather than hold the agent's
            // session open indefinitely.
            frontend_tool_timeout: Duration::from_secs(2 * 60),
            // Default 30s: in-line with `open_session_timeout`. ACP session
            // setting requests are RPC-style requests; an agent that takes
            // >30s is misbehaving.
            set_session_timeout: Duration::from_secs(30),
            // A cancelled prompt should normally finish immediately. Keep a
            // short, configurable window for its final updates before the
            // bridge discards an unknown session state.
            cancel_grace_timeout: Duration::from_secs(5),
            // A finite library default keeps accidental multi-threaded use
            // from opening unbounded agent subprocesses. Set to 0 only for
            // explicit development/unlimited mode.
            max_sessions: 128,
            // Bound the active-plus-queued prompt turns per session. Set to 0
            // only for explicit development/unlimited mode.
            max_queued_turns: 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_plan() {
        let c = BridgeConfig::default();
        assert_eq!(c.permission_timeout, Duration::from_secs(300));
        assert_eq!(c.idle_timeout, Duration::from_secs(120));
        assert_eq!(c.event_buffer, 64);
        assert_eq!(c.slow_consumer_timeout, Duration::from_secs(30));
        assert_eq!(c.open_session_timeout, Duration::from_secs(30));
        assert_eq!(c.frontend_tool_timeout, Duration::from_secs(120));
        assert_eq!(c.set_session_timeout, Duration::from_secs(30));
        assert_eq!(c.cancel_grace_timeout, Duration::from_secs(5));
        assert_eq!(c.max_sessions, 128);
        assert_eq!(c.max_queued_turns, 32);
    }
}
