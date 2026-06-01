use std::time::Duration;

/// Runtime tuning knobs for the bridge.
///
/// All values are wired:
/// - `permission_timeout` — applied by the session actor when a `Defer`
///   policy decision awaits external resolution; on timeout the bridge
///   responds `Cancelled` to the agent.
/// - `idle_timeout` — applied by the bridge's idle-session reaper (see
///   `BridgeAppState::spawn_reaper` in `agui-acp-bridge-server`), which
///   drops sessions that have been unused for longer than this duration.
///   Sessions with active in-flight prompts are skipped regardless of
///   `last_used` so long-running turns are not killed mid-flight.
/// - `event_buffer` — sets the bound of the per-prompt
///   [`BridgeStreamItem`](crate::stream::BridgeStreamItem) mpsc channel
///   (and the SSE-side translated-event channel). Larger values absorb
///   chatty agents at the cost of more memory; smaller values back-pressure
///   the agent task.
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
/// - `set_session_timeout` — applied to ACP `session/set_mode` and
///   `session/set_model` requests issued from the bridge's HTTP API. If
///   the agent does not respond within this budget the bridge returns
///   `503 Service Unavailable` to the HTTP caller rather than hanging
///   the request indefinitely. Tunable independently from
///   `open_session_timeout` because some agents take longer to apply a
///   model switch than to open a session.
/// - `max_sessions` — hard cap on the number of concurrently cached ACP
///   sessions. `0` means **unlimited** (the default, preserving the
///   historical behaviour). When non-zero, creating a session that would
///   exceed the cap first evicts the least-recently-used **idle** session
///   (one with no in-flight prompt). This bounds resource usage —
///   crucially the number of agent subprocesses — when clients churn
///   through many distinct `thread_id`s (e.g. a browser that mints a fresh
///   thread on every page refresh). In-flight prompts are never evicted;
///   if every cached session is busy the new session is allowed through
///   rather than killing live work.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub permission_timeout: Duration,
    pub idle_timeout: Duration,
    pub event_buffer: usize,
    pub open_session_timeout: Duration,
    pub frontend_tool_timeout: Duration,
    pub set_session_timeout: Duration,
    pub max_sessions: usize,
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
            open_session_timeout: Duration::from_secs(30),
            // Default 2 minutes: long enough for human-in-the-loop tools
            // (think confirmation dialogs), short enough to fail fast on
            // an unresponsive frontend rather than hold the agent's
            // session open indefinitely.
            frontend_tool_timeout: Duration::from_secs(2 * 60),
            // Default 30s: in-line with `open_session_timeout`. ACP
            // `session/set_mode` / `session/set_model` are RPC-style
            // requests; an agent that takes >30s is misbehaving.
            set_session_timeout: Duration::from_secs(30),
            // Default 0 = unlimited, preserving historical behaviour.
            // Operators fronting many short-lived browser threads should
            // set this (the CLI defaults it to 128) so churned-through
            // sessions can't accumulate unbounded agent subprocesses.
            max_sessions: 0,
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
        assert_eq!(c.open_session_timeout, Duration::from_secs(30));
        assert_eq!(c.frontend_tool_timeout, Duration::from_secs(120));
        assert_eq!(c.set_session_timeout, Duration::from_secs(30));
        assert_eq!(c.max_sessions, 0);
    }
}
