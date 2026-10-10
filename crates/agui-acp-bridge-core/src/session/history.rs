use super::*;

// ponytail: keep transient history/list budgets local until the existing
// BridgeConfig surface grows dedicated values for these operations.
pub(super) const MAX_LOAD_HISTORY_EVENTS: usize = 4096;
pub(super) const MAX_LOAD_HISTORY_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_LIST_SESSIONS: usize = 10_000;
pub(super) const MAX_LIST_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_LIST_PAGES: usize = 1000;

/// Bounded spill for `session/update` notifications that arrive with no
/// active prompt. ACP treats out-of-turn updates as protocol violations, but
/// they can also precede the actor's slot install by a scheduling hair —
/// dropping them silently violates the "no silent drops" contract. The next
/// prompt drains this buffer ahead of its own events; overflow warns and
/// drops (the session state is already beyond recovery at that point).
///
/// ponytail: spill lives on the actor's flush path, not a dedicated side
/// channel; if out-of-turn volume ever matters, replace with an unbounded
/// dedicated stream wired through `BridgeStreamItem`.
pub(super) const SPILL_CAPACITY: usize = 32;

pub(super) type SpillBuffer = Arc<Mutex<Vec<agent_client_protocol::schema::v1::SessionUpdate>>>;

/// Captures `session/update` notifications replayed by the agent during a
/// `session/load` call. The agent streams the conversation history as
/// notifications *before* any prompt is active, so they would otherwise be
/// dropped ("no active prompt"). When `Some`, the notification handler
/// appends each update here instead; the actor flushes the buffer onto the
/// first prompt's stream so the resuming client sees its prior conversation.
#[derive(Debug, Default)]
pub(super) struct LoadHistory {
    pub(super) updates: Vec<agent_client_protocol::schema::v1::SessionUpdate>,
    pub(super) bytes: usize,
    pub(super) exceeded: bool,
}

impl LoadHistory {
    pub(super) fn append(
        &mut self,
        update: agent_client_protocol::schema::v1::SessionUpdate,
        bytes: usize,
    ) -> Result<(), ()> {
        if self.exceeded
            || self.updates.len() >= MAX_LOAD_HISTORY_EVENTS
            || bytes > MAX_LOAD_HISTORY_BYTES
            || self.bytes > MAX_LOAD_HISTORY_BYTES - bytes
        {
            self.exceeded = true;
            return Err(());
        }
        self.bytes += bytes;
        self.updates.push(update);
        Ok(())
    }
}

pub(super) type LoadBuffer = Arc<Mutex<Option<LoadHistory>>>;

#[derive(Debug, Default)]
pub(super) struct BoundedSessionList {
    summaries: Vec<SessionSummary>,
    bytes: usize,
}

impl BoundedSessionList {
    pub(super) fn push(&mut self, summary: SessionSummary) -> Result<(), BridgeError> {
        let bytes = serde_json::to_vec(&summary)
            .map_err(BridgeError::Json)?
            .len();
        self.push_with_size(summary, bytes)
    }

    pub(super) fn push_with_size(
        &mut self,
        summary: SessionSummary,
        bytes: usize,
    ) -> Result<(), BridgeError> {
        if self.summaries.len() >= MAX_LIST_SESSIONS {
            return Err(session_limit_error("MAX_LIST_SESSIONS", MAX_LIST_SESSIONS));
        }
        if bytes > MAX_LIST_BYTES || self.bytes > MAX_LIST_BYTES - bytes {
            return Err(session_limit_error("MAX_LIST_BYTES", MAX_LIST_BYTES));
        }
        self.bytes += bytes;
        self.summaries.push(summary);
        Ok(())
    }

    pub(super) fn len(&self) -> usize {
        self.summaries.len()
    }

    pub(super) fn into_summaries(self) -> Vec<SessionSummary> {
        self.summaries
    }
}

pub(super) fn next_list_cursor(
    pages: usize,
    next: Option<String>,
) -> Result<Option<String>, BridgeError> {
    match next {
        Some(next) if pages < MAX_LIST_PAGES => Ok(Some(next)),
        Some(_) => Err(session_limit_error("MAX_LIST_PAGES", MAX_LIST_PAGES)),
        None => Ok(None),
    }
}

/// Bridge budget violation for `session/list`. Reports `-32603`
/// (internal error) with structured data naming the actual limit —
/// `-32800` (`request_cancelled`) would falsely imply the caller cancelled.
pub(super) fn session_limit_error(limit: &'static str, cap: usize) -> BridgeError {
    BridgeError::Acp(
        agent_client_protocol::Error::internal_error()
            .data(serde_json::json!({ "limit": limit, "cap": cap })),
    )
}
pub(super) fn load_history_limit_error() -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(serde_json::json!({
        "limit": "MAX_LOAD_HISTORY",
        "cap": { "events": MAX_LOAD_HISTORY_EVENTS, "bytes": MAX_LOAD_HISTORY_BYTES }
    }))
}
