use super::*;
use crate::policy::PermissionDecision;
use agent_client_protocol::schema::v1::{
    ContentBlock, SessionConfigOptionValue, SessionId, TextContent,
};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{mpsc, oneshot};

/// A live ACP session.
///
/// Use [`AcpSessionHandle::prompt`] to submit a turn and receive its [`PromptStream`].
/// Use [`AcpSessionHandle::cancel`] to abort the in-flight turn.
/// Use [`AcpSessionHandle::resolve_permission`] to resolve a pending permission request.
/// Use [`AcpSessionHandle::set_mode`] and
/// [`AcpSessionHandle::set_config_option`] to change session settings.
/// Use [`AcpSessionHandle::close`] for an agent-advertised ACP
/// `session/close`; dropping the handle remains the local fallback cleanup.
///
/// Dropping the handle terminates the underlying actor and subprocess.
#[derive(Debug)]
pub struct AcpSessionHandle {
    cmd_tx: mpsc::Sender<SessionCommand>,
    pending_permissions: PendingPermissions,
    turn_queue: Arc<SessionTurnQueue>,
    unusable: Arc<AtomicBool>,
    session_id: SessionId,
    supports_close: bool,
    /// Snapshot of `SessionModeState` returned by `session/new`, kept in sync
    /// with subsequent `session/set_mode` responses and `CurrentModeUpdate`
    /// notifications. The handler reads this on each new prompt to emit a
    /// fresh `SessionInit` so reconnecting clients still see the picker.
    init_state: Arc<StdMutex<SessionInitState>>,
    event_buffer: usize,
}

/// Removes a turn admission if the command send is cancelled or fails before
/// the actor takes ownership of it. Once the command is delivered, the actor
/// owns removal on every terminal path.
struct EnqueuedTurnGuard {
    queue: Arc<SessionTurnQueue>,
    turn: Option<Arc<TurnState>>,
}

impl EnqueuedTurnGuard {
    fn new(queue: Arc<SessionTurnQueue>, turn: Arc<TurnState>) -> Self {
        Self {
            queue,
            turn: Some(turn),
        }
    }

    fn disarm(&mut self) {
        self.turn = None;
    }
}

impl Drop for EnqueuedTurnGuard {
    fn drop(&mut self) {
        if let Some(turn) = &self.turn {
            self.queue.remove(turn);
        }
    }
}

/// Per-session ACP-level capability snapshot the handle keeps cached so the
/// HTTP layer can serve picker UIs without round-tripping to the agent on
/// every request. The actor updates this when:
///
/// 1. `session/new` returns (initial set).
/// 2. `session/set_mode` / `session/set_config_option` succeeds.
/// 3. A `session/update` `CurrentModeUpdate` notification arrives (agent
///    autonomously switched mode).
#[derive(Debug, Default, Clone)]
pub struct SessionInitState {
    pub modes: Option<SessionModesInit>,
    pub models: Option<SessionModelsInit>,
    /// Complete config-option snapshot returned by `session/new` or
    /// `session/load`, then replaced by config update/set responses.
    pub config_options: Option<Vec<SessionConfigOption>>,
}

impl AcpSessionHandle {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<SessionCommand>,
        pending_permissions: PendingPermissions,
        turn_queue: Arc<SessionTurnQueue>,
        unusable: Arc<AtomicBool>,
        session_id: SessionId,
        supports_close: bool,
        init_state: Arc<StdMutex<SessionInitState>>,
        event_buffer: usize,
    ) -> Self {
        Self {
            cmd_tx,
            pending_permissions,
            turn_queue,
            unusable,
            session_id,
            supports_close,
            init_state,
            event_buffer: event_buffer.max(1),
        }
    }

    /// Submit a text prompt and receive a [`PromptStream`] scoped to that turn.
    ///
    /// The returned channels are created fresh per call; previous prompts'
    /// channels are unaffected. The actor processes prompts sequentially, so
    /// concurrent calls on the same session will queue.
    pub async fn prompt(&self, text: impl Into<String>) -> Result<PromptStream, BridgeError> {
        self.prompt_blocks(vec![ContentBlock::Text(TextContent::new(text))])
            .await
    }

    /// Submit an ordered ACP content-block prompt and receive a [`PromptStream`]
    /// scoped to that turn. Every block is forwarded to ACP unchanged and in
    /// the supplied order.
    pub async fn prompt_blocks(
        &self,
        prompt: Vec<ContentBlock>,
    ) -> Result<PromptStream, BridgeError> {
        self.prompt_blocks_with_turn(prompt)
            .await
            .map(|(prompt, _turn_id)| prompt)
    }

    /// Submit a text prompt and return its stream together with the opaque
    /// identity used to cancel exactly this queued/in-flight turn.
    ///
    /// This additive API keeps [`PromptStream`] structurally compatible for
    /// downstream struct literals and exhaustive destructuring; callers that
    /// need disconnect-scoped cancellation can opt into the turn identity.
    pub async fn prompt_with_turn(
        &self,
        text: impl Into<String>,
    ) -> Result<(PromptStream, TurnId), BridgeError> {
        self.prompt_blocks_with_turn(vec![ContentBlock::Text(TextContent::new(text))])
            .await
    }

    /// Submit an ordered ACP content-block prompt and return its stream
    /// together with the opaque identity used to cancel exactly this
    /// queued/in-flight turn.
    pub async fn prompt_blocks_with_turn(
        &self,
        prompt: Vec<ContentBlock>,
    ) -> Result<(PromptStream, TurnId), BridgeError> {
        if self.cmd_tx.is_closed() || self.is_unusable() {
            return Err(BridgeError::SessionClosed);
        }
        let (events_tx, events_rx) = mpsc::channel(self.event_buffer);
        let (finished_tx, finished_rx) = oneshot::channel();
        let turn = self
            .turn_queue
            .try_enqueue()
            .map_err(|max_queued_turns| BridgeError::QueueCapacity { max_queued_turns })?;
        let mut enqueue_guard = EnqueuedTurnGuard::new(self.turn_queue.clone(), turn.clone());
        self.cmd_tx
            .send(SessionCommand::Prompt {
                prompt,
                events_tx,
                finished_tx,
                turn: turn.clone(),
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        enqueue_guard.disarm();
        Ok((
            PromptStream {
                events: events_rx,
                finished: finished_rx,
            },
            turn.id(),
        ))
    }

    /// Cancel the in-flight turn (if any).
    ///
    /// This signals the session actor to send an ACP `session/cancel`
    /// notification to the agent. It is fire-and-forget: if no prompt is in
    /// flight the call is a no-op. The corresponding turn's [`PromptStream`]
    /// will subsequently emit `Finished { stop_reason: Cancelled }` once the
    /// agent acks.
    pub fn cancel(&self) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        if let Some(turn) = self.turn_queue.current() {
            self.cancel_turn(turn.id())?;
        }
        Ok(())
    }

    /// Cancel one specific queued or in-flight turn.
    ///
    /// The identity must have come from a [`PromptStream`] owned by this
    /// session. A turn from another session is ignored, which prevents a
    /// stale SSE cleanup task from cancelling unrelated work.
    pub fn cancel_turn(&self, turn_id: TurnId) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        if let Some(turn) = self.turn_queue.find(turn_id) {
            turn.cancel_and_drain(&self.pending_permissions);
        }
        Ok(())
    }

    /// Resolve a pending permission request by its interrupt ID.
    ///
    /// The decision must satisfy:
    /// - For `PermissionDecision::Allow { option_id }`, the `option_id` must
    ///   match one of the choices the agent advertised in the original
    ///   `RequestPermissionRequest`. If it does not, the resolution is
    ///   rejected and `false` is returned (the pending request stays in the
    ///   map until it is resolved correctly or times out).
    /// - `Deny` is always accepted.
    /// - `Defer` is rejected (the policy has already decided to defer; this
    ///   value would loop the bridge).
    ///
    /// Returns `true` if the permission was found, validated, and resolved;
    /// `false` if no pending permission with that ID exists or the decision
    /// was rejected by validation.
    #[must_use]
    pub fn resolve_permission(&self, interrupt_id: &str, decision: PermissionDecision) -> bool {
        // Reject `Defer` early without touching the map.
        if matches!(decision, PermissionDecision::Defer { .. }) {
            return false;
        }
        // Validate `Allow.option_id` against the stored set before consuming
        // the oneshot — leaving the entry in place for retry if invalid.
        if let PermissionDecision::Allow { ref option_id } = decision {
            if let Some(entry) = self.pending_permissions.get(interrupt_id) {
                if !entry.allows_option(option_id.0.as_ref()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        let turn = self
            .pending_permissions
            .get(interrupt_id)
            .map(|entry| entry.turn());
        let Some(turn) = turn else {
            return false;
        };
        let mut turn_guard = turn.pending_ids.lock().expect("turn state poisoned");
        if turn.is_cancelled() {
            return false;
        }
        // Validation passed (or it's a Deny) — consume the entry.
        let Some((_, pending)) = self.pending_permissions.remove(interrupt_id) else {
            return false;
        };
        turn_guard.remove(interrupt_id);
        pending.resolver.send(decision).is_ok()
    }

    /// Access the pending permissions map (for external resolution via REST).
    pub fn pending_permissions(&self) -> &PendingPermissions {
        &self.pending_permissions
    }

    /// Snapshot the cached init state (modes / models) for HTTP discovery.
    ///
    /// The snapshot reflects the most recent state known to the bridge:
    /// the initial offering from `session/new`, plus any updates applied
    /// after a successful `session/set_mode` / `session/set_config_option` or a
    /// `CurrentModeUpdate` notification from the agent.
    #[must_use]
    pub fn init_state(&self) -> SessionInitState {
        self.init_state.lock().expect("init_state poisoned").clone()
    }

    /// Send an ACP `session/set_mode` request and await the agent's
    /// acknowledgement. Returns `Err(BridgeError::SessionClosed)` if the
    /// actor is gone, `Err(BridgeError::Acp)` if the agent rejected the
    /// mode_id (typically because it isn't in `availableModes`).
    pub async fn set_mode(&self, mode_id: impl Into<String>) -> Result<(), BridgeError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetMode {
                mode_id: mode_id.into(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Send an ACP `session/set_config_option` request with a select/value-id
    /// payload and await the agent's acknowledgement.
    pub async fn set_config_option(
        &self,
        config_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), BridgeError> {
        self.set_config_option_value(config_id, SessionConfigOptionValue::value_id(value.into()))
            .await
    }

    /// Send an ACP `session/set_config_option` request with its typed payload
    /// and await the agent's acknowledgement.
    pub async fn set_config_option_value(
        &self,
        config_id: impl Into<String>,
        value: SessionConfigOptionValue,
    ) -> Result<(), BridgeError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::SetConfigOption {
                config_id: config_id.into(),
                value,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Whether cancellation exceeded its grace window and this ACP session
    /// must not be reused.
    #[must_use]
    pub fn is_unusable(&self) -> bool {
        self.unusable.load(Ordering::Acquire)
    }

    /// Wait until the session actor has retired and can no longer deliver
    /// events. Long-lived stream writers can select on this alongside a
    /// downstream send to escape legacy unbounded-send mode after retirement.
    pub async fn closed(&self) {
        self.cmd_tx.closed().await;
    }

    /// The real ACP session identifier returned by `session/new` or
    /// `session/load`.
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Whether the agent advertised `sessionCapabilities.close`.
    #[must_use]
    pub fn supports_close(&self) -> bool {
        self.supports_close
    }

    /// Whether no prompt is active or queued for this session.
    #[must_use]
    pub fn turn_queue_empty(&self) -> bool {
        self.turn_queue.is_empty()
    }

    /// Ask the agent to close this ACP session when it advertises
    /// `sessionCapabilities.close`.
    ///
    /// The actor bounds the request with `set_session_timeout`. A successful,
    /// failed, or timed-out close makes the handle unusable; dropping the
    /// handle remains the local cleanup mechanism. Unsupported close leaves
    /// the actor usable and sends no ACP request.
    pub async fn close(&self) -> Result<(), BridgeError> {
        if self.cmd_tx.is_closed() {
            return Err(BridgeError::SessionClosed);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::Close { ack: ack_tx })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        ack_rx.await.map_err(|_| BridgeError::SessionClosed)?
    }

    /// Flush any history captured from a `session/load` onto a fresh
    /// [`PromptStream`] **without** prompting the agent.
    ///
    /// Used for "resume bootstrap" runs: the AG-UI client opens a previously
    /// persisted thread and expects to see its prior conversation, but is not
    /// submitting a new turn. The returned stream replays the loaded history
    /// (if any) and then finishes immediately. If the session was not resumed
    /// (no buffered history), the stream simply finishes with no updates.
    pub async fn drain_history(&self) -> Result<PromptStream, BridgeError> {
        let (events_tx, events_rx) = mpsc::channel(self.event_buffer);
        let (finished_tx, finished_rx) = oneshot::channel();
        self.cmd_tx
            .send(SessionCommand::DrainHistory {
                events_tx,
                finished_tx,
            })
            .await
            .map_err(|_| BridgeError::SessionClosed)?;
        Ok(PromptStream {
            events: events_rx,
            finished: finished_rx,
        })
    }
}
