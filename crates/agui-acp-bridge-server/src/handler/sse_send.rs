use std::sync::Arc;

use agui_rs_core::events::Event;
use agui_rs_server::error::Result as AgUiResult;
use tokio::sync::mpsc;

use agui_acp_bridge_core::acp::{AcpSessionHandle, TurnId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SseSendError {
    Closed,
    ActorClosed,
    TimedOut,
}

pub(super) const RETIRED_SSE_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Default)]
pub(super) struct RetiredSseDrain {
    deadline: Option<tokio::time::Instant>,
}

/// Send one item to the downstream SSE channel without allowing a stalled
/// consumer to pin the stream task forever. A zero duration is the explicit
/// legacy/unlimited mode documented on `BridgeConfig`.
pub(super) async fn send_sse_with_timeout<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    timeout: std::time::Duration,
) -> Result<(), SseSendError> {
    if timeout.is_zero() {
        return tx.send(item).await.map_err(|_| SseSendError::Closed);
    }
    match tokio::time::timeout(timeout, tx.send(item)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(SseSendError::Closed),
        Err(_) => Err(SseSendError::TimedOut),
    }
}

async fn send_sse_while_session<T>(
    tx: &mpsc::Sender<T>,
    item: T,
    timeout: std::time::Duration,
    session: &AcpSessionHandle,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    if !timeout.is_zero() {
        return send_sse_with_timeout(tx, item, timeout).await;
    }
    let send = tx.send(item);
    tokio::pin!(send);
    loop {
        if let Some(deadline) = retired.deadline {
            tokio::select! {
                biased;
                result = &mut send => return result.map_err(|_| SseSendError::Closed),
                () = tokio::time::sleep_until(deadline) => return Err(SseSendError::ActorClosed),
            }
        }
        tokio::select! {
            biased;
            result = &mut send => return result.map_err(|_| SseSendError::Closed),
            () = session.closed() => retired.deadline = Some(tokio::time::Instant::now() + RETIRED_SSE_DRAIN_GRACE),
        }
    }
}

fn log_sse_send_failure(
    failure: SseSendError,
    thread_id: &str,
    run_id: &str,
    timeout: std::time::Duration,
    stream_kind: &'static str,
) {
    match failure {
        SseSendError::TimedOut => {
            tracing::warn!(
                thread_id,
                run_id,
                timeout_ms = timeout.as_millis() as u64,
                stream_kind,
                "SSE slow consumer timeout"
            );
        }
        SseSendError::Closed => {
            tracing::debug!(thread_id, run_id, stream_kind, "SSE receiver closed");
        }
        SseSendError::ActorClosed => {
            tracing::debug!(
                thread_id,
                run_id,
                stream_kind,
                "ACP actor closed during SSE send"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn send_prompt_sse(
    tx: &mpsc::Sender<AgUiResult<Event>>,
    item: AgUiResult<Event>,
    timeout: std::time::Duration,
    session: &Arc<AcpSessionHandle>,
    thread_id: &str,
    run_id: &str,
    turn_id: TurnId,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    let result = send_sse_while_session(tx, item, timeout, session, retired).await;
    if let Err(failure) = result {
        log_sse_send_failure(failure, thread_id, run_id, timeout, "prompt");
        if let Err(error) = session.cancel_turn(turn_id) {
            tracing::debug!(
                thread_id,
                run_id,
                error = %error,
                "SSE teardown could not cancel ACP turn"
            );
        }
    }
    result
}

pub(super) async fn send_history_sse(
    tx: &mpsc::Sender<AgUiResult<Event>>,
    item: AgUiResult<Event>,
    timeout: std::time::Duration,
    thread_id: &str,
    run_id: &str,
    session: &AcpSessionHandle,
    retired: &mut RetiredSseDrain,
) -> Result<(), SseSendError> {
    let result = send_sse_while_session(tx, item, timeout, session, retired).await;
    if let Err(failure) = result {
        log_sse_send_failure(failure, thread_id, run_id, timeout, "history");
    }
    result
}
