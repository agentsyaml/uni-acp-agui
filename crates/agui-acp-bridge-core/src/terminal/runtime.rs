use super::CONTROL_BUFFER;
use super::cleanup::spawn_child_reaper;
use super::process_tree::ProcessTree;
use super::registry::{TerminalError, TerminalLifecycle, TerminalResource};
use super::supervisor::supervise;
use agent_client_protocol::schema::v1::{TerminalExitStatus, TerminalOutputResponse};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::process::Child;
use tokio::sync::{Notify, mpsc, oneshot};

#[derive(Debug)]
pub(super) struct Terminal {
    state: Arc<TerminalState>,
    control: mpsc::Sender<Control>,
    process_tree: Arc<ProcessTree>,
    lifecycle: Arc<TerminalLifecycle>,
}

impl Terminal {
    pub(super) fn new(
        child: Child,
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        output_limit: usize,
        resource: Arc<TerminalResource>,
    ) -> Result<Self, ()> {
        let process_tree = match ProcessTree::new(&child) {
            Ok(process_tree) => Arc::new(process_tree),
            Err(()) => {
                spawn_child_reaper(child, stdout, stderr);
                return Err(());
            }
        };
        #[cfg(windows)]
        if process_tree.resume(&child).is_err() {
            spawn_child_reaper(child, stdout, stderr);
            return Err(());
        }
        let state = Arc::new(TerminalState::new(output_limit));
        let (control, control_rx) = mpsc::channel(CONTROL_BUFFER);
        let lifecycle = Arc::new(TerminalLifecycle {
            resource,
            abort_signal: Arc::new(Notify::new()),
        });
        tokio::spawn(supervise(
            child,
            stdout,
            stderr,
            state.clone(),
            process_tree.clone(),
            lifecycle.clone(),
            control_rx,
        ));
        Ok(Self {
            state,
            control,
            process_tree,
            lifecycle,
        })
    }

    pub(super) fn abort(&self) {
        let _ = self.process_tree.terminate();
        // Keep the supervisor alive to own the final Child::wait(). The
        // notification is retained even if the task is between select arms.
        self.lifecycle.abort_signal.notify_one();
    }

    pub(super) async fn output(&self) -> Result<TerminalOutputResponse, TerminalError> {
        self.state.wait_for_output_completion().await?;
        self.state
            .snapshot()
            .map(|(output, truncated, exit_status)| {
                TerminalOutputResponse::new(String::from_utf8_lossy(&output), truncated)
                    .exit_status(exit_status)
            })
    }

    pub(super) async fn wait(&self) -> Result<TerminalExitStatus, TerminalError> {
        self.state.wait().await
    }

    pub(super) async fn kill(&self) -> Result<(), TerminalError> {
        self.send_control(Control::Kill).await
    }

    pub(super) async fn release(&self) -> Result<(), TerminalError> {
        self.send_control(Control::Release).await
    }

    async fn send_control(
        &self,
        make_control: fn(oneshot::Sender<Result<(), ()>>) -> Control,
    ) -> Result<(), TerminalError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(make_control(reply_tx))
            .await
            .map_err(|_| TerminalError::Internal)?;
        reply_rx
            .await
            .map_err(|_| TerminalError::Internal)?
            .map_err(|_| TerminalError::Internal)
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Debug)]
pub(super) struct TerminalState {
    pub(super) inner: Mutex<TerminalStateInner>,
    notify: Notify,
}

#[derive(Debug)]
pub(super) struct TerminalStateInner {
    pub(super) output: VecDeque<u8>,
    output_limit: usize,
    truncated: bool,
    // Direct-child status is available to waiters as soon as wait(2) returns;
    // terminal completion below is held until owned output readers finish.
    process_status: Option<Result<TerminalExitStatus, ()>>,
    exit_status: Option<Result<TerminalExitStatus, ()>>,
}

impl TerminalState {
    pub(super) fn new(output_limit: usize) -> Self {
        Self {
            inner: Mutex::new(TerminalStateInner {
                output: VecDeque::new(),
                output_limit,
                truncated: false,
                process_status: None,
                exit_status: None,
            }),
            notify: Notify::new(),
        }
    }

    pub(super) fn append_output(&self, bytes: &[u8]) {
        if let Ok(mut inner) = self.inner.lock() {
            let old_len = inner.output.len();
            let total_len = old_len + bytes.len();
            if total_len <= inner.output_limit {
                inner.output.extend(bytes);
            } else {
                inner.truncated = true;
                let discard = total_len - inner.output_limit;
                if discard < old_len {
                    for _ in 0..discard {
                        inner.output.pop_front();
                    }
                    inner.output.extend(bytes);
                } else {
                    inner.output.clear();
                    let skip = discard - old_len;
                    inner.output.extend(bytes.get(skip..).unwrap_or_default());
                }
                while inner
                    .output
                    .front()
                    .is_some_and(|byte| byte & 0b1100_0000 == 0b1000_0000)
                {
                    inner.output.pop_front();
                }
            }
        }
        self.notify.notify_waiters();
    }

    pub(super) fn complete(&self, status: Result<TerminalExitStatus, ()>) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.exit_status.is_none()
        {
            inner.exit_status = Some(status);
        }
        self.notify.notify_waiters();
    }

    pub(super) fn process_exit(&self, status: Result<TerminalExitStatus, ()>) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.process_status.is_none()
        {
            inner.process_status = Some(status);
        }
        self.notify.notify_waiters();
    }

    pub(super) fn mark_unavailable(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.process_status = Some(Err(()));
            inner.exit_status = Some(Err(()));
        }
        self.notify.notify_waiters();
    }

    pub(super) fn completion_result(&self) -> Result<(), ()> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.exit_status.clone())
            .map(|status| status.map(|_| ()).map_err(|_| ()))
            .unwrap_or(Err(()))
    }

    pub(super) fn snapshot(
        &self,
    ) -> Result<(Vec<u8>, bool, Option<TerminalExitStatus>), TerminalError> {
        let inner = self.inner.lock().map_err(|_| TerminalError::Internal)?;
        let exit_status = match &inner.exit_status {
            Some(Ok(status)) => Some(status.clone()),
            Some(Err(())) => return Err(TerminalError::Internal),
            None => None,
        };
        Ok((
            inner.output.iter().copied().collect(),
            inner.truncated,
            exit_status,
        ))
    }

    pub(super) async fn wait(&self) -> Result<TerminalExitStatus, TerminalError> {
        // Wait reports the direct child promptly; terminal/output applies the
        // separate reader-drain barrier before exposing completion.
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            let status = self
                .inner
                .lock()
                .map_err(|_| TerminalError::Internal)?
                .process_status
                .clone();
            if let Some(status) = status {
                return status.map_err(|_| TerminalError::Internal);
            }
            notified.await;
        }
    }

    async fn wait_for_output_completion(&self) -> Result<(), TerminalError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            let (process_exited, completed) = self
                .inner
                .lock()
                .map_err(|_| TerminalError::Internal)
                .map(|inner| (inner.process_status.is_some(), inner.exit_status.is_some()))?;
            if completed || !process_exited {
                return Ok(());
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
pub(super) enum Control {
    Kill(oneshot::Sender<Result<(), ()>>),
    Release(oneshot::Sender<Result<(), ()>>),
}
