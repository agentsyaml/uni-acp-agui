use super::*;

#[derive(Debug)]
pub(super) enum SessionAdmissionError {
    Http(AgUiError),
    Capacity(String),
    ResumeUnsupported(String),
    ResumeFailed(String),
    ResumeMappingMismatch(String),
}

pub(super) fn resume_open_error(error: BridgeError) -> SessionAdmissionError {
    match error {
        BridgeError::ResumeUnsupported(message) => {
            SessionAdmissionError::ResumeUnsupported(message)
        }
        BridgeError::Unsupported(message) => SessionAdmissionError::ResumeUnsupported(message),
        BridgeError::ResumeFailed(message) => SessionAdmissionError::ResumeFailed(message),
        other => SessionAdmissionError::ResumeFailed(format!("acp open_session failed: {other}")),
    }
}

pub(super) fn semaphore_for(max_sessions: usize) -> Option<Arc<Semaphore>> {
    (max_sessions != 0).then(|| Arc::new(Semaphore::new(max_sessions)))
}

/// Validate a listed ACP session's cwd for resume, returning the spelling to
/// hand back to the agent.
///
/// Containment is checked against the **canonicalized** path, so a symlink
/// pointing outside the bridge root is rejected. But the path returned is the
/// agent's own spelling: `session/load` compares the cwd it is given against
/// the cwd it persisted, and on a symlinked prefix (macOS `/var` ->
/// `private/var`, which every `std::env::temp_dir()` path goes through) the
/// canonical form differs textually, so returning it would fail every resume
/// with "session/load cwd does not match persisted cwd".
pub(super) fn resume_cwd(reported: &Path, root: &Path) -> Result<PathBuf, SessionAdmissionError> {
    if !reported.is_absolute() {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session has a non-absolute cwd".into(),
        ));
    }
    let canonical = std::fs::canonicalize(reported).map_err(|error| {
        SessionAdmissionError::ResumeFailed(format!(
            "listed ACP session cwd could not be canonicalized: {error}"
        ))
    })?;
    if !canonical.is_absolute() {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session has a non-absolute cwd".into(),
        ));
    }
    if !canonical.starts_with(root) {
        return Err(SessionAdmissionError::ResumeFailed(
            "listed ACP session cwd is outside the bridge root".into(),
        ));
    }
    Ok(reported.to_path_buf())
}

impl BridgeAppState {
    pub(super) async fn validated_resume_cwd(
        &self,
        session_id: &SessionId,
    ) -> Result<PathBuf, SessionAdmissionError> {
        let summaries = match self.list_sessions().await {
            Ok(summaries) => summaries,
            Err(BridgeError::Unsupported(message)) => {
                return Err(SessionAdmissionError::ResumeUnsupported(message));
            }
            Err(error) => {
                return Err(SessionAdmissionError::ResumeFailed(format!(
                    "acp session/list failed: {error}"
                )));
            }
        };
        let session_id_text = session_id.0.to_string();
        let Some(summary) = summaries
            .into_iter()
            .find(|summary| summary.session_id == session_id_text)
        else {
            return Err(SessionAdmissionError::ResumeFailed(format!(
                "ACP session `{session_id_text}` was not found in session/list"
            )));
        };

        resume_cwd(Path::new(&summary.cwd), &self.inner.cwd)
    }
}
impl BridgeAppState {
    pub(super) async fn session_for(
        &self,
        thread_id: &str,
    ) -> Result<Arc<SessionEntry>, SessionAdmissionError> {
        self.session_for_resume(thread_id, None).await
    }

    /// Like [`session_for`] but, on a cache miss, opens the session by
    /// **loading** the existing ACP session named by `resume` (replaying its
    /// history) instead of creating a fresh one. When `resume` is `None`, a
    /// normal `session/new` is created; a requested resume that cannot load
    /// returns an error.
    pub(super) async fn session_for_resume(
        &self,
        thread_id: &str,
        resume: Option<SessionId>,
    ) -> Result<Arc<SessionEntry>, SessionAdmissionError> {
        // Fast path: already cached.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            if existing.handle.is_unusable() {
                let expected = existing.clone();
                drop(existing);
                self.evict_unusable(thread_id, &expected);
            } else if resume
                .as_ref()
                .is_some_and(|session_id| existing.handle.session_id() != session_id)
            {
                return Err(SessionAdmissionError::ResumeMappingMismatch(
                    "requested ACP session does not match the cached thread mapping".into(),
                ));
            } else {
                existing.touch();
                return Ok(existing.clone());
            }
        }

        // Slow path: serialize concurrent first-time creators on the same
        // thread id behind a reference-counted per-key async mutex. The gate
        // stays in `create_locks` until every queued waiter has released it,
        // including waiters that follow a failed creation.
        let create_gate = self.inner.acquire_session_create_gate(thread_id).await;
        let _guard = create_gate.gate.lock.lock().await;

        // Re-check inside the critical section: another waiter may have
        // already populated the entry while we were queued for the lock.
        if let Some(existing) = self.inner.sessions.get(thread_id) {
            if existing.handle.is_unusable() {
                let expected = existing.clone();
                drop(existing);
                self.evict_unusable(thread_id, &expected);
            } else if resume
                .as_ref()
                .is_some_and(|session_id| existing.handle.session_id() != session_id)
            {
                return Err(SessionAdmissionError::ResumeMappingMismatch(
                    "requested ACP session does not match the cached thread mapping".into(),
                ));
            } else {
                existing.touch();
                return Ok(existing.clone());
            }
        }

        let resume_requested = resume.is_some();

        let load_cwd = match resume.as_ref() {
            Some(session_id) => match self.validated_resume_cwd(session_id).await {
                Ok(cwd) => cwd,
                Err(error) => {
                    self.inner.frontend_tools.drop_thread(thread_id);
                    return Err(error);
                }
            },
            None => self.inner.cwd.clone(),
        };

        let capacity_permit = match self.reserve_session_capacity().await {
            Ok(permit) => permit,
            Err(reason) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                return Err(SessionAdmissionError::Capacity(reason.to_string()));
            }
        };

        let mut opening_credential =
            if self.inner.bearer_token.is_some() && self.inner.self_url.is_some() {
                let credential = Arc::new(McpCredential(uuid::Uuid::new_v4().to_string()));
                self.inner
                    .mcp_credentials
                    .insert(thread_id.to_string(), credential.clone());
                Some(OpeningMcpCredential {
                    inner: self.inner.clone(),
                    thread_id: thread_id.to_string(),
                    credential,
                    committed: false,
                })
            } else {
                None
            };

        let handle_result = tokio::time::timeout(
            self.inner.config.open_session_timeout,
            self.inner.client.open_session(
                self.session_config_for_open(
                    thread_id,
                    load_cwd,
                    resume,
                    opening_credential
                        .as_ref()
                        .map(|guard| guard.credential.as_ref()),
                ),
            ),
        )
        .await;

        // A failed/timeout open drops `capacity_permit` here. A successful
        // open transfers it into the SessionEntry below.
        let handle = match handle_result {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                return Err(if resume_requested {
                    resume_open_error(e)
                } else {
                    SessionAdmissionError::Http(AgUiError::other(format!(
                        "acp open_session failed: {e}"
                    )))
                });
            }
            Err(_) => {
                self.inner.frontend_tools.drop_thread(thread_id);
                let message = format!(
                    "acp open_session timed out after {:?}",
                    self.inner.config.open_session_timeout
                );
                return Err(if resume_requested {
                    SessionAdmissionError::ResumeFailed(message)
                } else {
                    SessionAdmissionError::Http(AgUiError::other(message))
                });
            }
        };
        let credential = opening_credential
            .as_ref()
            .map(|guard| guard.credential.clone());
        let entry = Arc::new(SessionEntry::new_scoped(
            Arc::new(handle),
            capacity_permit,
            credential,
        ));
        self.inner
            .sessions
            .insert(thread_id.to_string(), entry.clone());
        if let Some(guard) = opening_credential.as_mut() {
            guard.committed = true;
        }
        self.inner.invalidate_sessions_list_cache().await;
        Ok(entry)
    }
}
