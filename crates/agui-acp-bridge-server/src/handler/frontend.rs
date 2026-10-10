use super::*;

pub(super) struct OpeningMcpCredential {
    pub(super) inner: Arc<Inner>,
    pub(super) thread_id: String,
    pub(super) credential: Arc<McpCredential>,
    pub(super) committed: bool,
}

impl Drop for OpeningMcpCredential {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self
                .inner
                .mcp_credentials
                .remove_if(&self.thread_id, |_, current| {
                    Arc::ptr_eq(current, &self.credential)
                });
        }
    }
}

impl BridgeAppState {
    /// Frontend-tool registry. Used by the MCP HTTP endpoint to read the
    /// per-thread tool list, register pending calls, and route results
    /// back into the live SSE stream.
    pub fn frontend_tools(&self) -> &FrontendToolRegistry {
        &self.inner.frontend_tools
    }

    pub(super) fn bearer_token(&self) -> Option<Arc<str>> {
        self.inner.bearer_token.clone()
    }

    pub(crate) fn mcp_origin_allowed(&self, headers: &HeaderMap) -> bool {
        crate::mcp_endpoint::origin_is_allowed(headers, &self.inner.mcp_allowed_origins)
    }

    pub(crate) fn mcp_allowed_origins(&self) -> Arc<HashSet<String>> {
        Arc::new(self.inner.mcp_allowed_origins.clone())
    }

    /// Headers handed to the ACP agent's MCP HTTP endpoint.
    ///
    /// MCP requests use a per-session opening credential, not the bridge's
    /// admin bearer token. The credential is scoped to this thread's MCP
    /// endpoint and is revoked when its session is removed.
    pub(super) fn mcp_headers(&self, credential: Option<&McpCredential>) -> Vec<HttpHeader> {
        credential
            .map(|token| {
                vec![HttpHeader::new(
                    "Authorization",
                    format!("Bearer {}", token.0),
                )]
            })
            .unwrap_or_default()
    }

    pub(crate) fn mcp_credential_valid(&self, thread_id: &str, headers: &HeaderMap) -> bool {
        if self.inner.bearer_token.is_none() {
            return true;
        }
        let Some(expected) = self.inner.mcp_credentials.get(thread_id) else {
            return false;
        };
        has_valid_bearer(headers, expected.0.as_bytes())
    }

    /// Resolve a frontend tool call posted back from the browser in its
    /// owning thread. Returns `true` if a pending entry existed and was
    /// consumed.
    pub fn resolve_frontend_tool(
        &self,
        thread_id: &str,
        tool_call_id: &str,
        response: FrontendToolResponse,
    ) -> bool {
        self.inner
            .frontend_tools
            .resolve_for_thread(thread_id, tool_call_id, response)
    }

    pub(super) fn session_config_for(&self, thread_token: &str) -> SessionConfig {
        self.session_config_for_with(thread_token, None)
    }

    pub(super) fn encode_mcp_path_segment(segment: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut encoded = String::with_capacity(segment.len());
        for byte in segment.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                encoded.push(byte as char);
            } else {
                encoded.push('%');
                encoded.push(HEX[(byte >> 4) as usize] as char);
                encoded.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
        encoded
    }

    pub(super) fn session_config_for_with(
        &self,
        thread_token: &str,
        load_session_id: Option<SessionId>,
    ) -> SessionConfig {
        self.session_config_for_with_cwd(thread_token, self.inner.cwd.clone(), load_session_id)
    }

    pub(super) fn session_config_for_with_cwd(
        &self,
        thread_token: &str,
        cwd: PathBuf,
        load_session_id: Option<SessionId>,
    ) -> SessionConfig {
        let mcp_url = self
            .inner
            .self_url
            .as_ref()
            .map(|base| format!("{base}/mcp/{}", Self::encode_mcp_path_segment(thread_token)));
        let mcp_headers = mcp_url
            .as_ref()
            .map(|_| self.mcp_headers(None))
            .unwrap_or_default();
        SessionConfig {
            cwd,
            policy: self.inner.policy.clone(),
            config: self.inner.config.clone(),
            mcp_url,
            mcp_headers,
            load_session_id,
        }
    }

    pub(super) fn session_config_for_open(
        &self,
        thread: &str,
        cwd: PathBuf,
        load: Option<SessionId>,
        credential: Option<&McpCredential>,
    ) -> SessionConfig {
        let mut config = self.session_config_for_with_cwd(thread, cwd, load);
        if config.mcp_url.is_some() {
            config.mcp_headers = self.mcp_headers(credential);
        }
        config
    }
}
///
/// The clear is **conditional** ([`ThreadEntry::clear_active_sender_if_same`]):
/// it only nulls the slot if it still holds the sender this run installed.
/// This prevents an older run's teardown from wiping a newer overlapping
/// run's sender on the same `thread_id`, which would otherwise strand the
/// newer run's in-flight frontend tool calls until they time out. The same
/// guard also covers cancellation or prompt-creation failure before the SSE
/// task takes ownership of the setup.
pub(super) struct ClearOnDrop {
    pub(super) entry: Arc<agui_acp_bridge_core::frontend_tools::ThreadEntry>,
    pub(super) sender: tokio::sync::mpsc::Sender<BridgeStreamItem>,
}

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        // Only act if the slot is still ours. If a newer overlapping run on
        // the same thread_id took over the sender, it now owns the pending
        // calls too, so we must not disturb them.
        if self.entry.clear_active_sender_if_same(&self.sender) {
            // We were the active run and we're going away (finished, errored,
            // or the client disconnected). Unblock any frontend-tool call
            // still parked on a oneshot so the agent's turn can unwind
            // instead of pinning the session until `frontend_tool_timeout`.
            self.entry
                .drain_pending("AG-UI run ended before tool resolved");
        }
    }
}
