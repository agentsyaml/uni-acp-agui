use super::*;

impl BridgeAppState {
    /// Construct with default `BridgeConfig` and an `AutoDeny` policy.
    /// Suitable for local development; production should use
    /// [`BridgeAppState::builder`] to configure the bearer token.
    ///
    /// `cwd` is canonicalized at construction time. If it does not exist or
    /// cannot be canonicalized, [`std::path::absolute`] is used as a
    /// fallback so the sandbox always has an absolute reference path. A
    /// non-existent cwd will fail closed for any agent path that touches
    /// the filesystem (`safe_resolve` requires the existing ancestor to be
    /// canonicalizable).
    ///
    /// Frontend-tool injection (`useFrontendTool`) is **disabled** in this
    /// constructor — `self_url` is `None`. Use the builder's `with_self_url`
    /// to enable it.
    #[must_use]
    pub fn new(client: Arc<dyn AcpClient>, cwd: PathBuf) -> Self {
        let cwd = canonicalize_cwd(&cwd).unwrap_or_else(|err| {
            tracing::warn!(error = %err, cwd = %cwd.display(),
                "cwd canonicalize failed; using path as-is");
            cwd
        });
        let config = BridgeConfig::default();
        Self {
            inner: Arc::new(Inner {
                sessions: DashMap::new(),
                mcp_credentials: DashMap::new(),
                active_runs: DashMap::new(),
                active_settings: DashMap::new(),
                create_locks: DashMap::new(),
                capacity_gate: tokio::sync::Mutex::new(()),
                sessions_list_cache: tokio::sync::Mutex::new(None),
                sessions_list_gate: tokio::sync::Mutex::new(()),
                session_capacity: semaphore_for(config.max_sessions),
                client,
                cwd,
                config,
                policy: Arc::new(AutoDeny),
                frontend_tools: FrontendToolRegistry::new(),
                self_url: None,
                bearer_token: None,
                mcp_allowed_origins: HashSet::new(),
                reaper: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Builder for full control over config + policy + frontend-tool
    /// injection.
    ///
    /// `cwd` is canonicalized at [`BridgeAppStateBuilder::build`] time; see
    /// [`BridgeAppState::new`] for the cwd resolution semantics.
    #[must_use]
    pub fn builder(client: Arc<dyn AcpClient>, cwd: PathBuf) -> BridgeAppStateBuilder {
        BridgeAppStateBuilder {
            client,
            cwd,
            config: BridgeConfig::default(),
            policy: Arc::new(AutoDeny),
            self_url: None,
            bearer_token: None,
            mcp_allowed_origins: HashSet::new(),
        }
    }

    /// Bridge configuration in effect for new sessions.
    #[must_use]
    pub fn config(&self) -> &BridgeConfig {
        &self.inner.config
    }

    /// Permission policy applied to ACP `requestPermission` requests.
    #[must_use]
    pub fn policy(&self) -> &Arc<dyn PermissionPolicy> {
        &self.inner.policy
    }

    /// How many sessions are currently cached. Test/observability hook.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.inner.sessions.len()
    }
}

impl std::fmt::Debug for BridgeAppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeAppState")
            .field("sessions_len", &self.inner.sessions.len())
            .field("cwd", &self.inner.cwd)
            .finish_non_exhaustive()
    }
}
/// Builder for [`BridgeAppState`] when you need a non-default `BridgeConfig`,
/// a custom `PermissionPolicy` (e.g. `AutoDeny`, `Allowlist`), or want to
/// enable frontend-tool injection via [`BridgeAppStateBuilder::with_self_url`].
pub struct BridgeAppStateBuilder {
    client: Arc<dyn AcpClient>,
    cwd: PathBuf,
    config: BridgeConfig,
    policy: Arc<dyn PermissionPolicy>,
    self_url: Option<String>,
    bearer_token: Option<Arc<str>>,
    mcp_allowed_origins: HashSet<String>,
}

impl BridgeAppStateBuilder {
    /// Override the bridge configuration (timeouts, buffer sizes).
    #[must_use]
    pub fn with_config(mut self, config: BridgeConfig) -> Self {
        self.config = config;
        self
    }

    /// Override the permission policy applied to ACP `requestPermission`
    /// requests.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Require a bearer token on every route except exact `GET/HEAD /health`.
    ///
    /// Tokens are validated before they enter bridge state; they are never
    /// logged or included in the MCP URL.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Result<Self, String> {
        let token = token.into();
        validate_bearer_token(&token)?;
        self.bearer_token = Some(Arc::from(token));
        Ok(self)
    }

    /// Restrict present MCP `Origin` headers to this explicit allowlist.
    ///
    /// Origins are canonicalized to lowercase scheme/host plus effective
    /// port. Paths, trailing slashes, wildcards, `null`, and user information
    /// are rejected. Missing `Origin` remains allowed for non-browser agents.
    pub fn with_mcp_allowed_origins<I, S>(mut self, origins: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for origin in origins {
            let origin = origin.into();
            let canonical = crate::mcp_endpoint::canonicalize_origin(&origin)
                .map_err(|error| format!("invalid MCP allowed origin {origin:?}: {error}"))?;
            self.mcp_allowed_origins.insert(canonical);
        }
        Ok(self)
    }

    /// Enable frontend-tool injection (`useFrontendTool`-style tools) by
    /// telling the bridge what URL agents should use to reach its built-in
    /// MCP HTTP endpoint.
    ///
    /// In typical local-dev setups this is `http://127.0.0.1:<port>`. For
    /// reverse-proxy deployments, point at the public origin that routes
    /// `/mcp/...` back to the bridge. Trailing slash is tolerated.
    ///
    /// When this is set, every new session is opened with `mcp_servers =
    /// [{ url: <self_url>/mcp/<thread-token> }]`, gated on the agent's
    /// `mcpCapabilities.http`.
    #[must_use]
    pub fn with_self_url(mut self, url: impl Into<String>) -> Self {
        self.self_url = Some(url.into());
        self
    }

    /// Finalize the builder into a [`BridgeAppState`].
    #[must_use]
    pub fn build(self) -> BridgeAppState {
        let cwd = canonicalize_cwd(&self.cwd).unwrap_or_else(|err| {
            tracing::warn!(error = %err, cwd = %self.cwd.display(),
                "cwd canonicalize failed; using path as-is");
            self.cwd
        });
        BridgeAppState {
            inner: Arc::new(Inner {
                sessions: DashMap::new(),
                mcp_credentials: DashMap::new(),
                active_runs: DashMap::new(),
                active_settings: DashMap::new(),
                create_locks: DashMap::new(),
                capacity_gate: tokio::sync::Mutex::new(()),
                sessions_list_cache: tokio::sync::Mutex::new(None),
                sessions_list_gate: tokio::sync::Mutex::new(()),
                session_capacity: semaphore_for(self.config.max_sessions),
                client: self.client,
                cwd,
                config: self.config,
                policy: self.policy,
                frontend_tools: FrontendToolRegistry::new(),
                self_url: self.self_url.map(|u| u.trim_end_matches('/').to_string()),
                bearer_token: self.bearer_token,
                mcp_allowed_origins: self.mcp_allowed_origins,
                reaper: std::sync::Mutex::new(None),
            }),
        }
    }
}
