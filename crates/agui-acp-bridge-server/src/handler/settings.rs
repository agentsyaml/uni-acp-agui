use super::*;

impl BridgeAppState {
    /// Resolve a deferred permission request for the session bound to a
    /// specific thread.
    ///
    /// The route body supplies `thread_id` — the same thread id that produced
    /// the pending interrupt — so one client's stale/malicious interrupt id
    /// can never consume a pending permission parked on a *different* thread
    /// (the cross-session hole the previous global scan left open). Mirrors
    /// the thread-scoping of `FrontendToolRegistry::resolve_for_thread`.
    /// Validates the decision against the agent-advertised option set (see
    /// [`AcpSessionHandle::resolve_permission`] for details). Returns:
    /// - `ResolveOutcome::Resolved` — the decision was accepted and delivered.
    /// - `ResolveOutcome::InvalidOption` — an `Allow` decision named an
    ///   `option_id` the agent did not offer; the entry remains pending.
    /// - `ResolveOutcome::NotFound` — the thread has no live session, or no
    ///   pending permission with that id (already resolved, timed out, or
    ///   never existed).
    #[must_use]
    pub fn resolve_permission(
        &self,
        thread_id: &str,
        interrupt_id: &str,
        decision: agui_acp_bridge_core::PermissionDecision,
    ) -> ResolveOutcome {
        let Some(entry) = self.inner.sessions.get(thread_id).map(|e| e.clone()) else {
            return ResolveOutcome::NotFound;
        };
        // Quick check: is the entry there, and is the option valid? We do
        // this without consuming the entry first, so an `InvalidOption`
        // outcome leaves the request retryable.
        let pending = entry.handle.pending_permissions();
        let Some(record) = pending.get(interrupt_id) else {
            return ResolveOutcome::NotFound;
        };
        if let agui_acp_bridge_core::PermissionDecision::Allow { ref option_id } = decision
            && !record.allows_option(option_id.0.as_ref())
        {
            return ResolveOutcome::InvalidOption;
        }
        // Drop the read-guard before calling resolve (which takes a
        // write-guard via DashMap::remove) to avoid deadlock.
        drop(record);
        if entry.handle.resolve_permission(interrupt_id, decision) {
            ResolveOutcome::Resolved
        } else {
            // Lost a race against another resolver.
            ResolveOutcome::NotFound
        }
    }
}
impl BridgeAppState {
    /// Send `session/set_mode` to the session bound to `thread_id`.
    ///
    /// Returns:
    /// - `Ok(())` — agent accepted the new mode.
    /// - `Err(SetSessionStatus::NotFound)` — no session exists for that thread.
    /// - `Err(SetSessionStatus::Busy)` — a lifecycle close/eviction owns the thread.
    /// - `Err(SetSessionStatus::Acp(_))` — agent rejected the request
    ///   (typically `mode_id` is not in `availableModes`).
    /// - `Err(SetSessionStatus::Timeout)` — agent did not respond within
    ///   `BridgeConfig.set_session_timeout`. The session is left intact;
    ///   the caller can retry.
    /// - `Err(SetSessionStatus::SessionClosed)` — actor died mid-flight; the
    ///   cache entry is evicted so a retry on the same `thread_id` rebuilds.
    pub async fn set_session_mode(
        &self,
        thread_id: &str,
        mode_id: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        let mode_id = mode_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        let snapshot = entry.handle.init_state();
        let result = if let Some(config_options) = snapshot.config_options {
            if let Some(option) = config_options
                .iter()
                .find(|option| option.category.as_ref() == Some(&SessionConfigOptionCategory::Mode))
            {
                let config_id = option.id.0.to_string();
                let value = SessionConfigOptionValue::value_id(mode_id.clone());
                validate_discovered_config_option(&config_id, option, &value)?;
                tokio::time::timeout(
                    timeout,
                    entry.handle.set_config_option_value(config_id, value),
                )
                .await
            } else if snapshot.modes.is_some() {
                // Mixed-capability agents may advertise an unrelated config
                // snapshot while retaining the legacy session/set_mode path.
                validate_legacy_mode(snapshot.modes.as_ref(), &mode_id)?;
                tokio::time::timeout(timeout, entry.handle.set_mode(mode_id)).await
            } else {
                return Err(SetSessionStatus::Acp(
                    "agent did not advertise a mode capability".into(),
                ));
            }
        } else {
            validate_legacy_mode(snapshot.modes.as_ref(), &mode_id)?;
            tokio::time::timeout(timeout, entry.handle.set_mode(mode_id)).await
        };
        match result {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Send the discovered model config option to the session bound to
    /// `thread_id`. This compatibility alias never emits ACP
    /// `session/set_model`.
    pub async fn set_session_model(
        &self,
        thread_id: &str,
        model_id: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        let model_id = model_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let timeout = self.inner.config.set_session_timeout;
        let snapshot = entry.handle.init_state();
        let Some(config_options) = snapshot.config_options else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise a model config option".into(),
            ));
        };
        let Some(config_id) = config_options.iter().find_map(|option| {
            (option.category.as_ref() == Some(&SessionConfigOptionCategory::Model))
                .then(|| option.id.0.to_string())
        }) else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise a model config option".into(),
            ));
        };
        let option = config_options
            .iter()
            .find(|option| option.id.0.as_ref() == config_id.as_str())
            .expect("model config option was found by category");
        let value = SessionConfigOptionValue::value_id(model_id);
        validate_discovered_config_option(&config_id, option, &value)?;
        match tokio::time::timeout(
            timeout,
            entry.handle.set_config_option_value(config_id, value),
        )
        .await
        {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }

    /// Send a select/value-id `session/set_config_option` request using the
    /// complete option list discovered during session initialization.
    pub async fn set_session_config_option(
        &self,
        thread_id: &str,
        config_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), SetSessionStatus> {
        self.set_session_config_option_value(
            thread_id,
            config_id,
            SessionConfigOptionValue::value_id(value.into()),
        )
        .await
    }

    /// Send a typed `session/set_config_option` request using the complete
    /// option list discovered during session initialization.
    pub async fn set_session_config_option_value(
        &self,
        thread_id: &str,
        config_id: impl Into<String>,
        value: SessionConfigOptionValue,
    ) -> Result<(), SetSessionStatus> {
        let config_id = config_id.into();
        let _setting_guard = self.enter_setting(thread_id)?;
        let entry = self
            .inner
            .sessions
            .get(thread_id)
            .map(|e| e.clone())
            .ok_or(SetSessionStatus::NotFound)?;
        let Some(config_options) = entry.handle.init_state().config_options else {
            return Err(SetSessionStatus::Acp(
                "agent did not advertise config options".into(),
            ));
        };
        let Some(option) = config_options
            .iter()
            .find(|option| option.id.0.as_ref() == config_id.as_str())
        else {
            return Err(SetSessionStatus::Acp(format!(
                "agent did not advertise config option `{config_id}`"
            )));
        };
        validate_discovered_config_option(&config_id, option, &value)?;
        let timeout = self.inner.config.set_session_timeout;
        match tokio::time::timeout(
            timeout,
            entry.handle.set_config_option_value(config_id, value),
        )
        .await
        {
            Ok(Ok(())) => {
                entry.touch();
                Ok(())
            }
            Ok(Err(BridgeError::SessionClosed)) => {
                remove_session_if_same(&self.inner, thread_id, &entry);
                Err(SetSessionStatus::SessionClosed)
            }
            Ok(Err(BridgeError::Timeout(_))) => Err(SetSessionStatus::Timeout),
            Ok(Err(other)) => Err(SetSessionStatus::Acp(other.to_string())),
            Err(_) => Err(SetSessionStatus::Timeout),
        }
    }
}
