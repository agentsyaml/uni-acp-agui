//! Subprocess-backed ACP client.
//!
//! `ProcessAcpClient` spawns a fresh ACP agent subprocess per session. The
//! command line is composed from `command` + `args` and forwarded through
//! `agent_client_protocol_tokio::AcpAgent::from_str`. Environment variable
//! overrides are not yet supported by the upstream API; [`ProcessAcpClient::with_env`]
//! is therefore a placeholder that logs a warning and discards the values
//! to make the missing functionality visible at construction time rather
//! than silently swallowed at runtime.

use std::collections::HashMap;
use std::str::FromStr;

use agent_client_protocol_tokio::AcpAgent;
use async_trait::async_trait;

use crate::acp::{AcpClient, AcpSessionHandle, SessionConfig};
use crate::error::BridgeError;
use crate::session::{list_sessions_via, spawn_session};
use crate::stream::SessionSummary;

/// An ACP client that spawns a subprocess for each session.
///
/// `command` is the program (e.g. `"./target/debug/examples/simple_agent"` or
/// `"npx"`); `args` is forwarded as additional argv tokens.
#[derive(Debug, Clone, Default)]
pub struct ProcessAcpClient {
    /// Program to spawn (path or PATH-resolved binary name).
    pub command: String,
    /// Additional argv tokens appended after `command`.
    pub args: Vec<String>,
}

impl ProcessAcpClient {
    /// Convenience: command-only client (no args).
    #[must_use]
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
        }
    }

    /// Builder-style: append argv tokens.
    ///
    /// Args containing whitespace are rejected because the upstream
    /// `AcpAgent::from_str` API takes a single shell-style string and
    /// re-splits on whitespace; embedded spaces cannot be represented
    /// without shell-quoting support that the SDK does not yet expose.
    ///
    /// # Panics
    ///
    /// Panics if any arg contains whitespace.
    #[must_use]
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for arg in args {
            let s: String = arg.into();
            assert!(
                !s.chars().any(char::is_whitespace),
                "ProcessAcpClient: arg {s:?} contains whitespace; shell-quoting is not yet supported"
            );
            self.args.push(s);
        }
        self
    }

    /// Builder-style: declare environment overrides for the spawned child.
    ///
    /// **Currently a no-op:** the upstream `AcpAgent::from_str` API does not
    /// accept env overrides and the spawned child inherits the parent's
    /// environment. Calling this method emits a `tracing::warn!` so a
    /// caller passing critical secrets (`OPENAI_API_KEY`, etc.) discovers
    /// the gap at startup instead of debugging a quiet failure.
    ///
    /// Workaround: set the variables in the parent process (e.g. via
    /// systemd `Environment=`, `docker run -e`, or shell `export`) before
    /// invoking the bridge.
    pub fn with_env(self, env: HashMap<String, String>) -> Self {
        if !env.is_empty() {
            let keys: Vec<&str> = env.keys().map(String::as_str).collect();
            tracing::warn!(
                keys = ?keys,
                "ProcessAcpClient::with_env is currently a no-op; the upstream \
                 AcpAgent API does not accept env overrides. Set these variables \
                 in the bridge's own environment instead."
            );
        }
        self
    }

    fn command_line(&self) -> String {
        if self.args.is_empty() {
            self.command.clone()
        } else {
            format!("{} {}", self.command, self.args.join(" "))
        }
    }
}

#[async_trait]
impl AcpClient for ProcessAcpClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let cmd_line = self.command_line();
        let agent = AcpAgent::from_str(&cmd_line).map_err(BridgeError::Acp)?;
        spawn_session(agent, cfg).await
    }

    async fn list_sessions(&self, cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        // Spawn a fresh, short-lived agent process for the listing query.
        // The connection (and subprocess) is torn down when the transient
        // connection task completes inside `list_sessions_via`.
        let cmd_line = self.command_line();
        let agent = AcpAgent::from_str(&cmd_line).map_err(BridgeError::Acp)?;
        list_sessions_via(agent, cfg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_env_does_not_change_command_line() {
        // The behavior contract: env overrides must NOT silently leak
        // expectations into the spawned command. The command line is
        // unchanged whether or not the caller supplied env.
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let a = ProcessAcpClient::new("agent");
        let b = ProcessAcpClient::new("agent").with_env(env);
        assert_eq!(a.command_line(), b.command_line());
    }

    #[test]
    fn with_args_joins_with_space() {
        let c = ProcessAcpClient::new("agent").with_args(["--foo", "bar"]);
        assert_eq!(c.command_line(), "agent --foo bar");
    }

    #[test]
    #[should_panic(expected = "shell-quoting is not yet supported")]
    fn with_args_rejects_whitespace() {
        let _ = ProcessAcpClient::new("agent").with_args(["with space"]);
    }
}
