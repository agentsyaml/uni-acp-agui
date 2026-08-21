//! Subprocess-backed ACP client.
//!
//! `ProcessAcpClient` spawns a fresh ACP agent subprocess per session. The
//! command, argv, and child environment are passed through the structured
//! `agent_client_protocol::AcpAgentConfig` API. `SessionConfig::cwd` remains
//! the ACP session working directory; it is not a subprocess launch setting.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use agent_client_protocol::{AcpAgent, AcpAgentConfig};
use async_trait::async_trait;

use crate::acp::{AcpClient, AcpSessionHandle, SessionConfig};
use crate::error::BridgeError;
use crate::session::{delete_session_via, list_sessions_via, spawn_session};
use crate::stream::SessionSummary;

/// An ACP client that spawns a subprocess for each session.
///
/// `command` is the program (e.g. `"./target/debug/examples/simple_agent"` or
/// `"npx"`); `args` and `env` are forwarded structurally.
#[derive(Clone, Default)]
pub struct ProcessAcpClient {
    /// Program to spawn (path or PATH-resolved binary name).
    pub command: String,
    /// Additional argv tokens appended after `command`.
    pub args: Vec<String>,
    /// Environment overrides passed to the child process.
    pub env: BTreeMap<String, String>,
}

impl fmt::Debug for ProcessAcpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessAcpClient")
            .field("command", &self.command)
            .field("args", &self.args)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ProcessAcpClient {
    /// Convenience: command-only client (no args).
    #[must_use]
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
        }
    }

    /// Builder-style: append argv tokens.
    ///
    /// Arguments are preserved as individual argv tokens, including tokens
    /// containing whitespace.
    #[must_use]
    pub fn with_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Builder-style: declare environment overrides for the spawned child.
    pub fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env.extend(env);
        self
    }

    fn agent(&self) -> AcpAgent {
        AcpAgent::new(
            AcpAgentConfig::new(self.command.clone())
                .args(self.args.clone())
                .envs(self.env.clone()),
        )
    }
}

#[async_trait]
impl AcpClient for ProcessAcpClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        spawn_session(self.agent(), cfg).await
    }

    async fn list_sessions(&self, cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        // Spawn a fresh, short-lived agent process for the listing query.
        // The connection (and subprocess) is torn down when the transient
        // connection task completes inside `list_sessions_via`.
        list_sessions_via(self.agent(), cfg).await
    }

    async fn delete_session(
        &self,
        cfg: SessionConfig,
        session_id: agent_client_protocol::schema::v1::SessionId,
    ) -> Result<(), BridgeError> {
        // Spawn a fresh, short-lived agent process for the delete query.
        delete_session_via(self.agent(), cfg, session_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_config_preserves_args_and_env() {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        let client = ProcessAcpClient::new("agent with space")
            .with_args(["arg with space", "--flag"])
            .with_env(env);
        let config = client.agent().into_config();
        assert_eq!(config.command().to_string_lossy(), "agent with space");
        assert_eq!(config.arguments(), ["arg with space", "--flag"]);
        assert_eq!(
            config.environment().get("FOO").map(String::as_str),
            Some("bar")
        );
        assert!(!format!("{client:?}").contains("bar"));
    }

    #[test]
    fn with_args_accepts_whitespace_without_splitting() {
        let c = ProcessAcpClient::new("agent").with_args(["with space"]);
        assert_eq!(c.args, vec!["with space"]);
    }
}
