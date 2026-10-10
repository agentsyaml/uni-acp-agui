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
use crate::guarded_transport::{GuardedAgent, ProcessDiagnostic};
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
        let mut env = self.env.clone();
        for key in ["AGUI_ACP_BRIDGE_TOKEN", "AGUI_BRIDGE_TOKEN"] {
            env.insert(key.to_string(), String::new());
        }
        AcpAgent::new(
            AcpAgentConfig::new(self.command.clone())
                .args(self.args.clone())
                .envs(env),
        )
    }
}

#[async_trait]
impl AcpClient for ProcessAcpClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let (agent, diagnostic) = GuardedAgent::new(self.agent());
        prefer_observed_process_failure(spawn_session(agent, cfg).await, &diagnostic)
    }

    async fn list_sessions(&self, cfg: SessionConfig) -> Result<Vec<SessionSummary>, BridgeError> {
        // Spawn a fresh, short-lived agent process for the listing query.
        // The connection (and subprocess) is torn down when the transient
        // connection task completes inside `list_sessions_via`.
        let (agent, diagnostic) = GuardedAgent::new(self.agent());
        prefer_observed_process_failure(list_sessions_via(agent, cfg).await, &diagnostic)
    }

    async fn delete_session(
        &self,
        cfg: SessionConfig,
        session_id: agent_client_protocol::schema::v1::SessionId,
    ) -> Result<(), BridgeError> {
        // Spawn a fresh, short-lived agent process for the delete query.
        let (agent, diagnostic) = GuardedAgent::new(self.agent());
        prefer_observed_process_failure(
            delete_session_via(agent, cfg, session_id).await,
            &diagnostic,
        )
    }
}

fn prefer_observed_process_failure<T>(
    result: Result<T, BridgeError>,
    diagnostic: &ProcessDiagnostic,
) -> Result<T, BridgeError> {
    match result {
        Err(error @ BridgeError::SessionClosed) | Err(error @ BridgeError::Acp(_))
            if matches!(error, BridgeError::SessionClosed)
                || matches!(&error, BridgeError::Acp(error) if agent_client_protocol::is_incoming_transport_closed(error)) =>
        {
            let (Some(status), stderr) = diagnostic.snapshot() else {
                return Err(error);
            };
            Err(BridgeError::Io(std::io::Error::other(format!(
                "ACP agent exited with {status}; stderr: {}",
                String::from_utf8_lossy(&stderr)
            ))))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incoming_closed_error() -> agent_client_protocol::Error {
        agent_client_protocol::Error::internal_error().data(
            serde_json::json!({"reason": agent_client_protocol::INCOMING_TRANSPORT_CLOSED_REASON}),
        )
    }

    #[cfg(unix)]
    fn failed_diagnostic() -> ProcessDiagnostic {
        use std::os::unix::process::ExitStatusExt;
        let diagnostic = ProcessDiagnostic::default();
        diagnostic.observe_failure(&std::process::ExitStatus::from_raw(17 << 8));
        diagnostic.append_stderr(b"bounded stderr");
        diagnostic
    }

    #[cfg(unix)]
    #[test]
    fn observed_exit_only_overrides_incoming_transport_closed() {
        let diagnostic = failed_diagnostic();
        let error = prefer_observed_process_failure::<()>(
            Err(BridgeError::Acp(incoming_closed_error())),
            &diagnostic,
        )
        .unwrap_err();
        assert!(
            matches!(error, BridgeError::Io(ref e) if e.to_string().contains("17") && e.to_string().contains("bounded stderr"))
        );

        let mut domain = agent_client_protocol::Error::internal_error();
        domain.message = "incoming transport closed".to_string();
        let result =
            prefer_observed_process_failure::<()>(Err(BridgeError::Acp(domain)), &diagnostic)
                .unwrap_err();
        assert!(
            matches!(result, BridgeError::Acp(ref e) if e.message == "incoming transport closed")
        );

        let timeout = BridgeError::Timeout(std::time::Duration::from_secs(1));
        assert!(matches!(
            prefer_observed_process_failure::<()>(Err(timeout), &diagnostic),
            Err(BridgeError::Timeout(_))
        ));
    }

    #[test]
    fn empty_diagnostic_and_success_are_unchanged() {
        let diagnostic = ProcessDiagnostic::default();
        let result = prefer_observed_process_failure::<()>(
            Err(BridgeError::Acp(incoming_closed_error())),
            &diagnostic,
        )
        .unwrap_err();
        assert!(
            matches!(result, BridgeError::Acp(ref e) if agent_client_protocol::is_incoming_transport_closed(e))
        );
        assert!(
            prefer_observed_process_failure(Ok::<_, BridgeError>(7), &diagnostic).unwrap() == 7
        );
        assert!(matches!(
            prefer_observed_process_failure::<()>(Err(BridgeError::SessionClosed), &diagnostic),
            Err(BridgeError::SessionClosed)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn observed_exit_overrides_background_first_session_closed() {
        use futures::FutureExt;
        use std::os::unix::process::ExitStatusExt;

        #[derive(Debug)]
        struct DenyPolicy;
        #[async_trait::async_trait]
        impl crate::policy::PermissionPolicy for DenyPolicy {
            async fn decide(
                &self,
                _request: &agent_client_protocol::schema::v1::RequestPermissionRequest,
            ) -> crate::policy::PermissionDecision {
                crate::policy::PermissionDecision::Deny
            }
        }

        struct FailingConnector(ProcessDiagnostic);
        impl agent_client_protocol::ConnectTo<agent_client_protocol::Client> for FailingConnector {
            async fn connect_to(
                self,
                _client: impl agent_client_protocol::ConnectTo<agent_client_protocol::Agent>,
            ) -> agent_client_protocol::Result<()> {
                unreachable!("the test connector supplies its transport future")
            }

            fn into_channel_and_future(
                self,
            ) -> (
                agent_client_protocol::Channel,
                futures::future::BoxFuture<'static, agent_client_protocol::Result<()>>,
            ) {
                let (endpoint, peer) = agent_client_protocol::Channel::duplex();
                let diagnostic = self.0;
                let future = async move {
                    let _peer = peer;
                    diagnostic.observe_failure(&std::process::ExitStatus::from_raw(17 << 8));
                    diagnostic.append_stderr(b"background failure");
                    Err(agent_client_protocol::Error::internal_error())
                }
                .boxed();
                (endpoint, future)
            }
        }

        let diagnostic = ProcessDiagnostic::default();
        let result = crate::session::spawn_session(
            FailingConnector(diagnostic.clone()),
            SessionConfig {
                cwd: std::env::current_dir().unwrap(),
                policy: std::sync::Arc::new(DenyPolicy),
                config: crate::BridgeConfig::default(),
                mcp_url: None,
                mcp_headers: Vec::new(),
                load_session_id: None,
            },
        )
        .await;
        assert!(matches!(result, Err(BridgeError::SessionClosed)));
        let observed = prefer_observed_process_failure(result, &diagnostic).unwrap_err();
        assert!(matches!(
            observed,
            BridgeError::Io(ref error)
                if error.to_string().contains("17")
                    && error.to_string().contains("background failure")
        ));
    }

    #[test]
    fn structured_config_preserves_args_and_env() {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        env.insert(
            "AGUI_ACP_BRIDGE_TOKEN".to_string(),
            "admin-secret-one".to_string(),
        );
        env.insert(
            "AGUI_BRIDGE_TOKEN".to_string(),
            "admin-secret-two".to_string(),
        );
        env.insert("VENDOR_API_KEY".to_string(), "vendor-secret".to_string());
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
        assert_eq!(
            config
                .environment()
                .get("VENDOR_API_KEY")
                .map(String::as_str),
            Some("vendor-secret")
        );
        assert_eq!(
            config
                .environment()
                .get("AGUI_ACP_BRIDGE_TOKEN")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            config
                .environment()
                .get("AGUI_BRIDGE_TOKEN")
                .map(String::as_str),
            Some("")
        );
        assert!(!format!("{client:?}").contains("bar"));
    }

    #[test]
    fn with_args_accepts_whitespace_without_splitting() {
        let c = ProcessAcpClient::new("agent").with_args(["with space"]);
        assert_eq!(c.args, vec!["with space"]);
    }
}
