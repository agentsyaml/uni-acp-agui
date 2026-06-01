#![doc = "Core types and traits for the AG-UI to ACP bridge."]

pub mod acp;
pub mod config;
#[doc(hidden)]
pub mod echo_agent;
pub mod error;
pub mod file_ops;
pub mod frontend_tools;
pub mod message_state;
pub mod policy;
pub mod process;
mod session;
pub mod stream;
pub mod translation;

pub use acp::{
    AcpClient, AcpSessionHandle, CustomAgentInProcessClient, InProcessAcpClient, PromptStream,
    SessionConfig, SessionInitState,
};
pub use config::BridgeConfig;
pub use error::BridgeError;
pub use file_ops::canonicalize_cwd;
pub use frontend_tools::{
    FrontendToolDef, FrontendToolRegistry, FrontendToolResponse, ThreadEntry,
};
pub use message_state::MessageState;
pub use policy::{PermissionDecision, PermissionPolicy};
pub use process::ProcessAcpClient;
pub use session::{MCP_SERVER_NAME, list_sessions_in_process_with, spawn_in_process_session_with};
pub use stream::{BridgeStreamItem, ModeOffering, SessionModesInit, SessionSummary};
#[cfg(feature = "unstable_session_model")]
pub use stream::{ModelOffering, SessionModelsInit};
pub use translation::{Translator, session_init_event};
