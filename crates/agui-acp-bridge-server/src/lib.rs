#![doc = "AG-UI HTTP/SSE server that bridges to ACP agents."]

mod handler;
mod mcp_endpoint;
#[doc(hidden)]
pub mod test_agents;

pub use agui_acp_bridge_core::acp;
pub use agui_acp_bridge_core::translation::{self, Translator, session_init_event};
pub use agui_acp_bridge_core::{
    AcpClient, AcpSessionHandle, BridgeConfig, BridgeError, BridgeStreamItem,
    CustomAgentInProcessClient, FrontendToolDef, FrontendToolRegistry, FrontendToolResponse,
    InProcessAcpClient, MCP_SERVER_NAME, ModeOffering, PermissionDecision, PermissionPolicy,
    ProcessAcpClient, PromptStream, SessionConfig, SessionConfigOption, SessionInitState,
    SessionModesInit, SessionSummary, ThreadEntry,
};
#[cfg(feature = "unstable_session_model")]
pub use agui_acp_bridge_core::{ModelOffering, SessionModelsInit};
pub use handler::{
    BridgeAppState, BridgeAppStateBuilder, SetSessionStatus, build_router, build_router_inner,
};
