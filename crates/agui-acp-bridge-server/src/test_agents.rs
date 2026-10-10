#![doc(hidden)]
#![allow(clippy::missing_errors_doc)]

//! In-process ACP agents used only by the bridge's own integration tests.

#[allow(unused_imports)]
use agent_client_protocol::schema::ProtocolVersion;
#[allow(unused_imports)]
use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, ImageContent, InitializeRequest,
    InitializeResponse, NewSessionRequest, NewSessionResponse, PermissionOption,
    PermissionOptionId, PermissionOptionKind, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus,
    PromptRequest, PromptResponse, RequestPermissionRequest, SessionConfigOptionValue, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent, ToolCallUpdate,
    ToolCallUpdateFields,
};
#[allow(unused_imports)]
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
#[allow(unused_imports)]
use agui_acp_bridge_core::BridgeError;
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::io::DuplexStream;
#[allow(unused_imports)]
use tokio::sync::Notify;
#[allow(unused_imports)]
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
#[allow(unused_imports)]
use uuid::Uuid;

mod basic;
mod close;
mod delete;
mod history;
mod late_updates;
mod permissions;
mod protocol;
mod settings_discovery;
mod settings_rejections;
mod settings_values;
mod timing;
mod timing_cancel;
mod timing_stream;
mod updates;

pub use basic::{
    run_failing_prompt_agent, run_image_agent, run_single_chunk_agent, run_stop_reason_agent,
};
pub use close::{
    CloseBehavior, LifecycleControl, SharedCloseSessionIds, run_close_agent,
    run_close_setting_agent,
};
pub use delete::{
    DeleteBehavior, DeleteLifecycleControl, SharedDeleteSessionIds, run_delete_agent,
    run_delete_agent_with_session_id, run_delete_lifecycle_agent,
};
pub use history::{SharedSessionStore, run_session_history_agent_with};
pub use late_updates::{run_late_notification_agent, run_late_notification_flood_agent};
pub use permissions::{
    run_multiple_pending_permission_agent, run_permission_after_cancel_agent,
    run_request_permission_agent,
};
pub use protocol::{run_wrong_protocol_agent, run_wrong_protocol_list_agent};
pub use settings_discovery::{run_mixed_mode_capabilities_agent, run_modes_models_agent};
pub use settings_rejections::{
    run_rejecting_config_agent, run_rejecting_undiscovered_settings_agent,
};
pub use settings_values::{
    BooleanConfigProbe, run_boolean_config_agent, run_config_update_agent,
    run_unresponsive_setting_agent,
};
pub use timing::{run_slow_handshake_agent, run_slow_prompt_agent};
pub use timing_cancel::{run_cancel_aware_slow_agent, run_unresponsive_cancel_agent};
pub use timing_stream::{run_counting_agent, run_long_running_agent};
pub use updates::{run_mixed_updates_agent, run_stateful_session_agent};
