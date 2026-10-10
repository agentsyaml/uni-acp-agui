//! ACP session actor.
//!
//! Owns the `connect_with(...)` future for one ACP session and bridges
//! between an external [`AcpSessionHandle`] (mpsc commands in,
//! per-prompt [`BridgeStreamItem`] streams out) and the in-protocol
//! `cx: ConnectionTo<Agent>`.
//!
//! # Per-prompt event routing
//!
//! ACP `session/update` notifications arrive on a single connection-level
//! callback installed at `Client::builder().on_receive_notification(...)`
//! time. To route them to the **current prompt's** event channel, the actor
//! holds a shared `Arc<Mutex<Option<mpsc::Sender<BridgeStreamItem>>>>` slot.
//! On `Prompt`, it installs the per-prompt sender; when the prompt completes
//! (success or error) it clears the slot. Notifications that arrive while
//! the slot is empty are spilled into a bounded buffer (capacity
//! [`SPILL_CAPACITY`]) that the next prompt drains ahead of its own events;
//! overflow warns and drops — they would be ACP protocol violations
//! (notification outside any active turn).
//!
//! # Request handling
//!
//! The actor registers `on_receive_request` handlers for:
//! - `RequestPermissionRequest` — consults the configured `PermissionPolicy`.
//!   `Allow`/`Deny` decisions respond inline; `Defer` decisions emit a
//!   `BridgeStreamItem::Interrupt` and await an external resolution via
//!   `AcpSessionHandle::resolve_permission` with `permission_timeout`.
//! - Filesystem and terminal request handlers are capability-gated by the
//!   configured [`PermissionPolicy`] and the initialized platform backend.

use std::future::Future;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
#[cfg(feature = "unstable_session_model")]
use agent_client_protocol::schema::v1::SessionConfigSelectOptions;
use agent_client_protocol::schema::v1::{
    BooleanConfigOptionCapabilities, ClientCapabilities, ClientSessionCapabilities,
    CloseSessionRequest, ContentBlock, CreateTerminalRequest, DeleteSessionRequest,
    FileSystemCapabilities, HttpHeader, InitializeRequest, KillTerminalRequest, McpServer,
    McpServerHttp, NewSessionRequest, NewSessionResponse, PromptRequest, ReadTextFileRequest,
    ReadTextFileResponse, ReleaseTerminalRequest, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome,
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigOptionValue,
    SessionConfigOptionsCapabilities, SessionId, SessionMode, SessionModeState,
    SessionNotification, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    SetSessionModeRequest, StopReason, TerminalOutputRequest, WaitForTerminalExitRequest,
    WriteTextFileRequest, WriteTextFileResponse,
};
use agent_client_protocol::{Agent, Client, ConnectTo, ConnectionTo, RequestCancellation};
use dashmap::DashMap;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
#[cfg(test)]
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::acp::{
    AcpSessionHandle, PendingPermissions, SessionCommand, SessionConfig, SessionInitState,
    SessionTurnQueue, TurnState,
};
use crate::echo_agent;
use crate::error::BridgeError;
use crate::policy::{PermissionDecision, PermissionPolicy};
use crate::stream::{BridgeStreamItem, ModeOffering, SessionModesInit, SessionSummary};
#[cfg(feature = "unstable_session_model")]
use crate::stream::{ModelOffering, SessionModelsInit};
use crate::terminal::TerminalRegistry;

mod admission;
mod mailbox;
mod notifications;

mod actor;
mod admin;
mod connection;
mod filesystem;
mod history;
mod history_drain;
mod initialize;
mod permissions;
mod prompt;
mod prompt_cancel;
mod settings;
mod spawn;
mod terminal_requests;
mod worker;

use actor::{SessionActorState, SessionReady, mailbox_limit_error, run_actor};
pub(crate) use admin::{delete_session_via, list_sessions_via};
use history::*;
use initialize::*;
use permissions::*;
use prompt_cancel::*;
use settings::*;
use spawn::AbortOnDrop;
pub(crate) use spawn::spawn_in_process_echo_session;
#[cfg(all(test, target_os = "linux"))]
use spawn::spawn_in_process_session_with_work;
pub(crate) use spawn::spawn_session;
pub use spawn::{
    delete_session_in_process_with, list_sessions_in_process_with, spawn_in_process_session_with,
};

use admission::WorkAdmission;
pub(crate) use admission::WorkPermit;
use mailbox::{
    EventLimit, EventMailbox, EventMailboxTx, EventRoute, EventSlot, RouteState, json_size_bounded,
    run_event_mailbox,
};

/// MCP server name advertised on `NewSessionRequest.mcp_servers`. Agents
/// typically prefix the tool names they surface to their LLM with this
/// (e.g. opencode renders our `say_hello` tool as
/// `agui-acp-bridge_say_hello`). Exported so the handler can compute the
/// prefixed variants for the translator's suppression filter.
pub const MCP_SERVER_NAME: &str = "agui-acp-bridge";

const EVENT_DRIVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EVENT_ITEM_LIMIT: usize = 4096;
const EVENT_BYTE_LIMIT: usize = 16 * 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = EVENT_ITEM_LIMIT + 1;
const FAILED_EVENT_DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
const SPAWNED_WORK_ITEMS: usize = 128;
const SPAWNED_WORK_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
mod tests;
