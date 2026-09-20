#![doc(hidden)]
#![allow(clippy::missing_errors_doc)]

//! In-process ACP agents used **only** by the bridge's own integration tests.
//! Not part of the public API; covered by `#![doc(hidden)]`.
//!
//! Scenarios:
//! - [`run_image_agent`] — single non-text (Image) chunk, ends cleanly.
//! - [`run_failing_prompt_agent`] — JSON-RPC error from `prompt`.
//! - [`run_stateful_session_agent`] — increments per-`SessionId` turn counter.
//! - [`run_mixed_updates_agent`] — Thought → Plan → AgentText → UserChunk → Image.
//! - [`run_request_permission_agent`] — issues a `requestPermission` request to
//!   the client, then completes the prompt based on the outcome. Exercises the
//!   bridge's permission handler end-to-end.
//! - [`run_single_chunk_agent`] — emits one chunk, returns cleanly (happy path).
//! - [`run_slow_prompt_agent`] — sleeps before responding (queued-prompt test).
//! - [`run_long_running_agent`] — emits up to 1000 chunks at 50ms each (~50s).
//! - [`run_late_notification_agent`] — sends notification AFTER PromptResponse.
//! - [`run_late_notification_flood_agent`] — sends >32 late notifications per
//!   prompt, exercising spill-buffer overflow.
//! - [`run_counting_agent`] — emits N sequentially-numbered text chunks as fast
//!   as possible, then ends. Used for streaming ordering / throughput tests.
//! - [`run_slow_handshake_agent`] — sleeps before answering `session/new`, so
//!   `open_session` exceeds a short `open_session_timeout`. Used to pin the
//!   startup-timeout path.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, ImageContent, InitializeRequest,
    InitializeResponse, NewSessionRequest, NewSessionResponse, PermissionOption,
    PermissionOptionId, PermissionOptionKind, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus,
    PromptRequest, PromptResponse, RequestPermissionRequest, SessionConfigOptionValue, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent, ToolCallUpdate,
    ToolCallUpdateFields,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::BridgeError;
use tokio::io::DuplexStream;
use tokio::sync::Notify;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use uuid::Uuid;

pub async fn run_image_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-image-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Image(
                        ImageContent::new("aGVsbG8=", "image/png"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

pub async fn run_failing_prompt_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-failing-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: PromptRequest,
                        responder,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                responder.respond_with_error(agent_client_protocol::util::internal_error(
                    "agent refused prompt (test fixture)",
                ))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that increments a per-session turn counter and echoes it back as
/// `"turn N: <prompt>"`. Used to verify that same `thread_id` reuses the same
/// `SessionId` across runs (per `BridgeAppState::session_for` cache).
pub async fn run_stateful_session_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let counter: std::sync::Arc<Mutex<std::collections::HashMap<String, u32>>> =
        std::sync::Arc::new(Mutex::new(std::collections::HashMap::new()));
    let counter_for_prompt = counter.clone();

    Agent
        .builder()
        .name("agui-bridge-stateful-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let prompt_text = req
                    .prompt
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text(t) => Some(t.text.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let session_key: String = req.session_id.clone().0.to_string();
                let turn = {
                    let mut guard = counter_for_prompt.lock().expect("counter poisoned");
                    let entry = guard.entry(session_key).or_insert(0);
                    *entry += 1;
                    *entry
                };

                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(format!("turn {turn}: {prompt_text}")),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that streams a heterogeneous set of `SessionUpdate` variants in a
/// single turn:
///
///   1. AgentThoughtChunk (text)
///   2. Plan (with two entries)
///   3. AgentMessageChunk (text)        ← only this should produce TEXT_MESSAGE_*
///   4. UserMessageChunk (text)         ← still goes through translator
///   5. AgentMessageChunk (image)       ← non-text → RawEvent
///
/// Verifies the translator's handling of the full SessionUpdate enum surface
/// per `translation.rs`.
pub async fn run_mixed_updates_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-mixed-updates-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("thinking..."),
                    ))),
                ))?;

                let plan = Plan::new(vec![
                    PlanEntry::new(
                        "step one",
                        PlanEntryPriority::High,
                        PlanEntryStatus::Pending,
                    ),
                    PlanEntry::new(
                        "step two",
                        PlanEntryPriority::Medium,
                        PlanEntryStatus::Pending,
                    ),
                ]);
                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::Plan(plan),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("hello "),
                    ))),
                ))?;
                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("world"),
                    ))),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::UserMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("(echoed user)"),
                    ))),
                ))?;

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Image(
                        ImageContent::new("aGVsbG8=", "image/png"),
                    ))),
                ))?;

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that issues a `requestPermission` request to the client, then
/// completes the prompt based on the outcome. Exercises the bridge's
/// permission handler (notification → policy → response) end-to-end.
///
/// Uses `SentRequest::on_receiving_ok_result` (the SDK-recommended pattern
/// for chaining requests inside a handler) instead of `block_task`, which
/// would deadlock the dispatch loop.
pub async fn run_request_permission_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-request-permission-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let tc_update = ToolCallUpdate::new(
                    "tc-1",
                    ToolCallUpdateFields::new().title("Read file".to_string()),
                );
                let options = vec![
                    PermissionOption::new(
                        PermissionOptionId::new("allow"),
                        "Allow".to_string(),
                        PermissionOptionKind::AllowOnce,
                    ),
                    PermissionOption::new(
                        PermissionOptionId::new("deny"),
                        "Deny".to_string(),
                        PermissionOptionKind::RejectOnce,
                    ),
                ];
                let perm_req =
                    RequestPermissionRequest::new(req.session_id.clone(), tc_update, options);

                // SDK-recommended pattern: schedule a task that runs when the
                // bridge's response arrives, *without* blocking the dispatch
                // loop (which would deadlock — the response can't be received
                // while the handler is awaiting it).
                cx.send_request(perm_req)
                    .on_receiving_result(async move |outcome| match outcome {
                        Ok(_resp) => responder.respond(PromptResponse::new(StopReason::EndTurn)),
                        Err(e) => responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "permission request failed: {e}"
                            )),
                        ),
                    })
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture that negotiates an unsupported ACP wire version. The bridge
/// must reject it before issuing `session/new`.
pub async fn run_wrong_protocol_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-wrong-protocol-test")
        .on_receive_request(
            async move |_req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(ProtocolVersion::V0)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// List-path counterpart to [`run_wrong_protocol_agent`]. It advertises
/// `session/list`, but the bridge must reject the version before sending the
/// list request.
pub async fn run_wrong_protocol_list_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ListSessionsRequest, SessionCapabilities, SessionListCapabilities,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-wrong-list-protocol-test")
        .on_receive_request(
            async move |_req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(ProtocolVersion::V0).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().list(SessionListCapabilities::default()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: ListSessionsRequest, responder, _cx| {
                let _ = req;
                responder.respond_with_error(agent_client_protocol::util::internal_error(
                    "session/list must not be sent after a protocol mismatch",
                ))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

fn permission_request(session_id: SessionId, tool_call_id: &str) -> RequestPermissionRequest {
    let fields = ToolCallUpdateFields::new().title(tool_call_id.to_string());
    RequestPermissionRequest::new(
        session_id,
        ToolCallUpdate::new(tool_call_id.to_string(), fields),
        vec![
            PermissionOption::new(
                PermissionOptionId::new("allow"),
                "Allow".to_string(),
                PermissionOptionKind::AllowOnce,
            ),
            PermissionOption::new(
                PermissionOptionId::new("deny"),
                "Deny".to_string(),
                PermissionOptionKind::RejectOnce,
            ),
        ],
    )
}

/// Agent fixture that keeps two permission requests pending at once and only
/// completes the prompt after both responses arrive.
pub async fn run_multiple_pending_permission_agent(
    stream: DuplexStream,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-multiple-permissions-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let session_id = req.session_id;
                let first = cx.send_request(permission_request(session_id.clone(), "tc-1"));
                let second = cx.send_request(permission_request(session_id.clone(), "tc-2"));
                let cx_for_finish = cx.clone();
                cx.spawn(async move {
                    let _ = first.block_task().await;
                    let _ = second.block_task().await;
                    cx_for_finish.send_notification(SessionNotification::new(
                        session_id,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("cancel tail"),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::Cancelled))
                })
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture that sends a second permission request only after the first
/// one has been answered. Tests cancel after the first interrupt; the second
/// request therefore races directly with post-cancel dispatch and must be
/// answered cancelled without entering the pending map.
pub async fn run_permission_after_cancel_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-permission-after-cancel-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let session_id = req.session_id;
                let second = permission_request(session_id.clone(), "tc-after-cancel");
                let cx_after_first = cx.clone();
                cx.send_request(permission_request(session_id, "tc-before-cancel"))
                    .on_receiving_result(async move |_first| {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        cx_after_first.send_request(second).on_receiving_result(
                            async move |_second| {
                                responder.respond(PromptResponse::new(StopReason::Cancelled))
                            },
                        )
                    })
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture that never returns the prompt response. It is used to verify
/// the bridge's cancel grace timeout and session eviction path.
pub async fn run_unresponsive_cancel_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-unresponsive-cancel-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: PromptRequest,
                        _responder,
                        _cx: ConnectionTo<agent_client_protocol::Client>| {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that emits one text chunk, then returns successfully.
///
/// Used to validate the basic AgentMessageChunk → TEXT_MESSAGE_* path with a
/// single chunk.
pub async fn run_single_chunk_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-single-chunk-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("only chunk"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture that emits one text chunk and completes with the supplied ACP
/// stop reason. Used to pin the bridge's terminal-event mapping.
pub async fn run_stop_reason_agent(
    stream: DuplexStream,
    stop_reason: StopReason,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-stop-reason-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("terminal tail"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(stop_reason))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that sleeps `delay_ms` before responding to `prompt`. Used to
/// verify that a second prompt on the same `thread_id` queues correctly
/// behind the first (see `BridgeAppState::session_for` reuse semantics).
pub async fn run_slow_prompt_agent(stream: DuplexStream, delay_ms: u64) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let delay_ms = u32::try_from(delay_ms).unwrap_or(u32::MAX);

    Agent
        .builder()
        .name("agui-bridge-slow-prompt-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                tokio::time::sleep(Duration::from_millis(u64::from(delay_ms))).await;

                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(format!("slow done after {delay_ms}ms")),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture whose slow prompt observes `session/cancel` and returns
/// `Cancelled`. It distinguishes cancelling the active turn from cancelling a
/// queued turn in the bridge's turn-identity tests.
pub async fn run_cancel_aware_slow_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::CancelNotification;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_for_notification = cancelled.clone();
    let cancelled_for_prompt = cancelled.clone();

    Agent
        .builder()
        .name("agui-bridge-cancel-aware-slow-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            async move |_req: CancelNotification, _cx| {
                cancelled_for_notification.store(true, Ordering::Release);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                for _ in 0..100 {
                    if cancelled_for_prompt.load(Ordering::Acquire) {
                        return responder.respond(PromptResponse::new(StopReason::Cancelled));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("active turn completed"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that emits up to 1000 text chunks at 50ms intervals (~50s total),
/// then responds with `EndTurn`. Exits the loop early if `send_notification`
/// fails (i.e. the consumer dropped). Used to test client-disconnect handling:
/// when the HTTP/SSE consumer drops, the bridge should ideally cancel the
/// session — see audit finding §6.
///
/// Tests should wrap calls in `tokio::time::timeout` with a budget shorter
/// than 50 seconds.
pub async fn run_long_running_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-long-running-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();
                for i in 0..1000u32 {
                    let chunk_res = cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!("chunk {i} ")),
                        ))),
                    ));
                    if chunk_res.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that emits a chunk, responds to prompt, **then** sends a late
/// notification AFTER the PromptResponse has been delivered.
///
/// Late notifications are spilled into the bridge's bounded spill buffer
/// (`session.rs` `SpillBuffer`) and rebroadcast at the start of the NEXT run
/// on the same thread, ahead of that run's own events. They never appear
/// retroactively in the run that already terminated (no events after the
/// terminal event). This agent emits its in-band chunk on every prompt, so
/// the draining run distinguishes the spilled update from turn-two's own
/// text.
pub async fn run_late_notification_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-late-notif-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("in-band"),
                    ))),
                ))?;

                // 100ms delay ensures bridge clears its event slot (session.rs:165) before late notif arrives.
                let cx_clone = cx.clone();
                let sid_for_late = sid.clone();
                let _ = cx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = cx_clone.send_notification(SessionNotification::new(
                        sid_for_late,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("LATE-AFTER-FINISH"),
                        ))),
                    ));
                    Ok(())
                });

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that spills past the bridge's capacity: every prompt gets one
/// in-band chunk plus `count` LATE-AFTER-FINISH-<i> chunks after the prompt
/// response (matching `SpillBuffer`'s 32-entry cap when `count > 32`). Used
/// to prove overflow drops with a warning while keeping the session usable.
pub async fn run_late_notification_flood_agent(
    stream: DuplexStream,
    count: usize,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-late-notif-flood-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();

                cx.send_notification(SessionNotification::new(
                    sid.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("in-band"),
                    ))),
                ))?;

                let cx_clone = cx.clone();
                let sid_for_late = sid.clone();
                let _ = cx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    for index in 0..count {
                        let _ = cx_clone.send_notification(SessionNotification::new(
                            sid_for_late.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!(
                                    "LATE-AFTER-FINISH-{index}"
                                ))),
                            )),
                        ));
                    }
                    Ok(())
                });

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that advertises a `SessionModeState` and select/value-id mode/model
/// config options in `NewSessionResponse`. It accepts the stable
/// `session/set_config_option` request and emits update notifications.
///
/// Used to exercise the bridge's mode/model discovery + switch surface
/// (`SessionInit` event, `/session/set-mode`, `/session/set-config-option`,
/// the compatibility `/session/set-model` alias, and `/session/init`).
///
/// The agent records the most recent set request so tests can assert it
/// flowed through. Concurrency: the inner `Mutex`es are tiny and only
/// touched on the dispatch loop, so contention is irrelevant.
pub async fn run_modes_models_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ConfigOptionUpdate, CurrentModeUpdate, SessionConfigKind, SessionConfigOption,
        SessionConfigOptionCategory, SessionConfigSelect, SessionConfigSelectOption, SessionMode,
        SessionModeState, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
        SetSessionModeRequest, SetSessionModeResponse,
    };
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let current_mode = std::sync::Arc::new(Mutex::new("ask".to_string()));
    let current_model = std::sync::Arc::new(Mutex::new("gpt-4o-mini".to_string()));

    let cm_for_new = current_mode.clone();
    let cmod_for_new = current_model.clone();
    let cm_for_set_mode = current_mode.clone();
    let cm_for_set_config = current_mode.clone();
    let cmod_for_set_config = current_model.clone();

    fn config_options_for(mode: &str, model: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    mode.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("architect", "Architect"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    model.to_string(),
                    vec![
                        SessionConfigSelectOption::new("gpt-4o-mini", "GPT-4o mini"),
                        SessionConfigSelectOption::new("gpt-4o", "GPT-4o"),
                        SessionConfigSelectOption::new("claude-sonnet", "Claude Sonnet"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    Agent
        .builder()
        .name("agui-bridge-modes-models-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cm = cm_for_new.clone();
                let cmod = cmod_for_new.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    let modes = SessionModeState::new(
                        cm.lock().expect("mode poisoned").clone(),
                        vec![
                            SessionMode::new("ask", "Ask")
                                .description("Read-only conversational mode".to_string()),
                            SessionMode::new("architect", "Architect")
                                .description("Plan-and-design mode".to_string()),
                            SessionMode::new("code", "Code")
                                .description("Edit-the-codebase mode".to_string()),
                        ],
                    );
                    let mode = cm.lock().expect("mode poisoned").clone();
                    let model = cmod.lock().expect("model poisoned").clone();
                    let response =
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .modes(modes)
                            .config_options(config_options_for(&mode, &model));
                    responder.respond(response)
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cm = cm_for_set_config.clone();
                let cmod = cmod_for_set_config.clone();
                async move |req: SetSessionConfigOptionRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let config_id = req.config_id.0.to_string();
                    let value = req
                        .value
                        .as_value_id()
                        .map(|id| id.0.to_string())
                        .unwrap_or_default();
                    match config_id.as_str() {
                        "mode" if ["ask", "architect", "code"].contains(&value.as_str()) => {
                            *cm.lock().expect("mode poisoned") = value;
                        }
                        "model"
                            if ["gpt-4o-mini", "gpt-4o", "claude-sonnet"]
                                .contains(&value.as_str()) =>
                        {
                            *cmod.lock().expect("model poisoned") = value;
                        }
                        _ => {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "unknown config option value",
                                ),
                            );
                        }
                    }
                    let mode = cm.lock().expect("mode poisoned").clone();
                    let model = cmod.lock().expect("model poisoned").clone();
                    let options = config_options_for(&mode, &model);
                    let _ = cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(options.clone())),
                    ));
                    responder.respond(SetSessionConfigOptionResponse::new(options))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let cm = cm_for_set_mode.clone();
                async move |req: SetSessionModeRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let mode_id = req.mode_id.0.to_string();
                    // Reject unknown modes so the bridge can surface a 422.
                    if !["ask", "architect", "code"].contains(&mode_id.as_str()) {
                        return responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "unknown mode_id: {mode_id}"
                            )),
                        );
                    }
                    *cm.lock().expect("mode poisoned") = mode_id.clone();
                    // Notify the bridge so its `init_state` cache and the
                    // SessionInit emitted on the next prompt reflect the
                    // change. This also gives translator coverage of the
                    // `CurrentModeUpdate → agent:mode_update` path under a
                    // real `set_mode` flow.
                    let _ = cx.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(mode_id)),
                    ));
                    responder.respond(SetSessionModeResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                // Keep a prompt in flight long enough for setting tests to
                // exercise the actor's serial command queue.
                tokio::time::sleep(Duration::from_millis(250)).await;
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("ok"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Probe shared with the boolean config-option integration tests.
#[derive(Debug, Default, Clone)]
pub struct BooleanConfigProbe {
    initialize_boolean_capability: Arc<Mutex<Option<bool>>>,
    requests: Arc<Mutex<Vec<(String, SessionConfigOptionValue)>>>,
}

impl BooleanConfigProbe {
    #[must_use]
    pub fn initialize_boolean_capability(&self) -> bool {
        self.initialize_boolean_capability
            .lock()
            .expect("boolean capability probe poisoned")
            .unwrap_or(false)
    }

    #[must_use]
    pub fn requests(&self) -> Vec<(String, SessionConfigOptionValue)> {
        self.requests
            .lock()
            .expect("boolean config probe poisoned")
            .clone()
    }
}

/// Agent fixture for the stable typed session configuration values.
pub async fn run_boolean_config_agent(
    stream: DuplexStream,
    probe: Arc<BooleanConfigProbe>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigOption, SessionConfigSelectOption, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse,
    };

    fn options(enabled: bool, mode: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::boolean("enabled", "Enabled", enabled),
            SessionConfigOption::select(
                "mode",
                "Mode",
                mode.to_string(),
                vec![
                    SessionConfigSelectOption::new("ask", "Ask"),
                    SessionConfigSelectOption::new("code", "Code"),
                ],
            ),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let enabled = Arc::new(Mutex::new(false));
    let mode = Arc::new(Mutex::new("ask".to_string()));

    Agent
        .builder()
        .name("agui-bridge-boolean-config-test")
        .on_receive_request(
            {
                let probe = probe.clone();
                async move |req: InitializeRequest, responder, _cx| {
                    let advertised = req
                        .client_capabilities
                        .session
                        .as_ref()
                        .and_then(|session| session.config_options.as_ref())
                        .and_then(|options| options.boolean.as_ref())
                        .is_some();
                    *probe
                        .initialize_boolean_capability
                        .lock()
                        .expect("boolean capability probe poisoned") = Some(advertised);
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let enabled = enabled.clone();
                let mode = mode.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .config_options(options(
                                *enabled.lock().expect("enabled value poisoned"),
                                &mode.lock().expect("mode value poisoned"),
                            )),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let probe = probe.clone();
                let enabled = enabled.clone();
                let mode = mode.clone();
                async move |req: SetSessionConfigOptionRequest,
                            responder,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    let config_id = req.config_id.0.to_string();
                    probe
                        .requests
                        .lock()
                        .expect("boolean config probe poisoned")
                        .push((config_id.clone(), req.value.clone()));
                    match (config_id.as_str(), req.value) {
                        ("enabled", SessionConfigOptionValue::Boolean { value }) => {
                            *enabled.lock().expect("enabled value poisoned") = value;
                        }
                        ("mode", SessionConfigOptionValue::ValueId { value }) => {
                            *mode.lock().expect("mode value poisoned") = value.0.to_string();
                        }
                        _ => {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "unexpected config option value",
                                ),
                            );
                        }
                    }
                    responder.respond(SetSessionConfigOptionResponse::new(options(
                        *enabled.lock().expect("enabled value poisoned"),
                        &mode.lock().expect("mode value poisoned"),
                    )))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("boolean config ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture for proving discovered config validation happens before ACP.
/// Any setting request is a test failure; invalid HTTP values must be rejected
/// from the cached snapshot without entering this handler.
pub async fn run_rejecting_config_agent(
    stream: DuplexStream,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest,
    };
    use std::sync::atomic::Ordering;

    fn options() -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "ask",
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "model-a",
                    vec![
                        SessionConfigSelectOption::new("model-a", "Model A"),
                        SessionConfigSelectOption::new("model-b", "Model B"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-rejecting-config-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                        .config_options(options()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let calls = calls.clone();
                async move |_req: SetSessionConfigOptionRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("invalid config value reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("config validation ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture with no config snapshot or legacy mode capability. Any
/// setting request is a test failure; the bridge must reject it locally.
pub async fn run_rejecting_undiscovered_settings_agent(
    stream: DuplexStream,
    calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{SetSessionConfigOptionRequest, SetSessionModeRequest};
    use std::sync::atomic::Ordering;

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-rejecting-undiscovered-settings-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let calls = calls.clone();
                async move |_req: SetSessionConfigOptionRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("undiscovered config option reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let calls = calls.clone();
                async move |_req: SetSessionModeRequest,
                            _responder,
                            _cx|
                            -> Result<(), agent_client_protocol::Error> {
                    calls.fetch_add(1, Ordering::SeqCst);
                    panic!("undiscovered legacy mode reached the ACP mock");
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("undiscovered settings ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that advertises legacy modes alongside a non-mode config snapshot.
/// The bridge must use `session/set_mode` instead of assuming every non-empty
/// `config_options` list contains a mode option.
pub async fn run_mixed_mode_capabilities_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CurrentModeUpdate, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelect, SessionConfigSelectOption, SessionMode, SessionModeState,
        SetSessionModeRequest, SetSessionModeResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    let current_mode = std::sync::Arc::new(Mutex::new("ask".to_string()));
    let current_mode_for_new = current_mode.clone();
    let current_mode_for_set = current_mode.clone();

    Agent
        .builder()
        .name("agui-bridge-mixed-mode-capabilities-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let current_mode = current_mode_for_new.clone();
                async move |_req: NewSessionRequest, responder, _cx| {
                    let mode = current_mode.lock().expect("mode poisoned").clone();
                    let modes = SessionModeState::new(
                        mode,
                        vec![
                            SessionMode::new("ask", "Ask"),
                            SessionMode::new("code", "Code"),
                        ],
                    );
                    let model = SessionConfigOption::new(
                        "model",
                        "Model",
                        SessionConfigKind::Select(SessionConfigSelect::new(
                            "gpt-4o-mini",
                            vec![SessionConfigSelectOption::new("gpt-4o-mini", "GPT-4o mini")],
                        )),
                    )
                    .category(SessionConfigOptionCategory::Model);
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .modes(modes)
                            .config_options(vec![model]),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let current_mode = current_mode_for_set.clone();
                async move |req: SetSessionModeRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let mode = req.mode_id.0.to_string();
                    if !["ask", "code"].contains(&mode.as_str()) {
                        return responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "unknown mode_id: {mode}"
                            )),
                        );
                    }
                    *current_mode.lock().expect("mode poisoned") = mode.clone();
                    cx.send_notification(SessionNotification::new(
                        req.session_id,
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(mode)),
                    ))?;
                    responder.respond(SetSessionModeResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("mixed mode ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent fixture whose setting RPC never responds. The bridge must bound the
/// actor-side wait and evict the now-uncertain session instead of reusing it.
pub async fn run_unresponsive_setting_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    };

    fn mode_options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-unresponsive-setting-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                        .config_options(mode_options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: SetSessionConfigOptionRequest, _responder, _cx| {
                tokio::time::sleep(Duration::from_secs(60)).await;
                // The actor must close the connection before this response is
                // reached; keeping the branch typed makes the fixture's
                // behavior explicit without introducing a never type.
                let _ = SetSessionConfigOptionResponse::new(mode_options("code"));
                Ok(())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("setting fixture ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that emits a complete replacement `ConfigOptionUpdate` during the
/// prompt. Used to prove the bridge does not merge stale option snapshots.
pub async fn run_config_update_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ConfigOptionUpdate, SessionConfigKind, SessionConfigOption, SessionConfigSelect,
        SessionConfigSelectOption,
    };

    fn options(id: &str, value: &str) -> Vec<SessionConfigOption> {
        vec![SessionConfigOption::new(
            id.to_string(),
            "Config",
            SessionConfigKind::Select(SessionConfigSelect::new(
                value.to_string(),
                vec![SessionConfigSelectOption::new(value.to_string(), "Value")],
            )),
        )]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());
    Agent
        .builder()
        .name("agui-bridge-config-update-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                        .config_options(options("initial", "before")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(options(
                        "replacement",
                        "after",
                    ))),
                ))?;
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("config updated"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that emits `count` text chunks, each carrying its own ordinal
/// (`"<i>;"`), back-to-back with no inter-chunk delay, then returns
/// `EndTurn`. Unlike [`run_long_running_agent`] (which paces at 50ms and is
/// meant for disconnect tests), this fixture maximises throughput so tests
/// can assert the bridge preserves chunk ordering and loses none of them
/// under load.
///
/// The ordinal-with-separator format (`"0;1;2;..."` once concatenated) lets
/// a test reconstruct the delivered sequence from the SSE body and assert it
/// is exactly `0..count` in order.
pub async fn run_counting_agent(stream: DuplexStream, count: u32) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-counting-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                let sid = req.session_id.clone();
                for i in 0..count {
                    cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(format!("{i};")),
                        ))),
                    ))?;
                }
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that sleeps `delay_ms` before answering the `session/new` request,
/// simulating a backend that is slow to establish a session. Used to pin the
/// bridge's `open_session_timeout` path: with a timeout shorter than
/// `delay_ms`, `BridgeAppState::session_for` must abort the handshake and
/// surface an error rather than hang.
///
/// The `initialize` handshake itself is answered promptly so the delay is
/// isolated to session creation.
pub async fn run_slow_handshake_agent(
    stream: DuplexStream,
    delay_ms: u64,
) -> Result<(), BridgeError> {
    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let delay_ms = u32::try_from(delay_ms).unwrap_or(u32::MAX);

    Agent
        .builder()
        .name("agui-bridge-slow-handshake-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                tokio::time::sleep(Duration::from_millis(u64::from(delay_ms))).await;
                responder.respond(NewSessionResponse::new(SessionId::from(
                    Uuid::new_v4().to_string(),
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("ready"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Close behavior used by the lifecycle test agent.
#[derive(Debug, Clone, Copy)]
pub enum CloseBehavior {
    Success,
    Error,
    Timeout,
}

/// ACP SessionIds received by a close-capable test agent.
pub type SharedCloseSessionIds = std::sync::Arc<Mutex<Vec<String>>>;

/// Coordination hooks for lifecycle claim race tests.
#[derive(Clone, Debug)]
pub struct LifecycleControl {
    close_started: std::sync::Arc<Notify>,
    allow_close: std::sync::Arc<Notify>,
    setting_started: std::sync::Arc<Notify>,
    setting_started_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    allow_setting: std::sync::Arc<Notify>,
}

impl LifecycleControl {
    #[must_use]
    pub fn new() -> Self {
        Self {
            close_started: std::sync::Arc::new(Notify::new()),
            allow_close: std::sync::Arc::new(Notify::new()),
            setting_started: std::sync::Arc::new(Notify::new()),
            setting_started_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            allow_setting: std::sync::Arc::new(Notify::new()),
        }
    }

    pub async fn wait_close_started(&self) {
        self.close_started.notified().await;
    }

    pub async fn wait_setting_started(&self) {
        self.setting_started.notified().await;
    }

    pub async fn wait_settings_started(&self, count: usize) {
        loop {
            let notified = self.setting_started.notified();
            if self
                .setting_started_count
                .load(std::sync::atomic::Ordering::Acquire)
                >= count
            {
                return;
            }
            notified.await;
        }
    }

    pub fn release_close(&self) {
        self.allow_close.notify_waiters();
    }

    pub fn release_setting(&self) {
        self.allow_setting.notify_waiters();
    }
}

impl Default for LifecycleControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Agent fixture that advertises `sessionCapabilities.close`, records the
/// typed ACP SessionId in each close request, and returns the selected close
/// outcome. The fixture also supports a normal prompt so endpoint admission
/// tests can create a cached session through the real route.
pub async fn run_close_agent(
    stream: DuplexStream,
    closed_ids: SharedCloseSessionIds,
    behavior: CloseBehavior,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CloseSessionRequest, CloseSessionResponse, SessionCapabilities, SessionCloseCapabilities,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-close-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().close(SessionCloseCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(
                    "real-close-session-id",
                )))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let closed_ids = closed_ids.clone();
                async move |req: CloseSessionRequest, responder, _cx| {
                    closed_ids
                        .lock()
                        .expect("close ids poisoned")
                        .push(req.session_id.0.to_string());
                    match behavior {
                        CloseBehavior::Success => responder.respond(CloseSessionResponse::new()),
                        CloseBehavior::Error => responder.respond_with_error(
                            agent_client_protocol::util::internal_error("close failed"),
                        ),
                        CloseBehavior::Timeout => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            responder.respond(CloseSessionResponse::new())
                        }
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("close agent prompt"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Close-capable fixture with independently gated close and setting RPCs.
/// Used to make lifecycle claim ordering deterministic in integration tests.
pub async fn run_close_setting_agent(
    stream: DuplexStream,
    closed_ids: SharedCloseSessionIds,
    control: LifecycleControl,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        CloseSessionRequest, CloseSessionResponse, SessionCapabilities, SessionCloseCapabilities,
        SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    };

    fn options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
            SessionConfigOption::new(
                "model",
                "Model",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    "model-a",
                    vec![SessionConfigSelectOption::new("model-a", "Model A")],
                )),
            )
            .category(SessionConfigOptionCategory::Model),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-close-setting-race-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().close(SessionCloseCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from("real-close-session-id"))
                        .config_options(options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let closed_ids = closed_ids.clone();
                let control = control.clone();
                async move |req: CloseSessionRequest, responder, _cx| {
                    closed_ids
                        .lock()
                        .expect("close ids poisoned")
                        .push(req.session_id.0.to_string());
                    let allowed = control.allow_close.notified();
                    control.close_started.notify_waiters();
                    allowed.await;
                    responder.respond(CloseSessionResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |_req: SetSessionConfigOptionRequest, responder, _cx| {
                    let allowed = control.allow_setting.notified();
                    control
                        .setting_started_count
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                    control.setting_started.notify_waiters();
                    allowed.await;
                    responder.respond(SetSessionConfigOptionResponse::new(options("code")))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("close setting race prompt"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Delete behavior used by the session-delete lifecycle tests.
#[derive(Debug, Clone, Copy)]
pub enum DeleteBehavior {
    Success,
    Error,
    Timeout,
}

/// ACP SessionIds received by a delete-capable test agent.
pub type SharedDeleteSessionIds = std::sync::Arc<Mutex<Vec<String>>>;

/// Agent fixture that advertises `sessionCapabilities.delete`, records the
/// typed ACP SessionId in each delete request, and returns the selected
/// outcome. It deliberately does not advertise `sessionCapabilities.list` so
/// the bridge's delete capability check remains independent of listing.
pub async fn run_delete_agent(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    behavior: DeleteBehavior,
) -> Result<(), BridgeError> {
    run_delete_agent_with_session_id(stream, deleted_ids, behavior, "real-delete-session-id").await
}

/// Variant of [`run_delete_agent`] that lets a test model different cached
/// ACP SessionIds under different logical thread keys.
pub async fn run_delete_agent_with_session_id(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    behavior: DeleteBehavior,
    session_id: &'static str,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        DeleteSessionRequest, DeleteSessionResponse, SessionCapabilities, SessionDeleteCapabilities,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-delete-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().delete(SessionDeleteCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(NewSessionResponse::new(SessionId::from(session_id)))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let deleted_ids = deleted_ids.clone();
                async move |req: DeleteSessionRequest, responder, _cx| {
                    deleted_ids
                        .lock()
                        .expect("delete ids poisoned")
                        .push(req.session_id.0.to_string());
                    match behavior {
                        DeleteBehavior::Success => responder.respond(DeleteSessionResponse::new()),
                        DeleteBehavior::Error => responder.respond_with_error(
                            agent_client_protocol::util::internal_error("delete failed"),
                        ),
                        DeleteBehavior::Timeout => {
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            responder.respond(DeleteSessionResponse::new())
                        }
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |req: PromptRequest,
                        responder,
                        cx: ConnectionTo<agent_client_protocol::Client>| {
                cx.send_notification(SessionNotification::new(
                    req.session_id,
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new("delete agent prompt"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Coordination hooks for delete admission and lifecycle-claim tests.
#[derive(Clone, Debug)]
pub struct DeleteLifecycleControl {
    prompt_started: std::sync::Arc<Notify>,
    allow_prompt: std::sync::Arc<Notify>,
    setting_started: std::sync::Arc<Notify>,
    allow_setting: std::sync::Arc<Notify>,
    delete_started: std::sync::Arc<Notify>,
    allow_delete: std::sync::Arc<Notify>,
}

impl DeleteLifecycleControl {
    #[must_use]
    pub fn new() -> Self {
        Self {
            prompt_started: std::sync::Arc::new(Notify::new()),
            allow_prompt: std::sync::Arc::new(Notify::new()),
            setting_started: std::sync::Arc::new(Notify::new()),
            allow_setting: std::sync::Arc::new(Notify::new()),
            delete_started: std::sync::Arc::new(Notify::new()),
            allow_delete: std::sync::Arc::new(Notify::new()),
        }
    }

    pub async fn wait_prompt_started(&self) {
        self.prompt_started.notified().await;
    }

    pub async fn wait_setting_started(&self) {
        self.setting_started.notified().await;
    }

    pub async fn wait_delete_started(&self) {
        self.delete_started.notified().await;
    }

    pub fn release_prompt(&self) {
        self.allow_prompt.notify_waiters();
    }

    pub fn release_setting(&self) {
        self.allow_setting.notify_waiters();
    }

    pub fn release_delete(&self) {
        self.allow_delete.notify_waiters();
    }
}

impl Default for DeleteLifecycleControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Delete-capable fixture with independently gated prompt, setting, and
/// delete requests.
pub async fn run_delete_lifecycle_agent(
    stream: DuplexStream,
    deleted_ids: SharedDeleteSessionIds,
    control: DeleteLifecycleControl,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        DeleteSessionRequest, DeleteSessionResponse, SessionCapabilities, SessionConfigKind,
        SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
        SessionConfigSelectOption, SessionDeleteCapabilities, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse,
    };

    fn options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "mode",
                "Mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![
                        SessionConfigSelectOption::new("ask", "Ask"),
                        SessionConfigSelectOption::new("code", "Code"),
                    ],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    Agent
        .builder()
        .name("agui-bridge-delete-lifecycle-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new().session_capabilities(
                            SessionCapabilities::new().delete(SessionDeleteCapabilities::new()),
                        ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |_req: NewSessionRequest, responder, _cx| {
                responder.respond(
                    NewSessionResponse::new(SessionId::from("real-delete-session-id"))
                        .config_options(options("ask")),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let deleted_ids = deleted_ids.clone();
                let control = control.clone();
                async move |req: DeleteSessionRequest, responder, _cx| {
                    deleted_ids
                        .lock()
                        .expect("delete ids poisoned")
                        .push(req.session_id.0.to_string());
                    control.delete_started.notify_waiters();
                    control.allow_delete.notified().await;
                    responder.respond(DeleteSessionResponse::new())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |_: SetSessionConfigOptionRequest, responder, _cx| {
                    control.setting_started.notify_waiters();
                    control.allow_setting.notified().await;
                    responder.respond(SetSessionConfigOptionResponse::new(options("code")))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let control = control.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    control.prompt_started.notify_waiters();
                    control.allow_prompt.notified().await;
                    cx.send_notification(SessionNotification::new(
                        req.session_id,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new("delete lifecycle prompt"),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Shared session store for [`run_session_history_agent_with`]. Maps
/// `session_id -> (title, persisted cwd, history lines)`. Sharing one store across multiple
/// spawned agent instances models a real agent that persists sessions to
/// disk across separate ACP connections.
pub type SharedSessionStore = std::sync::Arc<
    Mutex<std::collections::HashMap<String, (Option<String>, std::path::PathBuf, Vec<String>)>>,
>;

/// Agent that supports session persistence: `session/new`, `session/list`,
/// `session/load`, and `session/prompt`. Used to exercise the bridge's
/// conversation-history surface (`GET /sessions`, resume-via-`session/load`).
///
/// Behaviour:
/// - `initialize` advertises `loadSession = true` and
///   `sessionCapabilities.list = {}`.
/// - `session/new` mints a fresh `SessionId`, records it with an empty
///   history and a title derived from the first prompt.
/// - `session/prompt` appends a user+assistant turn to the session's stored
///   history and streams the assistant reply (`"echo: <text>"`).
/// - `session/list` returns one `SessionInfo` per recorded session.
/// - `session/load` replays the stored history as `AgentMessageChunk`
///   notifications (prefixed `HISTORY:`) before responding, mirroring how a
///   real agent surfaces a resumed conversation.
///
/// Each spawned instance gets a private store; pass [`SharedSessionStore`] to
/// share one store across instances (modelling cross-connection persistence).
pub async fn run_session_history_agent_with(
    stream: DuplexStream,
    store: SharedSessionStore,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::v1::{
        ListSessionsRequest, ListSessionsResponse, LoadSessionRequest, LoadSessionResponse,
        SessionCapabilities, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
        SessionConfigSelect, SessionConfigSelectOption, SessionInfo, SessionListCapabilities,
    };

    fn history_config_options(value: &str) -> Vec<SessionConfigOption> {
        vec![
            SessionConfigOption::new(
                "history-mode",
                "History mode",
                SessionConfigKind::Select(SessionConfigSelect::new(
                    value.to_string(),
                    vec![SessionConfigSelectOption::new("new", "New")],
                )),
            )
            .category(SessionConfigOptionCategory::Mode),
        ]
    }

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let store_new = store.clone();
    let store_list = store.clone();
    let store_load = store.clone();
    let store_prompt = store.clone();

    Agent
        .builder()
        .name("agui-bridge-session-history-test")
        .on_receive_request(
            async move |req: InitializeRequest, responder, _cx| {
                responder.respond(
                    InitializeResponse::new(req.protocol_version).agent_capabilities(
                        AgentCapabilities::new()
                            .load_session(true)
                            .session_capabilities(
                                SessionCapabilities::new().list(SessionListCapabilities::default()),
                            ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_new.clone();
                async move |req: NewSessionRequest, responder, _cx| {
                    let id = Uuid::new_v4().to_string();
                    store
                        .lock()
                        .expect("store poisoned")
                        .insert(id.clone(), (None, req.cwd, Vec::new()));
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(id))
                            .config_options(history_config_options("new")),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_list.clone();
                async move |_req: ListSessionsRequest, responder, _cx| {
                    let guard = store.lock().expect("store poisoned");
                    let sessions: Vec<SessionInfo> = guard
                        .iter()
                        .map(|(id, (title, cwd, _))| {
                            SessionInfo::new(SessionId::from(id.clone()), cwd.clone())
                                .title(title.clone())
                        })
                        .collect();
                    responder.respond(ListSessionsResponse::new(sessions))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_load.clone();
                async move |req: LoadSessionRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let sid = req.session_id.clone();
                    let history = {
                        let guard = store.lock().expect("store poisoned");
                        let Some((_, persisted_cwd, history)) = guard.get(&sid.0.to_string())
                        else {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error("unknown session id"),
                            );
                        };
                        if req.cwd != *persisted_cwd {
                            return responder.respond_with_error(
                                agent_client_protocol::util::internal_error(
                                    "session/load cwd does not match persisted cwd",
                                ),
                            );
                        }
                        history.clone()
                    };
                    // Replay the stored history as notifications before the
                    // load response (the contract `session/load` defines).
                    for line in history {
                        cx.send_notification(SessionNotification::new(
                            sid.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(format!("HISTORY:{line}"))),
                            )),
                        ))?;
                    }
                    responder.respond(
                        LoadSessionResponse::new().config_options(history_config_options("loaded")),
                    )
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let store = store_prompt.clone();
                async move |req: PromptRequest,
                            responder,
                            cx: ConnectionTo<agent_client_protocol::Client>| {
                    let text = req
                        .prompt
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text(t) => Some(t.text.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    let sid = req.session_id.clone();
                    let reply = format!("echo: {text}");

                    {
                        let mut guard = store.lock().expect("store poisoned");
                        let entry = guard
                            .entry(sid.0.to_string())
                            .or_insert_with(|| (None, std::path::PathBuf::new(), Vec::new()));
                        if entry.0.is_none() {
                            entry.0 = Some(text.clone());
                        }
                        entry.2.push(format!("user:{text}"));
                        entry.2.push(format!("assistant:{reply}"));
                    }

                    cx.send_notification(SessionNotification::new(
                        sid.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                            TextContent::new(reply),
                        ))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, _cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.route_with_result(result),
                    Dispatch::Request(_, responder) => responder.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled request"),
                    ),
                    Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
