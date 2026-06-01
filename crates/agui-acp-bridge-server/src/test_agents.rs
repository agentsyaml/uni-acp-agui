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
//! - [`run_counting_agent`] — emits N sequentially-numbered text chunks as fast
//!   as possible, then ends. Used for streaming ordering / throughput tests.
//! - [`run_slow_handshake_agent`] — sleeps before answering `session/new`, so
//!   `open_session` exceeds a short `open_session_timeout`. Used to pin the
//!   startup-timeout path.

use std::sync::Mutex;
use std::time::Duration;

use agent_client_protocol::schema::{
    AgentCapabilities, ContentBlock, ContentChunk, ImageContent, InitializeRequest,
    InitializeResponse, NewSessionRequest, NewSessionResponse, PermissionOption,
    PermissionOptionId, PermissionOptionKind, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus,
    PromptRequest, PromptResponse, RequestPermissionRequest, SessionId, SessionNotification,
    SessionUpdate, StopReason, TextContent, ToolCallUpdate, ToolCallUpdateFields,
};
use agent_client_protocol::{Agent, ByteStreams, ConnectionTo, Dispatch};
use agui_acp_bridge_core::BridgeError;
use tokio::io::DuplexStream;
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
///   5. AgentMessageChunk (image)       ← non-text → CustomEvent
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
/// Late notifications are intentionally dropped (with a tracing warning)
/// rather than buffered or routed to the next prompt. Per `session.rs`,
/// the per-prompt event slot is cleared as soon as the prompt completes,
/// so any notification arriving on the connection-level callback after
/// that point has nowhere to go and would be a protocol violation if the
/// agent took the contract literally.
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                // Forward Response messages to their awaiters; reject anything else.
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Agent that advertises a `SessionModeState` and (when the
/// `unstable_session_model` feature is on) a `SessionModelState` in
/// `NewSessionResponse`, accepts ACP `session/set_mode` /
/// `session/set_model` requests, and emits a `CurrentModeUpdate`
/// notification when the bridge issues a successful `set_mode`.
///
/// Used to exercise the bridge's mode/model discovery + switch surface
/// (`SessionInit` event, `/session/set-mode`, `/session/set-model`,
/// `/session/init`).
///
/// The agent records the most recent set request so tests can assert it
/// flowed through. Concurrency: the inner `Mutex`es are tiny and only
/// touched on the dispatch loop, so contention is irrelevant.
#[cfg(feature = "unstable_session_model")]
pub async fn run_modes_models_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::{
        CurrentModeUpdate, ModelInfo, SessionMode, SessionModeState, SessionModelState,
        SetSessionModeRequest, SetSessionModeResponse, SetSessionModelRequest,
        SetSessionModelResponse,
    };

    let (read, write) = tokio::io::split(stream);
    let transport = ByteStreams::new(write.compat_write(), read.compat());

    let current_mode = std::sync::Arc::new(Mutex::new("ask".to_string()));
    let current_model = std::sync::Arc::new(Mutex::new("gpt-4o-mini".to_string()));

    let cm_for_new = current_mode.clone();
    let cmod_for_new = current_model.clone();
    let cm_for_set_mode = current_mode.clone();
    let cmod_for_set_model = current_model.clone();

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
                    let models = SessionModelState::new(
                        cmod.lock().expect("model poisoned").clone(),
                        vec![
                            ModelInfo::new("gpt-4o-mini", "GPT-4o mini")
                                .description("Fast, cheap".to_string()),
                            ModelInfo::new("gpt-4o", "GPT-4o")
                                .description("More capable".to_string()),
                            ModelInfo::new("claude-sonnet", "Claude Sonnet")
                                .description("Anthropic flagship".to_string()),
                        ],
                    );
                    responder.respond(
                        NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string()))
                            .modes(modes)
                            .models(models),
                    )
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
            {
                let cmod = cmod_for_set_model.clone();
                async move |req: SetSessionModelRequest,
                            responder,
                            _cx: ConnectionTo<agent_client_protocol::Client>| {
                    let model_id = req.model_id.0.to_string();
                    if !["gpt-4o-mini", "gpt-4o", "claude-sonnet"].contains(&model_id.as_str()) {
                        return responder.respond_with_error(
                            agent_client_protocol::util::internal_error(format!(
                                "unknown model_id: {model_id}"
                            )),
                        );
                    }
                    *cmod.lock().expect("model poisoned") = model_id;
                    responder.respond(SetSessionModelResponse::new())
                }
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
                        TextContent::new("ok"),
                    ))),
                ))?;
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_dispatch(
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}

/// Shared session store for [`run_session_history_agent_with`]. Maps
/// `session_id -> (title, history lines)`. Sharing one store across multiple
/// spawned agent instances models a real agent that persists sessions to
/// disk across separate ACP connections.
pub type SharedSessionStore =
    std::sync::Arc<Mutex<std::collections::HashMap<String, (Option<String>, Vec<String>)>>>;

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
/// Each spawned instance gets a private store. Use
/// [`run_session_history_agent_with`] to share one store across instances
/// (modelling cross-connection persistence).
pub async fn run_session_history_agent(stream: DuplexStream) -> Result<(), BridgeError> {
    let store: SharedSessionStore =
        std::sync::Arc::new(Mutex::new(std::collections::HashMap::new()));
    run_session_history_agent_with(stream, store).await
}

/// Like [`run_session_history_agent`] but backed by a caller-supplied shared
/// store, so multiple spawned instances see the same persisted sessions.
pub async fn run_session_history_agent_with(
    stream: DuplexStream,
    store: SharedSessionStore,
) -> Result<(), BridgeError> {
    use agent_client_protocol::schema::{
        ListSessionsRequest, ListSessionsResponse, LoadSessionRequest, LoadSessionResponse,
        SessionCapabilities, SessionInfo, SessionListCapabilities,
    };

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
                async move |_req: NewSessionRequest, responder, _cx| {
                    let id = Uuid::new_v4().to_string();
                    store
                        .lock()
                        .expect("store poisoned")
                        .insert(id.clone(), (None, Vec::new()));
                    responder.respond(NewSessionResponse::new(SessionId::from(id)))
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
                        .map(|(id, (title, _))| {
                            SessionInfo::new(SessionId::from(id.clone()), "/").title(title.clone())
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
                        guard
                            .get(&sid.0.to_string())
                            .map(|(_, h)| h.clone())
                            .unwrap_or_default()
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
                    responder.respond(LoadSessionResponse::new())
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
                            .or_insert_with(|| (None, Vec::new()));
                        if entry.0.is_none() {
                            entry.0 = Some(text.clone());
                        }
                        entry.1.push(format!("user:{text}"));
                        entry.1.push(format!("assistant:{reply}"));
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
            async move |message: Dispatch, cx: ConnectionTo<agent_client_protocol::Client>| {
                match message {
                    Dispatch::Response(result, router) => router.respond_with_result(result),
                    other => other.respond_with_error(
                        agent_client_protocol::util::internal_error("unhandled message"),
                        cx,
                    ),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_to(transport)
        .await
        .map_err(BridgeError::Acp)
}
