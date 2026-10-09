mod support;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, ReadTextFileRequest,
    RequestId, SessionId, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{RawJsonRpcMessage, RawJsonRpcParams, TransportFrame};
use agui_acp_bridge_core::BridgeConfig;
use agui_acp_bridge_policy::AutoAllow;
use agui_acp_bridge_server::{AcpClient, BridgeAppState, CustomAgentInProcessClient, build_router};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use uuid::Uuid;

use support::{count_events, user_input};

const N: u32 = 500;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn final_response_eof_preserves_all_chunks() {
    assert_prefix_and_terminal(true).await;
}

#[tokio::test]
async fn missing_final_response_eof_is_run_error_after_all_chunks() {
    assert_prefix_and_terminal(false).await;
}

async fn assert_prefix_and_terminal(include_final_response: bool) {
    let published = Arc::new(Notify::new());
    let closed = Arc::new(Notify::new());
    let agent_published = published.clone();
    let agent_closed = closed.clone();
    let client: Arc<dyn AcpClient> = Arc::new(CustomAgentInProcessClient::new(move |stream| {
        let published = agent_published.clone();
        let closed = agent_closed.clone();
        async move {
            let (read, mut write) = tokio::io::split(stream);
            let mut read = BufReader::new(read);

            let initialize = read_request(&mut read, "initialize").await?;
            let initialize_params: InitializeRequest = params(&initialize)?;
            send_result(
                &mut write,
                initialize.id,
                &InitializeResponse::new(initialize_params.protocol_version)
                    .agent_capabilities(AgentCapabilities::new()),
            )
            .await?;

            let new_session = read_request(&mut read, "session/new").await?;
            let _: NewSessionRequest = params(&new_session)?;
            send_result(
                &mut write,
                new_session.id,
                &NewSessionResponse::new(SessionId::from(Uuid::new_v4().to_string())),
            )
            .await?;

            let prompt = read_request(&mut read, "session/prompt").await?;
            let prompt_id = prompt.id.clone();
            let prompt_params: PromptRequest = params(&prompt)?;
            for i in 0..N {
                let notification = SessionNotification::new(
                    prompt_params.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(format!("{i};")),
                    ))),
                );
                send_notification(&mut write, "session/update", &notification).await?;
            }

            let barrier_id = RequestId::Str("retired-prefix-barrier".into());
            let barrier = ReadTextFileRequest::new(prompt_params.session_id, "/fixture");
            send_request(
                &mut write,
                "fs/read_text_file",
                barrier_id.clone(),
                &barrier,
            )
            .await?;
            let response = read_response(&mut read, &barrier_id).await?;
            let value = response.to_json().map_err(protocol_io_error)?;
            let value: serde_json::Value =
                serde_json::from_str(&value).map_err(protocol_io_error)?;
            assert_eq!(
                value["error"]["code"], -32601,
                "expected exact method-not-found response: {value}"
            );

            if include_final_response {
                send_result(
                    &mut write,
                    prompt_id,
                    &PromptResponse::new(StopReason::EndTurn),
                )
                .await?;
            }
            // `send_*` flushes every frame; this signal marks the complete wire prefix.
            published.notify_one();
            tokio::time::timeout(IO_TIMEOUT, write.shutdown())
                .await
                .expect("agent write shutdown timed out")
                .map_err(protocol_io_error)?;
            drop(write);
            drop(read);
            closed.notify_one();
            Ok(())
        }
    }));

    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_policy(Arc::new(AutoAllow))
        .with_config(BridgeConfig {
            event_buffer: 1,
            slow_consumer_timeout: Duration::ZERO,
            ..BridgeConfig::default()
        })
        .build();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, build_router(state)).await });
    let _server_abort = AbortOnDrop(server);

    let response = reqwest::Client::new()
        .post(format!("http://{addr}/"))
        .header("accept", "text/event-stream")
        .json(&user_input("retired-prefix", "run", "go"))
        .send()
        .await
        .expect("HTTP request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    tokio::time::timeout(IO_TIMEOUT, published.notified())
        .await
        .expect("wire prefix published");
    tokio::time::timeout(IO_TIMEOUT, closed.notified())
        .await
        .expect("agent transport closed after wire flush");
    let mut stream = response.bytes_stream();
    let mut body = String::new();
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(4), stream.next())
        .await
        .expect("HTTP body stalled")
    {
        body.push_str(&String::from_utf8_lossy(&chunk.expect("HTTP body chunk")));
    }

    assert_eq!(
        count_events(&body, "RUN_FINISHED"),
        usize::from(include_final_response),
        "{body}"
    );
    assert_eq!(
        count_events(&body, "RUN_ERROR"),
        usize::from(!include_final_response),
        "{body}"
    );

    let mut seq = Vec::new();
    let mut terminal_seen = false;
    for line in body.lines().filter_map(|line| line.strip_prefix("data:")) {
        let payload = line.trim_start();
        let value: serde_json::Value = serde_json::from_str(payload).expect("SSE JSON");
        let event_type = value["type"].as_str().unwrap_or_default();
        if terminal_seen {
            panic!("event after terminal frame: {event_type}");
        }
        if event_type == "RUN_FINISHED" || event_type == "RUN_ERROR" {
            terminal_seen = true;
        }
        if event_type == "TEXT_MESSAGE_CONTENT"
            && let Some(delta) = value["delta"].as_str()
        {
            seq.extend(delta.split(';').filter_map(|part| part.parse::<u32>().ok()));
        }
    }
    assert_eq!(
        seq,
        (0..N).collect::<Vec<_>>(),
        "all chunks exactly once/in order"
    );
}

struct RequestFrame {
    id: RequestId,
    params: Option<RawJsonRpcParams>,
}

async fn read_request<R: tokio::io::AsyncRead + Unpin>(
    read: &mut BufReader<R>,
    expected_method: &str,
) -> Result<RequestFrame, agui_acp_bridge_core::BridgeError> {
    let frame = read_frame(read).await?;
    let RawJsonRpcMessage::Request(request) = frame else {
        return Err(protocol_error(format!(
            "expected {expected_method} request, got {frame:?}"
        )));
    };
    assert_eq!(&*request.method, expected_method);
    Ok(RequestFrame {
        id: request.id,
        params: request.params,
    })
}

async fn read_response<R: tokio::io::AsyncRead + Unpin>(
    read: &mut BufReader<R>,
    expected_id: &RequestId,
) -> Result<TransportFrame, agui_acp_bridge_core::BridgeError> {
    let frame = read_frame(read).await?;
    match &frame {
        RawJsonRpcMessage::Response(_) => {
            let json = TransportFrame::Single(frame.clone())
                .to_json()
                .map_err(|error| protocol_error(error.to_string()))?;
            let value: serde_json::Value =
                serde_json::from_str(&json).map_err(|error| protocol_error(error.to_string()))?;
            assert_eq!(value["id"], serde_json::to_value(expected_id).unwrap());
        }
        _ => return Err(protocol_error(format!("expected response, got {frame:?}"))),
    }
    Ok(TransportFrame::Single(frame))
}

async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    read: &mut BufReader<R>,
) -> Result<RawJsonRpcMessage, agui_acp_bridge_core::BridgeError> {
    let mut line = String::new();
    let count = tokio::time::timeout(IO_TIMEOUT, read.read_line(&mut line))
        .await
        .expect("agent fixture read timed out")
        .map_err(protocol_io_error)?;
    if count == 0 {
        return Err(protocol_error("unexpected EOF from bridge"));
    }
    match TransportFrame::parse_json(line.trim_end()) {
        TransportFrame::Single(message) => Ok(message),
        frame => Err(protocol_error(format!(
            "expected single JSON-RPC frame, got {frame:?}"
        ))),
    }
}

fn params<T: serde::de::DeserializeOwned>(
    request: &RequestFrame,
) -> Result<T, agui_acp_bridge_core::BridgeError> {
    let value = request
        .params
        .clone()
        .map(RawJsonRpcParams::into_value)
        .unwrap_or(serde_json::Value::Null);
    serde_json::from_value(value).map_err(|error| protocol_error(error.to_string()))
}

async fn send_result<W: tokio::io::AsyncWrite + Unpin, T: serde::Serialize>(
    write: &mut W,
    id: RequestId,
    result: &T,
) -> Result<(), agui_acp_bridge_core::BridgeError> {
    let value = serde_json::to_value(result).map_err(|error| protocol_error(error.to_string()))?;
    send_frame(
        write,
        TransportFrame::Single(RawJsonRpcMessage::response(id, Ok(value))),
    )
    .await
}

async fn send_notification<W: tokio::io::AsyncWrite + Unpin, T: serde::Serialize>(
    write: &mut W,
    method: &str,
    params: &T,
) -> Result<(), agui_acp_bridge_core::BridgeError> {
    let params = serde_json::to_value(params).map_err(|error| protocol_error(error.to_string()))?;
    let message = RawJsonRpcMessage::notification(method.to_owned(), params)
        .map_err(|error| protocol_error(error.to_string()))?;
    send_frame(write, TransportFrame::Single(message)).await
}

async fn send_request<W: tokio::io::AsyncWrite + Unpin, T: serde::Serialize>(
    write: &mut W,
    method: &str,
    id: RequestId,
    params: &T,
) -> Result<(), agui_acp_bridge_core::BridgeError> {
    let params = serde_json::to_value(params).map_err(|error| protocol_error(error.to_string()))?;
    let message = RawJsonRpcMessage::request(method.to_owned(), params, id)
        .map_err(|error| protocol_error(error.to_string()))?;
    send_frame(write, TransportFrame::Single(message)).await
}

async fn send_frame<W: tokio::io::AsyncWrite + Unpin>(
    write: &mut W,
    frame: TransportFrame,
) -> Result<(), agui_acp_bridge_core::BridgeError> {
    let json = frame
        .to_json()
        .map_err(|error| protocol_error(error.to_string()))?;
    tokio::time::timeout(IO_TIMEOUT, async {
        write.write_all(json.as_bytes()).await?;
        write.write_all(b"\n").await?;
        write.flush().await
    })
    .await
    .expect("agent fixture write timed out")
    .map_err(protocol_io_error)
}

fn protocol_error(message: impl std::fmt::Display) -> agui_acp_bridge_core::BridgeError {
    agui_acp_bridge_core::BridgeError::Acp(
        agent_client_protocol::Error::internal_error().data(message.to_string()),
    )
}

fn protocol_io_error(error: impl std::fmt::Display) -> agui_acp_bridge_core::BridgeError {
    protocol_error(io::Error::other(error.to_string()))
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
