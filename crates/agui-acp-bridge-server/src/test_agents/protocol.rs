use super::*;

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
