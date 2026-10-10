use super::*;

/// Open a short-lived ACP connection, confirm the agent advertises
/// `session/list`, fetch all session summaries (following `nextCursor`
/// pagination), and tear the connection down.
///
/// Stateless: the bridge stores nothing — this is a pass-through of what the
/// agent persists. Returns [`BridgeError::Unsupported`] when the agent does
/// not advertise `sessionCapabilities.list`.
pub(crate) async fn list_sessions_via<T>(
    connector: T,
    cfg: SessionConfig,
) -> Result<Vec<SessionSummary>, BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel::<Result<Vec<SessionSummary>, BridgeError>>();
    let cwd = cfg.cwd.clone();
    let request_timeout = cfg.config.set_session_timeout;

    // A minimal client: we issue requests from the connection task and never
    // receive notifications/requests we care about, so the builder only needs
    // a dispatch handler to route responses back to their awaiters.
    let result_tx = std::sync::Mutex::new(Some(result_tx));
    let connect_result = agent_client_protocol::Client
        .builder()
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let cwd = cwd.clone();
            let result_slot = result_tx;
            async move {
                let outcome = list_sessions_inner(&cx, cwd, request_timeout).await;
                if let Some(tx) = result_slot.lock().expect("result slot poisoned").take() {
                    let _ = tx.send(outcome);
                }
                Ok(())
            }
        })
        .await;

    // If the connection itself failed (spawn/handshake transport error),
    // surface that; otherwise return whatever the inner task produced.
    match result_rx.await {
        Ok(res) => res,
        Err(_) => match connect_result {
            Ok(()) => Err(BridgeError::SessionClosed),
            Err(err) => Err(BridgeError::Acp(err)),
        },
    }
}

/// Open a short-lived ACP connection, confirm the agent advertises
/// `session/delete`, delete the supplied persisted session, and tear the
/// connection down.
pub(crate) async fn delete_session_via<T>(
    connector: T,
    cfg: SessionConfig,
    session_id: SessionId,
) -> Result<(), BridgeError>
where
    T: ConnectTo<Client> + Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel::<Result<(), BridgeError>>();
    let request_timeout = cfg.config.set_session_timeout;

    let result_tx = std::sync::Mutex::new(Some(result_tx));
    let connect_result = agent_client_protocol::Client
        .builder()
        .on_receive_dispatch(
            async move |message: agent_client_protocol::Dispatch, _cx: ConnectionTo<Agent>| {
                match message {
                    agent_client_protocol::Dispatch::Response(result, router) => {
                        router.route_with_result(result)
                    }
                    agent_client_protocol::Dispatch::Request(_, responder) => responder
                        .respond_with_error(agent_client_protocol::Error::method_not_found()),
                    agent_client_protocol::Dispatch::Notification(_) => Ok(()),
                }
            },
            agent_client_protocol::on_receive_dispatch!(),
        )
        .connect_with(connector, move |cx: ConnectionTo<Agent>| {
            let result_slot = result_tx;
            async move {
                let outcome = delete_session_inner(&cx, session_id, request_timeout).await;
                if let Some(tx) = result_slot.lock().expect("result slot poisoned").take() {
                    let _ = tx.send(outcome);
                }
                Ok(())
            }
        })
        .await;

    match result_rx.await {
        Ok(res) => res,
        Err(_) => match connect_result {
            Ok(()) => Err(BridgeError::SessionClosed),
            Err(err) => Err(BridgeError::Acp(err)),
        },
    }
}

/// Inner body of [`list_sessions_via`]: initialize, capability-gate, then
/// page through `session/list`.
pub(super) async fn list_sessions_inner(
    cx: &ConnectionTo<Agent>,
    _cwd: PathBuf,
    request_timeout: Duration,
) -> Result<Vec<SessionSummary>, BridgeError> {
    use agent_client_protocol::schema::v1::ListSessionsRequest;

    let init = tokio::time::timeout(
        request_timeout,
        cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map_err(BridgeError::Acp)?;

    require_protocol_v1(init.protocol_version)?;

    if init.agent_capabilities.session_capabilities.list.is_none() {
        return Err(BridgeError::Unsupported("session/list".into()));
    }

    let mut summaries = BoundedSessionList::default();
    let mut cursor: Option<String> = None;
    // Guard against a misbehaving agent returning an endless cursor chain.
    let mut pages = 0usize;
    loop {
        // Intentionally do NOT filter by `cwd`: the history UI wants every
        // conversation the agent persists, and an exact-path filter is
        // fragile across canonicalization differences (Windows `\\?\`
        // extended-length prefixes, trailing slashes, symlinks). Listing
        // unfiltered and letting the client decide is both more useful and
        // more robust.
        let mut req = ListSessionsRequest::new();
        if let Some(c) = cursor.take() {
            req = req.cursor(c);
        }
        let resp = tokio::time::timeout(request_timeout, cx.send_request(req).block_task())
            .await
            .map_err(|_| BridgeError::Timeout(request_timeout))?
            .map_err(BridgeError::Acp)?;

        tracing::debug!(
            page = pages,
            count = resp.sessions.len(),
            has_next = resp.next_cursor.is_some(),
            "session/list page received"
        );

        // Bound enforcement lives in `BoundedSessionList::push_with_size`.
        for info in resp.sessions {
            summaries.push(SessionSummary {
                session_id: info.session_id.0.to_string(),
                cwd: info.cwd.to_string_lossy().into_owned(),
                title: info.title,
                updated_at: info.updated_at,
            })?;
        }

        pages += 1;
        match next_list_cursor(pages, resp.next_cursor)? {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    tracing::info!(total = summaries.len(), "session/list complete");
    Ok(summaries.into_summaries())
}

pub(super) async fn delete_session_inner(
    cx: &ConnectionTo<Agent>,
    session_id: SessionId,
    request_timeout: Duration,
) -> Result<(), BridgeError> {
    let init = tokio::time::timeout(
        request_timeout,
        cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map_err(BridgeError::Acp)?;

    require_protocol_v1(init.protocol_version)?;

    if init
        .agent_capabilities
        .session_capabilities
        .delete
        .is_none()
    {
        return Err(BridgeError::Unsupported("session/delete".into()));
    }

    tokio::time::timeout(
        request_timeout,
        cx.send_request(DeleteSessionRequest::new(session_id))
            .block_task(),
    )
    .await
    .map_err(|_| BridgeError::Timeout(request_timeout))?
    .map(|_| ())
    .map_err(BridgeError::Acp)
}
