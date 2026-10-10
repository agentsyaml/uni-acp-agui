use super::*;

pub(super) async fn create(
    req: CreateTerminalRequest,
    responder: agent_client_protocol::Responder<
        agent_client_protocol::schema::v1::CreateTerminalResponse,
    >,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    registry: Option<TerminalRegistry>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let Some(registry) = registry else {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    };
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! { biased; _ = retired.changed() => Ok(()), () = std::future::pending() => Ok(()) };
        }
    };
    let cancellation = responder.cancellation();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit.clone();
        if let Err(error) = crate::terminal::create_request(
            req,
            registry,
            cancellation,
            responder,
            Some(permit.clone()),
        )
        .await
        {
            tracing::debug!(?error, "terminal create response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "terminal create task could not be spawned");
    }
    Ok(())
}

pub(super) async fn output(
    req: TerminalOutputRequest,
    responder: agent_client_protocol::Responder<
        agent_client_protocol::schema::v1::TerminalOutputResponse,
    >,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    registry: Option<TerminalRegistry>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let Some(registry) = registry else {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    };
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! { biased; _ = retired.changed() => Ok(()), () = std::future::pending() => Ok(()) };
        }
    };
    let cancellation = responder.cancellation();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit;
        let result = crate::terminal::output_request(req, registry, cancellation).await;
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "terminal output response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "terminal output task could not be spawned");
    }
    Ok(())
}

pub(super) async fn wait(
    req: WaitForTerminalExitRequest,
    responder: agent_client_protocol::Responder<
        agent_client_protocol::schema::v1::WaitForTerminalExitResponse,
    >,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    registry: Option<TerminalRegistry>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let Some(registry) = registry else {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    };
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! { biased; _ = retired.changed() => Ok(()), () = std::future::pending() => Ok(()) };
        }
    };
    let cancellation = responder.cancellation();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit;
        let result = crate::terminal::wait_request(req, registry, cancellation).await;
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "terminal wait response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "terminal wait task could not be spawned");
    }
    Ok(())
}

pub(super) async fn kill(
    req: KillTerminalRequest,
    responder: agent_client_protocol::Responder<
        agent_client_protocol::schema::v1::KillTerminalResponse,
    >,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    registry: Option<TerminalRegistry>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let Some(registry) = registry else {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    };
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! { biased; _ = retired.changed() => Ok(()), () = std::future::pending() => Ok(()) };
        }
    };
    let cancellation = responder.cancellation();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit;
        let result = crate::terminal::kill_request(req, registry, cancellation).await;
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "terminal kill response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "terminal kill task could not be spawned");
    }
    Ok(())
}

pub(super) async fn release(
    req: ReleaseTerminalRequest,
    responder: agent_client_protocol::Responder<
        agent_client_protocol::schema::v1::ReleaseTerminalResponse,
    >,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    registry: Option<TerminalRegistry>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let Some(registry) = registry else {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    };
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! { biased; _ = retired.changed() => Ok(()), () = std::future::pending() => Ok(()) };
        }
    };
    let cancellation = responder.cancellation();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit;
        let result = crate::terminal::release_request(req, registry, cancellation).await;
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "terminal release response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "terminal release task could not be spawned");
    }
    Ok(())
}
