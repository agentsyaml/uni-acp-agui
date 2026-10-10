use super::registry::{TerminalError, TerminalRegistry, wire_error};
use agent_client_protocol::RequestCancellation;
use agent_client_protocol::schema::v1::{
    CreateTerminalRequest, CreateTerminalResponse, KillTerminalRequest, KillTerminalResponse,
    ReleaseTerminalRequest, ReleaseTerminalResponse, TerminalOutputRequest, TerminalOutputResponse,
    WaitForTerminalExitRequest, WaitForTerminalExitResponse,
};

pub(crate) async fn create_request(
    request: CreateTerminalRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
    responder: agent_client_protocol::Responder<CreateTerminalResponse>,
    permit: Option<crate::session::WorkPermit>,
) -> Result<(), agent_client_protocol::Error> {
    // fork/exec under the registry's std Mutex is blocking work; keep it off
    // the async dispatch thread without restructuring the lock-around-spawn
    // invariant that makes the per-session cap race-free.
    let created = cancellation
        .run_until_cancelled(async move {
            let create_result = match tokio::task::spawn_blocking(move || {
                let _permit = permit;
                registry.create(&request)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => return Err(wire_error(TerminalError::Internal)),
            };
            create_result.map_err(wire_error)
        })
        .await;
    let (mut result, guard, mut keep) = match created {
        Ok((_id, guard)) if cancellation.is_cancelled() => (
            Err(agent_client_protocol::Error::request_cancelled()),
            Some(guard),
            false,
        ),
        Ok((id, guard)) => (Ok(CreateTerminalResponse::new(id)), Some(guard), true),
        Err(_error) if cancellation.is_cancelled() => (
            Err(agent_client_protocol::Error::request_cancelled()),
            None,
            false,
        ),
        Err(error) => (Err(error), None, false),
    };
    if keep && cancellation.is_cancelled() {
        result = Err(agent_client_protocol::Error::request_cancelled());
        keep = false;
    }
    let send_result = responder.respond_with_result(result);
    if keep
        && send_result.is_ok()
        && let Some(guard) = guard
    {
        guard.disarm();
    }
    send_result
}

pub(crate) async fn output_request(
    request: TerminalOutputRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
) -> Result<TerminalOutputResponse, agent_client_protocol::Error> {
    let result = cancellation
        .run_until_cancelled(async move {
            registry
                .get(&request.terminal_id)
                .map_err(wire_error)?
                .output()
                .await
                .map_err(wire_error)
        })
        .await;
    if cancellation.is_cancelled() {
        Err(agent_client_protocol::Error::request_cancelled())
    } else {
        result
    }
}

pub(crate) async fn wait_request(
    request: WaitForTerminalExitRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
) -> Result<WaitForTerminalExitResponse, agent_client_protocol::Error> {
    let result = cancellation
        .run_until_cancelled(async move {
            let terminal = registry.get(&request.terminal_id).map_err(wire_error)?;
            terminal
                .wait()
                .await
                .map(WaitForTerminalExitResponse::new)
                .map_err(wire_error)
        })
        .await;
    if cancellation.is_cancelled() {
        Err(agent_client_protocol::Error::request_cancelled())
    } else {
        result
    }
}

pub(crate) async fn kill_request(
    request: KillTerminalRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
) -> Result<KillTerminalResponse, agent_client_protocol::Error> {
    let result = cancellation
        .run_until_cancelled(async move {
            let terminal = registry.get(&request.terminal_id).map_err(wire_error)?;
            terminal.kill().await.map_err(wire_error)
        })
        .await;
    if cancellation.is_cancelled() {
        Err(agent_client_protocol::Error::request_cancelled())
    } else {
        result.map(|_| KillTerminalResponse::new())
    }
}

pub(crate) async fn release_request(
    request: ReleaseTerminalRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
) -> Result<ReleaseTerminalResponse, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    let terminal = registry.take(&request.terminal_id).map_err(wire_error)?;
    let result = cancellation
        .run_until_cancelled(async { terminal.release().await.map_err(wire_error) })
        .await;
    if cancellation.is_cancelled() {
        terminal.abort();
        Err(agent_client_protocol::Error::request_cancelled())
    } else {
        result.map(|_| ReleaseTerminalResponse::new())
    }
}
