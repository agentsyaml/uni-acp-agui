use super::*;

pub(super) fn file_operation_error(error: BridgeError) -> agent_client_protocol::Error {
    match crate::file_ops::error_kind(&error) {
        crate::file_ops::FileErrorKind::InvalidParams => {
            agent_client_protocol::Error::invalid_params()
        }
        crate::file_ops::FileErrorKind::ResourceNotFound => {
            agent_client_protocol::Error::resource_not_found(None)
        }
        crate::file_ops::FileErrorKind::Internal => agent_client_protocol::Error::internal_error(),
    }
}

pub(super) fn request_path(path: &std::path::Path) -> Result<&str, agent_client_protocol::Error> {
    path.to_str()
        .ok_or_else(agent_client_protocol::Error::invalid_params)
}

pub(super) fn request_line(
    value: Option<u32>,
) -> Result<Option<usize>, agent_client_protocol::Error> {
    value
        .map(|value| {
            usize::try_from(value).map_err(|_| agent_client_protocol::Error::invalid_params())
        })
        .transpose()
}

pub(super) fn write_content_validation_error(
    content: &str,
) -> Option<agent_client_protocol::Error> {
    (content.len() > crate::file_ops::MAX_TEXT_FILE_BYTES)
        .then(agent_client_protocol::Error::invalid_params)
}

pub(super) async fn read_file_request_with_work(
    request: ReadTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
    permit: Option<WorkPermit>,
) -> Result<ReadTextFileResponse, agent_client_protocol::Error> {
    let path = request_path(&request.path)?;
    let line = request_line(request.line)?;
    let limit = request_line(request.limit)?;
    let _guard = filesystem_lock.lock().await;
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    crate::file_ops::read_text_file_range_with_work(cwd.as_path(), path, line, limit, permit)
        .await
        .map(ReadTextFileResponse::new)
        .map_err(file_operation_error)
}

pub(super) async fn write_file_request_with_work(
    request: WriteTextFileRequest,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    cancellation: RequestCancellation,
    permit: Option<WorkPermit>,
) -> Result<WriteTextFileResponse, agent_client_protocol::Error> {
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }
    if let Some(error) = write_content_validation_error(&request.content) {
        return Err(error);
    }

    let path = request_path(&request.path)?;
    let _guard = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err(agent_client_protocol::Error::request_cancelled());
        }
        guard = filesystem_lock.lock() => guard,
    };
    if cancellation.is_cancelled() {
        return Err(agent_client_protocol::Error::request_cancelled());
    }

    crate::file_ops::write_text_file_with_work(cwd.as_path(), path, &request.content, permit)
        .await
        .map(|_| WriteTextFileResponse::new())
        .map_err(file_operation_error)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn read_request(
    req: ReadTextFileRequest,
    responder: agent_client_protocol::Responder<ReadTextFileResponse>,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! {
                biased;
                changed = retired.changed() => { let _ = changed; Ok(()) },
                () = std::future::pending() => Ok(()),
            };
        }
    };

    let cancellation = responder.cancellation();
    let cwd = cwd.clone();
    let filesystem_lock = filesystem_lock.clone();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit.clone();
        let result = cancellation
            .run_until_cancelled(read_file_request_with_work(
                req,
                cwd,
                filesystem_lock,
                cancellation.clone(),
                Some(permit.clone()),
            ))
            .await;
        let result = if cancellation.is_cancelled() {
            Err(agent_client_protocol::Error::request_cancelled())
        } else {
            result
        };
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "filesystem read response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "filesystem read task could not be spawned");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_request(
    req: WriteTextFileRequest,
    responder: agent_client_protocol::Responder<WriteTextFileResponse>,
    cx: ConnectionTo<Agent>,
    enabled: bool,
    cwd: Arc<PathBuf>,
    filesystem_lock: Arc<AsyncMutex<()>>,
    work: Arc<WorkAdmission>,
) -> Result<(), agent_client_protocol::Error> {
    if !enabled {
        return responder.respond_with_error(agent_client_protocol::Error::method_not_found());
    }
    if let Some(error) = write_content_validation_error(&req.content) {
        return responder.respond_with_error(error);
    }
    let permit = match work.try_acquire(&req, &responder.id().to_string()) {
        Ok(permit) => permit,
        Err(()) => {
            let mut retired = work.retire_tx.subscribe();
            return tokio::select! {
                biased;
                changed = retired.changed() => { let _ = changed; Ok(()) },
                () = std::future::pending() => Ok(()),
            };
        }
    };

    let cancellation = responder.cancellation();
    let cwd = cwd.clone();
    let filesystem_lock = filesystem_lock.clone();
    if let Err(error) = cx.spawn(async move {
        let _permit = permit.clone();
        let result = write_file_request_with_work(
            req,
            cwd,
            filesystem_lock,
            cancellation.clone(),
            Some(permit.clone()),
        )
        .await;
        if let Err(error) = responder.respond_with_result(result) {
            tracing::debug!(?error, "filesystem write response could not be sent");
        }
        Ok(())
    }) {
        tracing::debug!(?error, "filesystem write task could not be spawned");
    }
    Ok(())
}
