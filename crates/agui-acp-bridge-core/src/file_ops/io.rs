use super::*;

#[cfg(target_os = "linux")]
fn secure_filesystem_error(error: std::io::Error) -> BridgeError {
    if error.kind() == std::io::ErrorKind::InvalidInput {
        invalid_params("invalid filesystem path")
    } else if linux_secure_write::is_sandbox_error(&error) {
        invalid_params("path escapes working directory")
    } else if error.kind() == std::io::ErrorKind::Unsupported {
        BridgeError::Io(error)
    } else {
        filesystem_io(error)
    }
}

#[cfg(target_os = "linux")]
async fn write_to_secure_path(
    cwd: &Path,
    path: &Path,
    content: &str,
    root: Arc<linux_secure_write::RootDirectory>,
    permit: Option<crate::session::WorkPermit>,
) -> Result<(), BridgeError> {
    let cwd = cwd.to_path_buf();
    let path = path.to_path_buf();
    let content = content.to_owned();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut file = linux_secure_write::open(&root, &cwd, &path)?;
        std::io::Write::write_all(&mut file, content.as_bytes())
    })
    .await
    .map_err(|_| filesystem_io(std::io::Error::other("filesystem operation failed")))?
    .map_err(secure_filesystem_error)
}

#[cfg(not(target_os = "linux"))]
async fn write_to_secure_path(
    _cwd: &Path,
    _path: &Path,
    _content: &str,
    _permit: Option<crate::session::WorkPermit>,
) -> Result<(), BridgeError> {
    // ponytail: fail closed outside Linux; enable other targets only with
    // their native descriptor/handle-relative traversal primitive.
    Err(BridgeError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "secure filesystem writes are unsupported on this platform",
    )))
}

/// Read a text file from an absolute ACP path under `cwd`.
///
/// `cwd` MUST be canonicalized (see [`canonicalize_cwd`]). `limit` is a
/// maximum **line count**, matching ACP semantics. Reading starts at line 1;
/// use [`read_text_file_range`] for a different starting line.
pub async fn read_text_file(
    cwd: &Path,
    path: &str,
    limit: Option<usize>,
) -> Result<String, BridgeError> {
    read_text_file_range(cwd, path, None, limit).await
}

/// Read at most `limit` lines beginning at the 1-based `line` offset.
/// Newlines are preserved in the returned text.
pub async fn read_text_file_range(
    cwd: &Path,
    path: &str,
    line: Option<usize>,
    limit: Option<usize>,
) -> Result<String, BridgeError> {
    read_text_file_range_with_work(cwd, path, line, limit, None).await
}

pub(crate) async fn read_text_file_range_with_work(
    cwd: &Path,
    path: &str,
    line: Option<usize>,
    limit: Option<usize>,
    permit: Option<crate::session::WorkPermit>,
) -> Result<String, BridgeError> {
    let start_line = line.unwrap_or(1);
    if start_line == 0 {
        return Err(invalid_params("ACP read line is 1-based"));
    }

    require_absolute(path)?;
    #[cfg(target_os = "linux")]
    let root = linux_secure_write::root_for(cwd).map_err(secure_filesystem_error)?;
    let full_path = safe_resolve_read(cwd, path)?;
    #[cfg(test)]
    wait_for_read_gate(&full_path).await;
    let _cwd = cwd.to_path_buf();
    let full_path = full_path.to_path_buf();
    let bytes = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        #[cfg(target_os = "linux")]
        {
            let mut file = linux_secure_write::open_read(&root, &_cwd, &full_path)
                .map_err(secure_filesystem_error)?;
            let mut bytes = Vec::new();
            let mut bounded = std::io::Read::take(&mut file, (MAX_TEXT_FILE_BYTES + 1) as u64);
            std::io::Read::read_to_end(&mut bounded, &mut bytes).map_err(filesystem_io)?;
            Ok::<_, BridgeError>(bytes)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = full_path;
            Err::<Vec<u8>, _>(BridgeError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "secure filesystem reads are unsupported on this platform",
            )))
        }
    })
    .await
    .map_err(|_| filesystem_io(std::io::Error::other("filesystem operation failed")))??;
    if bytes.len() > MAX_TEXT_FILE_BYTES {
        return Err(invalid_params("text file exceeds the core size limit"));
    }

    let text = String::from_utf8(bytes).map_err(|_| {
        filesystem_io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "UTF-8",
        ))
    })?;
    let max_lines = limit.unwrap_or(usize::MAX);
    if max_lines == 0 {
        return Ok(String::new());
    }

    let mut content = String::new();
    let mut selected_lines = 0usize;
    for (index, current) in text.split_inclusive('\n').enumerate() {
        let current_line = index + 1;
        if current_line >= start_line {
            content.push_str(current);
            selected_lines += 1;
            if selected_lines == max_lines {
                break;
            }
        }
    }

    Ok(content)
}

/// Write a text file from an absolute ACP path under `cwd`.
///
/// `cwd` MUST be canonicalized (see [`canonicalize_cwd`]). The deepest
/// existing ancestor must be inside `cwd`, and the actual write is rooted at
/// an opened `cwd` directory so intermediate symlink swaps cannot redirect
/// it. Missing parent directories are created relative to that directory.
pub async fn write_text_file(cwd: &Path, path: &str, content: &str) -> Result<(), BridgeError> {
    write_text_file_with_work(cwd, path, content, None).await
}

pub(crate) async fn write_text_file_with_work(
    cwd: &Path,
    path: &str,
    content: &str,
    permit: Option<crate::session::WorkPermit>,
) -> Result<(), BridgeError> {
    if content.len() > MAX_TEXT_FILE_BYTES {
        return Err(invalid_params("text file exceeds the core size limit"));
    }

    require_absolute(path)?;
    #[cfg(target_os = "linux")]
    let root = linux_secure_write::root_for(cwd).map_err(secure_filesystem_error)?;
    let full_path = safe_resolve_write(cwd, path)?;
    #[cfg(test)]
    wait_for_write_gate(&full_path).await;
    #[cfg(target_os = "linux")]
    {
        write_to_secure_path(cwd, &full_path, content, root, permit).await
    }
    #[cfg(not(target_os = "linux"))]
    {
        write_to_secure_path(cwd, &full_path, content, permit).await
    }
}
