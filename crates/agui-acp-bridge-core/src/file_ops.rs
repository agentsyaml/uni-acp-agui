//! File operations for ACP agent requests.
//!
//! Implements `ReadTextFile` and `WriteTextFile` with path sandboxing
//! to prevent directory traversal and symlink-based escapes.
//!
//! # Sandbox model
//!
//! Every operation receives a `cwd` (the session's working directory) and an
//! **absolute** path provided by the agent. We canonicalize the path through
//! the OS (resolving symlinks), and verify the canonical path is inside the
//! canonical `cwd`.
//!
//! For reads we canonicalize the full target. For writes, the target may not
//! exist yet, so we canonicalize the **deepest existing ancestor** for the
//! initial sandbox check. The actual write is then rooted at an opened `cwd`
//! directory and performed with descriptor-relative Linux filesystem
//! operations, so a later symlink swap cannot redirect it.
//!
//! Callers are expected to pass an already-canonicalized `cwd`. The
//! [`canonicalize_cwd`] helper returns one — `BridgeAppState` calls it
//! once at construction.

use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::Arc;

use crate::error::BridgeError;

#[cfg(target_os = "linux")]
mod linux_secure_write;

#[must_use]
pub(crate) fn read_text_file_supported() -> bool {
    #[cfg(target_os = "linux")]
    {
        linux_secure_write::supported()
    }

    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[must_use]
pub(crate) fn write_text_file_supported() -> bool {
    read_text_file_supported()
}

#[cfg(test)]
mod test_gates;
#[cfg(test)]
#[cfg(target_os = "linux")]
pub(crate) use test_gates::{ReadGate, WriteGate};
#[cfg(all(test, target_os = "linux"))]
pub(crate) use test_gates::{
    clear_read_gate, clear_write_gate, install_read_gate, install_write_gate,
};
#[cfg(test)]
pub(crate) use test_gates::{wait_for_read_gate, wait_for_write_gate};

/// Fixed upper bound for one text-file operation.
///
/// Reads and writes are bounded before any disk access so a single request
/// cannot grow the in-memory buffer without limit.
pub const MAX_TEXT_FILE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileErrorKind {
    InvalidParams,
    ResourceNotFound,
    Internal,
}

pub(crate) fn error_kind(error: &BridgeError) -> FileErrorKind {
    match error {
        BridgeError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
            FileErrorKind::InvalidParams
        }
        BridgeError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            FileErrorKind::ResourceNotFound
        }
        _ => FileErrorKind::Internal,
    }
}

fn invalid_params(message: &'static str) -> BridgeError {
    BridgeError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

fn resource_not_found(message: &'static str) -> BridgeError {
    BridgeError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, message))
}

fn filesystem_io(error: std::io::Error) -> BridgeError {
    let kind = if error.kind() == std::io::ErrorKind::NotFound {
        std::io::ErrorKind::NotFound
    } else {
        std::io::ErrorKind::Other
    };
    let message = if kind == std::io::ErrorKind::NotFound {
        // Keep the historical fixed string for the mapped-NotFound case.
        "filesystem resource not found".to_string()
    } else {
        // Preserve fidelity: the display of an io::Error carries its kind and
        // raw OS error (e.g. "PermissionDenied (os error 13)"), so the agent
        // can distinguish EACCES vs EISDIR vs ENAMETOOLONG.
        format!("filesystem operation failed: {error}")
    };
    // Chain the original error as `source` (io::Error::source() forwards to
    // the inner error) without changing the public FileErrorKind taxonomy:
    // `error_kind()` still sees the same preserved `kind`.
    BridgeError::Io(std::io::Error::new(
        kind,
        SourcedIoMessage {
            message,
            source: error,
        },
    ))
}

/// Message wrapper keeping the original [`std::io::Error`] reachable as the
/// [`std::error::Error::source`] of the wrapped io error.
#[derive(Debug)]
struct SourcedIoMessage {
    message: String,
    source: std::io::Error,
}

impl std::fmt::Display for SourcedIoMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SourcedIoMessage {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Canonicalize `cwd` for use as a sandbox root.
///
/// Resolves the path through the OS (handles `.`, `..`, and symlinks) and
/// returns an absolute `PathBuf`. If `cwd` does not exist, falls back to
/// [`std::path::absolute`] so callers still get an absolute path — the
/// sandbox check will then fail closed for any agent path that escapes.
///
/// # Errors
///
/// Returns the underlying I/O error if `cwd` does not exist **and**
/// `std::path::absolute` cannot resolve it (e.g. on platforms with no
/// notion of a current directory).
pub fn canonicalize_cwd(cwd: &Path) -> std::io::Result<PathBuf> {
    let cwd = match std::fs::canonicalize(cwd) {
        Ok(p) => Ok(p),
        Err(_) => std::path::absolute(cwd),
    }?;

    Ok(cwd)
}

/// A strong lease on the filesystem directory identity used by secure file
/// operations. Keep this alive for the lifetime of a session that uses the
/// same canonical root across requests; a `PathBuf` alone does not retain that
/// identity and may refer to a replacement directory later.
#[must_use = "the lease must stay alive to retain the anchored filesystem root"]
pub struct FilesystemRootGuard {
    #[cfg(target_os = "linux")]
    _root: std::sync::Arc<linux_secure_write::RootDirectory>,
    #[cfg(not(target_os = "linux"))]
    _private: (),
}

/// Pin a canonical filesystem root for a session or other multi-operation
/// owner. Individual read/write calls also hold an operation lease while in
/// flight. This does not canonicalize `cwd`; callers must pass the exact
/// canonical path used for their operations.
///
/// ```no_run
/// # use std::path::Path;
/// # use agui_acp_bridge_core::file_ops::pin_filesystem_root;
/// let canonical_cwd = std::fs::canonicalize(".")?;
/// let _root_lease = pin_filesystem_root(&canonical_cwd)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn pin_filesystem_root(cwd: &Path) -> std::io::Result<FilesystemRootGuard> {
    #[cfg(target_os = "linux")]
    {
        let root = linux_secure_write::root_for(cwd)?;
        Ok(FilesystemRootGuard { _root: root })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cwd;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "secure filesystem roots are unsupported on this platform",
        ))
    }
}

/// Return a "clean" form of `cwd` suitable for sending to an ACP agent in
/// `session/new` / `session/load` / `session/list`.
///
/// On Windows, [`std::fs::canonicalize`] yields an extended-length path with
/// a `\\?\` verbatim prefix (e.g. `\\?\D:\work\project`). Agents persist and
/// key sessions by this directory string, and many (opencode included) derive
/// a project identity from it; the verbatim prefix makes the agent's stored
/// directory differ from the plain `D:\work\project` a user or other tool
/// would use, which breaks directory-scoped session listing. We strip the
/// prefix so the agent sees a conventional absolute path.
///
/// We deliberately keep the **internal** sandbox `cwd` canonicalized (prefix
/// intact): file-op containment compares it against agent paths that are
/// themselves canonicalized with the prefix, so both must carry it. Only the
/// ACP-facing copy is cleaned.
///
/// On non-Windows platforms this is a no-op clone.
#[must_use]
pub fn acp_cwd(cwd: &Path) -> PathBuf {
    let s = cwd.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        // `\\?\UNC\server\share` → `\\server\share`; `\\?\D:\x` → `D:\x`.
        if let Some(unc) = rest.strip_prefix("UNC\\") {
            return PathBuf::from(format!(r"\\{unc}"));
        }
        return PathBuf::from(rest);
    }
    cwd.to_path_buf()
}

/// Resolve an agent-supplied path for **reading**.
///
/// Returns the canonical target for a descriptor-relative open. Errors with
/// `PermissionDenied` if the resolved path escapes `cwd`, and propagates I/O
/// errors otherwise.
fn safe_resolve_read(cwd: &Path, path: &str) -> Result<PathBuf, BridgeError> {
    let candidate = require_absolute(path)?;
    match std::fs::canonicalize(&candidate) {
        Ok(canonical) => {
            enforce_sandbox(cwd, &canonical, &candidate)?;
            Ok(canonical)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let anchor = deepest_existing_ancestor(&candidate)?;
            enforce_sandbox(cwd, &anchor, &candidate)?;
            Err(resource_not_found("filesystem resource not found"))
        }
        Err(error) => Err(filesystem_io(error)),
    }
}

/// Resolve an agent-supplied path for **writing**.
///
/// The target may not exist yet, but its deepest existing ancestor must be
/// inside `cwd`. Returns the joined (un-canonicalized) target path so the
/// caller can create directories and write the file.
fn safe_resolve_write(cwd: &Path, path: &str) -> Result<PathBuf, BridgeError> {
    let candidate = require_absolute(path)?;

    // Find the deepest existing ancestor and canonicalize it. That gives us
    // an OS-honest answer about where the path actually lives, including
    // any symlinks the attacker may have placed in writable parts of the
    // filesystem.
    let anchor = deepest_existing_ancestor(&candidate)?;

    enforce_sandbox(cwd, &anchor, &candidate)?;
    Ok(candidate)
}

fn deepest_existing_ancestor(path: &Path) -> Result<PathBuf, BridgeError> {
    let mut probe = path;
    loop {
        match std::fs::canonicalize(probe) {
            Ok(path) => return Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                probe = probe
                    .parent()
                    .ok_or_else(|| resource_not_found("no existing filesystem ancestor"))?;
            }
            Err(error) => return Err(filesystem_io(error)),
        }
    }
}

fn require_absolute(path: &str) -> Result<PathBuf, BridgeError> {
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(invalid_params("ACP filesystem paths must be absolute"));
    }
    Ok(path.to_path_buf())
}

fn enforce_sandbox(cwd: &Path, canonical: &Path, _original: &Path) -> Result<(), BridgeError> {
    if !canonical.starts_with(cwd) {
        return Err(invalid_params("path escapes working directory"));
    }
    Ok(())
}

pub use io::{read_text_file, read_text_file_range, write_text_file};
pub(crate) use io::{read_text_file_range_with_work, write_text_file_with_work};
mod io;

#[cfg(test)]
mod tests;
