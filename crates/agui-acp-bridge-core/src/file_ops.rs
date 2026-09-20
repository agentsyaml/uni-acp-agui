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

use crate::error::BridgeError;

#[cfg(target_os = "linux")]
mod linux_secure_write {
    use std::collections::HashMap;
    use std::ffi::CString;
    use std::fs::File;
    use std::io;
    use std::mem::size_of;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
    use std::os::raw::{c_char, c_int, c_long, c_uint};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Component, Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};

    const AT_FDCWD: RawFd = -100;
    const SYS_OPENAT2: c_long = 437;

    const O_RDONLY: c_int = 0;
    const O_WRONLY: c_int = 1;
    const O_CREAT: c_int = 0o100;
    const O_TRUNC: c_int = 0o1000;
    const O_DIRECTORY: c_int = 0o200000;
    const O_CLOEXEC: c_int = 0o2000000;
    const O_PATH: c_int = 0o10000000;

    const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
    const RESOLVE_NO_SYMLINKS: u64 = 0x04;
    const RESOLVE_BENEATH: u64 = 0x08;

    const ERRNO_ELOOP: i32 = 40;
    const ERRNO_ENOENT: i32 = 2;
    const ERRNO_ENOSYS: i32 = 38;
    const ERRNO_EXDEV: i32 = 18;

    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    unsafe extern "C" {
        fn syscall(number: c_long, ...) -> c_long;
        fn mkdirat(dirfd: c_int, path: *const c_char, mode: c_uint) -> c_int;
    }

    pub(super) struct RootDirectory {
        fd: OwnedFd,
    }

    // ponytail: retain bindings for the existing `Path` API; an explicit
    // per-session sandbox owner can replace this registry if teardown matters.
    static ROOT_DIRECTORIES: OnceLock<Mutex<HashMap<PathBuf, Arc<RootDirectory>>>> =
        OnceLock::new();

    fn root_directories() -> &'static Mutex<HashMap<PathBuf, Arc<RootDirectory>>> {
        ROOT_DIRECTORIES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn path_cstring(path: &Path) -> io::Result<CString> {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "filesystem path contains NUL")
        })
    }

    fn openat2_fd(
        dirfd: RawFd,
        path: &Path,
        flags: c_int,
        mode: c_uint,
        resolve: u64,
    ) -> io::Result<OwnedFd> {
        let path = path_cstring(path)?;
        let how = OpenHow {
            flags: flags as u64,
            mode: mode as u64,
            resolve,
        };
        // SAFETY: `path` and `how` remain alive for the syscall, and the
        // kernel writes no memory through either pointer.
        let fd = unsafe {
            syscall(
                SYS_OPENAT2,
                dirfd,
                path.as_ptr(),
                &how,
                size_of::<OpenHow>(),
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: a non-negative openat2 result is an owned file
            // descriptor returned by the kernel.
            Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
        }
    }

    fn mkdirat_dir(dirfd: RawFd, path: &Path) -> io::Result<()> {
        let path = path_cstring(path)?;
        // SAFETY: `path` remains alive for the syscall and is NUL-terminated.
        let result = unsafe { mkdirat(dirfd, path.as_ptr(), 0o777) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn file_from_fd(fd: OwnedFd) -> File {
        let raw_fd = fd.into_raw_fd();
        // SAFETY: `raw_fd` is transferred from `OwnedFd`, so `File` becomes
        // its sole owner and will close it exactly once.
        unsafe { File::from_raw_fd(raw_fd) }
    }

    fn open_root(cwd: &Path) -> io::Result<OwnedFd> {
        let flags = O_PATH | O_DIRECTORY | O_CLOEXEC;
        // Do not fall back to path-based `openat`: resolving an absolute cwd
        // that way leaves its intermediate components racy.
        openat2_fd(AT_FDCWD, cwd, flags, 0, RESOLVE_NO_SYMLINKS).map_err(|error| {
            if error.raw_os_error() == Some(ERRNO_ENOSYS) {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Linux openat2 is required for secure filesystem writes",
                )
            } else {
                error
            }
        })
    }

    pub(super) fn initialize_root(cwd: &Path) -> io::Result<()> {
        let _ = root_for(cwd)?;
        Ok(())
    }

    pub(super) fn root_for(cwd: &Path) -> io::Result<Arc<RootDirectory>> {
        let mut roots = root_directories()
            .lock()
            .map_err(|_| io::Error::other("sandbox root registry is poisoned"))?;
        if let Some(root) = roots.get(cwd).cloned() {
            return Ok(root);
        }
        let root = Arc::new(RootDirectory {
            fd: open_root(cwd)?,
        });
        roots.insert(cwd.to_path_buf(), root.clone());
        Ok(root)
    }

    pub(super) fn supported() -> bool {
        match openat2_fd(
            AT_FDCWD,
            Path::new(""),
            O_PATH | O_DIRECTORY | O_CLOEXEC,
            0,
            RESOLVE_NO_SYMLINKS,
        ) {
            Ok(_) => true,
            Err(error) => error.raw_os_error() == Some(ERRNO_ENOENT),
        }
    }

    fn open_dir_beneath(root_fd: RawFd, path: &Path) -> io::Result<OwnedFd> {
        openat2_fd(
            root_fd,
            path,
            O_PATH | O_DIRECTORY | O_CLOEXEC,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS,
        )
    }

    fn ensure_parent_openat2(root_fd: RawFd, parent: &Path) -> io::Result<()> {
        let mut prefix = PathBuf::new();
        let mut current: Option<OwnedFd> = None;

        for component in parent.components() {
            match component {
                Component::CurDir => continue,
                Component::Normal(name) => {
                    prefix.push(name);
                    let fd = match open_dir_beneath(root_fd, &prefix) {
                        Ok(fd) => fd,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {
                            let parent_fd = current
                                .as_ref()
                                .map_or(root_fd, |directory| directory.as_raw_fd());
                            match mkdirat_dir(parent_fd, Path::new(name)) {
                                Ok(()) => {}
                                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                                Err(error) => return Err(error),
                            }
                            open_dir_beneath(root_fd, &prefix)?
                        }
                        Err(error) => return Err(error),
                    };
                    current = Some(fd);
                }
                Component::ParentDir => {
                    prefix.push("..");
                    current = Some(open_dir_beneath(root_fd, &prefix)?);
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "write path is not relative to cwd",
                    ));
                }
            }
        }

        Ok(())
    }

    pub(super) fn open_read(
        root: &RootDirectory,
        cwd: &Path,
        candidate: &Path,
    ) -> io::Result<File> {
        let relative = candidate.strip_prefix(cwd).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "read path is not relative to cwd",
            )
        })?;
        let fd = openat2_fd(
            root.fd.as_raw_fd(),
            relative,
            O_RDONLY | O_CLOEXEC,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS,
        )?;
        Ok(file_from_fd(fd))
    }

    pub(super) fn open(root: &RootDirectory, cwd: &Path, candidate: &Path) -> io::Result<File> {
        let relative = candidate.strip_prefix(cwd).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "write path is not relative to cwd",
            )
        })?;
        let parent = relative.parent().unwrap_or_else(|| Path::new("."));
        ensure_parent_openat2(root.fd.as_raw_fd(), parent)?;
        let fd = openat2_fd(
            root.fd.as_raw_fd(),
            relative,
            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
            0o666,
            RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS,
        )?;
        Ok(file_from_fd(fd))
    }

    pub(super) fn is_sandbox_error(error: &io::Error) -> bool {
        matches!(error.raw_os_error(), Some(ERRNO_ELOOP | ERRNO_EXDEV))
    }
}

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
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(test)]
use tokio::sync::Notify;

/// Fixed upper bound for one text-file operation.
///
/// Reads and writes are bounded before any disk access so a single request
/// cannot grow the in-memory buffer without limit.
pub const MAX_TEXT_FILE_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
pub(crate) struct WriteGate {
    pub(crate) path: PathBuf,
    pub(crate) started: Notify,
    pub(crate) release: Notify,
}

#[cfg(test)]
static TEST_WRITE_GATE: OnceLock<Mutex<Option<Arc<WriteGate>>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn install_write_gate(gate: Arc<WriteGate>) {
    *TEST_WRITE_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("write gate lock") = Some(gate);
}

#[cfg(test)]
pub(crate) fn clear_write_gate() {
    if let Some(slot) = TEST_WRITE_GATE.get() {
        *slot.lock().expect("write gate lock") = None;
    }
}

#[cfg(test)]
async fn wait_for_write_gate(path: &Path) {
    let gate = TEST_WRITE_GATE
        .get()
        .and_then(|slot| slot.lock().expect("write gate lock").clone());
    let Some(gate) = gate.filter(|gate| gate.path == path) else {
        return;
    };
    gate.started.notify_waiters();
    gate.release.notified().await;
}

#[cfg(test)]
pub(crate) struct ReadGate {
    pub(crate) path: PathBuf,
    pub(crate) started: Notify,
    pub(crate) release: Notify,
}

#[cfg(test)]
static TEST_READ_GATE: OnceLock<Mutex<Option<Arc<ReadGate>>>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn install_read_gate(gate: Arc<ReadGate>) {
    *TEST_READ_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("read gate lock") = Some(gate);
}

#[cfg(test)]
pub(crate) fn clear_read_gate() {
    if let Some(slot) = TEST_READ_GATE.get() {
        *slot.lock().expect("read gate lock") = None;
    }
}

#[cfg(test)]
async fn wait_for_read_gate(path: &Path) {
    let gate = TEST_READ_GATE
        .get()
        .and_then(|slot| slot.lock().expect("read gate lock").clone());
    let Some(gate) = gate.filter(|gate| gate.path == path) else {
        return;
    };
    gate.started.notify_waiters();
    gate.release.notified().await;
}

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

    #[cfg(target_os = "linux")]
    {
        // Bind the sandbox to the directory object while it is constructed.
        // A later rename/replace of this path must not make file operations
        // reopen a different root. If the native primitive is unavailable,
        // operations fail closed instead of falling back to path-based opens.
        let _ = linux_secure_write::initialize_root(&cwd);
    }

    Ok(cwd)
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
async fn open_secure_read(cwd: &Path, path: &Path) -> Result<tokio::fs::File, BridgeError> {
    let root = linux_secure_write::root_for(cwd).map_err(secure_filesystem_error)?;
    let cwd = cwd.to_path_buf();
    let path = path.to_path_buf();
    let file =
        tokio::task::spawn_blocking(move || linux_secure_write::open_read(&root, &cwd, &path))
            .await
            .map_err(|_| filesystem_io(std::io::Error::other("filesystem operation failed")))?
            .map_err(secure_filesystem_error)?;
    Ok(tokio::fs::File::from_std(file))
}

#[cfg(not(target_os = "linux"))]
async fn open_secure_read(_cwd: &Path, _path: &Path) -> Result<tokio::fs::File, BridgeError> {
    Err(BridgeError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "secure filesystem reads are unsupported on this platform",
    )))
}

#[cfg(target_os = "linux")]
async fn write_to_secure_path(cwd: &Path, path: &Path, content: &str) -> Result<(), BridgeError> {
    let root = linux_secure_write::root_for(cwd).map_err(secure_filesystem_error)?;
    let cwd = cwd.to_path_buf();
    let path = path.to_path_buf();
    let content = content.to_owned();
    tokio::task::spawn_blocking(move || {
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
) -> Result<(), BridgeError> {
    // ponytail: fail closed outside Linux; enable other targets only with
    // their native descriptor/handle-relative traversal primitive.
    Err(BridgeError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "secure filesystem writes are unsupported on this platform",
    )))
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
    let start_line = line.unwrap_or(1);
    if start_line == 0 {
        return Err(invalid_params("ACP read line is 1-based"));
    }

    let full_path = safe_resolve_read(cwd, path)?;
    #[cfg(test)]
    wait_for_read_gate(&full_path).await;
    let file = open_secure_read(cwd, &full_path).await?;
    let mut bounded = tokio::io::AsyncReadExt::take(file, (MAX_TEXT_FILE_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut bounded, &mut bytes)
        .await
        .map_err(filesystem_io)?;
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
    if content.len() > MAX_TEXT_FILE_BYTES {
        return Err(invalid_params("text file exceeds the core size limit"));
    }

    let full_path = safe_resolve_write(cwd, path)?;
    #[cfg(test)]
    wait_for_write_gate(&full_path).await;
    write_to_secure_path(cwd, &full_path, content).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acp_cwd_strips_windows_verbatim_prefix() {
        assert_eq!(
            acp_cwd(Path::new(r"\\?\D:\work\project")),
            PathBuf::from(r"D:\work\project")
        );
    }

    #[test]
    fn acp_cwd_strips_verbatim_unc_prefix() {
        assert_eq!(
            acp_cwd(Path::new(r"\\?\UNC\server\share\dir")),
            PathBuf::from(r"\\server\share\dir")
        );
    }

    #[test]
    fn acp_cwd_leaves_plain_paths_unchanged() {
        assert_eq!(
            acp_cwd(Path::new("/home/user/x")),
            PathBuf::from("/home/user/x")
        );
        assert_eq!(acp_cwd(Path::new(r"D:\plain")), PathBuf::from(r"D:\plain"));
    }

    fn temp_cwd() -> TempDir {
        let raw = std::env::temp_dir().join(format!("agui-fileops-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&raw).unwrap();
        let canonical = canonicalize_cwd(&raw).unwrap();
        TempDir {
            path: canonical,
            raw_for_cleanup: raw,
        }
    }

    struct TempDir {
        path: PathBuf,
        raw_for_cleanup: PathBuf,
    }

    impl TempDir {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    #[cfg(target_os = "linux")]
    struct ReplacedRoot {
        path: PathBuf,
        original: PathBuf,
    }

    #[cfg(target_os = "linux")]
    impl Drop for ReplacedRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
            let _ = std::fs::rename(&self.original, &self.path);
        }
    }

    fn absolute(dir: &TempDir, relative: &str) -> String {
        dir.path().join(relative).to_string_lossy().into_owned()
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.raw_for_cleanup);
        }
    }

    #[tokio::test]
    async fn read_within_cwd_succeeds() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("foo.txt"), "hello").unwrap();
        let out = read_text_file(dir.path(), &absolute(&dir, "foo.txt"), None)
            .await
            .unwrap();
        assert_eq!(out, "hello");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn initialized_root_handle_survives_root_path_replacement() {
        if !write_text_file_supported() {
            return;
        }

        let dir = temp_cwd();
        let root = dir.path().to_path_buf();
        let target = root.join("target.txt");
        std::fs::write(&target, "original").unwrap();

        let original = root.with_file_name(format!(
            "agui-fileops-root-original-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::rename(&root, &original).unwrap();
        let _root_guard = ReplacedRoot {
            path: root.clone(),
            original: original.clone(),
        };
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("target.txt"), "replacement").unwrap();

        let target_path = target.to_string_lossy().into_owned();
        let read = read_text_file(&root, &target_path, None).await.unwrap();
        assert_eq!(read, "original");

        write_text_file(&root, &target_path, "updated")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(original.join("target.txt")).unwrap(),
            "updated"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("target.txt")).unwrap(),
            "replacement"
        );
    }

    #[tokio::test]
    async fn read_missing_file_is_resource_not_found() {
        let dir = temp_cwd();
        let result = read_text_file(dir.path(), &absolute(&dir, "missing.txt"), None).await;
        let error = result.expect_err("missing file must fail");
        assert_eq!(error_kind(&error), FileErrorKind::ResourceNotFound);
    }

    #[tokio::test]
    async fn invalid_utf8_is_an_internal_filesystem_error() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("invalid.txt"), [0xff, 0xfe]).unwrap();
        let result = read_text_file(dir.path(), &absolute(&dir, "invalid.txt"), None).await;
        let error = result.expect_err("invalid UTF-8 must fail");
        assert_eq!(error_kind(&error), FileErrorKind::Internal);
    }

    #[test]
    fn filesystem_io_preserves_source_error_fidelity() {
        use std::error::Error as _;

        // Raw os error 13 (EACCES) without pulling libc into the deps.
        let original = std::io::Error::from_raw_os_error(13);
        let wrapped = filesystem_io(original);
        let BridgeError::Io(wrapped) = &wrapped else {
            panic!("filesystem_io must return BridgeError::Io");
        };
        // Kind and taxonomy preserved: non-NotFound io errors collapse to
        // `Other` by design (see `filesystem_io`), never a specific kind.
        assert_eq!(wrapped.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            error_kind(&BridgeError::Io(clone_io(wrapped))),
            FileErrorKind::Internal
        );
        // Display carries the kind + raw os error, not a fixed opaque string.
        let text = wrapped.to_string();
        assert!(
            text.contains("os error 13"),
            "message must keep the os error, got: {text}"
        );
        // The original error stays reachable as `source`.
        let source = wrapped
            .source()
            .and_then(|s| s.downcast_ref::<std::io::Error>())
            .expect("original io error must be chained as source");
        assert_eq!(source.raw_os_error(), Some(13));
    }

    fn clone_io(error: &std::io::Error) -> std::io::Error {
        std::io::Error::new(error.kind(), error.to_string())
    }

    #[tokio::test]
    async fn oversized_text_file_is_rejected_before_line_buffering() {
        let dir = temp_cwd();
        std::fs::write(
            dir.path().join("large.txt"),
            vec![b'x'; MAX_TEXT_FILE_BYTES + 1],
        )
        .unwrap();
        let result = read_text_file(dir.path(), &absolute(&dir, "large.txt"), None).await;
        let error = result.expect_err("file-size bound must be enforced");
        assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
    }

    #[tokio::test]
    async fn relative_paths_are_rejected() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("foo.txt"), "hello").unwrap();
        let read = read_text_file(dir.path(), "foo.txt", None).await;
        let write = write_text_file(dir.path(), "new.txt", "hello").await;
        assert!(read.is_err(), "ACP read paths must be absolute");
        assert!(write.is_err(), "ACP write paths must be absolute");
    }

    #[tokio::test]
    async fn read_with_absolute_path_outside_cwd_is_rejected() {
        let dir = temp_cwd();
        let other = std::env::temp_dir().join(format!("agui-other-{}.txt", uuid::Uuid::new_v4()));
        std::fs::write(&other, "secret").unwrap();
        let result = read_text_file(dir.path(), other.to_str().unwrap(), None).await;
        let _ = std::fs::remove_file(&other);
        let err = result.expect_err("must reject absolute path outside cwd");
        assert!(
            format!("{err}").contains("escapes working directory"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn read_with_parent_traversal_is_rejected() {
        let dir = temp_cwd();
        let result = read_text_file(dir.path(), &absolute(&dir, "../escape.txt"), None).await;
        // The traversal target probably doesn't exist, but even if it does,
        // canonicalize will resolve it outside cwd. Either way: error.
        assert!(result.is_err(), "must reject parent traversal");
    }

    /// Regression for the audit's P0 "cwd='.' renders sandbox a no-op"
    /// finding. With the old `normalize_path` lexical implementation,
    /// `cwd="."` normalized to the empty path, against which `starts_with`
    /// trivially returned true for every input (including `/etc/passwd`).
    /// The fix is to canonicalize cwd before any check.
    #[tokio::test]
    async fn read_with_dot_cwd_does_not_escape_sandbox() {
        // canonicalize_cwd(".") must produce a real absolute path, against
        // which `starts_with` is meaningful.
        let canonical = canonicalize_cwd(Path::new(".")).expect("cwd canonicalize");
        assert!(
            canonical.is_absolute(),
            "canonical cwd must be absolute: {}",
            canonical.display()
        );
        // Pick a path we know is outside cwd.
        let probe = if cfg!(windows) {
            "C:/Windows/System32/drivers/etc/hosts"
        } else {
            "/etc/hostname"
        };
        let result = read_text_file(&canonical, probe, None).await;
        // We can't assume the probe exists in CI, but it MUST NOT be Ok
        // for any reason that lets the agent read it. Either NotFound
        // (canonicalize errored before sandbox check — still safe) or
        // PermissionDenied (sandbox blocked it). Both are acceptable.
        assert!(result.is_err(), "must not return file content from {probe}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn write_within_cwd_creates_file_and_parents() {
        if !write_text_file_supported() {
            return;
        }
        let dir = temp_cwd();
        write_text_file(dir.path(), &absolute(&dir, "nested/deep/foo.txt"), "ok")
            .await
            .unwrap();
        let actual = std::fs::read_to_string(dir.path().join("nested/deep/foo.txt")).unwrap();
        assert_eq!(actual, "ok");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn write_overwrites_existing_file() {
        if !write_text_file_supported() {
            return;
        }
        let dir = temp_cwd();
        let path = absolute(&dir, "foo.txt");
        write_text_file(dir.path(), &path, "first").await.unwrap();
        write_text_file(dir.path(), &path, "second").await.unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("foo.txt")).unwrap(),
            "second"
        );
    }

    #[tokio::test]
    async fn oversized_write_is_rejected_before_disk_access() {
        let dir = temp_cwd();
        let path = dir.path().join("oversized.txt");
        let result = write_text_file(
            dir.path(),
            &path.to_string_lossy(),
            &"x".repeat(MAX_TEXT_FILE_BYTES + 1),
        )
        .await;
        let error = result.expect_err("oversized write must fail");
        assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
        assert!(!path.exists());
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn write_fails_closed_when_secure_primitive_is_unavailable() {
        assert!(!write_text_file_supported());
        let dir = temp_cwd();
        let path = dir.path().join("unsupported.txt");
        let result = write_text_file(dir.path(), &path.to_string_lossy(), "nope").await;
        let error = result.expect_err("writes must fail closed on unsupported targets");
        assert_eq!(error_kind(&error), FileErrorKind::Internal);
        assert!(
            matches!(error, BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported)
        );
        assert!(!path.exists());
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn read_fails_closed_when_secure_primitive_is_unavailable() {
        let dir = temp_cwd();
        let path = dir.path().join("unsupported.txt");
        std::fs::write(&path, "nope").unwrap();
        let result = read_text_file(dir.path(), &path.to_string_lossy(), None).await;
        let error = result.expect_err("reads must fail closed on unsupported targets");
        assert!(matches!(
            error,
            BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
        ));
    }

    #[tokio::test]
    async fn write_with_parent_traversal_is_rejected() {
        let dir = temp_cwd();
        let result = write_text_file(dir.path(), &absolute(&dir, "../escape.txt"), "boom").await;
        assert!(result.is_err(), "must reject parent traversal write");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_through_symlink_pointing_outside_is_rejected() {
        use std::os::unix::fs::symlink;
        let dir = temp_cwd();
        let target = std::env::temp_dir().join(format!("agui-target-{}.txt", uuid::Uuid::new_v4()));
        std::fs::write(&target, "secret").unwrap();
        symlink(&target, dir.path().join("link.txt")).unwrap();

        let result = read_text_file(dir.path(), &absolute(&dir, "link.txt"), None).await;
        let _ = std::fs::remove_file(&target);
        let err = result.expect_err("symlink must not bypass sandbox");
        assert!(
            format!("{err}").contains("escapes working directory"),
            "unexpected error: {err}"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn read_rejects_symlink_swapped_after_validation() {
        use std::os::unix::fs::symlink;

        let secure_reads_supported = read_text_file_supported();
        let dir = temp_cwd();
        let target = dir.path().join("read.txt");
        std::fs::write(&target, "inside").unwrap();
        let outside = std::env::temp_dir().join(format!("agui-read-swap-{}", uuid::Uuid::new_v4()));
        std::fs::write(&outside, "outside").unwrap();
        let path = target.to_string_lossy().into_owned();
        let gate = Arc::new(ReadGate {
            path: target.clone(),
            started: Notify::new(),
            release: Notify::new(),
        });
        install_read_gate(gate.clone());

        let started = gate.started.notified();
        let cwd = dir.path().to_path_buf();
        let read = tokio::spawn(async move { read_text_file(&cwd, &path, None).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .expect("read must reach the post-validation gate");

        let preserved = dir.path().join("read-original.txt");
        std::fs::rename(&target, &preserved).unwrap();
        symlink(&outside, &target).unwrap();
        gate.release.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), read)
            .await
            .expect("swapped read must finish")
            .expect("swapped read task must not panic");
        clear_read_gate();

        let error = result.expect_err("a swapped read must not return outside content");
        if secure_reads_supported {
            assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
        } else {
            assert!(matches!(
                error,
                BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
            ));
        }
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "outside");

        std::fs::remove_file(&target).unwrap();
        std::fs::rename(preserved, target).unwrap();
        std::fs::remove_file(outside).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_through_symlinked_parent_pointing_outside_is_rejected() {
        use std::os::unix::fs::symlink;
        let dir = temp_cwd();
        let outside = std::env::temp_dir().join(format!("agui-outside-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, dir.path().join("escape_dir")).unwrap();

        let result =
            write_text_file(dir.path(), &absolute(&dir, "escape_dir/foo.txt"), "boom").await;
        let _ = std::fs::remove_dir_all(&outside);
        assert!(
            result.is_err(),
            "write through symlinked parent must be rejected"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn write_rejects_parent_symlink_swapped_after_validation() {
        use std::os::unix::fs::symlink;

        let secure_writes_supported = write_text_file_supported();
        let dir = temp_cwd();
        let parent = dir.path().join("swappable");
        std::fs::create_dir(&parent).unwrap();
        let outside =
            std::env::temp_dir().join(format!("agui-swap-outside-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&outside).unwrap();
        let target = parent.join("foo.txt");
        let target_path = target.to_string_lossy().into_owned();
        let gate = Arc::new(WriteGate {
            path: target.clone(),
            started: Notify::new(),
            release: Notify::new(),
        });
        install_write_gate(gate.clone());

        let started = gate.started.notified();
        let cwd = dir.path().to_path_buf();
        let write = tokio::spawn(async move { write_text_file(&cwd, &target_path, "boom").await });
        tokio::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .expect("write must reach the post-validation gate");

        let preserved = dir.path().join("swappable-original");
        std::fs::rename(&parent, &preserved).unwrap();
        symlink(&outside, &parent).unwrap();
        gate.release.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), write)
            .await
            .expect("swapped write must finish")
            .expect("swapped write task must not panic");
        clear_write_gate();

        let error = result.expect_err("a swapped parent must not be written");
        if secure_writes_supported {
            assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
        } else {
            assert!(matches!(
                error,
                BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
            ));
        }
        assert!(!outside.join("foo.txt").exists());
        assert!(!preserved.join("foo.txt").exists());

        std::fs::remove_file(&parent).unwrap();
        std::fs::rename(preserved, parent).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }

    #[tokio::test]
    async fn read_limit_counts_lines_and_preserves_multibyte_text() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("lines.txt"), "one\n世界\nthree\n").unwrap();
        let out = read_text_file(dir.path(), &absolute(&dir, "lines.txt"), Some(2))
            .await
            .unwrap();
        assert_eq!(out, "one\n世界\n");
    }

    #[tokio::test]
    async fn read_line_and_limit_use_one_based_line_ranges() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("lines.txt"), "one\ntwo\nthree\nfour\n").unwrap();
        let out = read_text_file_range(dir.path(), &absolute(&dir, "lines.txt"), Some(2), Some(2))
            .await
            .unwrap();
        assert_eq!(out, "two\nthree\n");
    }

    #[tokio::test]
    async fn read_line_zero_is_rejected() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("lines.txt"), "one\n").unwrap();
        let result =
            read_text_file_range(dir.path(), &absolute(&dir, "lines.txt"), Some(0), Some(1)).await;
        assert!(result.is_err(), "ACP line numbers are 1-based");
    }

    #[tokio::test]
    async fn read_limit_zero_returns_no_lines() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("lines.txt"), "one\ntwo\n").unwrap();
        let out = read_text_file(dir.path(), &absolute(&dir, "lines.txt"), Some(0))
            .await
            .unwrap();
        assert!(out.is_empty());
    }
}
