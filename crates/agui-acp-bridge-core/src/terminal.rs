//! Per-session ACP terminal supervision.
//!
//! Terminal identifiers and process state live below one `TerminalRegistry`,
//! which is created for each live ACP session.  The registry deliberately has
//! no process-global state: a terminal from one session cannot be looked up by
//! another session.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};

use agent_client_protocol::RequestCancellation;
use agent_client_protocol::schema::v1::{
    CreateTerminalRequest, CreateTerminalResponse, KillTerminalRequest, KillTerminalResponse,
    ReleaseTerminalRequest, ReleaseTerminalResponse, TerminalExitStatus, TerminalId,
    TerminalOutputRequest, TerminalOutputResponse, WaitForTerminalExitRequest,
    WaitForTerminalExitResponse,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::{Notify, mpsc, oneshot};

const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const READ_CHUNK_BYTES: usize = 8192;
const CONTROL_BUFFER: usize = 8;
const POST_EXIT_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
// ponytail: keep this local until terminal limits are exposed through the
// existing BridgeConfig policy surface.
const MAX_TERMINALS_PER_SESSION: usize = 32;
const PROCESS_CLEANUP_DEADLINE: Duration = Duration::from_secs(1);
const PROCESS_CLEANUP_ATTEMPTS: usize = 8;
const PROCESS_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(unix)]
const CHILD_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalError {
    InvalidParams,
    Capacity,
    ResourceNotFound,
    Internal,
}

#[derive(Debug)]
struct RegistryInner {
    cwd: PathBuf,
    root: ApprovedCwd,
    terminals: Mutex<HashMap<TerminalId, Arc<Terminal>>>,
}

#[derive(Debug)]
struct TerminalResource {
    registry: Weak<RegistryInner>,
    id: TerminalId,
}

impl TerminalResource {
    fn release(&self) {
        if let Some(registry) = self.registry.upgrade()
            && let Ok(mut terminals) = registry.terminals.lock()
        {
            terminals.remove(&self.id);
        }
    }
}

#[derive(Debug)]
struct TerminalLifecycle {
    resource: Arc<TerminalResource>,
    abort_signal: Arc<Notify>,
}

#[derive(Debug)]
struct ApprovedCwd {
    #[cfg(unix)]
    directory: std::fs::File,
    #[cfg(windows)]
    directory: WindowsDirectoryLock,
    #[cfg(windows)]
    path: PathBuf,
}

impl ApprovedCwd {
    fn open(root: &Path, canonical: &Path) -> io::Result<Self> {
        if !canonical.is_absolute() || !canonical.starts_with(root) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "working directory escapes the sandbox",
            ));
        }

        #[cfg(unix)]
        {
            let directory = open_approved_unix(root, canonical)?;
            let metadata = directory.metadata()?;
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "working directory is not a directory",
                ));
            }

            // The path was checked before this open. Check the opened object
            // against the path again so a replacement between those two
            // operations is rejected; after this point fchdir uses the fixed
            // directory object rather than resolving a path again.
            let resolved = std::fs::canonicalize(canonical)?;
            let path_metadata = std::fs::metadata(&resolved)?;
            if resolved != canonical
                || !resolved.starts_with(root)
                || metadata.dev() != path_metadata.dev()
                || metadata.ino() != path_metadata.ino()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "working directory changed during validation",
                ));
            }
            Ok(Self { directory })
        }

        #[cfg(windows)]
        {
            let directory = WindowsDirectoryLock::open(root, canonical)?;
            Ok(Self {
                directory,
                path: canonical.to_path_buf(),
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (root, canonical);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "secure terminal working directories are unsupported on this platform",
            ))
        }
    }

    fn open_beneath(root: &Self, root_path: &Path, canonical: &Path) -> io::Result<Self> {
        if !canonical.is_absolute() || !canonical.starts_with(root_path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "working directory escapes the sandbox",
            ));
        }

        #[cfg(target_os = "linux")]
        {
            let relative = canonical.strip_prefix(root_path).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "working directory is outside root",
                )
            })?;
            let directory = open_linux_dir_beneath(root.directory.as_raw_fd(), relative)?;
            let metadata = directory.metadata()?;
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "working directory is not a directory",
                ));
            }

            // The descriptor-relative open is the security boundary. This
            // second check preserves the existing canonical-path behavior and
            // rejects a path that no longer names the opened object.
            let resolved = std::fs::canonicalize(canonical)?;
            let path_metadata = std::fs::metadata(&resolved)?;
            if resolved != canonical
                || !resolved.starts_with(root_path)
                || metadata.dev() != path_metadata.dev()
                || metadata.ino() != path_metadata.ino()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "working directory changed during validation",
                ));
            }
            Ok(Self { directory })
        }

        #[cfg(windows)]
        {
            let _ = root;
            let directory = WindowsDirectoryLock::open(root_path, canonical)?;
            Ok(Self {
                directory,
                path: canonical.to_path_buf(),
            })
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            let _ = root;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "descriptor-relative terminal working directories are unsupported on this Unix platform",
            ))
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = root;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "secure terminal working directories are unsupported on this platform",
            ))
        }
    }

    fn configure(&self, command: &mut Command) {
        #[cfg(unix)]
        {
            let fd = self.directory.as_raw_fd();
            // SAFETY: the closure only calls async-signal-safe fchdir after
            // fork. The file remains open through spawn, and CLOEXEC closes
            // the inherited descriptor immediately after this succeeds.
            unsafe {
                command.pre_exec(move || {
                    if fchdir(fd) == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::last_os_error())
                    }
                });
            }
        }

        #[cfg(windows)]
        {
            let _ = &self.directory;
            // CreateProcess accepts only a path for lpCurrentDirectory, not a
            // directory handle. Every ancestor is held without
            // FILE_SHARE_DELETE, so this path cannot be replaced between the
            // reparse-point check and CreateProcess under normal Windows
            // filesystem semantics.
            command.current_dir(&self.path);
        }
    }
}

#[cfg(unix)]
unsafe extern "C" {
    fn fchdir(fd: RawFd) -> i32;
}

#[cfg(target_os = "linux")]
fn open_approved_unix(root: &Path, canonical: &Path) -> io::Result<std::fs::File> {
    let _ = root;
    let fd = open_linux_dir_absolute(canonical)?;
    Ok(std::fs::File::from(fd))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn open_approved_unix(_root: &Path, _canonical: &Path) -> io::Result<std::fs::File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative terminal working directories are unsupported on this Unix platform",
    ))
}

#[cfg(target_os = "linux")]
fn open_linux_dir_absolute(path: &Path) -> io::Result<OwnedFd> {
    open_linux_dir(AT_FDCWD, path, RESOLVE_NO_SYMLINKS)
}

#[cfg(target_os = "linux")]
fn open_linux_dir_beneath(root_fd: RawFd, path: &Path) -> io::Result<std::fs::File> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let fd = open_linux_dir(root_fd, path, RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)?;
    Ok(std::fs::File::from(fd))
}

#[cfg(target_os = "linux")]
fn open_linux_dir(dirfd: RawFd, path: &Path, resolve: u64) -> io::Result<OwnedFd> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "working directory contains NUL",
        )
    })?;
    let how = OpenHow {
        flags: (O_PATH | O_DIRECTORY | O_CLOEXEC) as u64,
        mode: 0,
        resolve,
    };
    let fd = unsafe {
        syscall(
            SYS_OPENAT2,
            dirfd,
            path.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a non-negative openat2 result is an owned directory fd.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    }
}

#[cfg(target_os = "linux")]
const AT_FDCWD: RawFd = -100;
#[cfg(target_os = "linux")]
const O_DIRECTORY: i32 = 0o200000;
#[cfg(target_os = "linux")]
const O_CLOEXEC: i32 = 0o2000000;
#[cfg(target_os = "linux")]
const O_PATH: i32 = 0o10000000;
#[cfg(target_os = "linux")]
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
#[cfg(target_os = "linux")]
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
#[cfg(target_os = "linux")]
const RESOLVE_BENEATH: u64 = 0x08;
#[cfg(target_os = "linux")]
const SYS_OPENAT2: LibcLong = 437;
#[cfg(target_os = "linux")]
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

/// The cloneable view handed to request handlers.
#[derive(Clone, Debug)]
pub(crate) struct TerminalRegistry {
    inner: Arc<RegistryInner>,
}

/// Non-cloneable owner used by the session actor to clean up on every exit
/// path, including task aborts while request handlers still hold a registry
/// clone.
#[derive(Debug)]
pub(crate) struct TerminalRegistryGuard {
    inner: Arc<RegistryInner>,
}

impl TerminalRegistry {
    pub(crate) fn new(cwd: PathBuf) -> Result<(Self, TerminalRegistryGuard), ()> {
        let cwd = crate::file_ops::canonicalize_cwd(&cwd).map_err(|_| ())?;
        let root = ApprovedCwd::open(&cwd, &cwd).map_err(|_| ())?;
        if !ProcessTree::supported() {
            return Err(());
        }
        let inner = Arc::new(RegistryInner {
            cwd,
            root,
            terminals: Mutex::new(HashMap::new()),
        });
        Ok((
            Self {
                inner: inner.clone(),
            },
            TerminalRegistryGuard { inner },
        ))
    }

    fn create(
        &self,
        request: &CreateTerminalRequest,
    ) -> Result<(TerminalId, CreatedTerminalGuard), TerminalError> {
        validate_command(request)?;
        let cwd = self.resolve_cwd(request.cwd.as_deref())?;
        let approved_cwd = ApprovedCwd::open_beneath(&self.inner.root, &self.inner.cwd, &cwd)
            .map_err(|_| TerminalError::InvalidParams)?;
        validate_environment(request)?;
        let output_limit = output_limit(request);

        let mut command = Command::new(&request.command);
        command
            .args(&request.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        // Keep the process suspended until Terminal::new assigns its job.
        command.creation_flags(CREATE_SUSPENDED);
        approved_cwd.configure(&mut command);
        #[cfg(unix)]
        command.process_group(0);
        for variable in &request.env {
            command.env(&variable.name, &variable.value);
        }

        // Hold the registry lock through spawn and insertion so concurrent
        // create requests cannot race the per-session cap.
        let mut terminals = self
            .inner
            .terminals
            .lock()
            .map_err(|_| TerminalError::Internal)?;
        if terminals.len() >= MAX_TERMINALS_PER_SESSION {
            return Err(TerminalError::Capacity);
        }
        let id = loop {
            let candidate = TerminalId::new(uuid::Uuid::new_v4().to_string());
            if !terminals.contains_key(&candidate) {
                break candidate;
            }
        };
        let resource = Arc::new(TerminalResource {
            registry: Arc::downgrade(&self.inner),
            id: id.clone(),
        });
        let mut child = command.spawn().map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::PermissionDenied => TerminalError::InvalidParams,
            _ => TerminalError::Internal,
        })?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let terminal = Arc::new(
            Terminal::new(child, stdout, stderr, output_limit, resource)
                .map_err(|_| TerminalError::Internal)?,
        );

        terminals.insert(id.clone(), terminal);
        drop(terminals);

        Ok((
            id.clone(),
            CreatedTerminalGuard {
                registry: self.clone(),
                id,
                active: true,
            },
        ))
    }

    fn resolve_cwd(&self, requested: Option<&Path>) -> Result<PathBuf, TerminalError> {
        let cwd = requested.unwrap_or(&self.inner.cwd);
        if !cwd.is_absolute() {
            return Err(TerminalError::InvalidParams);
        }

        let canonical = std::fs::canonicalize(cwd).map_err(|_| TerminalError::InvalidParams)?;
        if !canonical.starts_with(&self.inner.cwd)
            || !std::fs::metadata(&canonical)
                .map(|metadata| metadata.is_dir())
                .unwrap_or(false)
        {
            return Err(TerminalError::InvalidParams);
        }
        Ok(canonical)
    }

    fn get(&self, id: &TerminalId) -> Result<Arc<Terminal>, TerminalError> {
        self.inner
            .terminals
            .lock()
            .map_err(|_| TerminalError::Internal)?
            .get(id)
            .cloned()
            .ok_or(TerminalError::ResourceNotFound)
    }

    fn take(&self, id: &TerminalId) -> Result<Arc<Terminal>, TerminalError> {
        self.inner
            .terminals
            .lock()
            .map_err(|_| TerminalError::Internal)?
            .remove(id)
            .ok_or(TerminalError::ResourceNotFound)
    }

    fn abort(&self, id: &TerminalId) {
        let terminal = self
            .inner
            .terminals
            .lock()
            .ok()
            .and_then(|mut terminals| terminals.remove(id));
        if let Some(terminal) = terminal {
            terminal.abort();
        }
    }

    pub(crate) fn shutdown(&self) {
        let terminals = self
            .inner
            .terminals
            .lock()
            .map(|mut terminals| terminals.drain().map(|(_, terminal)| terminal).collect())
            .unwrap_or_else(|_| Vec::new());
        for terminal in terminals {
            terminal.abort();
        }
    }
}

impl Drop for TerminalRegistryGuard {
    fn drop(&mut self) {
        let registry = TerminalRegistry {
            inner: self.inner.clone(),
        };
        registry.shutdown();
    }
}

struct CreatedTerminalGuard {
    registry: TerminalRegistry,
    id: TerminalId,
    active: bool,
}

impl CreatedTerminalGuard {
    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for CreatedTerminalGuard {
    fn drop(&mut self) {
        if self.active {
            self.registry.abort(&self.id);
        }
    }
}

fn validate_command(request: &CreateTerminalRequest) -> Result<(), TerminalError> {
    if request.command.is_empty()
        || request.command.contains('\0')
        || request.args.iter().any(|arg| arg.contains('\0'))
    {
        return Err(TerminalError::InvalidParams);
    }
    Ok(())
}

fn validate_environment(request: &CreateTerminalRequest) -> Result<(), TerminalError> {
    if request.env.iter().any(|variable| {
        variable.name.is_empty()
            || variable.name.contains(['=', '\0'])
            || variable.value.contains('\0')
    }) {
        return Err(TerminalError::InvalidParams);
    }
    Ok(())
}

fn output_limit(request: &CreateTerminalRequest) -> usize {
    // ACP's u64 limit permits zero: retain no bytes and mark any output as
    // truncated. An absent limit keeps the bridge default.
    request.output_byte_limit.map_or(MAX_OUTPUT_BYTES, |limit| {
        usize::try_from(limit.min(MAX_OUTPUT_BYTES as u64)).unwrap_or(MAX_OUTPUT_BYTES)
    })
}

fn wire_error(error: TerminalError) -> agent_client_protocol::Error {
    match error {
        TerminalError::InvalidParams => agent_client_protocol::Error::invalid_params(),
        TerminalError::Capacity => agent_client_protocol::Error::request_cancelled().data(format!(
            "terminal capacity reached (limit {MAX_TERMINALS_PER_SESSION})"
        )),
        TerminalError::ResourceNotFound => agent_client_protocol::Error::resource_not_found(None),
        TerminalError::Internal => agent_client_protocol::Error::internal_error(),
    }
}

pub(crate) async fn create_request(
    request: CreateTerminalRequest,
    registry: TerminalRegistry,
    cancellation: RequestCancellation,
    responder: agent_client_protocol::Responder<CreateTerminalResponse>,
) -> Result<(), agent_client_protocol::Error> {
    let created = cancellation
        .run_until_cancelled(async move { registry.create(&request).map_err(wire_error) })
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

#[derive(Debug)]
struct Terminal {
    state: Arc<TerminalState>,
    control: mpsc::Sender<Control>,
    process_tree: Arc<ProcessTree>,
    lifecycle: Arc<TerminalLifecycle>,
}

impl Terminal {
    fn new(
        child: Child,
        stdout: Option<tokio::process::ChildStdout>,
        stderr: Option<tokio::process::ChildStderr>,
        output_limit: usize,
        resource: Arc<TerminalResource>,
    ) -> Result<Self, ()> {
        let process_tree = match ProcessTree::new(&child) {
            Ok(process_tree) => Arc::new(process_tree),
            Err(()) => {
                spawn_child_reaper(child, stdout, stderr);
                return Err(());
            }
        };
        #[cfg(windows)]
        if process_tree.resume(&child).is_err() {
            spawn_child_reaper(child, stdout, stderr);
            return Err(());
        }
        let state = Arc::new(TerminalState::new(output_limit));
        let (control, control_rx) = mpsc::channel(CONTROL_BUFFER);
        let lifecycle = Arc::new(TerminalLifecycle {
            resource,
            abort_signal: Arc::new(Notify::new()),
        });
        tokio::spawn(supervise(
            child,
            stdout,
            stderr,
            state.clone(),
            process_tree.clone(),
            lifecycle.clone(),
            control_rx,
        ));
        Ok(Self {
            state,
            control,
            process_tree,
            lifecycle,
        })
    }

    fn abort(&self) {
        let _ = self.process_tree.terminate();
        // Keep the supervisor alive to own the final Child::wait(). The
        // notification is retained even if the task is between select arms.
        self.lifecycle.abort_signal.notify_one();
    }

    async fn output(&self) -> Result<TerminalOutputResponse, TerminalError> {
        self.state.wait_for_output_completion().await?;
        self.state
            .snapshot()
            .map(|(output, truncated, exit_status)| {
                TerminalOutputResponse::new(String::from_utf8_lossy(&output), truncated)
                    .exit_status(exit_status)
            })
    }

    async fn wait(&self) -> Result<TerminalExitStatus, TerminalError> {
        self.state.wait().await
    }

    async fn kill(&self) -> Result<(), TerminalError> {
        self.send_control(Control::Kill).await
    }

    async fn release(&self) -> Result<(), TerminalError> {
        self.send_control(Control::Release).await
    }

    async fn send_control(
        &self,
        make_control: fn(oneshot::Sender<Result<(), ()>>) -> Control,
    ) -> Result<(), TerminalError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(make_control(reply_tx))
            .await
            .map_err(|_| TerminalError::Internal)?;
        reply_rx
            .await
            .map_err(|_| TerminalError::Internal)?
            .map_err(|_| TerminalError::Internal)
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Debug)]
struct ProcessTree {
    #[cfg(unix)]
    process_group: i32,
    #[cfg(unix)]
    cleanup: Mutex<CleanupState>,
    #[cfg(target_os = "linux")]
    leader: LinuxLeader,
    #[cfg(windows)]
    job: WindowsJob,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupState {
    Pending,
    Complete,
}

impl ProcessTree {
    fn new(child: &Child) -> Result<Self, ()> {
        #[cfg(unix)]
        {
            let process_group = child.id().and_then(|id| i32::try_from(id).ok()).ok_or(())?;
            #[cfg(target_os = "linux")]
            let leader = match LinuxLeader::open(process_group) {
                Ok(leader) => leader,
                Err(_) => {
                    // The child has not been waited on yet, so its PID/PGID
                    // cannot have been recycled. Clean up this failed
                    // creation while that identity is still anchored.
                    let _ = unsafe { kill_process_group(-process_group, SIGKILL) };
                    return Err(());
                }
            };
            Ok(Self {
                process_group,
                cleanup: Mutex::new(CleanupState::Pending),
                #[cfg(target_os = "linux")]
                leader,
            })
        }

        #[cfg(windows)]
        {
            Ok(Self {
                job: WindowsJob::for_child(child).map_err(|_| ())?,
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = child;
            Ok(Self {})
        }
    }

    fn supported() -> bool {
        #[cfg(target_os = "linux")]
        {
            pidfd_open(std::process::id() as i32).is_ok()
        }

        #[cfg(windows)]
        {
            WindowsJob::supported()
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            false
        }

        #[cfg(not(any(unix, windows)))]
        {
            false
        }
    }

    #[cfg(windows)]
    fn resume(&self, child: &Child) -> io::Result<()> {
        self.job.resume(child)
    }

    fn terminate(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let mut cleanup = self
                .cleanup
                .lock()
                .map_err(|_| io::Error::other("process cleanup state poisoned"))?;
            if *cleanup == CleanupState::Complete {
                return Ok(());
            }
            #[cfg(target_os = "linux")]
            let leader_result = ignore_esrch(self.leader.kill());
            #[cfg(not(target_os = "linux"))]
            let leader_result = Ok(());
            // A negative PID targets the complete process group, including
            // descendants that inherited the command's group. On Unix this
            // call is made before the child is reaped, so the live child or
            // zombie still anchors the PGID and it cannot target a recycled
            // group.
            let result = unsafe { kill_process_group(-self.process_group, SIGKILL) };
            let group_result = if result == 0 {
                Ok(())
            } else {
                ignore_esrch(Err(std::io::Error::last_os_error()))
            };
            let result = leader_result.and(group_result);
            if result.is_ok() {
                *cleanup = CleanupState::Complete;
            }
            result
        }

        #[cfg(windows)]
        {
            self.job.terminate()
        }

        #[cfg(not(any(unix, windows)))]
        {
            Ok(())
        }
    }

    #[cfg(unix)]
    fn terminate_after_leader_exit(&self) -> std::io::Result<()> {
        let mut cleanup = self
            .cleanup
            .lock()
            .map_err(|_| io::Error::other("process cleanup state poisoned"))?;
        if *cleanup == CleanupState::Complete {
            return Ok(());
        }

        // The exit observer uses WNOWAIT, so the direct child is a zombie and
        // its PID/PGID remains reserved until the group has been signalled.
        // That is the lifecycle identity binding for Unix process groups.
        let result = unsafe { kill_process_group(-self.process_group, SIGKILL) };
        let result = if result == 0 {
            Ok(())
        } else {
            ignore_esrch(Err(std::io::Error::last_os_error()))
        };
        if result.is_ok() {
            *cleanup = CleanupState::Complete;
        }
        result
    }

    #[cfg(unix)]
    fn leader_pid(&self) -> i32 {
        #[cfg(target_os = "linux")]
        {
            self.leader.pid
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.process_group
        }
    }
}

#[cfg(unix)]
fn ignore_esrch(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(error) if error.raw_os_error() == Some(ESRCH) => Ok(()),
        result => result,
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LinuxLeader {
    pid: i32,
    pidfd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl LinuxLeader {
    fn open(pid: i32) -> io::Result<Self> {
        let pidfd = pidfd_open(pid)?;
        Ok(Self { pid, pidfd })
    }

    fn kill(&self) -> io::Result<()> {
        let result = unsafe {
            syscall(
                SYS_PIDFD_SEND_SIGNAL,
                self.pidfd.as_raw_fd(),
                SIGKILL,
                std::ptr::null::<LinuxSigInfo>(),
                0_u32,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "linux")]
const SYS_PIDFD_OPEN: LibcLong = 434;

#[cfg(target_os = "linux")]
const SYS_PIDFD_SEND_SIGNAL: LibcLong = 424;

#[cfg(target_os = "linux")]
type LibcLong = std::os::raw::c_long;

#[cfg(target_os = "linux")]
#[repr(C)]
struct LinuxSigInfo {
    _unused: [u8; 128],
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn syscall(number: LibcLong, ...) -> LibcLong;
}

#[cfg(target_os = "linux")]
fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
    let fd = unsafe { syscall(SYS_PIDFD_OPEN, pid, 0_u32) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a non-negative pidfd_open result is an owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    }
}

#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(unix)]
const ESRCH: i32 = 3;

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn kill_process_group(process_group: i32, signal: i32) -> i32;
}

#[cfg(windows)]
#[derive(Debug)]
struct WindowsDirectoryLock {
    handles: Vec<usize>,
}

#[cfg(windows)]
impl WindowsDirectoryLock {
    fn open(_root: &Path, canonical: &Path) -> io::Result<Self> {
        let mut ancestors: Vec<PathBuf> = canonical
            .ancestors()
            .filter(|path| !path.as_os_str().is_empty())
            .map(PathBuf::from)
            .collect();
        ancestors.reverse();
        ancestors.dedup();

        let mut lock = Self {
            handles: Vec::with_capacity(ancestors.len()),
        };
        for ancestor in ancestors {
            let handle = open_windows_directory(&ancestor)?;
            let actual = match windows_final_path(handle) {
                Ok(actual) => actual,
                Err(error) => {
                    unsafe {
                        let _ = CloseHandle(handle);
                    }
                    return Err(error);
                }
            };
            if !windows_paths_equal(&actual, &ancestor) {
                unsafe {
                    let _ = CloseHandle(handle);
                }
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "working directory reparse point changed during validation",
                ));
            }
            lock.handles.push(handle as usize);
        }
        Ok(lock)
    }
}

#[cfg(windows)]
impl Drop for WindowsDirectoryLock {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            unsafe {
                let _ = CloseHandle(handle as WindowsHandle);
            }
        }
    }
}

#[cfg(windows)]
fn windows_wide(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;

    if path.as_os_str().encode_wide().any(|unit| unit == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "working directory contains NUL",
        ));
    }
    Ok(path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect())
}

#[cfg(windows)]
fn open_windows_directory(path: &Path) -> io::Result<WindowsHandle> {
    let path = windows_wide(path)?;
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES,
            // Do not share delete/rename. Holding every ancestor handle makes
            // the CreateProcess current-directory string stable until spawn.
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        Err(io::Error::last_os_error())
    } else {
        Ok(handle)
    }
}

#[cfg(windows)]
fn windows_final_path(handle: WindowsHandle) -> io::Result<PathBuf> {
    let mut buffer = vec![0_u16; 256];
    loop {
        let length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, 0)
        };
        if length == 0 {
            return Err(io::Error::last_os_error());
        }
        if length < buffer.len() as u32 {
            return Ok(PathBuf::from(String::from_utf16_lossy(
                &buffer[..length as usize],
            )));
        }
        buffer.resize(length as usize + 1, 0);
    }
}

#[cfg(windows)]
fn windows_paths_equal(left: &Path, right: &Path) -> bool {
    let normalize = |path: &Path| {
        let mut path = path
            .to_string_lossy()
            .replace('/', "\\")
            .to_ascii_lowercase();
        if let Some(unc) = path.strip_prefix(r"\\?\unc\") {
            path = format!(r"\\{unc}");
        } else if let Some(plain) = path.strip_prefix(r"\\?\") {
            path = plain.to_owned();
        }
        path.trim_end_matches('\\').to_owned()
    };
    normalize(left) == normalize(right)
}

#[cfg(windows)]
const FILE_READ_ATTRIBUTES: u32 = 0x80;
#[cfg(windows)]
const FILE_SHARE_READ: u32 = 0x1;
#[cfg(windows)]
const FILE_SHARE_WRITE: u32 = 0x2;
#[cfg(windows)]
const OPEN_EXISTING: u32 = 3;
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
#[cfg(windows)]
const INVALID_HANDLE_VALUE: WindowsHandle = -1_isize as WindowsHandle;
#[cfg(windows)]
const CREATE_SUSPENDED: u32 = 0x0000_0004;
#[cfg(windows)]
const THREAD_SUSPEND_RESUME_FAILED: u32 = u32::MAX;
#[cfg(windows)]
const TH32CS_SNAPTHREAD: u32 = 0x0000_0004;
#[cfg(windows)]
const THREAD_SUSPEND_RESUME: u32 = 0x0002;

#[cfg(windows)]
#[repr(C)]
struct ThreadEntry32 {
    size: u32,
    usage: u32,
    thread_id: u32,
    owner_process_id: u32,
    base_priority: i32,
    delta_priority: i32,
    flags: u32,
}

#[cfg(windows)]
unsafe extern "system" {
    fn CreateFileW(
        path: *const u16,
        desired_access: u32,
        share_mode: u32,
        security_attributes: *mut std::ffi::c_void,
        creation_disposition: u32,
        flags_and_attributes: u32,
        template_file: WindowsHandle,
    ) -> WindowsHandle;
    fn GetFinalPathNameByHandleW(
        file: WindowsHandle,
        path: *mut u16,
        path_length: u32,
        flags: u32,
    ) -> u32;
    fn CreateToolhelp32Snapshot(flags: u32, process_id: u32) -> WindowsHandle;
    fn Thread32First(snapshot: WindowsHandle, entry: *mut ThreadEntry32) -> i32;
    fn Thread32Next(snapshot: WindowsHandle, entry: *mut ThreadEntry32) -> i32;
    fn OpenThread(desired_access: u32, inherit_handle: i32, thread_id: u32) -> WindowsHandle;
}

#[cfg(windows)]
#[derive(Debug)]
struct WindowsJob {
    handle: usize,
}

#[cfg(windows)]
impl WindowsJob {
    fn supported() -> bool {
        let handle = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if handle.is_null() {
            return false;
        }
        let supported = Self::configure(handle).is_ok();
        unsafe {
            let _ = CloseHandle(handle);
        }
        supported
    }

    fn configure(handle: WindowsHandle) -> io::Result<()> {
        let mut limits = JobObjectBasicLimitInformation {
            per_process_user_time_limit: 0,
            per_job_user_time_limit: 0,
            limit_flags: JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            minimum_working_set_size: 0,
            maximum_working_set_size: 0,
            active_process_limit: 0,
            affinity: 0,
            priority_class: 0,
            scheduling_class: 0,
        };
        if unsafe {
            SetInformationJobObject(
                handle,
                JOB_OBJECT_BASIC_LIMIT_INFORMATION_CLASS,
                (&mut limits as *mut JobObjectBasicLimitInformation).cast(),
                std::mem::size_of::<JobObjectBasicLimitInformation>() as u32,
            )
        } == 0
        {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn for_child(child: &Child) -> std::io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let job = Self {
            handle: handle as usize,
        };
        Self::configure(job.raw_handle())?;
        let process = child
            .raw_handle()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "child exited"))?;
        if unsafe { AssignProcessToJobObject(job.raw_handle(), process) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(job)
    }

    fn raw_handle(&self) -> WindowsHandle {
        self.handle as WindowsHandle
    }

    fn terminate(&self) -> std::io::Result<()> {
        if unsafe { TerminateJobObject(self.raw_handle(), 1) } != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn resume(&self, child: &Child) -> std::io::Result<()> {
        let pid = child
            .id()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "child exited"))?;
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        let mut entry = ThreadEntry32 {
            size: std::mem::size_of::<ThreadEntry32>() as u32,
            usage: 0,
            thread_id: 0,
            owner_process_id: 0,
            base_priority: 0,
            delta_priority: 0,
            flags: 0,
        };
        let mut result = Err(io::Error::new(
            io::ErrorKind::NotFound,
            "suspended child thread not found",
        ));
        let mut found = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while found {
            if entry.owner_process_id == pid {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.thread_id) };
                if !thread.is_null() {
                    let resumed = unsafe { ResumeThread(thread) };
                    unsafe {
                        let _ = CloseHandle(thread);
                    }
                    if resumed != THREAD_SUSPEND_RESUME_FAILED {
                        result = Ok(());
                        break;
                    }
                    result = Err(io::Error::last_os_error());
                }
            }
            found = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        unsafe {
            let _ = CloseHandle(snapshot);
        }
        result
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.raw_handle());
        }
    }
}

#[cfg(windows)]
type WindowsHandle = *mut std::ffi::c_void;

#[cfg(windows)]
const JOB_OBJECT_BASIC_LIMIT_INFORMATION_CLASS: u32 = 2;

#[cfg(windows)]
const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x2000;

#[cfg(windows)]
#[repr(C)]
struct JobObjectBasicLimitInformation {
    per_process_user_time_limit: i64,
    per_job_user_time_limit: i64,
    limit_flags: u32,
    minimum_working_set_size: usize,
    maximum_working_set_size: usize,
    active_process_limit: u32,
    affinity: usize,
    priority_class: u32,
    scheduling_class: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateJobObjectW(job_attributes: *mut std::ffi::c_void, name: *const u16) -> WindowsHandle;
    fn SetInformationJobObject(
        job: WindowsHandle,
        information_class: u32,
        information: *mut std::ffi::c_void,
        information_length: u32,
    ) -> i32;
    fn AssignProcessToJobObject(job: WindowsHandle, process: WindowsHandle) -> i32;
    fn TerminateJobObject(job: WindowsHandle, exit_code: u32) -> i32;
    fn ResumeThread(thread: WindowsHandle) -> u32;
    fn CloseHandle(handle: WindowsHandle) -> i32;
}

#[derive(Debug)]
struct TerminalState {
    inner: Mutex<TerminalStateInner>,
    notify: Notify,
}

#[derive(Debug)]
struct TerminalStateInner {
    output: VecDeque<u8>,
    output_limit: usize,
    truncated: bool,
    // Direct-child status is available to waiters as soon as wait(2) returns;
    // terminal completion below is held until owned output readers finish.
    process_status: Option<Result<TerminalExitStatus, ()>>,
    exit_status: Option<Result<TerminalExitStatus, ()>>,
}

impl TerminalState {
    fn new(output_limit: usize) -> Self {
        Self {
            inner: Mutex::new(TerminalStateInner {
                output: VecDeque::new(),
                output_limit,
                truncated: false,
                process_status: None,
                exit_status: None,
            }),
            notify: Notify::new(),
        }
    }

    fn append_output(&self, bytes: &[u8]) {
        if let Ok(mut inner) = self.inner.lock() {
            let old_len = inner.output.len();
            let total_len = old_len + bytes.len();
            if total_len <= inner.output_limit {
                inner.output.extend(bytes);
            } else {
                inner.truncated = true;
                let discard = total_len - inner.output_limit;
                if discard < old_len {
                    for _ in 0..discard {
                        inner.output.pop_front();
                    }
                    inner.output.extend(bytes);
                } else {
                    inner.output.clear();
                    let skip = discard - old_len;
                    inner.output.extend(bytes.get(skip..).unwrap_or_default());
                }
                while inner
                    .output
                    .front()
                    .is_some_and(|byte| byte & 0b1100_0000 == 0b1000_0000)
                {
                    inner.output.pop_front();
                }
            }
        }
        self.notify.notify_waiters();
    }

    fn complete(&self, status: Result<TerminalExitStatus, ()>) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.exit_status.is_none()
        {
            inner.exit_status = Some(status);
        }
        self.notify.notify_waiters();
    }

    fn process_exit(&self, status: Result<TerminalExitStatus, ()>) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.process_status.is_none()
        {
            inner.process_status = Some(status);
        }
        self.notify.notify_waiters();
    }

    fn mark_unavailable(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.process_status = Some(Err(()));
            inner.exit_status = Some(Err(()));
        }
        self.notify.notify_waiters();
    }

    fn completion_result(&self) -> Result<(), ()> {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.exit_status.clone())
            .map(|status| status.map(|_| ()).map_err(|_| ()))
            .unwrap_or(Err(()))
    }

    fn snapshot(&self) -> Result<(Vec<u8>, bool, Option<TerminalExitStatus>), TerminalError> {
        let inner = self.inner.lock().map_err(|_| TerminalError::Internal)?;
        let exit_status = match &inner.exit_status {
            Some(Ok(status)) => Some(status.clone()),
            Some(Err(())) => return Err(TerminalError::Internal),
            None => None,
        };
        Ok((
            inner.output.iter().copied().collect(),
            inner.truncated,
            exit_status,
        ))
    }

    async fn wait(&self) -> Result<TerminalExitStatus, TerminalError> {
        // Wait reports the direct child promptly; terminal/output applies the
        // separate reader-drain barrier before exposing completion.
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            let status = self
                .inner
                .lock()
                .map_err(|_| TerminalError::Internal)?
                .process_status
                .clone();
            if let Some(status) = status {
                return status.map_err(|_| TerminalError::Internal);
            }
            notified.await;
        }
    }

    async fn wait_for_output_completion(&self) -> Result<(), TerminalError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            let (process_exited, completed) = self
                .inner
                .lock()
                .map_err(|_| TerminalError::Internal)
                .map(|inner| (inner.process_status.is_some(), inner.exit_status.is_some()))?;
            if completed || !process_exited {
                return Ok(());
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
enum Control {
    Kill(oneshot::Sender<Result<(), ()>>),
    Release(oneshot::Sender<Result<(), ()>>),
}

async fn supervise(
    mut child: Child,
    mut stdout: Option<tokio::process::ChildStdout>,
    mut stderr: Option<tokio::process::ChildStderr>,
    state: Arc<TerminalState>,
    process_tree: Arc<ProcessTree>,
    lifecycle: Arc<TerminalLifecycle>,
    mut controls: mpsc::Receiver<Control>,
) {
    let mut stdout_buffer = [0_u8; READ_CHUNK_BYTES];
    let mut stderr_buffer = [0_u8; READ_CHUNK_BYTES];
    let mut release_reply: Option<oneshot::Sender<Result<(), ()>>> = None;
    let mut process_exited = false;
    let mut process_status = None;
    let mut read_failed = false;
    let mut pipes_closed_deliberately = false;
    let mut cleanup_failed = false;
    let mut post_exit_deadline = None;

    loop {
        if process_exited && stdout.is_none() && stderr.is_none() {
            let status = if cleanup_failed || (read_failed && !pipes_closed_deliberately) {
                Err(())
            } else {
                process_status.clone().unwrap_or(Err(()))
            };
            state.complete(status);
            tokio::select! {
                _ = lifecycle.abort_signal.notified() => {
                    let cleanup_ok = terminate_process_tree(&process_tree).await;
                    if !cleanup_ok {
                        state.mark_unavailable();
                    }
                    lifecycle.resource.release();
                    if let Some(reply) = release_reply.take() {
                        let _ = reply.send(Err(()));
                    }
                    break;
                }
                control = controls.recv() => match control {
                    Some(Control::Kill(reply)) => {
                        let result = terminate_process_tree(&process_tree).await;
                        if !result {
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(result.then_some(()).ok_or(()));
                    }
                    Some(Control::Release(reply)) => {
                        let result = terminate_process_tree(&process_tree).await;
                        if !result {
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(if result {
                            state.completion_result()
                        } else {
                            Err(())
                        });
                        break;
                    }
                    None => {
                        let _ = terminate_process_tree(&process_tree).await;
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            let _ = reply.send(Err(()));
                        }
                        break;
                    }
                }
            }
            continue;
        }

        let drain_deadline = async {
            match post_exit_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            _ = lifecycle.abort_signal.notified() => {
                let _ = terminate_and_reap(&mut child, &process_tree).await;
                drop(stdout.take());
                drop(stderr.take());
                state.mark_unavailable();
                lifecycle.resource.release();
                if let Some(reply) = release_reply.take() {
                    let _ = reply.send(Err(()));
                }
                break;
            }
            _ = drain_deadline, if process_exited => {
                // A descendant can keep inherited descriptors open forever.
                // Drain until this deadline, then close our handles without
                // delaying terminal/output indefinitely.
                pipes_closed_deliberately = true;
                stdout = None;
                stderr = None;
                state.complete(if cleanup_failed || read_failed {
                    Err(())
                } else {
                    process_status.clone().unwrap_or(Err(()))
                });
                post_exit_deadline = None;
            }
            control = controls.recv() => {
                match control {
                    Some(Control::Kill(reply)) => {
                        let result = terminate_child(&mut child, &process_tree).await;
                        if result.is_ok() && !process_exited {
                            // Kill is an explicit pipe-closure contract. A
                            // descendant may otherwise keep these handles
                            // open after the direct child is gone.
                            pipes_closed_deliberately = true;
                            stdout = None;
                            stderr = None;
                        } else if result.is_err() {
                            pipes_closed_deliberately = true;
                            stdout = None;
                            stderr = None;
                            cleanup_failed = true;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(result);
                    }
                    Some(Control::Release(reply)) => {
                        if release_reply.is_some() {
                            let _ = reply.send(Err(()));
                            continue;
                        }
                        if process_exited {
                            // A descendant may retain the inherited pipe
                            // descriptors after the direct child exits. The
                            // release operation owns explicit pipe cleanup;
                            // it must not wait for those descriptors to close.
                            let result = terminate_process_tree(&process_tree).await;
                            drop(stdout.take());
                            drop(stderr.take());
                            if result {
                                state.complete(process_status.clone().unwrap_or(Err(())));
                            } else {
                                state.mark_unavailable();
                                lifecycle.resource.release();
                            }
                            let _ = reply.send(if result {
                                state.completion_result()
                            } else {
                                Err(())
                            });
                            break;
                        }
                        release_reply = Some(reply);
                        if terminate_child(&mut child, &process_tree).await.is_err() {
                            drop(stdout.take());
                            drop(stderr.take());
                            pipes_closed_deliberately = true;
                            cleanup_failed = true;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                    }
                    None => {
                        let _ = terminate_and_reap(&mut child, &process_tree).await;
                        drop(stdout.take());
                        drop(stderr.take());
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            let _ = reply.send(Err(()));
                        }
                        break;
                    }
                }
            }
            result = wait_for_child_and_cleanup(&mut child, &process_tree), if !process_exited => {
                match result {
                    Ok((status, cleanup_ok)) => {
                        let status = exit_status(status);
                        process_status = if cleanup_ok && !cleanup_failed {
                            Some(Ok(status.clone()))
                        } else {
                            Some(Err(()))
                        };
                        process_exited = true;
                        cleanup_failed = cleanup_failed || !cleanup_ok;
                        if cleanup_failed {
                            stdout = None;
                            stderr = None;
                            pipes_closed_deliberately = true;
                            post_exit_deadline = None;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        } else {
                            post_exit_deadline = Some(
                                tokio::time::Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
                            );
                            state.process_exit(Ok(status));
                        }
                        if let Some(reply) = release_reply.take() {
                            drop(stdout);
                            drop(stderr);
                            state.complete(if cleanup_failed {
                                Err(())
                            } else {
                                process_status.clone().unwrap_or(Err(()))
                            });
                            let _ = reply.send(state.completion_result());
                            break;
                        }
                    }
                    Err(_) => {
                        process_exited = true;
                        process_status = Some(Err(()));
                        cleanup_failed = true;
                        post_exit_deadline = None;
                        stdout = None;
                        stderr = None;
                        pipes_closed_deliberately = true;
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            state.complete(Err(()));
                            let _ = reply.send(state.completion_result());
                            break;
                        }
                    }
                }
            }
            result = read_pipe(stdout.as_mut(), &mut stdout_buffer), if stdout.is_some() => {
                let Some(result) = result else { continue };
                match result {
                    Ok(0) => stdout = None,
                    Err(_) => {
                        read_failed = true;
                        stdout = None;
                    }
                    Ok(count) => state.append_output(&stdout_buffer[..count]),
                }
            }
            result = read_pipe(stderr.as_mut(), &mut stderr_buffer), if stderr.is_some() => {
                let Some(result) = result else { continue };
                match result {
                    Ok(0) => stderr = None,
                    Err(_) => {
                        read_failed = true;
                        stderr = None;
                    }
                    Ok(count) => state.append_output(&stderr_buffer[..count]),
                }
            }
        }
    }
}

async fn read_pipe<R: AsyncRead + Unpin>(
    reader: Option<&mut R>,
    buffer: &mut [u8],
) -> Option<std::io::Result<usize>> {
    Some(reader?.read(buffer).await)
}

fn spawn_child_reaper(
    mut child: Child,
    stdout: Option<tokio::process::ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
) {
    drop(stdout);
    drop(stderr);
    let _ = child.start_kill();
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
}

async fn retry_cleanup(mut operation: impl FnMut() -> io::Result<()>) -> bool {
    let deadline = tokio::time::Instant::now() + PROCESS_CLEANUP_DEADLINE;
    for attempt in 0..PROCESS_CLEANUP_ATTEMPTS {
        if operation().is_ok() {
            return true;
        }
        if attempt + 1 >= PROCESS_CLEANUP_ATTEMPTS {
            break;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep_until(std::cmp::min(
            deadline,
            now + PROCESS_CLEANUP_RETRY_INTERVAL,
        ))
        .await;
    }
    false
}

async fn terminate_process_tree(process_tree: &ProcessTree) -> bool {
    retry_cleanup(|| process_tree.terminate()).await
}

#[cfg(unix)]
async fn terminate_process_tree_after_leader_exit(process_tree: &ProcessTree) -> bool {
    retry_cleanup(|| process_tree.terminate_after_leader_exit()).await
}

fn start_child_kill(child: &mut Child) -> bool {
    match child.start_kill() {
        Ok(()) => true,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
            ) =>
        {
            true
        }
        Err(_) => false,
    }
}

async fn terminate_and_reap(child: &mut Child, process_tree: &ProcessTree) -> bool {
    let cleanup_ok = terminate_process_tree(process_tree).await;
    let child_kill_ok = start_child_kill(child);
    let wait_ok = child.wait().await.is_ok();
    cleanup_ok && child_kill_ok && wait_ok
}

#[cfg(unix)]
#[repr(C, align(8))]
struct WaitInfo {
    bytes: [u8; 128],
}

#[cfg(unix)]
impl WaitInfo {
    fn pid(&self) -> i32 {
        i32::from_ne_bytes(self.bytes[16..20].try_into().unwrap_or_default())
    }
}

#[cfg(unix)]
const WAITID_PID: u32 = 1;
#[cfg(unix)]
const WAITID_WNOHANG: i32 = 1;
#[cfg(unix)]
const WAITID_WEXITED: i32 = 4;
#[cfg(target_os = "linux")]
const WAITID_WNOWAIT: i32 = 0x0100_0000;
#[cfg(all(
    unix,
    not(target_os = "linux"),
    any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    )
))]
const WAITID_WNOWAIT: i32 = 0x20;
#[cfg(all(
    unix,
    not(target_os = "linux"),
    not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))
))]
const WAITID_WNOWAIT: i32 = 0x20;

#[cfg(unix)]
unsafe extern "C" {
    fn waitid(id_type: u32, id: i32, info: *mut WaitInfo, options: i32) -> i32;
}

#[cfg(unix)]
fn child_has_exited(pid: i32) -> io::Result<bool> {
    let mut info = WaitInfo { bytes: [0; 128] };
    let result = unsafe {
        waitid(
            WAITID_PID,
            pid,
            &mut info,
            WAITID_WNOHANG | WAITID_WEXITED | WAITID_WNOWAIT,
        )
    };
    if result == 0 {
        Ok(info.pid() == pid)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
async fn wait_for_child_exit(pid: i32) -> io::Result<()> {
    loop {
        match child_has_exited(pid) {
            Ok(true) => return Ok(()),
            Ok(false) => tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

#[cfg(unix)]
async fn wait_for_child_and_cleanup(
    child: &mut Child,
    process_tree: &ProcessTree,
) -> Result<(std::process::ExitStatus, bool), ()> {
    let observed = wait_for_child_exit(process_tree.leader_pid()).await.is_ok();
    let cleanup_ok = if observed {
        // Keep the child unreaped while retrying. Its zombie PID continues to
        // anchor the process group, so a later attempt cannot hit a recycled
        // PGID.
        terminate_process_tree_after_leader_exit(process_tree).await
    } else {
        // Fail closed if the exit observer is unavailable: kill while this
        // process still owns the unreaped child identity, then reap it.
        terminate_process_tree(process_tree).await
    };
    let child_kill_ok = if observed {
        true
    } else {
        start_child_kill(child)
    };
    let status = child.wait().await.map_err(|_| ())?;
    Ok((status, cleanup_ok && child_kill_ok))
}

#[cfg(not(unix))]
async fn wait_for_child_and_cleanup(
    child: &mut Child,
    _process_tree: &ProcessTree,
) -> Result<(std::process::ExitStatus, bool), ()> {
    child
        .wait()
        .await
        .map(|status| (status, true))
        .map_err(|_| ())
}

async fn terminate_child(child: &mut Child, process_tree: &ProcessTree) -> Result<(), ()> {
    let cleanup_ok = terminate_process_tree(process_tree).await;
    let child_kill_ok = start_child_kill(child);
    if cleanup_ok && child_kill_ok {
        Ok(())
    } else {
        Err(())
    }
}

fn exit_status(status: std::process::ExitStatus) -> TerminalExitStatus {
    let mut result = TerminalExitStatus::new();
    if let Some(code) = status.code()
        && let Ok(code) = u32::try_from(code)
    {
        result = result.exit_code(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            result = result.signal(signal_name(signal));
        }
    }
    result
}

#[cfg(unix)]
fn signal_name(signal: i32) -> String {
    match signal {
        1 => "SIGHUP".into(),
        2 => "SIGINT".into(),
        3 => "SIGQUIT".into(),
        6 => "SIGABRT".into(),
        9 => "SIGKILL".into(),
        14 => "SIGALRM".into(),
        15 => "SIGTERM".into(),
        signal => format!("SIG{signal}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_cwd_is_fd_bound_across_replacement() {
        use std::os::unix::fs::symlink;

        let root =
            std::env::temp_dir().join(format!("agui-terminal-cwd-race-{}", uuid::Uuid::new_v4()));
        let approved = root.join("approved");
        let moved = root.join("moved");
        let outside = root.join("outside");
        std::fs::create_dir_all(&approved).expect("approved cwd");
        std::fs::create_dir_all(&outside).expect("outside cwd");
        let root = std::fs::canonicalize(root).expect("root canonicalizes");
        let approved_path = std::fs::canonicalize(&approved).expect("approved canonicalizes");
        let outside = std::fs::canonicalize(outside).expect("outside canonicalizes");
        let cwd = ApprovedCwd::open(&root, &approved_path).expect("approved cwd opens");

        std::fs::rename(&approved_path, &moved).expect("approved cwd is replaced");
        symlink(&outside, &approved_path).expect("replacement symlink creates");
        assert!(
            ApprovedCwd::open(&root, &approved_path).is_err(),
            "a replacement must not pass the second identity check"
        );

        let mut command = Command::new("pwd");
        command.args(["-P"]).stdout(Stdio::piped());
        cwd.configure(&mut command);
        let output = command.output().await.expect("pwd runs");
        assert!(output.status.success());
        let actual = String::from_utf8_lossy(&output.stdout);
        let expected = std::fs::canonicalize(&moved).expect("moved cwd canonicalizes");
        assert_eq!(actual.trim(), expected.to_string_lossy());
        assert!(!actual.contains(outside.to_string_lossy().as_ref()));

        let _ = std::fs::remove_file(approved_path);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn completed_terminal_remains_releasable() {
        let (registry, guard) =
            TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
        let request = CreateTerminalRequest::new("session", "true");
        let (id, created) = registry.create(&request).expect("terminal creates");
        created.disarm();

        let terminal = registry.take(&id).expect("terminal remains registered");
        let status = terminal.wait().await.expect("terminal exits");
        assert_eq!(status.exit_code, Some(0));
        terminal
            .release()
            .await
            .expect("completed terminal releases");

        drop(terminal);
        drop(guard);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn output_after_wait_includes_final_bytes() {
        let (registry, guard) =
            TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
        let request = CreateTerminalRequest::new("session", "sh")
            .args(vec!["-c".into(), "printf final-output; exit 0".into()]);
        let (id, created) = registry.create(&request).expect("terminal creates");
        created.disarm();

        let terminal = registry.take(&id).expect("terminal remains registered");
        terminal.wait().await.expect("terminal exits");
        let output = terminal.output().await.expect("terminal output");
        assert_eq!(output.output, "final-output");

        drop(terminal);
        drop(guard);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn wait_output_and_release_do_not_require_inherited_pipe_eof() {
        let marker = std::env::temp_dir().join(format!(
            "agui-terminal-descendant-{}.pid",
            uuid::Uuid::new_v4()
        ));
        let (registry, guard) =
            TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
        let request = CreateTerminalRequest::new("session", "sh")
            .args(vec![
                "-c".into(),
                "printf final-output; sleep 30 & echo $! > \"$ACP_PID_FILE\"; exit 7".into(),
            ])
            .env(vec![agent_client_protocol::schema::v1::EnvVariable::new(
                "ACP_PID_FILE",
                marker.to_string_lossy(),
            )]);
        let (id, created) = registry.create(&request).expect("terminal creates");
        created.disarm();
        let terminal = registry.get(&id).expect("terminal remains registered");

        let wait = tokio::time::timeout(std::time::Duration::from_secs(2), terminal.wait()).await;
        let output =
            tokio::time::timeout(std::time::Duration::from_secs(2), terminal.output()).await;

        let descendant_stopped = if let Ok(pid) = std::fs::read_to_string(&marker)
            && let Ok(pid) = pid.trim().parse::<u32>()
        {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let status = std::process::Command::new("kill")
                        .args(["-0", &pid.to_string()])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .expect("kill command runs");
                    if !status.success() {
                        break true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or(false)
        } else {
            false
        };
        let release =
            tokio::time::timeout(std::time::Duration::from_secs(2), terminal.release()).await;
        if !descendant_stopped
            && let Some(pid) = std::fs::read_to_string(&marker)
                .ok()
                .and_then(|pid| pid.trim().parse::<u32>().ok())
        {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        let _ = std::fs::remove_file(marker);

        assert_eq!(
            wait.expect("wait must not hang")
                .expect("wait succeeds")
                .exit_code,
            Some(7)
        );
        assert_eq!(
            output
                .expect("output must not hang")
                .expect("output succeeds")
                .output,
            "final-output"
        );
        release
            .expect("release must not hang")
            .expect("release succeeds");
        assert!(
            descendant_stopped,
            "releasing a terminal must kill inherited descendants"
        );
        drop(terminal);
        drop(guard);
    }

    #[test]
    fn output_buffer_retains_exact_suffix_and_marks_truncation() {
        let state = TerminalState::new(MAX_OUTPUT_BYTES);
        state.append_output(&vec![b'a'; MAX_OUTPUT_BYTES]);
        state.append_output(b"tail");
        let (output, truncated, _) = state.snapshot().expect("snapshot");
        assert_eq!(output.len(), MAX_OUTPUT_BYTES);
        assert!(truncated);
        assert_eq!(&output[MAX_OUTPUT_BYTES - 4..], b"tail");
    }

    #[test]
    fn output_buffer_is_lazily_allocated() {
        let state = TerminalState::new(MAX_OUTPUT_BYTES);
        let inner = state.inner.lock().expect("state lock");
        assert_eq!(inner.output.capacity(), 0);
    }

    #[test]
    fn output_limit_honors_request_and_zero_retains_nothing() {
        let default = CreateTerminalRequest::new("session", "true");
        assert_eq!(output_limit(&default), MAX_OUTPUT_BYTES);

        let requested = default.clone().output_byte_limit(4);
        assert_eq!(output_limit(&requested), 4);

        let zero = default.clone().output_byte_limit(0);
        assert_eq!(output_limit(&zero), 0);
        let state = TerminalState::new(output_limit(&zero));
        state.append_output(b"output");
        let (output, truncated, _) = state.snapshot().expect("snapshot");
        assert!(output.is_empty());
        assert!(truncated);

        let bounded = default.output_byte_limit(u64::MAX);
        assert_eq!(output_limit(&bounded), MAX_OUTPUT_BYTES);
    }

    #[test]
    fn output_buffer_truncates_only_at_utf8_character_boundaries() {
        let state = TerminalState::new(4);
        state.append_output("abcéXYZ".as_bytes());
        let (output, truncated, _) = state.snapshot().expect("snapshot");

        assert!(truncated);
        assert_eq!(output, b"XYZ");
        assert!(std::str::from_utf8(&output).is_ok());
    }

    #[test]
    fn output_buffer_keeps_invalid_utf8_until_response_building() {
        let state = TerminalState::new(MAX_OUTPUT_BYTES);
        state.append_output(&[0xff, b'x']);
        let (output, _, _) = state.snapshot().expect("snapshot");
        assert_eq!(output, vec![0xff, b'x']);
        let response = TerminalOutputResponse::new(String::from_utf8_lossy(&output), false);
        assert_eq!(response.output, "�x");
    }

    #[tokio::test]
    async fn cleanup_failure_is_bounded_and_marks_terminal_unavailable() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = AtomicUsize::new(0);
        assert!(
            !retry_cleanup(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::other("test cleanup failure"))
            })
            .await
        );
        assert_eq!(attempts.load(Ordering::Relaxed), PROCESS_CLEANUP_ATTEMPTS);

        let state = TerminalState::new(0);
        state.mark_unavailable();
        assert_eq!(state.completion_result(), Err(()));
        assert!(matches!(state.snapshot(), Err(TerminalError::Internal)));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn terminal_registry_enforces_per_session_capacity() {
        let (registry, guard) =
            TerminalRegistry::new(PathBuf::from("/")).expect("terminal backend initializes");
        let request = CreateTerminalRequest::new("session", "true");
        let mut created = Vec::new();
        for _ in 0..MAX_TERMINALS_PER_SESSION {
            let (_, terminal) = registry
                .create(&request)
                .expect("terminal creates below cap");
            created.push(terminal);
        }

        assert!(matches!(
            registry.create(&request),
            Err(TerminalError::Capacity)
        ));
        assert_eq!(i32::from(wire_error(TerminalError::Capacity).code), -32800);

        drop(created);
        drop(guard);
    }
}
