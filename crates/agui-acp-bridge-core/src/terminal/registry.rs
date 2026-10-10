use super::cwd::ApprovedCwd;
use super::process_tree::ProcessTree;
use super::runtime::Terminal;
#[cfg(windows)]
use super::windows::CREATE_SUSPENDED;
use super::{MAX_OUTPUT_BYTES, MAX_TERMINALS_PER_SESSION};
use agent_client_protocol::schema::v1::{CreateTerminalRequest, TerminalId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, Weak};
use tokio::process::Command;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalError {
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
pub(super) struct TerminalResource {
    registry: Weak<RegistryInner>,
    id: TerminalId,
}

impl TerminalResource {
    pub(super) fn release(&self) {
        if let Some(registry) = self.registry.upgrade()
            && let Ok(mut terminals) = registry.terminals.lock()
        {
            terminals.remove(&self.id);
        }
    }
}

#[derive(Debug)]
pub(super) struct TerminalLifecycle {
    pub(super) resource: Arc<TerminalResource>,
    pub(super) abort_signal: Arc<Notify>,
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

    pub(super) fn create(
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

    pub(super) fn get(&self, id: &TerminalId) -> Result<Arc<Terminal>, TerminalError> {
        self.inner
            .terminals
            .lock()
            .map_err(|_| TerminalError::Internal)?
            .get(id)
            .cloned()
            .ok_or(TerminalError::ResourceNotFound)
    }

    pub(super) fn take(&self, id: &TerminalId) -> Result<Arc<Terminal>, TerminalError> {
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

pub(super) struct CreatedTerminalGuard {
    registry: TerminalRegistry,
    id: TerminalId,
    active: bool,
}

impl CreatedTerminalGuard {
    pub(super) fn disarm(mut self) {
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

pub(super) fn output_limit(request: &CreateTerminalRequest) -> usize {
    // ACP's u64 limit permits zero: retain no bytes and mark any output as
    // truncated. An absent limit keeps the bridge default.
    request.output_byte_limit.map_or(MAX_OUTPUT_BYTES, |limit| {
        usize::try_from(limit.min(MAX_OUTPUT_BYTES as u64)).unwrap_or(MAX_OUTPUT_BYTES)
    })
}

pub(super) fn wire_error(error: TerminalError) -> agent_client_protocol::Error {
    match error {
        TerminalError::InvalidParams => agent_client_protocol::Error::invalid_params(),
        // A per-session budget violation, not a caller cancellation: report
        // -32603 with the actual limit instead of mislabeled -32800.
        TerminalError::Capacity => agent_client_protocol::Error::internal_error().data(
            serde_json::json!({"limit": "MAX_TERMINALS_PER_SESSION", "cap": MAX_TERMINALS_PER_SESSION}),
        ),
        TerminalError::ResourceNotFound => agent_client_protocol::Error::resource_not_found(None),
        TerminalError::Internal => agent_client_protocol::Error::internal_error(),
    }
}
