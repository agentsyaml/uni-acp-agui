use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::raw::{c_int, c_uint};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

const AT_FDCWD: RawFd = libc::AT_FDCWD;
const SYS_OPENAT2: libc::c_long = libc::SYS_openat2;
const O_RDONLY: libc::c_int = libc::O_RDONLY;
const O_WRONLY: libc::c_int = libc::O_WRONLY;
const O_CREAT: libc::c_int = libc::O_CREAT;
const O_TRUNC: libc::c_int = libc::O_TRUNC;
const O_DIRECTORY: libc::c_int = libc::O_DIRECTORY;
const O_CLOEXEC: libc::c_int = libc::O_CLOEXEC;
const O_PATH: libc::c_int = libc::O_PATH;

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

pub(super) struct RootDirectory {
    fd: OwnedFd,
}

static ROOT_DIRECTORIES: OnceLock<Mutex<HashMap<PathBuf, Weak<RootDirectory>>>> = OnceLock::new();

fn root_directories() -> &'static Mutex<HashMap<PathBuf, Weak<RootDirectory>>> {
    ROOT_DIRECTORIES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "filesystem path contains NUL"))
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
        libc::syscall(
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
    let result = unsafe { libc::mkdirat(dirfd, path.as_ptr(), 0o777) };
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

pub(super) fn root_for(cwd: &Path) -> io::Result<Arc<RootDirectory>> {
    let mut roots = root_directories()
        .lock()
        .map_err(|_| io::Error::other("sandbox root registry is poisoned"))?;
    roots.retain(|_, root| root.strong_count() != 0);
    if let Some(root) = roots.get(cwd).and_then(Weak::upgrade) {
        return Ok(root);
    }
    let root = Arc::new(RootDirectory {
        fd: open_root(cwd)?,
    });
    roots.insert(cwd.to_path_buf(), Arc::downgrade(&root));
    Ok(root)
}

#[cfg(test)]
pub(super) fn cached_root_paths() -> Vec<PathBuf> {
    root_directories()
        .lock()
        .expect("sandbox root registry is poisoned")
        .keys()
        .cloned()
        .collect()
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

pub(super) fn open_read(root: &RootDirectory, cwd: &Path, candidate: &Path) -> io::Result<File> {
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
