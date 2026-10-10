#[cfg(windows)]
use super::windows::WindowsDirectoryLock;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
use tokio::process::Command;

#[derive(Debug)]
pub(super) struct ApprovedCwd {
    #[cfg(unix)]
    directory: std::fs::File,
    #[cfg(windows)]
    directory: WindowsDirectoryLock,
    #[cfg(windows)]
    path: PathBuf,
}

impl ApprovedCwd {
    pub(super) fn open(root: &Path, canonical: &Path) -> io::Result<Self> {
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

    pub(super) fn open_beneath(
        root: &Self,
        root_path: &Path,
        canonical: &Path,
    ) -> io::Result<Self> {
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

    pub(super) fn configure(&self, command: &mut Command) {
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
        libc::syscall(
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
const AT_FDCWD: RawFd = libc::AT_FDCWD;
#[cfg(target_os = "linux")]
const O_DIRECTORY: i32 = libc::O_DIRECTORY;
#[cfg(target_os = "linux")]
const O_CLOEXEC: i32 = libc::O_CLOEXEC;
#[cfg(target_os = "linux")]
const O_PATH: i32 = libc::O_PATH;
#[cfg(target_os = "linux")]
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
#[cfg(target_os = "linux")]
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
#[cfg(target_os = "linux")]
const RESOLVE_BENEATH: u64 = 0x08;
#[cfg(target_os = "linux")]
pub(super) type LibcLong = std::os::raw::c_long;
#[cfg(target_os = "linux")]
const SYS_OPENAT2: LibcLong = libc::SYS_openat2;
#[cfg(target_os = "linux")]
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}
