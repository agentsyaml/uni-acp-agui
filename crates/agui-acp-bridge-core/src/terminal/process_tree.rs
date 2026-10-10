#[cfg(target_os = "linux")]
use super::cwd::LibcLong;
#[cfg(windows)]
use super::windows::WindowsJob;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::sync::Mutex;
use tokio::process::Child;

pub(crate) fn kill_acp_process_group(pid: u32) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(pid)
        && pid > 0
    {
        let result = unsafe { kill_process_group(-pid, SIGKILL) };
        if result != 0 && io::Error::last_os_error().raw_os_error() != Some(ESRCH) {
            tracing::debug!(pid, "failed to signal ACP process group");
        }
    }
    #[cfg(not(unix))]
    let _ = pid;
}

#[derive(Debug)]
pub(super) struct ProcessTree {
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
    pub(super) fn new(child: &Child) -> Result<Self, ()> {
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

    pub(super) fn supported() -> bool {
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
    pub(super) fn resume(&self, child: &Child) -> io::Result<()> {
        self.job.resume(child)
    }

    pub(super) fn terminate(&self) -> std::io::Result<()> {
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
    pub(super) fn terminate_after_leader_exit(&self) -> std::io::Result<()> {
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
    pub(super) fn leader_pid(&self) -> i32 {
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
            libc::syscall(
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
const SYS_PIDFD_OPEN: LibcLong = libc::SYS_pidfd_open;

#[cfg(target_os = "linux")]
const SYS_PIDFD_SEND_SIGNAL: LibcLong = libc::SYS_pidfd_send_signal;

#[cfg(target_os = "linux")]
#[repr(C)]
struct LinuxSigInfo {
    _unused: [u8; 128],
}

#[cfg(target_os = "linux")]
#[cfg(target_os = "linux")]
fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(SYS_PIDFD_OPEN, pid, 0_u32) };
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
