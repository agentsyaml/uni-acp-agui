#[cfg(unix)]
use super::CHILD_EXIT_POLL_INTERVAL;
use super::process_tree::ProcessTree;
use super::{PROCESS_CLEANUP_ATTEMPTS, PROCESS_CLEANUP_DEADLINE, PROCESS_CLEANUP_RETRY_INTERVAL};
use agent_client_protocol::schema::v1::TerminalExitStatus;
use std::io;
use tokio::process::Child;

pub(super) fn spawn_child_reaper(
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

pub(super) async fn retry_cleanup(mut operation: impl FnMut() -> io::Result<()>) -> bool {
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

pub(super) async fn terminate_process_tree(process_tree: &ProcessTree) -> bool {
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

pub(super) async fn terminate_and_reap(child: &mut Child, process_tree: &ProcessTree) -> bool {
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
pub(super) async fn wait_for_child_and_cleanup(
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
pub(super) async fn wait_for_child_and_cleanup(
    child: &mut Child,
    _process_tree: &ProcessTree,
) -> Result<(std::process::ExitStatus, bool), ()> {
    child
        .wait()
        .await
        .map(|status| (status, true))
        .map_err(|_| ())
}

pub(super) async fn terminate_child(
    child: &mut Child,
    process_tree: &ProcessTree,
) -> Result<(), ()> {
    let cleanup_ok = terminate_process_tree(process_tree).await;
    let child_kill_ok = start_child_kill(child);
    if cleanup_ok && child_kill_ok {
        Ok(())
    } else {
        Err(())
    }
}

pub(super) fn exit_status(status: std::process::ExitStatus) -> TerminalExitStatus {
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
