use super::*;
use std::sync::atomic::AtomicUsize;

#[cfg(target_os = "linux")]
static FILESYSTEM_WRITE_GATE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(target_os = "linux")]
struct WriteGateReset;

#[cfg(target_os = "linux")]
impl Drop for WriteGateReset {
    fn drop(&mut self) {
        crate::file_ops::clear_write_gate();
    }
}

mod admission_mailbox;
mod filesystem;
mod filesystem_boundaries;
mod filesystem_fixtures;
mod flood_permissions;
mod history;
mod initialization;
mod wire_limits;
mod work_quota;
mod write_cancellation;
