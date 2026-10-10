use std::path::{Path, PathBuf};

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use std::sync::{Mutex, OnceLock};
#[cfg(test)]
use tokio::sync::Notify;

#[cfg(test)]
pub(crate) struct WriteGate {
    pub(crate) path: PathBuf,
    pub(crate) started: Notify,
    pub(crate) release: Notify,
}

#[cfg(test)]
static TEST_WRITE_GATE: OnceLock<Mutex<Option<Arc<WriteGate>>>> = OnceLock::new();

/// Install a write gate. Only the Linux-gated symlink-swap and cancellation
/// probes drive these; they pair with the `#[cfg(target_os = "linux")]` gate on
/// those tests, which is why this is gated to Linux as well — on other
/// platforms the secure-open path does not exist to be probed.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn install_write_gate(gate: Arc<WriteGate>) {
    *TEST_WRITE_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("write gate lock") = Some(gate);
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn clear_write_gate() {
    if let Some(slot) = TEST_WRITE_GATE.get() {
        *slot.lock().expect("write gate lock") = None;
    }
}

#[cfg(test)]
pub(crate) async fn wait_for_write_gate(path: &Path) {
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

/// Install a read gate. See `install_write_gate` for why this is Linux-gated.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn install_read_gate(gate: Arc<ReadGate>) {
    *TEST_READ_GATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("read gate lock") = Some(gate);
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn clear_read_gate() {
    if let Some(slot) = TEST_READ_GATE.get() {
        *slot.lock().expect("read gate lock") = None;
    }
}

#[cfg(test)]
pub(crate) async fn wait_for_read_gate(path: &Path) {
    let gate = TEST_READ_GATE
        .get()
        .and_then(|slot| slot.lock().expect("read gate lock").clone());
    let Some(gate) = gate.filter(|gate| gate.path == path) else {
        return;
    };
    gate.started.notify_waiters();
    gate.release.notified().await;
}
