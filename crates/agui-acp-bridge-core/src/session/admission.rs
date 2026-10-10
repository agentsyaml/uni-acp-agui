use std::sync::{Arc, Mutex};

use super::{SPAWNED_WORK_BYTES, SPAWNED_WORK_ITEMS};

#[derive(Default)]
pub(super) struct WorkUsage {
    pub(super) items: usize,
    bytes: usize,
}

pub(super) struct WorkAdmission {
    pub(super) usage: Mutex<WorkUsage>,
    limits: (usize, usize),
    pub(super) retire_tx: tokio::sync::watch::Sender<Option<agent_client_protocol::Error>>,
}

#[derive(Clone)]
pub(crate) struct WorkPermit {
    _lease: Arc<WorkLease>,
}

struct WorkLease {
    admission: Arc<WorkAdmission>,
    bytes: usize,
}

impl Drop for WorkLease {
    fn drop(&mut self) {
        let mut usage = self.admission.usage.lock().expect("work usage poisoned");
        usage.items -= 1;
        usage.bytes -= self.bytes;
    }
}

impl WorkAdmission {
    pub(super) fn new() -> Arc<Self> {
        Self::with_limits(SPAWNED_WORK_ITEMS, SPAWNED_WORK_BYTES)
    }

    pub(super) fn with_limits(items: usize, bytes: usize) -> Arc<Self> {
        let (retire_tx, _) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            usage: Mutex::new(WorkUsage::default()),
            limits: (items, bytes),
            retire_tx,
        })
    }

    pub(super) fn try_acquire<T: serde::Serialize, I: serde::Serialize + ?Sized>(
        self: &Arc<Self>,
        request: &T,
        id: &I,
    ) -> Result<WorkPermit, ()> {
        let bytes = super::mailbox::json_size_bounded(&(request, id), self.limits.1);
        let mut usage = self.usage.lock().expect("work usage poisoned");
        if self.retire_tx.borrow().is_some()
            || usage.items >= self.limits.0
            || bytes == usize::MAX
            || bytes > self.limits.1
            || usage.bytes > self.limits.1 - bytes
        {
            drop(usage);
            self.retire_tx.send_if_modified(|current| {
                if current.is_none() {
                    *current = Some(agent_client_protocol::Error::internal_error().data(serde_json::json!({"limit":"ACP_SPAWNED_WORK","items":self.limits.0,"bytes":self.limits.1})));
                    true
                } else { false }
            });
            return Err(());
        }
        usage.items += 1;
        usage.bytes += bytes;
        Ok(WorkPermit {
            _lease: Arc::new(WorkLease {
                admission: self.clone(),
                bytes,
            }),
        })
    }
}
