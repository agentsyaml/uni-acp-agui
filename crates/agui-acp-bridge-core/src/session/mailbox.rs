use std::io::Write;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{mpsc, oneshot};

use crate::acp::TurnState;
use crate::stream::BridgeStreamItem;

use super::{
    EVENT_BYTE_LIMIT, EVENT_CHANNEL_CAPACITY, EVENT_ITEM_LIMIT, FAILED_EVENT_DELIVERY_TIMEOUT,
};

pub(super) type EventSlot = Arc<Mutex<Option<Arc<EventRoute>>>>;

pub(super) struct EventRoute {
    pub(super) events_tx: mpsc::Sender<BridgeStreamItem>,
    pub(super) turn: Arc<TurnState>,
    pub(super) state: Mutex<RouteState>,
}

#[derive(Default)]
pub(super) struct RouteState {
    pub(super) terminal: bool,
    pub(super) failed: bool,
    pub(super) limit: Option<EventLimit>,
}

pub(super) struct EventMailbox {
    pub(super) tx: mpsc::Sender<QueuedEvent>,
    pub(super) budget: Arc<Mutex<EventBudget>>,
    item_limit: usize,
    byte_limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum EventLimit {
    Count,
    Bytes,
}

impl EventLimit {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Count => "item_count",
            Self::Bytes => "serialized_bytes",
        }
    }
}

impl EventMailbox {
    pub(super) fn new() -> (EventMailboxTx, mpsc::Receiver<QueuedEvent>) {
        Self::with_limits(EVENT_ITEM_LIMIT, EVENT_BYTE_LIMIT)
    }

    pub(super) fn with_limits(
        item_limit: usize,
        byte_limit: usize,
    ) -> (EventMailboxTx, mpsc::Receiver<QueuedEvent>) {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        (
            Arc::new(Self {
                tx,
                budget: Arc::new(Mutex::new(EventBudget::default())),
                item_limit,
                byte_limit,
            }),
            rx,
        )
    }

    pub(super) fn enqueue_data(
        &self,
        route: &Arc<EventRoute>,
        item: BridgeStreamItem,
    ) -> Result<(), EventLimit> {
        let bytes = event_payload_bytes(&item);
        let mut route_state = route.state.lock().expect("event route poisoned");
        if route_state.terminal || route_state.failed {
            return Err(EventLimit::Count);
        }
        let mut budget = self.budget.lock().expect("event budget poisoned");
        let limit = if budget.items >= self.item_limit {
            Some(EventLimit::Count)
        } else if bytes > self.byte_limit || budget.bytes > self.byte_limit - bytes {
            Some(EventLimit::Bytes)
        } else {
            None
        };
        if let Some(limit) = limit {
            route_state.failed = true;
            route_state.limit = Some(limit);
            route.turn.fail();
            return Err(limit);
        }
        budget.items += 1;
        budget.bytes += bytes;
        drop(budget);
        let credit = EventCredit {
            budget: self.budget.clone(),
            bytes,
        };
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => {
                permit.send(QueuedEvent {
                    route: route.clone(),
                    item,
                    credit: Some(credit),
                    terminal_ack: None,
                });
                Ok(())
            }
            Err(_) => {
                drop(credit);
                route_state.failed = true;
                route_state.limit = Some(EventLimit::Count);
                route.turn.fail();
                Err(EventLimit::Count)
            }
        }
    }

    pub(super) fn enqueue_terminal(
        &self,
        route: &Arc<EventRoute>,
        item: BridgeStreamItem,
    ) -> oneshot::Receiver<Result<(), ()>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let mut state = route.state.lock().expect("event route poisoned");
        if state.terminal {
            state.failed = true;
            state.limit.get_or_insert(EventLimit::Count);
            route.turn.fail();
            let _ = ack_tx.send(Err(()));
            return ack_rx;
        }
        state.terminal = true;
        match self.tx.clone().try_reserve_owned() {
            Ok(permit) => {
                permit.send(QueuedEvent {
                    route: route.clone(),
                    item,
                    credit: None,
                    terminal_ack: Some(ack_tx),
                });
            }
            Err(_) => {
                state.failed = true;
                state.limit.get_or_insert(EventLimit::Count);
                route.turn.fail();
                let _ = ack_tx.send(Err(()));
            }
        }
        ack_rx
    }
}

fn event_payload_bytes(item: &BridgeStreamItem) -> usize {
    match item {
        BridgeStreamItem::Update(update) => json_size_bounded(update, EVENT_BYTE_LIMIT),
        BridgeStreamItem::Interrupt { id, request } => {
            let remaining = EVENT_BYTE_LIMIT.saturating_sub(id.len());
            let size = json_size_bounded(request, remaining);
            if size == usize::MAX {
                usize::MAX
            } else {
                size.saturating_add(id.len())
            }
        }
        _ => 0,
    }
}

pub(super) fn json_size_bounded(value: &impl serde::Serialize, limit: usize) -> usize {
    struct Counter {
        size: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.size) {
                return Err(std::io::Error::other("serialized size limit exceeded"));
            }
            self.size += bytes.len();
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { size: 0, limit };
    if serde_json::to_writer(&mut counter, value).is_err() {
        usize::MAX
    } else {
        counter.size
    }
}

#[derive(Default)]
pub(super) struct EventBudget {
    pub(super) items: usize,
    bytes: usize,
}

struct EventCredit {
    budget: Arc<Mutex<EventBudget>>,
    bytes: usize,
}

impl Drop for EventCredit {
    fn drop(&mut self) {
        let mut budget = self.budget.lock().expect("event budget poisoned");
        budget.items -= 1;
        budget.bytes -= self.bytes;
    }
}

/// One [`BridgeStreamItem`] queued by a handler running on the ACP SDK's
/// single dispatch loop, awaiting off-loop delivery to the per-prompt
/// channel named by `events_tx`.
pub(super) struct QueuedEvent {
    route: Arc<EventRoute>,
    item: BridgeStreamItem,
    credit: Option<EventCredit>,
    terminal_ack: Option<oneshot::Sender<Result<(), ()>>>,
}

pub(super) type EventMailboxTx = Arc<EventMailbox>;

/// Sole consumer of the dispatch-loop event mailbox.
///
/// Items are enqueued strictly in dispatch order by the single-threaded
/// dispatch loop and delivered by this single driver in the same order, so
/// per-turn message ordering is exact — which a naive `cx.spawn`-per-send
/// (concurrent `FuturesUnordered` tasks racing an unordered lock) could not
/// guarantee for adjacent `AgentMessageChunk`s. The driver, not the dispatch
/// loop, absorbs back-pressure: it awaits the bounded per-prompt channel.
///
/// The turn's TERMINAL item (`Finished`/`RunError`) travels through this
/// same FIFO: because this driver is the mailbox's only consumer, FIFO
/// order alone guarantees every update enqueued before the terminal has
/// been pushed into the per-prompt channel before the consumer can observe
/// the terminal. Without this barrier the `session/prompt` response (routed
/// on the dispatch loop while the mailbox still holds undelivered updates)
/// lets the actor finish the turn early — the SSE stream ends on
/// `Finished` and the mailbox tail is lost.
///
/// The mailbox outlives individual turns (one connection = one driver), so
/// a send failure — meaning that turn's consumer dropped and the turn is
/// terminating via the `events_tx.closed()` watcher in
/// `run_prompt_with_cancel` — skips only that item; queued items of the
/// dead turn fail against its dead sender one by one, while items of any
/// later turn carry their own live sender and deliver normally. The driver
/// itself must NOT exit here: doing so would permanently kill event
/// delivery for every future turn on the connection.
pub(super) async fn run_event_mailbox(
    mut rx: mpsc::Receiver<QueuedEvent>,
    unusable: Arc<AtomicBool>,
) -> Result<(), agent_client_protocol::Error> {
    let mut failed_deadline: Option<(crate::acp::TurnId, tokio::time::Instant)> = None;
    while let Some(event) = rx.recv().await {
        let QueuedEvent {
            route,
            item,
            credit,
            terminal_ack,
        } = event;
        let mut failed = route.turn.is_failed();
        let mut permit = None;
        if !failed {
            let notified = route.turn.failure_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if route.turn.is_failed() {
                failed = true;
            } else {
                tokio::select! {
                    result = route.events_tx.reserve() => {
                        permit = result.ok();
                    }
                    () = &mut notified => failed = true,
                }
            }
        }

        if failed {
            let turn_id = route.turn.id();
            let deadline = match failed_deadline {
                Some((id, deadline)) if id == turn_id => deadline,
                _ => {
                    let deadline = tokio::time::Instant::now() + FAILED_EVENT_DELIVERY_TIMEOUT;
                    failed_deadline = Some((turn_id, deadline));
                    deadline
                }
            };
            permit = match tokio::time::timeout_at(deadline, route.events_tx.reserve()).await {
                Ok(Ok(permit)) => Some(permit),
                _ => None,
            };
            if permit.is_none() {
                unusable.store(true, Ordering::Release);
                drop(credit);
                if let Some(ack) = terminal_ack {
                    let _ = ack.send(Err(()));
                }
                while let Ok(mut queued) = rx.try_recv() {
                    if let Some(ack) = queued.terminal_ack.take() {
                        let _ = ack.send(Err(()));
                    }
                    drop(queued);
                }
                continue;
            }
            permit.expect("reserved event slot").send(item);
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        } else if let Some(permit) = permit {
            permit.send(item);
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        } else {
            drop(credit);
            if let Some(ack) = terminal_ack {
                let _ = ack.send(Ok(()));
            }
        }
    }
    Ok(())
}
