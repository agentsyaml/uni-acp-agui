use super::*;

#[tokio::test]
async fn work_admission_latches_once_and_blocking_clone_keeps_credit() {
    let work = WorkAdmission::with_limits(2, 4096);
    let retired = work.retire_tx.subscribe();
    let first = work.try_acquire(&"first", "id-1").unwrap();
    let second = work.try_acquire(&"second", "id-2").unwrap();
    let blocking_lease = first.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocking = tokio::task::spawn_blocking(move || {
        let _lease = blocking_lease;
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    started_rx.await.unwrap();
    drop(first);
    assert!(work.try_acquire(&"third", "id-3").is_err());
    let error = retired.borrow().clone().expect("quota error is latched");
    assert_eq!(
        error.data,
        Some(serde_json::json!({"limit":"ACP_SPAWNED_WORK","items":2,"bytes":4096}))
    );
    assert!(work.try_acquire(&"fourth", "id-4").is_err());
    assert_eq!(
        retired.borrow().as_ref(),
        Some(&error),
        "first quota error remains authoritative"
    );
    drop(second);
    assert!(
        work.try_acquire(&"fifth", "id-5").is_err(),
        "blocking clone retains the first credit"
    );
    release_tx.send(()).unwrap();
    blocking.await.unwrap();
    assert_eq!(work.usage.lock().unwrap().items, 0);
}

fn test_route(events_tx: mpsc::Sender<BridgeStreamItem>) -> Arc<EventRoute> {
    Arc::new(EventRoute {
        events_tx,
        turn: Arc::new(TurnState::new()),
        state: Mutex::new(RouteState::default()),
    })
}

#[tokio::test]
async fn mailbox_limits_fail_closed_without_poisoning_later_turns() {
    use agent_client_protocol::schema::v1::{CurrentModeUpdate, SessionUpdate};

    let (mailbox, rx) = EventMailbox::with_limits(1, 1024);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    events_tx
        .send(BridgeStreamItem::SessionInit {
            modes: None,
            models: None,
            config_options: None,
        })
        .await
        .unwrap();
    let route = test_route(events_tx.clone());
    let driver = tokio::spawn(run_event_mailbox(rx, Arc::new(AtomicBool::new(false))));
    let pending_permissions: PendingPermissions = Arc::new(DashMap::new());
    let (permission_tx, permission_rx) = oneshot::channel();
    assert!(route.turn.register_pending(
        &pending_permissions,
        "mailbox-permission".into(),
        crate::acp::PendingPermission::new(
            permission_tx,
            std::collections::HashSet::new(),
            route.turn.clone(),
            route.turn.reserve_permission(16).unwrap(),
        ),
    ));
    let first = BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
        "prefix",
    )));
    mailbox.enqueue_data(&route, first).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while mailbox.tx.capacity() != EVENT_CHANNEL_CAPACITY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("driver dequeued the event and is blocked on the full turn channel");
    assert_eq!(
        mailbox.budget.lock().unwrap().items,
        1,
        "driver in-flight event remains charged"
    );
    assert!(matches!(
        mailbox.enqueue_data(
            &route,
            BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "overflow"
            ),)),
        ),
        Err(EventLimit::Count)
    ));
    route.turn.cancel_and_drain(&pending_permissions);
    assert!(matches!(permission_rx.await, Ok(PermissionDecision::Deny)));
    let terminal_ack = mailbox.enqueue_terminal(
        &route,
        BridgeStreamItem::RunError {
            message: "event item limit".into(),
        },
    );
    assert!(matches!(
        events_rx.recv().await,
        Some(BridgeStreamItem::SessionInit { .. })
    ));
    assert!(matches!(
        events_rx.recv().await,
        Some(BridgeStreamItem::Update(_))
    ));
    assert!(matches!(
        events_rx.recv().await,
        Some(BridgeStreamItem::RunError { .. })
    ));
    assert!(
        events_rx.try_recv().is_err(),
        "one terminal only; no Finished follows"
    );
    assert_eq!(terminal_ack.await.unwrap(), Ok(()));
    drop(mailbox);
    driver.await.unwrap().unwrap();

    let (mailbox, _rx) = EventMailbox::with_limits(8, 8);
    let (events_tx, _events_rx) = mpsc::channel(1);
    let route = test_route(events_tx);
    assert!(matches!(
        mailbox.enqueue_data(
            &route,
            BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "payload too large"
            ),)),
        ),
        Err(EventLimit::Bytes)
    ));

    let (mailbox, rx) = EventMailbox::with_limits(1, 1024);
    let (events_tx, _events_rx) = mpsc::channel(1);
    let route = test_route(events_tx.clone());
    events_tx
        .send(BridgeStreamItem::SessionInit {
            modes: None,
            models: None,
            config_options: None,
        })
        .await
        .unwrap();
    let unusable = Arc::new(AtomicBool::new(false));
    let driver = tokio::spawn(run_event_mailbox(rx, unusable.clone()));
    mailbox
        .enqueue_data(
            &route,
            BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "accepted",
            ))),
        )
        .unwrap();
    assert!(
        mailbox
            .enqueue_data(
                &route,
                BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                    "overflow"
                ),)),
            )
            .is_err()
    );
    let terminal_ack = mailbox.enqueue_terminal(
        &route,
        BridgeStreamItem::RunError {
            message: "event item limit".into(),
        },
    );
    assert_eq!(
        tokio::time::timeout(
            FAILED_EVENT_DELIVERY_TIMEOUT + Duration::from_secs(1),
            terminal_ack
        )
        .await
        .expect("failed route must retire within its independent deadline")
        .unwrap(),
        Err(())
    );
    assert!(unusable.load(Ordering::Acquire));
    drop(mailbox);
    driver.await.unwrap().unwrap();

    // An ordinary dead-turn receiver is item-local: later route events
    // remain deliverable on the same driver.
    let (mailbox, rx) = EventMailbox::with_limits(4, 1024);
    let driver = tokio::spawn(run_event_mailbox(rx, Arc::new(AtomicBool::new(false))));
    let (dead_tx, dead_rx) = mpsc::channel(1);
    drop(dead_rx);
    mailbox
        .enqueue_data(
            &test_route(dead_tx),
            BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "dead",
            ))),
        )
        .unwrap();
    let (healthy_tx, mut healthy_rx) = mpsc::channel(1);
    mailbox
        .enqueue_data(
            &test_route(healthy_tx),
            BridgeStreamItem::Update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "healthy",
            ))),
        )
        .unwrap();
    assert!(matches!(
        healthy_rx.recv().await,
        Some(BridgeStreamItem::Update(_))
    ));
    drop(mailbox);
    driver.await.unwrap().unwrap();
}
