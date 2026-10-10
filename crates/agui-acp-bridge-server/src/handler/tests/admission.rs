use super::*;

fn test_cwd() -> PathBuf {
    std::env::current_dir().expect("test working directory is absolute")
}

#[test]
fn run_admission_releases_only_its_own_claim() {
    let state = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    let first = state
        .try_claim_run("thread", "run-1")
        .expect("first run claims thread");
    assert!(state.try_claim_run("thread", "run-2").is_none());
    drop(first);
    assert!(state.try_claim_run("thread", "run-2").is_some());
}

struct SessionCreationRaceClient {
    opens: Arc<std::sync::atomic::AtomicUsize>,
    first_started: Arc<tokio::sync::Notify>,
    release_first: Arc<tokio::sync::Semaphore>,
    follower_started: Arc<tokio::sync::Notify>,
    third_started: Arc<tokio::sync::Notify>,
    release_followers: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl AcpClient for SessionCreationRaceClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        let call = self.opens.fetch_add(1, Ordering::SeqCst);
        match call {
            0 => {
                self.first_started.notify_one();
                self.release_first
                    .acquire()
                    .await
                    .expect("first release semaphore is live")
                    .forget();
                Err(BridgeError::SessionClosed)
            }
            1 => {
                self.follower_started.notify_one();
                self.release_followers
                    .acquire()
                    .await
                    .expect("follower release semaphore is live")
                    .forget();
                InProcessAcpClient::new().open_session(cfg).await
            }
            _ => {
                self.third_started.notify_one();
                self.release_followers
                    .acquire()
                    .await
                    .expect("follower release semaphore is live")
                    .forget();
                InProcessAcpClient::new().open_session(cfg).await
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_creation_gate_survives_failure_with_queued_waiters() {
    let client = Arc::new(SessionCreationRaceClient {
        opens: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        first_started: Arc::new(tokio::sync::Notify::new()),
        release_first: Arc::new(tokio::sync::Semaphore::new(0)),
        follower_started: Arc::new(tokio::sync::Notify::new()),
        third_started: Arc::new(tokio::sync::Notify::new()),
        release_followers: Arc::new(tokio::sync::Semaphore::new(0)),
    });
    let state = BridgeAppState::new(client.clone(), PathBuf::from("/"));

    let first = {
        let state = state.clone();
        tokio::spawn(async move { state.session_for("creation-race").await })
    };
    tokio::time::timeout(Duration::from_secs(1), client.first_started.notified())
        .await
        .expect("first open_session must start");

    let queued = {
        let state = state.clone();
        tokio::spawn(async move { state.session_for("creation-race").await })
    };

    // The extra Arc proves the second caller retained the same gate before
    // the first creator is allowed to fail.
    let gate = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(gate) = state.inner.create_locks.get("creation-race") {
                let gate = gate.clone();
                if Arc::strong_count(&gate) >= 4 {
                    break gate;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued caller must retain the creation gate");
    drop(gate);

    client.release_first.add_permits(1);
    assert!(first.await.expect("first creator task").is_err());

    tokio::time::timeout(Duration::from_secs(1), client.follower_started.notified())
        .await
        .expect("queued caller must reach the retrying open_session");

    let contender = {
        let state = state.clone();
        tokio::spawn(async move { state.session_for("creation-race").await })
    };

    assert!(
        tokio::time::timeout(Duration::from_millis(250), client.third_started.notified())
            .await
            .is_err(),
        "a new caller must queue behind the old gate, not open a second session"
    );

    client.release_followers.add_permits(2);
    let queued_entry = tokio::time::timeout(Duration::from_secs(2), queued)
        .await
        .expect("queued creation must finish")
        .expect("queued creation task must not panic")
        .expect("queued creation must succeed");
    let contender_entry = tokio::time::timeout(Duration::from_secs(2), contender)
        .await
        .expect("contender creation must finish")
        .expect("contender creation task must not panic")
        .expect("contender creation must reuse the queued session");

    assert_eq!(client.opens.load(Ordering::SeqCst), 2);
    assert!(Arc::ptr_eq(&queued_entry, &contender_entry));
    let cached_entry = state
        .inner
        .sessions
        .get("creation-race")
        .expect("session is cached")
        .clone();
    assert!(Arc::ptr_eq(&cached_entry, &queued_entry));
    assert_eq!(state.session_count(), 1);
    assert!(state.inner.create_locks.is_empty());
}

struct ListCountingClient {
    lists: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl AcpClient for ListCountingClient {
    async fn open_session(&self, cfg: SessionConfig) -> Result<AcpSessionHandle, BridgeError> {
        InProcessAcpClient::new().open_session(cfg).await
    }

    async fn list_sessions(
        &self,
        _cfg: SessionConfig,
    ) -> Result<Vec<agui_acp_bridge_core::SessionSummary>, BridgeError> {
        self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(vec![agui_acp_bridge_core::SessionSummary {
            session_id: "listed-session".into(),
            cwd: test_cwd().to_string_lossy().into_owned(),
            title: None,
            updated_at: None,
        }])
    }
}

/// FIX 2 regression: N concurrent `GET /sessions` must collapse into ONE
/// `session/list` spawn (each spawn forks an agent subprocess).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_session_listings_collapse_into_one_spawn() {
    let client = Arc::new(ListCountingClient {
        lists: std::sync::atomic::AtomicUsize::new(0),
    });
    let state = BridgeAppState::new(client.clone(), PathBuf::from("/"));

    let mut tasks = Vec::new();
    for _ in 0..25 {
        let state = state.clone();
        tasks.push(tokio::spawn(async move { state.list_sessions().await }));
    }
    for task in tasks {
        task.await.expect("list task").expect("listing succeeds");
    }
    assert_eq!(
        client.lists.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "25 concurrent listings must produce exactly one agent query"
    );

    // Errors are NOT cached: an Unsupported agent keeps failing (and the
    // gate still bounds concurrency), so callers see the real error.
    let unsupported = BridgeAppState::new(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"));
    assert!(unsupported.list_sessions().await.is_err());
    assert!(unsupported.list_sessions().await.is_err());
}

/// Resume validation may list before capacity admission, but the listing
/// remains globally single-flight and a rejected request cannot evict a
/// busy session or open an actor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_validates_before_capacity_rejection_without_open_or_eviction() {
    // Client that counts every `session/list`: the resume path uses it.
    let client = Arc::new(ListCountingClient {
        lists: std::sync::atomic::AtomicUsize::new(0),
    });
    let state = BridgeAppState::builder(client.clone(), test_cwd())
        .with_config(BridgeConfig {
            max_sessions: 1,
            ..BridgeConfig::default()
        })
        .build();

    // Fill the single capacity slot out-of-band.
    let handle = Arc::new(
        state
            .inner
            .client
            .open_session(state.session_config_for("filler"))
            .await
            .expect("filler session opens"),
    );
    let permit = state
        .reserve_session_capacity()
        .await
        .expect("capacity is free")
        .expect("one permit available");
    let s1 = Arc::new(SessionEntry::new(handle, Some(permit)));
    state.inner.sessions.insert("filler".into(), s1.clone());
    // Mark the slot busy so LRU eviction cannot free it: only then does a
    // second resume hit the hard capacity wall instead of evicting.
    let _prompt_guard = s1.enter_prompt();

    // The valid marker is checked first; the full busy pool then rejects
    // admission without an open or eviction.
    let error = state
        .session_for_resume(
            "resume-thread",
            Some(SessionId::from("listed-session".to_string())),
        )
        .await
        .expect_err("capacity is exhausted");
    assert!(
        matches!(error, SessionAdmissionError::Capacity(_)),
        "expected capacity rejection, got: {error:?}"
    );
    assert_eq!(
        client.lists.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "resume validation should perform one bounded listing"
    );
    assert_eq!(state.session_count(), 1, "busy session must remain cached");
    assert!(state.inner.sessions.contains_key("filler"));
}

/// #4 regression: when a free permit exists, capacity reservation must
/// NOT evict an idle session — the two-step fast path takes the permit
/// before any LRU victim selection runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_fast_path_never_evicts_an_idle_session() {
    let state = BridgeAppState::builder(Arc::new(InProcessAcpClient::new()), PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 2,
            ..BridgeConfig::default()
        })
        .build();

    // Park one idle (unclaimed) session.
    let handle = state
        .inner
        .client
        .open_session(state.session_config_for("idle-victim"))
        .await
        .expect("idle victim opens");
    state.inner.sessions.insert(
        "idle-victim".into(),
        Arc::new(SessionEntry::new(Arc::new(handle), None)),
    );

    // The pool has a free slot: acquiring it must leave the idle session
    // cached, not evict it to make room.
    let permit = state
        .reserve_session_capacity()
        .await
        .expect("a free permit exists")
        .expect("one permit available");
    drop(permit);
    assert_eq!(state.session_count(), 1, "fast path must not evict");
    assert!(
        state.inner.sessions.contains_key("idle-victim"),
        "an idle cached session must survive a free-permit acquisition"
    );
}
