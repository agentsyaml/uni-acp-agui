//! Smoke + performance / stability suite for the AG-UI ↔ ACP bridge.
//!
//! These tests complement the behavioural coverage in `bridge_mock_agent.rs`
//! and `http_sse_roundtrip.rs`. They focus on the stability properties that
//! matter most in production:
//!
//!  * **Startup latency** — `open_session` (the ACP handshake) must complete
//!    quickly for the in-process path, and `open_session_timeout` must bound
//!    a slow handshake instead of hanging.
//!  * **Session lifecycle** — distinct `thread_id`s create distinct sessions;
//!    reuse keeps the count flat; the idle reaper drains everything back to
//!    zero; a dead session is evicted and transparently rebuilt.
//!  * **Streaming integrity** — under a high-volume single turn the bridge
//!    preserves chunk ordering and drops none, and always frames the run with
//!    exactly one `RUN_STARTED` … `RUN_FINISHED` pair.
//!  * **Throughput / no-leak under load** — many sequential and concurrent
//!    runs complete cleanly and leave the session map bounded.
//!
//! All timing assertions use generous upper bounds so the suite stays robust
//! on slow CI runners; they are sanity ceilings, not micro-benchmarks.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use agui_acp_bridge_core::{BridgeConfig, SessionConfig};
use agui_acp_bridge_policy::AutoAllow;
use agui_acp_bridge_server::{
    AcpClient, BridgeAppState, CustomAgentInProcessClient, InProcessAcpClient, build_router,
    test_agents,
};
use axum::http::StatusCode;

use support::{collect_sse_body, count_events, extract_event_types, state_with_client, user_input};

fn client_for<F, Fut>(factory: F) -> Arc<dyn AcpClient>
where
    F: Fn(tokio::io::DuplexStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), agui_acp_bridge_server::BridgeError>>
        + Send
        + 'static,
{
    Arc::new(CustomAgentInProcessClient::new(factory))
}

struct DropFlag(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn test_session_config() -> SessionConfig {
    SessionConfig {
        cwd: PathBuf::from("/"),
        policy: Arc::new(AutoAllow),
        config: BridgeConfig::default(),
        mcp_url: None,
        mcp_headers: Vec::new(),
        load_session_id: None,
    }
}

// ---------------------------------------------------------------------------
// Startup latency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn startup_in_process_open_session_is_fast() {
    // The in-process handshake (initialize + session/new over an in-memory
    // duplex) involves no process spawn, so it should complete in low
    // milliseconds. We assert a generous 2s ceiling: the point is to catch a
    // regression that turns the handshake into seconds (e.g. an accidental
    // blocking call or a lost wakeup), not to benchmark.
    let client = InProcessAcpClient::new();

    let started = Instant::now();
    let handle = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_session(test_session_config()),
    )
    .await
    .expect("open_session deadlocked")
    .expect("open_session failed");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "in-process open_session should be fast, took {elapsed:?}"
    );

    // The handle must be immediately usable: a prompt right after open
    // should stream and finish well within a couple of seconds.
    let prompt_started = Instant::now();
    let mut stream = tokio::time::timeout(Duration::from_secs(5), handle.prompt("ping"))
        .await
        .expect("prompt deadlocked")
        .expect("prompt failed");
    // Drain to completion.
    while let Ok(Some(_item)) =
        tokio::time::timeout(Duration::from_secs(2), stream.events.recv()).await
    {}
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.finished).await;
    assert!(
        prompt_started.elapsed() < Duration::from_secs(3),
        "first prompt after open should complete promptly, took {:?}",
        prompt_started.elapsed()
    );
}

#[tokio::test]
async fn startup_repeated_open_sessions_have_stable_latency() {
    // Open and tear down many independent sessions back to back. This guards
    // against a per-open resource leak that would make later opens slower (or
    // fail outright). We assert every open stays under a generous ceiling.
    let client = InProcessAcpClient::new();

    for i in 0..32 {
        let started = Instant::now();
        let handle = tokio::time::timeout(
            Duration::from_secs(5),
            client.open_session(test_session_config()),
        )
        .await
        .unwrap_or_else(|_| panic!("open_session #{i} deadlocked"))
        .unwrap_or_else(|e| panic!("open_session #{i} failed: {e}"));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "open_session #{i} regressed to {elapsed:?}"
        );
        // Drop the handle immediately; the embedded agent task must wind down
        // on its own without blocking the next open.
        drop(handle);
    }
}

#[tokio::test]
async fn startup_timeout_bounds_a_slow_handshake() {
    // A backend that stalls on `session/new` must not hang the bridge: the
    // configured `open_session_timeout` has to abort the handshake and surface
    // an error to the HTTP layer. We give the agent a 2s handshake delay and a
    // 200ms timeout, then assert the request returns quickly (well under the
    // delay) rather than blocking for the full 2s.
    let agent_dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dropped_for_agent = agent_dropped.clone();
    let client = client_for(move |s| {
        let dropped = dropped_for_agent.clone();
        async move {
            let _drop_flag = DropFlag(dropped);
            test_agents::run_slow_handshake_agent(s, 2_000).await
        }
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            open_session_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();

    let app = build_router(state.clone());
    let body = serde_json::to_vec(&user_input("thread-slow-hs", "run-1", "hi")).unwrap();

    let started = Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        tower::ServiceExt::oneshot(
            app,
            axum::http::Request::post("/")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        ),
    )
    .await
    .expect("router must not hang past the open_session_timeout")
    .expect("router error");
    let elapsed = started.elapsed();

    // handler.rs returns Err(AgUiError) from `session_for` on timeout, which
    // the agui-rs server maps to HTTP 500.
    assert_eq!(
        response.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a timed-out handshake must surface as an error status, not a hung 200"
    );
    assert!(
        elapsed < Duration::from_millis(1_500),
        "must fail fast on the configured timeout (200ms), took {elapsed:?}"
    );
    // No session should have been cached for the failed open.
    assert_eq!(
        state.session_count(),
        0,
        "a failed open_session must not leave a cached entry"
    );
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "a failed session admission must not leave frontend registry state"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while agent_dropped.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed-out handshake actor must be aborted, not detached");
}

#[tokio::test]
async fn capacity_gate_does_not_cover_concurrent_handshakes() {
    let state = BridgeAppState::builder(
        client_for(|s| test_agents::run_slow_handshake_agent(s, 300)),
        PathBuf::from("/"),
    )
    .with_config(BridgeConfig {
        max_sessions: 2,
        open_session_timeout: Duration::from_secs(2),
        ..BridgeConfig::default()
    })
    .build();

    let started = Instant::now();
    let first = {
        let state = state.clone();
        tokio::spawn(async move {
            collect_sse_body(state, user_input("parallel-hs-a", "run", "hi")).await
        })
    };
    let second = {
        let state = state.clone();
        tokio::spawn(async move {
            collect_sse_body(state, user_input("parallel-hs-b", "run", "hi")).await
        })
    };
    let (first_status, _) = first.await.expect("first handshake task");
    let (second_status, _) = second.await.expect("second handshake task");

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "capacity selection must not serialize 300ms handshakes: {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lifecycle_distinct_threads_create_distinct_sessions() {
    // Each unique thread_id must map to its own session; the count must equal
    // the number of distinct ids regardless of run order.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    for i in 0..10 {
        let thread = format!("thread-{i}");
        let (status, body) =
            collect_sse_body(state.clone(), user_input(&thread, "run-1", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    }

    assert_eq!(
        state.session_count(),
        10,
        "ten distinct thread_ids must yield ten cached sessions"
    );
}

#[tokio::test]
async fn lifecycle_reuse_keeps_session_count_flat() {
    // Many runs on a single thread_id must never grow the session map beyond
    // one entry — the core reuse contract that keeps memory bounded for a
    // long-lived conversation.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));

    for i in 0..25 {
        let (status, body) = collect_sse_body(
            state.clone(),
            user_input("thread-reuse", &format!("run-{i}"), "ping"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert_eq!(
            state.session_count(),
            1,
            "session count must stay at 1 across reuse (run {i})"
        );
    }

    // The stateful agent counts turns per SessionId; 25 reuses must have all
    // landed on the same session, so the final turn number is 25.
    let (_, last) = collect_sse_body(
        state.clone(),
        user_input("thread-reuse", "run-final", "last"),
    )
    .await;
    assert!(
        last.contains("turn 26: last"),
        "all reuses must share one session (expected turn 26), body:\n{last}"
    );
}

#[tokio::test]
async fn lifecycle_idle_reaper_drains_many_sessions_to_zero() {
    // Materialize many distinct sessions, then let the reaper drain them all.
    // Guards the reaper's ability to keep the map bounded after a burst of
    // distinct conversations that then go idle.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    for i in 0..12 {
        let thread = format!("idle-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(state.session_count(), 12, "all sessions cached after burst");

    // Reaper interval is min(idle/4, 30s) floored to 1s; 1.5s guarantees at
    // least one sweep with the idle window elapsed.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert_eq!(
        state.session_count(),
        0,
        "idle reaper must drain every idle session back to zero"
    );
}

#[tokio::test]
async fn lifecycle_failing_prompt_does_not_poison_other_threads() {
    // Error isolation: a thread whose agent errors on prompt must surface a
    // RUN_ERROR for that thread only. A different, healthy thread sharing the
    // same bridge must be entirely unaffected and complete cleanly. This
    // guards against one bad conversation taking down the whole gateway.
    let failing = state_with_client(client_for(test_agents::run_failing_prompt_agent));
    let (fstatus, fbody) = collect_sse_body(
        failing.clone(),
        user_input("thread-bad", "run-1", "explode"),
    )
    .await;
    assert_eq!(fstatus, StatusCode::OK);
    assert!(
        fbody.contains("\"type\":\"RUN_ERROR\""),
        "the failing thread must surface RUN_ERROR, body:\n{fbody}"
    );

    // Re-running the same failing thread must keep failing cleanly (no panic,
    // no hang) — the bridge stays responsive.
    let (fstatus2, fbody2) =
        collect_sse_body(failing.clone(), user_input("thread-bad", "run-2", "again")).await;
    assert_eq!(fstatus2, StatusCode::OK);
    assert!(
        fbody2.contains("\"type\":\"RUN_ERROR\""),
        "a repeat run on the failing thread must still error cleanly, body:\n{fbody2}"
    );

    // A separate bridge with a healthy agent is unaffected — error handling on
    // one conversation does not leak global state.
    let healthy = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (hstatus, hbody) =
        collect_sse_body(healthy, user_input("thread-good", "run-1", "hi")).await;
    assert_eq!(hstatus, StatusCode::OK);
    assert!(
        hbody.contains("\"type\":\"RUN_FINISHED\""),
        "a healthy thread must finish cleanly regardless of other failures, body:\n{hbody}"
    );
}

// ---------------------------------------------------------------------------
// Streaming integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn streaming_preserves_order_and_loses_no_chunks_under_load() {
    // A single turn that streams 500 sequentially-numbered chunks as fast as
    // the agent can push them. The bridge must deliver all of them, in order,
    // with exactly one run frame.
    const N: u32 = 500;

    let state = state_with_client(client_for(|s| test_agents::run_counting_agent(s, N)));
    let (status, body) = collect_sse_body(state, user_input("thread-count", "run-1", "go")).await;

    assert_eq!(status, StatusCode::OK, "body:\n{body}");

    // Exactly one run frame.
    assert_eq!(count_events(&body, "RUN_STARTED"), 1, "one RUN_STARTED");
    assert_eq!(count_events(&body, "RUN_FINISHED"), 1, "one RUN_FINISHED");
    assert_eq!(count_events(&body, "RUN_ERROR"), 0, "no RUN_ERROR");

    // Reconstruct the delivered ordinal sequence from the TEXT_MESSAGE_CONTENT
    // deltas. Each delta carries one "<i>;" token; concatenating all deltas in
    // arrival order and splitting on ';' yields the sequence the client saw.
    let mut seq: Vec<u32> = Vec::new();
    for line in body.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim_start();
        if !payload.contains("\"type\":\"TEXT_MESSAGE_CONTENT\"") {
            continue;
        }
        let v: serde_json::Value = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(delta) = v.get("delta").and_then(serde_json::Value::as_str) {
            for tok in delta.split(';').filter(|t| !t.is_empty()) {
                if let Ok(n) = tok.parse::<u32>() {
                    seq.push(n);
                }
            }
        }
    }

    assert_eq!(
        seq.len(),
        N as usize,
        "every chunk must be delivered exactly once (got {} of {N})",
        seq.len()
    );
    let expected: Vec<u32> = (0..N).collect();
    assert_eq!(
        seq, expected,
        "chunks must arrive strictly in order with none dropped or reordered"
    );

    // AG-UI text-message invariant: exactly one START / one END around the
    // single streamed message.
    assert_eq!(count_events(&body, "TEXT_MESSAGE_START"), 1, "one START");
    assert_eq!(count_events(&body, "TEXT_MESSAGE_END"), 1, "one END");
}

#[tokio::test]
async fn streaming_run_frame_is_always_well_formed() {
    // Across a variety of agent behaviours, every successful run must lead
    // with RUN_STARTED and end with RUN_FINISHED, and the START must precede
    // any text. This pins the framing invariant the frontend relies on.
    let state = state_with_client(client_for(test_agents::run_mixed_updates_agent));
    let (status, body) = collect_sse_body(state, user_input("thread-frame", "run-1", "go")).await;
    assert_eq!(status, StatusCode::OK);

    let types = extract_event_types(&body);
    assert_eq!(
        types.first().map(String::as_str),
        Some("RUN_STARTED"),
        "must lead with RUN_STARTED, got: {types:?}"
    );
    assert_eq!(
        types.last().map(String::as_str),
        Some("RUN_FINISHED"),
        "must end with RUN_FINISHED, got: {types:?}"
    );
    if let (Some(start), Some(text)) = (
        types.iter().position(|t| t == "RUN_STARTED"),
        types.iter().position(|t| t == "TEXT_MESSAGE_START"),
    ) {
        assert!(
            start < text,
            "RUN_STARTED must precede text, got: {types:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Throughput / no-leak under load
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_many_sequential_runs_complete_and_stay_bounded() {
    // A soak-style loop: 50 sequential runs on one thread. Every run must
    // finish cleanly and the session map must never exceed one entry.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    let started = Instant::now();
    for i in 0..50 {
        let (status, body) = collect_sse_body(
            state.clone(),
            user_input("thread-soak", &format!("run-{i}"), "ping"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "run {i} failed, body:\n{body}");
        assert!(
            body.contains("\"type\":\"RUN_FINISHED\""),
            "run {i} must finish, body:\n{body}"
        );
        assert_eq!(state.session_count(), 1, "session map must stay bounded");
    }

    assert!(
        started.elapsed() < Duration::from_secs(20),
        "50 in-process runs should be quick, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn load_many_concurrent_distinct_threads_all_finish() {
    // Fan out 32 concurrent runs across 32 distinct thread_ids. All must
    // finish cleanly, and the resulting session count must equal the number
    // of distinct threads (no lost or duplicated sessions under concurrency).
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    let mut handles = Vec::new();
    for i in 0..32 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            let thread = format!("conc-{i}");
            let (status, body) = collect_sse_body(s, user_input(&thread, "run-1", "ping")).await;
            assert_eq!(status, StatusCode::OK);
            assert!(
                body.contains("\"type\":\"RUN_FINISHED\""),
                "thread {i} must finish, body:\n{body}"
            );
        }));
    }
    for h in handles {
        h.await.expect("task panicked");
    }

    assert_eq!(
        state.session_count(),
        32,
        "32 distinct concurrent threads must yield 32 sessions"
    );
}

#[tokio::test]
async fn load_concurrent_reuse_on_one_thread_uses_admission_gate() {
    // Fan out 16 concurrent runs on a SINGLE thread_id against a stateful
    // agent. The per-thread admission gate allows exactly one run through;
    // the others fail fast rather than queueing behind it.
    let state = state_with_client(client_for(|stream| async move {
        // Keep the first session opening long enough for the batch to contend
        // on the admission claim instead of relying on scheduler timing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        test_agents::run_stateful_session_agent(stream).await
    }));

    let mut handles = Vec::new();
    for i in 0..16 {
        let s = state.clone();
        handles.push(tokio::spawn(async move {
            let (status, body) =
                collect_sse_body(s, user_input("thread-serial", &format!("run-{i}"), "ping")).await;
            (i, status, body)
        }));
    }

    let mut finished = 0;
    let mut concurrent_errors = 0;
    for h in handles {
        let (i, status, body) = h.await.expect("task panicked");
        assert_eq!(status, StatusCode::OK, "run-{i} body:\n{body}");
        assert_eq!(count_events(&body, "RUN_STARTED"), 1, "run-{i}: {body}");
        assert!(
            body.contains(&format!("\"runId\":\"run-{i}\"")),
            "response must identify run-{i}:\n{body}"
        );

        let events = extract_event_types(&body);
        assert_eq!(
            events.first().map(String::as_str),
            Some("RUN_STARTED"),
            "run-{i} must start with RUN_STARTED: {body}"
        );
        assert_eq!(
            count_events(&body, "RUN_FINISHED") + count_events(&body, "RUN_ERROR"),
            1,
            "run-{i} must have exactly one terminal event: {body}"
        );

        if count_events(&body, "RUN_FINISHED") == 1 {
            finished += 1;
            assert_eq!(events.last().map(String::as_str), Some("RUN_FINISHED"));
            assert!(body.contains("turn 1: ping"), "winning run body:\n{body}");
            assert!(!body.contains("CONCURRENT_RUN"), "run-{i} body:\n{body}");
        } else {
            concurrent_errors += 1;
            assert_eq!(events.last().map(String::as_str), Some("RUN_ERROR"));
            assert!(
                body.contains("\"code\":\"CONCURRENT_RUN\""),
                "run-{i} must fail at admission: {body}"
            );
            assert!(!body.contains("RUN_FINISHED"), "run-{i} body:\n{body}");
        }
    }

    assert_eq!(finished, 1, "exactly one run must win admission");
    assert_eq!(concurrent_errors, 15, "fifteen runs must be rejected");

    assert_eq!(
        state.session_count(),
        1,
        "concurrent runs on one thread must share exactly one session"
    );

    // One more run must claim the now-free thread and reuse the one session.
    let (status, body) =
        collect_sse_body(state, user_input("thread-serial", "run-final", "last")).await;
    assert_eq!(status, StatusCode::OK, "sequential follow-up body:\n{body}");
    assert!(
        body.contains("\"runId\":\"run-final\"")
            && body.contains("\"type\":\"RUN_FINISHED\"")
            && body.contains("turn 2: last"),
        "claim must be released and the session reused:\n{body}"
    );
    assert!(
        !body.contains("CONCURRENT_RUN"),
        "follow-up must not be rejected:\n{body}"
    );
}

// ---------------------------------------------------------------------------
// Real-socket (TCP + SSE) performance & stability
//
// The helpers above use `tower::oneshot`, which buffers the whole body and
// never exercises socket-level streaming or backpressure. The tests below
// bind a real `127.0.0.1` listener and drive it with `reqwest` so we cover
// the production transport: time-to-first-event, incremental delivery, and
// graceful completion.
// ---------------------------------------------------------------------------

use tokio::net::TcpListener;

/// Spawn the bridge on an ephemeral `127.0.0.1` port. Returns the bound
/// address and the server task handle (abort it to shut down).
async fn spawn_bridge(
    state: BridgeAppState,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = build_router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, server)
}

#[tokio::test]
async fn realsocket_time_to_first_event_is_low() {
    // End-to-end startup latency over a real socket: from issuing the POST to
    // receiving the first SSE byte (RUN_STARTED). For the in-process agent this
    // covers HTTP parse → session_for (open_session handshake) → first event.
    // We assert a generous 2s ceiling to catch a handshake-on-the-hot-path
    // regression without being flaky on slow CI.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));
    let (addr, server) = spawn_bridge(state).await;

    let client = reqwest::Client::new();
    let input = user_input("thread-ttfb", "run-1", "ping");

    let started = Instant::now();
    let resp = client
        .post(format!("http://{addr}/"))
        .header("accept", "text/event-stream")
        .json(&input)
        .send()
        .await
        .expect("request failed");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Pull the first chunk of bytes off the stream.
    let mut stream = resp.bytes_stream();
    use futures::StreamExt;
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for first SSE event")
        .expect("stream ended before any event")
        .expect("stream error");
    let ttfb = started.elapsed();

    let text = String::from_utf8_lossy(&first);
    assert!(
        text.contains("RUN_STARTED") || text.contains("data:"),
        "first frame should carry SSE data, got: {text}"
    );
    assert!(
        ttfb < Duration::from_secs(2),
        "time-to-first-event regressed to {ttfb:?}"
    );

    server.abort();
}

#[tokio::test]
async fn realsocket_high_volume_stream_delivers_all_chunks_in_order() {
    // Stream 500 ordered chunks over a real socket and reassemble them from
    // the wire. Verifies that socket-level chunking / backpressure does not
    // drop, duplicate, or reorder events, and that the run terminates with
    // RUN_FINISHED.
    const N: u32 = 500;
    let state = state_with_client(client_for(|s| test_agents::run_counting_agent(s, N)));
    let (addr, server) = spawn_bridge(state).await;

    let client = reqwest::Client::new();
    let input = user_input("thread-rt-count", "run-1", "go");
    let resp = client
        .post(format!("http://{addr}/"))
        .header("accept", "text/event-stream")
        .json(&input)
        .send()
        .await
        .expect("request failed");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut body = String::new();
    while let Ok(Some(chunk)) = tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        let chunk = chunk.expect("stream error");
        body.push_str(&String::from_utf8_lossy(&chunk));
        if body.contains("\"type\":\"RUN_FINISHED\"") {
            break;
        }
    }

    assert_eq!(count_events(&body, "RUN_STARTED"), 1, "one RUN_STARTED");
    assert_eq!(count_events(&body, "RUN_FINISHED"), 1, "one RUN_FINISHED");

    // Reassemble the ordinal sequence from TEXT_MESSAGE_CONTENT deltas.
    let mut seq: Vec<u32> = Vec::new();
    for line in body.lines() {
        let Some(payload) = line.strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim_start();
        if !payload.contains("\"type\":\"TEXT_MESSAGE_CONTENT\"") {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(payload)
            && let Some(delta) = v.get("delta").and_then(serde_json::Value::as_str)
        {
            for tok in delta.split(';').filter(|t| !t.is_empty()) {
                if let Ok(n) = tok.parse::<u32>() {
                    seq.push(n);
                }
            }
        }
    }

    let expected: Vec<u32> = (0..N).collect();
    assert_eq!(
        seq,
        expected,
        "all {N} chunks must arrive in order over a real socket (got {})",
        seq.len()
    );

    server.abort();
}

#[tokio::test]
async fn realsocket_back_to_back_runs_reuse_session_over_http() {
    // Two sequential HTTP runs on the same thread_id over a real socket must
    // reuse one session (no per-run open_session cost) and both finish
    // cleanly. Pins the reuse contract on the real transport.
    let state = state_with_client(client_for(test_agents::run_stateful_session_agent));
    let (addr, server) = spawn_bridge(state.clone()).await;

    let client = reqwest::Client::new();

    for run in 1..=2u32 {
        let input = user_input("thread-http-reuse", &format!("run-{run}"), "ping");
        let resp = client
            .post(format!("http://{addr}/"))
            .header("accept", "text/event-stream")
            .json(&input)
            .send()
            .await
            .expect("request failed");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body = resp.text().await.expect("body");
        assert!(
            body.contains(&format!("turn {run}: ping")),
            "run {run} must advance the shared session, body:\n{body}"
        );
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
    }

    assert_eq!(
        state.session_count(),
        1,
        "back-to-back HTTP runs on one thread must reuse a single session"
    );

    server.abort();
}

// ---------------------------------------------------------------------------
// Session-cap & registry-cleanup regressions
//
// These pin the two production bugs reported for the multi-session case:
//  1. The idle reaper must also drop the matching frontend-tools registry
//     entry, or those entries leak for the life of the process.
//  2. `max_sessions` must bound the cached-session count by evicting the LRU
//     idle session, so a browser that mints a fresh thread on every refresh
//     can't accumulate unbounded agent subprocesses.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reaper_also_drops_frontend_tools_registry_entry() {
    // Drive a run that registers the thread in the frontend-tools registry,
    // then let the idle reaper drop the session. The registry entry must be
    // dropped too — otherwise it leaks.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    let (status, _) =
        collect_sse_body(state.clone(), user_input("thread-reg", "run-1", "hi")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state.session_count(), 1, "session cached after run");
    assert_eq!(
        state.frontend_tools().thread_count(),
        1,
        "registry entry created for the thread"
    );

    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert_eq!(state.session_count(), 0, "reaper drops the session");
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "reaper must also drop the frontend-tools registry entry (no leak)"
    );
}

#[tokio::test]
async fn max_sessions_evicts_lru_idle_session() {
    // With max_sessions = 3, running 6 distinct idle threads in sequence must
    // never let the cached count exceed 3: each new session past the cap
    // evicts the least-recently-used idle one. This models a browser that
    // mints a fresh threadId on every refresh and proves sessions can't grow
    // without bound.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 3,
            ..BridgeConfig::default()
        })
        .build();

    for i in 0..6 {
        let thread = format!("cap-{i}");
        let (status, body) =
            collect_sse_body(state.clone(), user_input(&thread, "run-1", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert!(
            state.session_count() <= 3,
            "cached session count {} must never exceed the cap of 3 (after thread {i})",
            state.session_count()
        );
    }

    assert_eq!(
        state.session_count(),
        3,
        "exactly the cap's worth of sessions should remain"
    );
    // The registry must track the cap too — evicted threads' entries are
    // dropped, so the registry never exceeds the cap either.
    assert!(
        state.frontend_tools().thread_count() <= 3,
        "registry entries must be dropped alongside evicted sessions, got {}",
        state.frontend_tools().thread_count()
    );
}

#[tokio::test]
async fn max_sessions_zero_means_unlimited() {
    // Zero is an explicit development override: no eviction.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 0,
            ..BridgeConfig::default()
        })
        .build();

    for i in 0..8 {
        let thread = format!("unl-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
    }

    assert_eq!(
        state.session_count(),
        8,
        "with max_sessions=0 all distinct threads stay cached"
    );
}

#[tokio::test]
async fn max_sessions_does_not_evict_busy_sessions() {
    // A session with an in-flight (slow) prompt must NOT be evicted even when
    // the cap is reached — live work is never killed. We set cap = 1, start a
    // slow run on thread A, and while it is in flight start a run on thread B.
    // B must be rejected before a second ACP actor is opened.
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_agent = opens.clone();
    let client = client_for(move |s| {
        opens_for_agent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_slow_prompt_agent(s, 400)
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 1,
            ..BridgeConfig::default()
        })
        .build();

    let s1 = state.clone();
    let a =
        tokio::spawn(
            async move { collect_sse_body(s1, user_input("busy-A", "run-A", "wait")).await },
        );
    // Give A time to enter its prompt (active_prompts > 0).
    tokio::time::sleep(Duration::from_millis(100)).await;

    let s2 = state.clone();
    let b =
        tokio::spawn(
            async move { collect_sse_body(s2, user_input("busy-B", "run-B", "wait")).await },
        );

    let (sa, ba) = a.await.unwrap();
    let (sb, bb) = b.await.unwrap();
    assert_eq!(sa, StatusCode::OK);
    assert_eq!(sb, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        ba.contains("\"type\":\"RUN_FINISHED\""),
        "busy session A must complete cleanly, not be evicted mid-flight:\n{ba}"
    );
    assert!(
        bb.contains("session capacity reached"),
        "session B must explain the capacity rejection:\n{bb}"
    );
    assert!(
        bb.contains("ACP_SESSION_CAPACITY"),
        "session B must expose a non-success capacity error code:\n{bb}"
    );
    assert_eq!(
        opens.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "capacity rejection must not open a second ACP actor"
    );
    assert!(
        !state.frontend_tools().has("busy-B"),
        "capacity rejection must not leave a speculative frontend registry entry"
    );
}

#[tokio::test]
async fn max_sessions_hard_cap_bounds_concurrent_first_use() {
    let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opens_for_agent = opens.clone();
    let client = client_for(move |s| {
        opens_for_agent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        test_agents::run_slow_prompt_agent(s, 400)
    });
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 2,
            ..BridgeConfig::default()
        })
        .build();

    let mut tasks = Vec::new();
    for i in 0..8 {
        let state = state.clone();
        tasks.push(tokio::spawn(async move {
            collect_sse_body(
                state,
                user_input(&format!("cap-concurrent-{i}"), "run", "wait"),
            )
            .await
        }));
    }

    let mut rejected = 0;
    for task in tasks {
        let (status, _) = task.await.expect("concurrent request task");
        if status == StatusCode::INTERNAL_SERVER_ERROR {
            rejected += 1;
        }
    }
    assert!(
        rejected > 0,
        "the hard cap must reject excess busy sessions"
    );
    assert!(state.session_count() <= 2);
    assert_eq!(
        opens.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "concurrent first-use must open at most the configured cap"
    );
}

#[tokio::test]
async fn stable_thread_id_across_many_runs_stays_one_session() {
    // Models the *fixed* frontend behaviour: the browser pins one threadId
    // and reuses it across every reload/run. The bridge must keep exactly one
    // cached session no matter how many runs arrive — this is the property the
    // demo's persisted-threadId change relies on.
    let state = state_with_client(client_for(test_agents::run_single_chunk_agent));

    for i in 0..40 {
        let (status, body) = collect_sse_body(
            state.clone(),
            user_input("pinned-thread", &format!("run-{i}"), "hi"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"type\":\"RUN_FINISHED\""));
        assert_eq!(
            state.session_count(),
            1,
            "a pinned threadId must never create more than one session (run {i})"
        );
    }
}

#[tokio::test]
async fn churned_thread_ids_are_bounded_by_cap_and_reaper() {
    // Models the *unfixed* / worst-case frontend behaviour: a fresh threadId
    // on every run (e.g. a client that doesn't pin one). Even then the bridge
    // must not grow without bound — the cap holds the count, and once runs go
    // idle the reaper drains them. This is the server-side safety net behind
    // the client fix.
    let client = client_for(test_agents::run_single_chunk_agent);
    let state = BridgeAppState::builder(client, PathBuf::from("/"))
        .with_config(BridgeConfig {
            max_sessions: 8,
            idle_timeout: Duration::from_millis(200),
            ..BridgeConfig::default()
        })
        .build();
    state.spawn_reaper();

    // 40 distinct threads (like 40 refreshes), each a quick completed run.
    for i in 0..40 {
        let thread = format!("churn-{i}");
        let (status, _) = collect_sse_body(state.clone(), user_input(&thread, "r", "hi")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            state.session_count() <= 8,
            "cap must bound churned sessions, saw {} after thread {i}",
            state.session_count()
        );
    }

    // After everything goes idle, the reaper drains the survivors to zero.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        state.session_count(),
        0,
        "reaper must drain all idle churned sessions"
    );
    assert_eq!(
        state.frontend_tools().thread_count(),
        0,
        "registry must be drained alongside the sessions"
    );
}
