use super::*;

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
