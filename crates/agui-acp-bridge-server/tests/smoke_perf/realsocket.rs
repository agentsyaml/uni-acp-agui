use super::*;

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
