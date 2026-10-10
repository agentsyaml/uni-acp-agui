use super::*;

fn limits(frame: usize, bytes: usize, entries: usize) -> Limits {
    Limits {
        frame,
        bytes,
        entries,
    }
}

#[tokio::test]
async fn rejects_frame_and_queue_byte_overflow_without_consuming_more() {
    let (mut writer, reader) = tokio::io::duplex(128);
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"x\"}\n")
        .await
        .unwrap();
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"x\"}\n")
        .await
        .unwrap();
    drop(writer);
    let (tx, _rx) = futures::channel::mpsc::unbounded();
    let error = read_frames_with_limits(reader, tx, limits(64, 40, 10))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("queued wire bytes"));

    let (mut writer, reader) = tokio::io::duplex(128);
    writer.write_all(b"123456789").await.unwrap();
    drop(writer);
    let (tx, _rx) = futures::channel::mpsc::unbounded();
    let error = read_frames_with_limits(reader, tx, limits(8, 128, 10))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("frame bytes"));
}

#[tokio::test]
async fn batch_entries_are_charged_and_dequeue_releases_credit() {
    let (mut writer, reader) = tokio::io::duplex(256);
    writer
        .write_all(
            b"[{\"jsonrpc\":\"2.0\",\"method\":\"a\"},{\"jsonrpc\":\"2.0\",\"method\":\"b\"}]\n",
        )
        .await
        .unwrap();
    drop(writer);
    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let error = read_frames_with_limits(reader, tx, limits(256, 256, 1))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("queued entries"));
    assert!(rx.next().await.is_none());

    let (mut writer, reader) = tokio::io::duplex(256);
    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let read = tokio::spawn(read_frames_with_limits(reader, tx, limits(128, 128, 1)));
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"a\"}\n")
        .await
        .unwrap();
    assert!(rx.next().await.is_some());
    writer
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"b\"}\n")
        .await
        .unwrap();
    assert!(rx.next().await.is_some());
    drop(writer);
    read.await.unwrap().unwrap();
}

#[tokio::test]
async fn output_is_sdk_serialized_and_final_unterminated_input_is_preserved() {
    let (tx, mut rx) = futures::channel::mpsc::unbounded();
    let (physical_out, mut physical_in) = tokio::io::duplex(512);
    let output = write_frames(physical_out, &mut rx);
    tx.unbounded_send(TransportFrame::parse_json(
        "{\"jsonrpc\":\"2.0\",\"method\":\"out\"}",
    ))
    .unwrap();
    drop(tx);
    output.await.unwrap();
    let mut output_bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    physical_in.read_to_end(&mut output_bytes).await.unwrap();
    assert_eq!(output_bytes.last(), Some(&b'\n'));

    let (mut input, reader) = tokio::io::duplex(128);
    let (tx, mut frames) = futures::channel::mpsc::unbounded();
    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"tail\"}")
        .await
        .unwrap();
    drop(input);
    read_frames(reader, tx).await.unwrap();
    assert!(matches!(
        frames.next().await,
        Some(TransportFrame::Single(_))
    ));
    assert!(frames.next().await.is_none());
}
