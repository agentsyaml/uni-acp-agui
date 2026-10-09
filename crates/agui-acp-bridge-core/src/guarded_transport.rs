use std::{
    collections::VecDeque,
    io,
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use agent_client_protocol::{AcpAgent, Channel, Client, ConnectTo, Error, Result, TransportFrame};
use futures::FutureExt;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

const FRAME_LIMIT: usize = 16 * 1024 * 1024;
const QUEUED_BYTE_LIMIT: usize = 16 * 1024 * 1024;
const QUEUED_ENTRY_LIMIT: usize = 4096;
const STDERR_PREFIX_LIMIT: usize = 4096;

#[derive(Clone, Default)]
pub(crate) struct ProcessDiagnostic(Arc<Mutex<ProcessDiagnosticState>>);

#[derive(Default)]
struct ProcessDiagnosticState {
    failure: Option<ExitStatus>,
    stderr_prefix: Vec<u8>,
}

impl ProcessDiagnostic {
    pub(crate) fn snapshot(&self) -> (Option<ExitStatus>, Vec<u8>) {
        let diagnostic = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (diagnostic.failure, diagnostic.stderr_prefix.clone())
    }

    pub(crate) fn observe_failure(&self, status: &ExitStatus) {
        if !status.success() {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure = Some(*status);
        }
    }

    pub(crate) fn append_stderr(&self, bytes: &[u8]) {
        let mut diagnostic = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keep = STDERR_PREFIX_LIMIT
            .saturating_sub(diagnostic.stderr_prefix.len())
            .min(bytes.len());
        diagnostic.stderr_prefix.extend_from_slice(&bytes[..keep]);
    }
}

pub(crate) struct GuardedByteStreams<W, R> {
    writer: W,
    reader: R,
}

impl<W, R> GuardedByteStreams<W, R> {
    pub(crate) fn new(writer: W, reader: R) -> Self {
        Self { writer, reader }
    }
}

impl<W, R> ConnectTo<Client> for GuardedByteStreams<W, R>
where
    W: AsyncWrite + Unpin + Send + 'static,
    R: AsyncRead + Unpin + Send + 'static,
{
    async fn connect_to(self, client: impl ConnectTo<agent_client_protocol::Agent>) -> Result<()> {
        let (endpoint, client_future) = client.into_channel_and_future();
        tokio::try_join!(self.run_io(endpoint), client_future)?;
        Ok(())
    }

    fn into_channel_and_future(self) -> (Channel, futures::future::BoxFuture<'static, Result<()>>) {
        let (sdk_endpoint, io_endpoint) = Channel::duplex();
        (sdk_endpoint, self.run_io(io_endpoint).boxed())
    }
}

impl<W, R> GuardedByteStreams<W, R>
where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    async fn run_io(self, channel: Channel) -> Result<()> {
        // The reader owns the only input sender: EOF must close the SDK
        // input before the outgoing writer can finish draining.
        let Channel { tx, mut rx } = channel;
        let mut reader = Box::pin(read_frames(self.reader, tx));
        let mut writer = Box::pin(write_frames(self.writer, &mut rx));
        tokio::select! {
            result = &mut reader => {
                result?;
                tokio::time::timeout(std::time::Duration::from_millis(250), &mut writer).await
                    .map_err(|_| Error::into_internal_error(io::Error::new(io::ErrorKind::TimedOut, "ACP output drain timed out")))??;
            }
            result = &mut writer => {
                result?;
                tokio::time::timeout(std::time::Duration::from_millis(250), &mut reader).await
                    .map_err(|_| Error::into_internal_error(io::Error::new(io::ErrorKind::TimedOut, "ACP input drain timed out")))??;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Limits {
    frame: usize,
    bytes: usize,
    entries: usize,
}

async fn read_frames<R: AsyncRead + Unpin>(
    reader: R,
    tx: futures::channel::mpsc::UnboundedSender<TransportFrame>,
) -> Result<()> {
    read_frames_with_limits(
        reader,
        tx,
        Limits {
            frame: FRAME_LIMIT,
            bytes: QUEUED_BYTE_LIMIT,
            entries: QUEUED_ENTRY_LIMIT,
        },
    )
    .await
}

async fn read_frames_with_limits<R: AsyncRead + Unpin>(
    reader: R,
    tx: futures::channel::mpsc::UnboundedSender<TransportFrame>,
    limits: Limits,
) -> Result<()> {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    let mut ledger: VecDeque<(usize, usize)> = VecDeque::new();
    let (mut bytes, mut entries) = (0usize, 0usize);
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(Error::into_internal_error)?;
        if available.is_empty() {
            if !line.is_empty() {
                enqueue(
                    &mut line,
                    &tx,
                    &mut ledger,
                    &mut bytes,
                    &mut entries,
                    limits,
                )?;
            }
            return Ok(());
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1);
        if line
            .len()
            .checked_add(take)
            .is_none_or(|n| n > limits.frame)
        {
            return Err(limit_error("frame bytes", limits.frame));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            enqueue(
                &mut line,
                &tx,
                &mut ledger,
                &mut bytes,
                &mut entries,
                limits,
            )?;
        }
    }
}

fn enqueue(
    line: &mut Vec<u8>,
    tx: &futures::channel::mpsc::UnboundedSender<TransportFrame>,
    ledger: &mut VecDeque<(usize, usize)>,
    queued_bytes: &mut usize,
    queued_entries: &mut usize,
    limits: Limits,
) -> Result<()> {
    let wire_bytes = line.len();
    let queued = tx.len();
    while ledger.len() > queued {
        if let Some((old_bytes, old_entries)) = ledger.pop_front() {
            *queued_bytes -= old_bytes;
            *queued_entries -= old_entries;
        }
    }
    if wire_bytes > limits.bytes.saturating_sub(*queued_bytes) {
        return Err(limit_error("queued wire bytes", limits.bytes));
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    let text = std::str::from_utf8(line).map_err(Error::into_internal_error)?;
    let frame = TransportFrame::parse_json(text);
    let count = match &frame {
        TransportFrame::Batch(batch) => batch.len().max(1),
        _ => 1,
    };
    if count > limits.entries.saturating_sub(*queued_entries) {
        return Err(limit_error("queued entries", limits.entries));
    }
    tx.unbounded_send(frame)
        .map_err(Error::into_internal_error)?;
    ledger.push_back((wire_bytes, count));
    *queued_bytes += wire_bytes;
    *queued_entries += count;
    line.clear();
    Ok(())
}

fn limit_error(name: &str, limit: usize) -> Error {
    Error::into_internal_error(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("ACP {name} limit exceeded (limit {limit})"),
    ))
}

async fn write_frames<W: AsyncWrite + Unpin>(
    mut writer: W,
    rx: &mut futures::channel::mpsc::UnboundedReceiver<TransportFrame>,
) -> Result<()> {
    while let Some(frame) = rx.next().await {
        let json = frame.to_json()?;
        writer
            .write_all(json.as_bytes())
            .await
            .map_err(Error::into_internal_error)?;
        writer
            .write_all(b"\n")
            .await
            .map_err(Error::into_internal_error)?;
        writer.flush().await.map_err(Error::into_internal_error)?;
    }
    Ok(())
}

struct ProcessGuard<C> {
    child: C,
    pid: Option<u32>,
    kill: fn(&mut C),
}
impl<C> Drop for ProcessGuard<C> {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            crate::terminal::kill_acp_process_group(pid);
        }
        (self.kill)(&mut self.child);
    }
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct GuardedAgent {
    agent: AcpAgent,
    diagnostic: ProcessDiagnostic,
}
impl GuardedAgent {
    pub(crate) fn new(agent: AcpAgent) -> (Self, ProcessDiagnostic) {
        let diagnostic = ProcessDiagnostic::default();
        (
            Self {
                agent,
                diagnostic: diagnostic.clone(),
            },
            diagnostic,
        )
    }
}

impl ConnectTo<Client> for GuardedAgent {
    async fn connect_to(self, client: impl ConnectTo<agent_client_protocol::Agent>) -> Result<()> {
        let (stdin, stdout, stderr, child) = self.agent.spawn_process()?;
        use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
        let pid = child.id();
        let mut guard = ProcessGuard {
            child,
            pid: Some(pid),
            kill: |child| {
                let _ = child.kill();
            },
        };
        let diagnostic = self.diagnostic;
        let stderr_diagnostic = diagnostic.clone();
        let mut stderr_task = AbortOnDrop(tokio::spawn(async move {
            use futures::io::AsyncReadExt;
            let mut stderr = stderr;
            let mut buf = [0u8; 2048];
            loop {
                let n = stderr.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                stderr_diagnostic.append_stderr(&buf[..n]);
            }
            Ok::<_, std::io::Error>(())
        }));
        let connector = GuardedByteStreams::new(stdin.compat_write(), stdout.compat());
        let (endpoint, client_future) = client.into_channel_and_future();
        let mut io_future = Box::pin(async {
            tokio::try_join!(connector.run_io(endpoint), client_future).map(|_| ())
        });
        let observed = {
            let status_future = guard.child.status();
            tokio::pin!(status_future);
            tokio::select! {
                biased;
                status = &mut status_future => (Some(status), Ok(())),
                result = &mut io_future => (None, result),
            }
        };
        let (early_status, connection) = observed;
        if let Some(status) = early_status {
            let status = status.map_err(Error::into_internal_error)?;
            diagnostic.observe_failure(&status);
            let pid = guard.pid.take();
            if let Some(pid) = pid {
                crate::terminal::kill_acp_process_group(pid);
            }
            let drain =
                tokio::time::timeout(std::time::Duration::from_millis(250), &mut io_future).await;
            let _ = drain;
            if !status.success() {
                let (_, stderr) = diagnostic.snapshot();
                return Err(Error::into_internal_error(io::Error::other(format!(
                    "ACP agent exited with {status}; stderr: {}",
                    String::from_utf8_lossy(&stderr)
                ))));
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(250), &mut stderr_task.0)
                .await;
            connection?;
            return Ok(());
        }
        if let Err(connection_error) = connection {
            if let Ok(Ok(status)) =
                tokio::time::timeout(std::time::Duration::from_millis(250), guard.child.status())
                    .await
            {
                diagnostic.observe_failure(&status);
                let pid = guard.pid.take();
                if let Some(pid) = pid {
                    crate::terminal::kill_acp_process_group(pid);
                }
                if !status.success() {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_millis(250),
                        &mut stderr_task.0,
                    )
                    .await;
                    let (_, stderr) = diagnostic.snapshot();
                    return Err(Error::into_internal_error(io::Error::other(format!(
                        "ACP agent exited with {status}; stderr: {}",
                        String::from_utf8_lossy(&stderr)
                    ))));
                }
            }
            return Err(connection_error);
        }
        let status = tokio::time::timeout(std::time::Duration::from_secs(2), guard.child.status())
            .await
            .map_err(|_| {
                Error::into_internal_error(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "ACP agent shutdown timed out",
                ))
            })?
            .map_err(Error::into_internal_error)?;
        diagnostic.observe_failure(&status);
        let pid = guard.pid.take();
        if !status.success() {
            if let Some(pid) = pid {
                crate::terminal::kill_acp_process_group(pid);
            }
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut stderr_task.0).await;
            let (_, stderr) = diagnostic.snapshot();
            return Err(Error::into_internal_error(io::Error::other(format!(
                "ACP agent exited with {status}; stderr: {}",
                String::from_utf8_lossy(&stderr)
            ))));
        }
        if let Some(pid) = pid {
            crate::terminal::kill_acp_process_group(pid);
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), &mut stderr_task.0).await;
        Ok(())
    }

    fn into_channel_and_future(self) -> (Channel, futures::future::BoxFuture<'static, Result<()>>) {
        let (endpoint, peer) = Channel::duplex();
        let future = async move { self.connect_to(peer).await }.boxed();
        (endpoint, future)
    }
}

use futures::StreamExt;

#[cfg(test)]
mod tests {
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
        writer.write_all(b"[{\"jsonrpc\":\"2.0\",\"method\":\"a\"},{\"jsonrpc\":\"2.0\",\"method\":\"b\"}]\n").await.unwrap();
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
}
