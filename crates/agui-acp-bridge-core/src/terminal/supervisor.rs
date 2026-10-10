use super::cleanup::{
    exit_status, terminate_and_reap, terminate_child, terminate_process_tree,
    wait_for_child_and_cleanup,
};
use super::process_tree::ProcessTree;
use super::registry::TerminalLifecycle;
use super::runtime::{Control, TerminalState};
use super::{POST_EXIT_DRAIN_TIMEOUT, READ_CHUNK_BYTES};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Child;
use tokio::sync::{mpsc, oneshot};

pub(super) async fn supervise(
    mut child: Child,
    mut stdout: Option<tokio::process::ChildStdout>,
    mut stderr: Option<tokio::process::ChildStderr>,
    state: Arc<TerminalState>,
    process_tree: Arc<ProcessTree>,
    lifecycle: Arc<TerminalLifecycle>,
    mut controls: mpsc::Receiver<Control>,
) {
    let mut stdout_buffer = [0_u8; READ_CHUNK_BYTES];
    let mut stderr_buffer = [0_u8; READ_CHUNK_BYTES];
    let mut release_reply: Option<oneshot::Sender<Result<(), ()>>> = None;
    let mut process_exited = false;
    let mut process_status = None;
    let mut read_failed = false;
    let mut pipes_closed_deliberately = false;
    let mut cleanup_failed = false;
    let mut post_exit_deadline = None;

    loop {
        if process_exited && stdout.is_none() && stderr.is_none() {
            let status = if cleanup_failed || (read_failed && !pipes_closed_deliberately) {
                Err(())
            } else {
                process_status.clone().unwrap_or(Err(()))
            };
            state.complete(status);
            tokio::select! {
                _ = lifecycle.abort_signal.notified() => {
                    let cleanup_ok = terminate_process_tree(&process_tree).await;
                    if !cleanup_ok {
                        state.mark_unavailable();
                    }
                    lifecycle.resource.release();
                    if let Some(reply) = release_reply.take() {
                        let _ = reply.send(Err(()));
                    }
                    break;
                }
                control = controls.recv() => match control {
                    Some(Control::Kill(reply)) => {
                        let result = terminate_process_tree(&process_tree).await;
                        if !result {
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(result.then_some(()).ok_or(()));
                    }
                    Some(Control::Release(reply)) => {
                        let result = terminate_process_tree(&process_tree).await;
                        if !result {
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(if result {
                            state.completion_result()
                        } else {
                            Err(())
                        });
                        break;
                    }
                    None => {
                        let _ = terminate_process_tree(&process_tree).await;
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            let _ = reply.send(Err(()));
                        }
                        break;
                    }
                }
            }
            continue;
        }

        let drain_deadline = async {
            match post_exit_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            _ = lifecycle.abort_signal.notified() => {
                let _ = terminate_and_reap(&mut child, &process_tree).await;
                drop(stdout.take());
                drop(stderr.take());
                state.mark_unavailable();
                lifecycle.resource.release();
                if let Some(reply) = release_reply.take() {
                    let _ = reply.send(Err(()));
                }
                break;
            }
            _ = drain_deadline, if process_exited => {
                // A descendant can keep inherited descriptors open forever.
                // Drain until this deadline, then close our handles without
                // delaying terminal/output indefinitely.
                pipes_closed_deliberately = true;
                stdout = None;
                stderr = None;
                state.complete(if cleanup_failed || read_failed {
                    Err(())
                } else {
                    process_status.clone().unwrap_or(Err(()))
                });
                post_exit_deadline = None;
            }
            control = controls.recv() => {
                match control {
                    Some(Control::Kill(reply)) => {
                        let result = terminate_child(&mut child, &process_tree).await;
                        if result.is_ok() && !process_exited {
                            // Kill is an explicit pipe-closure contract. A
                            // descendant may otherwise keep these handles
                            // open after the direct child is gone.
                            pipes_closed_deliberately = true;
                            stdout = None;
                            stderr = None;
                        } else if result.is_err() {
                            pipes_closed_deliberately = true;
                            stdout = None;
                            stderr = None;
                            cleanup_failed = true;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                        let _ = reply.send(result);
                    }
                    Some(Control::Release(reply)) => {
                        if release_reply.is_some() {
                            let _ = reply.send(Err(()));
                            continue;
                        }
                        if process_exited {
                            // A descendant may retain the inherited pipe
                            // descriptors after the direct child exits. The
                            // release operation owns explicit pipe cleanup;
                            // it must not wait for those descriptors to close.
                            let result = terminate_process_tree(&process_tree).await;
                            drop(stdout.take());
                            drop(stderr.take());
                            if result {
                                state.complete(process_status.clone().unwrap_or(Err(())));
                            } else {
                                state.mark_unavailable();
                                lifecycle.resource.release();
                            }
                            let _ = reply.send(if result {
                                state.completion_result()
                            } else {
                                Err(())
                            });
                            break;
                        }
                        release_reply = Some(reply);
                        if terminate_child(&mut child, &process_tree).await.is_err() {
                            drop(stdout.take());
                            drop(stderr.take());
                            pipes_closed_deliberately = true;
                            cleanup_failed = true;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        }
                    }
                    None => {
                        let _ = terminate_and_reap(&mut child, &process_tree).await;
                        drop(stdout.take());
                        drop(stderr.take());
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            let _ = reply.send(Err(()));
                        }
                        break;
                    }
                }
            }
            result = wait_for_child_and_cleanup(&mut child, &process_tree), if !process_exited => {
                match result {
                    Ok((status, cleanup_ok)) => {
                        let status = exit_status(status);
                        process_status = if cleanup_ok && !cleanup_failed {
                            Some(Ok(status.clone()))
                        } else {
                            Some(Err(()))
                        };
                        process_exited = true;
                        cleanup_failed = cleanup_failed || !cleanup_ok;
                        if cleanup_failed {
                            stdout = None;
                            stderr = None;
                            pipes_closed_deliberately = true;
                            post_exit_deadline = None;
                            state.mark_unavailable();
                            lifecycle.resource.release();
                        } else {
                            post_exit_deadline = Some(
                                tokio::time::Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
                            );
                            state.process_exit(Ok(status));
                        }
                        if let Some(reply) = release_reply.take() {
                            drop(stdout);
                            drop(stderr);
                            state.complete(if cleanup_failed {
                                Err(())
                            } else {
                                process_status.clone().unwrap_or(Err(()))
                            });
                            let _ = reply.send(state.completion_result());
                            break;
                        }
                    }
                    Err(_) => {
                        process_exited = true;
                        process_status = Some(Err(()));
                        cleanup_failed = true;
                        post_exit_deadline = None;
                        stdout = None;
                        stderr = None;
                        pipes_closed_deliberately = true;
                        state.mark_unavailable();
                        lifecycle.resource.release();
                        if let Some(reply) = release_reply.take() {
                            state.complete(Err(()));
                            let _ = reply.send(state.completion_result());
                            break;
                        }
                    }
                }
            }
            result = read_pipe(stdout.as_mut(), &mut stdout_buffer), if stdout.is_some() => {
                let Some(result) = result else { continue };
                match result {
                    Ok(0) => stdout = None,
                    Err(_) => {
                        read_failed = true;
                        stdout = None;
                    }
                    Ok(count) => state.append_output(&stdout_buffer[..count]),
                }
            }
            result = read_pipe(stderr.as_mut(), &mut stderr_buffer), if stderr.is_some() => {
                let Some(result) = result else { continue };
                match result {
                    Ok(0) => stderr = None,
                    Err(_) => {
                        read_failed = true;
                        stderr = None;
                    }
                    Ok(count) => state.append_output(&stderr_buffer[..count]),
                }
            }
        }
    }
}

async fn read_pipe<R: AsyncRead + Unpin>(
    reader: Option<&mut R>,
    buffer: &mut [u8],
) -> Option<std::io::Result<usize>> {
    Some(reader?.read(buffer).await)
}
