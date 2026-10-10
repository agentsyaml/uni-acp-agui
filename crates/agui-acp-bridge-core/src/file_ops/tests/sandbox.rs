use super::*;
#[cfg(target_os = "linux")]
use tokio::sync::Notify;

#[test]
fn acp_cwd_strips_windows_verbatim_prefix() {
    assert_eq!(
        acp_cwd(Path::new(r"\\?\D:\work\project")),
        PathBuf::from(r"D:\work\project")
    );
}

#[test]
fn acp_cwd_strips_verbatim_unc_prefix() {
    assert_eq!(
        acp_cwd(Path::new(r"\\?\UNC\server\share\dir")),
        PathBuf::from(r"\\server\share\dir")
    );
}

#[test]
fn acp_cwd_leaves_plain_paths_unchanged() {
    assert_eq!(
        acp_cwd(Path::new("/home/user/x")),
        PathBuf::from("/home/user/x")
    );
    assert_eq!(acp_cwd(Path::new(r"D:\plain")), PathBuf::from(r"D:\plain"));
}

#[test]
fn filesystem_io_preserves_source_error_fidelity() {
    use std::error::Error as _;

    // Raw os error 13 (EACCES) without pulling libc into the deps.
    let original = std::io::Error::from_raw_os_error(13);
    let wrapped = filesystem_io(original);
    let BridgeError::Io(wrapped) = &wrapped else {
        panic!("filesystem_io must return BridgeError::Io");
    };
    // Kind and taxonomy preserved: non-NotFound io errors collapse to
    // `Other` by design (see `filesystem_io`), never a specific kind.
    assert_eq!(wrapped.kind(), std::io::ErrorKind::Other);
    assert_eq!(
        error_kind(&BridgeError::Io(clone_io(wrapped))),
        FileErrorKind::Internal
    );
    // Display carries the kind + raw os error, not a fixed opaque string.
    let text = wrapped.to_string();
    assert!(
        text.contains("os error 13"),
        "message must keep the os error, got: {text}"
    );
    // The original error stays reachable as `source`.
    let source = wrapped
        .source()
        .and_then(|s| s.downcast_ref::<std::io::Error>())
        .expect("original io error must be chained as source");
    assert_eq!(source.raw_os_error(), Some(13));
}

fn clone_io(error: &std::io::Error) -> std::io::Error {
    std::io::Error::new(error.kind(), error.to_string())
}

// ponytail: covers the openat2(RESOLVE_BENEATH) Linux path; non-Linux fails closed by design, nothing to assert.
#[tokio::test]
async fn relative_paths_are_rejected() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("foo.txt"), "hello").unwrap();
    let read = read_text_file(dir.path(), "foo.txt", None).await;
    let write = write_text_file(dir.path(), "new.txt", "hello").await;
    assert!(read.is_err(), "ACP read paths must be absolute");
    assert!(write.is_err(), "ACP write paths must be absolute");
}

#[tokio::test]
async fn read_with_absolute_path_outside_cwd_is_rejected() {
    let dir = temp_cwd();
    let other = std::env::temp_dir().join(format!("agui-other-{}.txt", uuid::Uuid::new_v4()));
    std::fs::write(&other, "secret").unwrap();
    let result = read_text_file(dir.path(), other.to_str().unwrap(), None).await;
    let _ = std::fs::remove_file(&other);
    let err = result.expect_err("must reject absolute path outside cwd");
    assert!(
        format!("{err}").contains("escapes working directory"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn read_with_parent_traversal_is_rejected() {
    let dir = temp_cwd();
    let result = read_text_file(dir.path(), &absolute(&dir, "../escape.txt"), None).await;
    // The traversal target probably doesn't exist, but even if it does,
    // canonicalize will resolve it outside cwd. Either way: error.
    assert!(result.is_err(), "must reject parent traversal");
}

/// Regression for the audit's P0 "cwd='.' renders sandbox a no-op"
/// finding. With the old `normalize_path` lexical implementation,
/// `cwd="."` normalized to the empty path, against which `starts_with`
/// trivially returned true for every input (including `/etc/passwd`).
/// The fix is to canonicalize cwd before any check.
#[tokio::test]
async fn read_with_dot_cwd_does_not_escape_sandbox() {
    // canonicalize_cwd(".") must produce a real absolute path, against
    // which `starts_with` is meaningful.
    let canonical = canonicalize_cwd(Path::new(".")).expect("cwd canonicalize");
    assert!(
        canonical.is_absolute(),
        "canonical cwd must be absolute: {}",
        canonical.display()
    );
    // Pick a path we know is outside cwd.
    let probe = if cfg!(windows) {
        "C:/Windows/System32/drivers/etc/hosts"
    } else {
        "/etc/hostname"
    };
    let result = read_text_file(&canonical, probe, None).await;
    // We can't assume the probe exists in CI, but it MUST NOT be Ok
    // for any reason that lets the agent read it. Either NotFound
    // (canonicalize errored before sandbox check — still safe) or
    // PermissionDenied (sandbox blocked it). Both are acceptable.
    assert!(result.is_err(), "must not return file content from {probe}");
}

#[tokio::test]
async fn write_with_parent_traversal_is_rejected() {
    let dir = temp_cwd();
    let result = write_text_file(dir.path(), &absolute(&dir, "../escape.txt"), "boom").await;
    assert!(result.is_err(), "must reject parent traversal write");
}

#[cfg(unix)]
#[tokio::test]
async fn read_through_symlink_pointing_outside_is_rejected() {
    use std::os::unix::fs::symlink;
    let dir = temp_cwd();
    let target = std::env::temp_dir().join(format!("agui-target-{}.txt", uuid::Uuid::new_v4()));
    std::fs::write(&target, "secret").unwrap();
    symlink(&target, dir.path().join("link.txt")).unwrap();

    let result = read_text_file(dir.path(), &absolute(&dir, "link.txt"), None).await;
    let _ = std::fs::remove_file(&target);
    let err = result.expect_err("symlink must not bypass sandbox");
    assert!(
        format!("{err}").contains("escapes working directory"),
        "unexpected error: {err}"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_rejects_symlink_swapped_after_validation() {
    use std::os::unix::fs::symlink;

    let secure_reads_supported = read_text_file_supported();
    let dir = temp_cwd();
    let target = dir.path().join("read.txt");
    std::fs::write(&target, "inside").unwrap();
    let outside = std::env::temp_dir().join(format!("agui-read-swap-{}", uuid::Uuid::new_v4()));
    std::fs::write(&outside, "outside").unwrap();
    let path = target.to_string_lossy().into_owned();
    let gate = Arc::new(ReadGate {
        path: target.clone(),
        started: Notify::new(),
        release: Notify::new(),
    });
    install_read_gate(gate.clone());

    let started = gate.started.notified();
    let cwd = dir.path().to_path_buf();
    let read = tokio::spawn(async move { read_text_file(&cwd, &path, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), started)
        .await
        .expect("read must reach the post-validation gate");

    let preserved = dir.path().join("read-original.txt");
    std::fs::rename(&target, &preserved).unwrap();
    symlink(&outside, &target).unwrap();
    gate.release.notify_waiters();

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), read)
        .await
        .expect("swapped read must finish")
        .expect("swapped read task must not panic");
    clear_read_gate();

    let error = result.expect_err("a swapped read must not return outside content");
    if secure_reads_supported {
        assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
    } else {
        assert!(matches!(
            error,
            BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
        ));
    }
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "outside");

    std::fs::remove_file(&target).unwrap();
    std::fs::rename(preserved, target).unwrap();
    std::fs::remove_file(outside).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn write_through_symlinked_parent_pointing_outside_is_rejected() {
    use std::os::unix::fs::symlink;
    let dir = temp_cwd();
    let outside = std::env::temp_dir().join(format!("agui-outside-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&outside).unwrap();
    symlink(&outside, dir.path().join("escape_dir")).unwrap();

    let result = write_text_file(dir.path(), &absolute(&dir, "escape_dir/foo.txt"), "boom").await;
    let _ = std::fs::remove_dir_all(&outside);
    assert!(
        result.is_err(),
        "write through symlinked parent must be rejected"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_rejects_parent_symlink_swapped_after_validation() {
    use std::os::unix::fs::symlink;

    let secure_writes_supported = write_text_file_supported();
    let dir = temp_cwd();
    let parent = dir.path().join("swappable");
    std::fs::create_dir(&parent).unwrap();
    let outside = std::env::temp_dir().join(format!("agui-swap-outside-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&outside).unwrap();
    let target = parent.join("foo.txt");
    let target_path = target.to_string_lossy().into_owned();
    let gate = Arc::new(WriteGate {
        path: target.clone(),
        started: Notify::new(),
        release: Notify::new(),
    });
    install_write_gate(gate.clone());

    let started = gate.started.notified();
    let cwd = dir.path().to_path_buf();
    let write = tokio::spawn(async move { write_text_file(&cwd, &target_path, "boom").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), started)
        .await
        .expect("write must reach the post-validation gate");

    let preserved = dir.path().join("swappable-original");
    std::fs::rename(&parent, &preserved).unwrap();
    symlink(&outside, &parent).unwrap();
    gate.release.notify_waiters();

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), write)
        .await
        .expect("swapped write must finish")
        .expect("swapped write task must not panic");
    clear_write_gate();

    let error = result.expect_err("a swapped parent must not be written");
    if secure_writes_supported {
        assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
    } else {
        assert!(matches!(
            error,
            BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
        ));
    }
    assert!(!outside.join("foo.txt").exists());
    assert!(!preserved.join("foo.txt").exists());

    std::fs::remove_file(&parent).unwrap();
    std::fs::rename(preserved, parent).unwrap();
    std::fs::remove_dir_all(outside).unwrap();
}

// ponytail: covers the openat2(RESOLVE_BENEATH) Linux path; non-Linux fails closed by design, nothing to assert.
