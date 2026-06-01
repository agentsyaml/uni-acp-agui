//! File operations for ACP agent requests.
//!
//! Implements `ReadTextFile` and `WriteTextFile` with path sandboxing
//! to prevent directory traversal and symlink-based escapes.
//!
//! # Sandbox model
//!
//! Every operation receives a `cwd` (the session's working directory) and a
//! relative or absolute path provided by the agent. We resolve the path
//! against `cwd`, **canonicalize** the result through the OS (resolving
//! symlinks), and verify the canonical path is inside the canonical `cwd`.
//!
//! For reads we canonicalize the full target. For writes, the target may
//! not exist yet, so we canonicalize the **deepest existing ancestor** —
//! the only thing the OS can canonicalize without TOCTOU. As long as the
//! existing ancestor is inside `cwd`, the write is contained: subsequent
//! `create_dir_all`/`write` calls only create or modify descendants.
//!
//! Callers are expected to pass an already-canonicalized `cwd`. The
//! [`canonicalize_cwd`] helper returns one — `BridgeAppState` calls it
//! once at construction.

use std::path::{Path, PathBuf};

use crate::error::BridgeError;

/// Canonicalize `cwd` for use as a sandbox root.
///
/// Resolves the path through the OS (handles `.`, `..`, and symlinks) and
/// returns an absolute `PathBuf`. If `cwd` does not exist, falls back to
/// [`std::path::absolute`] so callers still get an absolute path — the
/// sandbox check will then fail closed for any agent path that escapes.
///
/// # Errors
///
/// Returns the underlying I/O error if `cwd` does not exist **and**
/// `std::path::absolute` cannot resolve it (e.g. on platforms with no
/// notion of a current directory).
pub fn canonicalize_cwd(cwd: &Path) -> std::io::Result<PathBuf> {
    match std::fs::canonicalize(cwd) {
        Ok(p) => Ok(p),
        Err(_) => std::path::absolute(cwd),
    }
}

/// Return a "clean" form of `cwd` suitable for sending to an ACP agent in
/// `session/new` / `session/load` / `session/list`.
///
/// On Windows, [`std::fs::canonicalize`] yields an extended-length path with
/// a `\\?\` verbatim prefix (e.g. `\\?\D:\work\project`). Agents persist and
/// key sessions by this directory string, and many (opencode included) derive
/// a project identity from it; the verbatim prefix makes the agent's stored
/// directory differ from the plain `D:\work\project` a user or other tool
/// would use, which breaks directory-scoped session listing. We strip the
/// prefix so the agent sees a conventional absolute path.
///
/// We deliberately keep the **internal** sandbox `cwd` canonicalized (prefix
/// intact): file-op containment compares it against agent paths that are
/// themselves canonicalized with the prefix, so both must carry it. Only the
/// ACP-facing copy is cleaned.
///
/// On non-Windows platforms this is a no-op clone.
#[must_use]
pub fn acp_cwd(cwd: &Path) -> PathBuf {
    let s = cwd.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        // `\\?\UNC\server\share` → `\\server\share`; `\\?\D:\x` → `D:\x`.
        if let Some(unc) = rest.strip_prefix("UNC\\") {
            return PathBuf::from(format!(r"\\{unc}"));
        }
        return PathBuf::from(rest);
    }
    cwd.to_path_buf()
}

/// Resolve an agent-supplied path for **reading**.
///
/// Returns the canonical path on success. Errors with `PermissionDenied` if
/// the resolved path escapes `cwd`, and propagates I/O errors otherwise.
fn safe_resolve_read(cwd: &Path, relative: &str) -> Result<PathBuf, BridgeError> {
    let candidate = join_for_resolve(cwd, relative);
    let canonical = std::fs::canonicalize(&candidate).map_err(BridgeError::Io)?;
    enforce_sandbox(cwd, &canonical, &candidate)?;
    Ok(canonical)
}

/// Resolve an agent-supplied path for **writing**.
///
/// The target may not exist yet, but its deepest existing ancestor must be
/// inside `cwd`. Returns the joined (un-canonicalized) target path so the
/// caller can create directories and write the file.
fn safe_resolve_write(cwd: &Path, relative: &str) -> Result<PathBuf, BridgeError> {
    let candidate = join_for_resolve(cwd, relative);

    // Find the deepest existing ancestor and canonicalize it. That gives us
    // an OS-honest answer about where the path actually lives, including
    // any symlinks the attacker may have placed in writable parts of the
    // filesystem.
    let mut probe: &Path = &candidate;
    let anchor = loop {
        match std::fs::canonicalize(probe) {
            Ok(p) => break p,
            Err(_) => match probe.parent() {
                Some(parent) => probe = parent,
                None => {
                    return Err(BridgeError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "no existing ancestor for {} could be canonicalized",
                            candidate.display()
                        ),
                    )));
                }
            },
        }
    };

    enforce_sandbox(cwd, &anchor, &candidate)?;
    Ok(candidate)
}

fn join_for_resolve(cwd: &Path, relative: &str) -> PathBuf {
    if Path::new(relative).is_absolute() {
        PathBuf::from(relative)
    } else {
        cwd.join(relative)
    }
}

fn enforce_sandbox(cwd: &Path, canonical: &Path, original: &Path) -> Result<(), BridgeError> {
    if !canonical.starts_with(cwd) {
        return Err(BridgeError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "path escapes working directory: {} (resolved to {}) is not under {}",
                original.display(),
                canonical.display(),
                cwd.display()
            ),
        )));
    }
    Ok(())
}

/// Read a text file relative to `cwd`.
///
/// `cwd` MUST be canonicalized (see [`canonicalize_cwd`]). The agent path is
/// resolved against `cwd`, canonicalized through the OS, and verified to be
/// under `cwd` before any bytes are read.
///
/// If `limit` is provided, at most that many **bytes** are read from the
/// file. The returned string is always cut at a UTF-8 character boundary so
/// multi-byte codepoints (CJK, emoji) at the cap are dropped intact rather
/// than split.
pub async fn read_text_file(
    cwd: &Path,
    path: &str,
    limit: Option<usize>,
) -> Result<String, BridgeError> {
    let full_path = safe_resolve_read(cwd, path)?;

    let bytes = match limit {
        Some(n) => {
            // Stream-read up to `n` bytes so a `limit=1024` request against
            // a 10 GB file does not allocate 10 GB.
            use tokio::io::AsyncReadExt;
            let file = tokio::fs::File::open(&full_path).await?;
            let mut buf = Vec::with_capacity(n.min(8192));
            file.take(n as u64).read_to_end(&mut buf).await?;
            buf
        }
        None => tokio::fs::read(&full_path).await?,
    };

    // Validate UTF-8. If `limit` cut the file mid-character, drop the
    // partial codepoint at the tail.
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(err) => {
            let valid_up_to = err.utf8_error().valid_up_to();
            // The bytes after `valid_up_to` may be a partial multibyte char
            // (when limit landed mid-codepoint) OR genuine invalid UTF-8
            // (file is binary). For the limit case we want to retain the
            // valid prefix; for genuine binary we still surface a clean
            // truncation rather than panic. Use lossy fallback for the
            // tail only if `limit` was set.
            let mut bytes = err.into_bytes();
            bytes.truncate(valid_up_to);
            // SAFETY: `valid_up_to` bytes are valid UTF-8 by definition.
            Ok(String::from_utf8(bytes).expect("valid_up_to is UTF-8 by construction"))
        }
    }
}

/// Write a text file relative to `cwd`.
///
/// `cwd` MUST be canonicalized (see [`canonicalize_cwd`]). The path is
/// resolved against `cwd`; the deepest existing ancestor must be inside
/// `cwd` (preventing symlink redirection of intermediate directories).
/// Missing parent directories are then created and the file written.
pub async fn write_text_file(cwd: &Path, path: &str, content: &str) -> Result<(), BridgeError> {
    let full_path = safe_resolve_write(cwd, path)?;
    if let Some(parent) = full_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&full_path, content).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn temp_cwd() -> TempDir {
        let raw = std::env::temp_dir().join(format!("agui-fileops-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&raw).unwrap();
        let canonical = canonicalize_cwd(&raw).unwrap();
        TempDir {
            path: canonical,
            raw_for_cleanup: raw,
        }
    }

    struct TempDir {
        path: PathBuf,
        raw_for_cleanup: PathBuf,
    }

    impl TempDir {
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.raw_for_cleanup);
        }
    }

    #[tokio::test]
    async fn read_within_cwd_succeeds() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("foo.txt"), "hello").unwrap();
        let out = read_text_file(dir.path(), "foo.txt", None).await.unwrap();
        assert_eq!(out, "hello");
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
        let result = read_text_file(dir.path(), "../escape.txt", None).await;
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
    async fn write_within_cwd_creates_file_and_parents() {
        let dir = temp_cwd();
        write_text_file(dir.path(), "nested/deep/foo.txt", "ok")
            .await
            .unwrap();
        let actual = std::fs::read_to_string(dir.path().join("nested/deep/foo.txt")).unwrap();
        assert_eq!(actual, "ok");
    }

    #[tokio::test]
    async fn write_with_parent_traversal_is_rejected() {
        let dir = temp_cwd();
        let result = write_text_file(dir.path(), "../escape.txt", "boom").await;
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

        let result = read_text_file(dir.path(), "link.txt", None).await;
        let _ = std::fs::remove_file(&target);
        let err = result.expect_err("symlink must not bypass sandbox");
        assert!(
            format!("{err}").contains("escapes working directory"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_through_symlinked_parent_pointing_outside_is_rejected() {
        use std::os::unix::fs::symlink;
        let dir = temp_cwd();
        let outside = std::env::temp_dir().join(format!("agui-outside-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, dir.path().join("escape_dir")).unwrap();

        let result = write_text_file(dir.path(), "escape_dir/foo.txt", "boom").await;
        let _ = std::fs::remove_dir_all(&outside);
        assert!(
            result.is_err(),
            "write through symlinked parent must be rejected"
        );
    }

    #[tokio::test]
    async fn read_text_file_with_limit_inside_multibyte_does_not_panic() {
        // "Hello,世界" = "Hello," (6 ASCII) + "世" (3 bytes) + "界" (3 bytes)
        let dir = temp_cwd();
        std::fs::write(dir.path().join("hello.txt"), "Hello,世界").unwrap();

        // limit=7 lands inside the first byte of "世" — must drop the
        // partial codepoint and keep "Hello,".
        let out = read_text_file(dir.path(), "hello.txt", Some(7))
            .await
            .expect("read with limit must not error");
        assert_eq!(out, "Hello,", "got {out:?}");
    }

    #[tokio::test]
    async fn read_text_file_with_limit_on_boundary_returns_full_prefix() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("hello.txt"), "Hello,世界").unwrap();
        let out = read_text_file(dir.path(), "hello.txt", Some(9))
            .await
            .unwrap();
        assert_eq!(out, "Hello,世");
    }

    #[tokio::test]
    async fn read_text_file_with_limit_larger_than_file_returns_full() {
        let dir = temp_cwd();
        std::fs::write(dir.path().join("hello.txt"), "abc").unwrap();
        let out = read_text_file(dir.path(), "hello.txt", Some(1024))
            .await
            .unwrap();
        assert_eq!(out, "abc");
    }

    #[tokio::test]
    async fn read_text_file_with_limit_does_not_read_more_than_requested() {
        // Functional check that we're streaming, not reading-then-truncating.
        // We can't easily prove the optimization without instrumentation,
        // but we can at least confirm behavior with a 1MB file is fast and
        // returns only the first byte requested.
        let dir = temp_cwd();
        let big = "a".repeat(1024 * 1024);
        std::fs::write(dir.path().join("big.txt"), &big).unwrap();
        let started = std::time::Instant::now();
        let out = read_text_file(dir.path(), "big.txt", Some(8))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(out, "aaaaaaaa");
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "limited read should be near-instant, took {elapsed:?}"
        );
    }
}
