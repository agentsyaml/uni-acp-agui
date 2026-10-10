use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_within_cwd_succeeds() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("foo.txt"), "hello").unwrap();
    let out = read_text_file(dir.path(), &absolute(&dir, "foo.txt"), None)
        .await
        .unwrap();
    assert_eq!(out, "hello");
}

#[tokio::test]
async fn read_missing_file_is_resource_not_found() {
    let dir = temp_cwd();
    let result = read_text_file(dir.path(), &absolute(&dir, "missing.txt"), None).await;
    let error = result.expect_err("missing file must fail");
    assert_eq!(error_kind(&error), FileErrorKind::ResourceNotFound);
}

#[tokio::test]
async fn invalid_utf8_is_an_internal_filesystem_error() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("invalid.txt"), [0xff, 0xfe]).unwrap();
    let result = read_text_file(dir.path(), &absolute(&dir, "invalid.txt"), None).await;
    let error = result.expect_err("invalid UTF-8 must fail");
    assert_eq!(error_kind(&error), FileErrorKind::Internal);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn oversized_text_file_is_rejected_before_line_buffering() {
    let dir = temp_cwd();
    std::fs::write(
        dir.path().join("large.txt"),
        vec![b'x'; MAX_TEXT_FILE_BYTES + 1],
    )
    .unwrap();
    let result = read_text_file(dir.path(), &absolute(&dir, "large.txt"), None).await;
    let error = result.expect_err("file-size bound must be enforced");
    assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_within_cwd_creates_file_and_parents() {
    if !write_text_file_supported() {
        return;
    }
    let dir = temp_cwd();
    write_text_file(dir.path(), &absolute(&dir, "nested/deep/foo.txt"), "ok")
        .await
        .unwrap();
    let actual = std::fs::read_to_string(dir.path().join("nested/deep/foo.txt")).unwrap();
    assert_eq!(actual, "ok");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_overwrites_existing_file() {
    if !write_text_file_supported() {
        return;
    }
    let dir = temp_cwd();
    let path = absolute(&dir, "foo.txt");
    write_text_file(dir.path(), &path, "first").await.unwrap();
    write_text_file(dir.path(), &path, "second").await.unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.path().join("foo.txt")).unwrap(),
        "second"
    );
}

#[tokio::test]
async fn oversized_write_is_rejected_before_disk_access() {
    let dir = temp_cwd();
    let path = dir.path().join("oversized.txt");
    let result = write_text_file(
        dir.path(),
        &path.to_string_lossy(),
        &"x".repeat(MAX_TEXT_FILE_BYTES + 1),
    )
    .await;
    let error = result.expect_err("oversized write must fail");
    assert_eq!(error_kind(&error), FileErrorKind::InvalidParams);
    assert!(!path.exists());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn write_fails_closed_when_secure_primitive_is_unavailable() {
    assert!(!write_text_file_supported());
    let dir = temp_cwd();
    let path = dir.path().join("unsupported.txt");
    let result = write_text_file(dir.path(), &path.to_string_lossy(), "nope").await;
    let error = result.expect_err("writes must fail closed on unsupported targets");
    assert_eq!(error_kind(&error), FileErrorKind::Internal);
    assert!(
        matches!(error, BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported)
    );
    assert!(!path.exists());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn read_fails_closed_when_secure_primitive_is_unavailable() {
    let dir = temp_cwd();
    let path = dir.path().join("unsupported.txt");
    std::fs::write(&path, "nope").unwrap();
    let result = read_text_file(dir.path(), &path.to_string_lossy(), None).await;
    let error = result.expect_err("reads must fail closed on unsupported targets");
    assert!(matches!(
        error,
        BridgeError::Io(error) if error.kind() == std::io::ErrorKind::Unsupported
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_limit_counts_lines_and_preserves_multibyte_text() {
    let dir = temp_cwd();
    std::fs::write(
        dir.path().join("lines.txt"),
        "one\n\u{4e16}\u{754c}\nthree\n",
    )
    .unwrap();
    let out = read_text_file(dir.path(), &absolute(&dir, "lines.txt"), Some(2))
        .await
        .unwrap();
    assert_eq!(out, "one\n\u{4e16}\u{754c}\n");
}

// ponytail: covers the openat2(RESOLVE_BENEATH) Linux path; non-Linux fails closed by design, nothing to assert.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_line_and_limit_use_one_based_line_ranges() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("lines.txt"), "one\ntwo\nthree\nfour\n").unwrap();
    let out = read_text_file_range(dir.path(), &absolute(&dir, "lines.txt"), Some(2), Some(2))
        .await
        .unwrap();
    assert_eq!(out, "two\nthree\n");
}

#[tokio::test]
async fn read_line_zero_is_rejected() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("lines.txt"), "one\n").unwrap();
    let result =
        read_text_file_range(dir.path(), &absolute(&dir, "lines.txt"), Some(0), Some(1)).await;
    assert!(result.is_err(), "ACP line numbers are 1-based");
}

// ponytail: covers the openat2(RESOLVE_BENEATH) Linux path; non-Linux fails closed by design, nothing to assert.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn read_limit_zero_returns_no_lines() {
    let dir = temp_cwd();
    std::fs::write(dir.path().join("lines.txt"), "one\ntwo\n").unwrap();
    let out = read_text_file(dir.path(), &absolute(&dir, "lines.txt"), Some(0))
        .await
        .unwrap();
    assert!(out.is_empty());
}
