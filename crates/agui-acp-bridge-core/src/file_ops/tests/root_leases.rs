#[cfg(target_os = "linux")]
use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn initialized_root_handle_survives_root_path_replacement() {
    if !write_text_file_supported() {
        return;
    }

    let dir = temp_cwd();
    let root = dir.path().to_path_buf();
    let target = root.join("target.txt");
    std::fs::write(&target, "original").unwrap();
    let session_lease = pin_filesystem_root(&root).unwrap();
    let operation_lease = linux_secure_write::root_for(&root).unwrap();
    let operation_weak = Arc::downgrade(&operation_lease);

    let original = root.with_file_name(format!(
        "agui-fileops-root-original-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::rename(&root, &original).unwrap();
    let _root_guard = ReplacedRoot {
        path: root.clone(),
        original: original.clone(),
    };
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("target.txt"), "replacement").unwrap();

    let target_path = target.to_string_lossy().into_owned();
    let read = read_text_file(&root, &target_path, None).await.unwrap();
    assert_eq!(read, "original");

    write_text_file(&root, &target_path, "updated")
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(original.join("target.txt")).unwrap(),
        "updated"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("target.txt")).unwrap(),
        "replacement"
    );
    drop(session_lease);
    assert!(operation_weak.upgrade().is_some());
    drop(operation_lease);
    assert!(operation_weak.upgrade().is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pinned_root_survives_replacement_and_churn_releases_roots() {
    assert!(
        write_text_file_supported(),
        "Linux openat2 must be supported"
    );
    let prefix = std::env::temp_dir().join(format!("agui-root-churn-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&prefix).unwrap();
    let mut retired_roots = Vec::new();

    for index in 0..512 {
        let raw = prefix.join(format!("nested-{index}"));
        std::fs::create_dir_all(&raw).unwrap();
        let root = std::fs::canonicalize(&raw).unwrap();
        let lease = pin_filesystem_root(&root).unwrap();
        let weak = linux_secure_write::root_for(&root)
            .map(|root| Arc::downgrade(&root))
            .unwrap();
        let path = root.join("entry.txt").to_string_lossy().into_owned();
        write_text_file(&root, &path, "live").await.unwrap();
        assert_eq!(read_text_file(&root, &path, None).await.unwrap(), "live");
        drop(lease);
        assert!(
            weak.upgrade().is_none(),
            "expired lease retained root {index}"
        );
        retired_roots.push(root);
    }

    let sentinel = prefix.join("sentinel");
    std::fs::create_dir(&sentinel).unwrap();
    let sentinel = std::fs::canonicalize(sentinel).unwrap();
    let _sentinel_lease = pin_filesystem_root(&sentinel).unwrap();
    let roots = linux_secure_write::cached_root_paths();
    assert!(
        roots
            .iter()
            .all(|path| !path.starts_with(&prefix) || path == &sentinel)
    );

    #[cfg(target_os = "linux")]
    {
        for entry in std::fs::read_dir("/proc/self/fd").unwrap() {
            let target = std::fs::read_link(entry.unwrap().path()).unwrap_or_default();
            assert!(
                !retired_roots.iter().any(|root| target.starts_with(root)),
                "retired root descriptor remains open: {}",
                target.display()
            );
        }
    }
    std::fs::remove_dir_all(prefix).unwrap();
}
