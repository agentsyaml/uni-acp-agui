use super::*;

mod io;
mod root_leases;
mod sandbox;

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

#[cfg(target_os = "linux")]
struct ReplacedRoot {
    path: PathBuf,
    original: PathBuf,
}

#[cfg(target_os = "linux")]
impl Drop for ReplacedRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
        let _ = std::fs::rename(&self.original, &self.path);
    }
}

fn absolute(dir: &TempDir, relative: &str) -> String {
    dir.path().join(relative).to_string_lossy().into_owned()
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.raw_for_cleanup);
    }
}
