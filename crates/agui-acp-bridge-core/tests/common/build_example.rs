//! Build a named example binary from the external `acp-rust-demo` crate.
//!
//! Returns `None` if the upstream checkout is missing, allowing tests to
//! gracefully skip in environments without the sibling repo. Returns the
//! absolute path to the compiled binary on success.
//!
//! Builds happen via `cargo build -p acp-rust-demo --example {name}` in the
//! checkout's own target directory (NOT this workspace's), so concurrent
//! workspace builds are not blocked.
//!
//! Set `ACP_RUST_PATH` to override the default checkout location (defaults
//! to `../acp-rust` relative to the workspace root).

use std::path::{Path, PathBuf};
use std::process::Command;

/// Default location: a sibling checkout of `acp-rust` next to this workspace.
const DEFAULT_ACP_RUST_PATH: &str = "../acp-rust";

pub fn build_example_agent(name: &str) -> Option<PathBuf> {
    let path = std::env::var("ACP_RUST_PATH").unwrap_or_else(|_| DEFAULT_ACP_RUST_PATH.to_owned());
    let checkout = Path::new(&path);
    if !checkout.join("Cargo.toml").exists() {
        return None;
    }

    let status = Command::new("cargo")
        .args(["build", "-p", "acp-rust-demo", "--example", name])
        .current_dir(checkout)
        .status()
        .ok()?;

    if !status.success() {
        return None;
    }

    let bin = checkout.join("target/debug/examples").join(name);
    bin.exists().then_some(bin)
}
