use super::*;
/// `resume_cwd` must validate containment against the CANONICAL path but
/// hand the agent back its OWN spelling.
///
/// The symlinked prefix is built explicitly so this bites on Linux too:
/// `/tmp` is not a symlink there, so the macOS bug this guards
/// (`/var` -> `/private/var`, which every `std::env::temp_dir()` path
/// traverses) was invisible to CI and only reproduced on developer
/// machines — where it failed every resume under a temp cwd.
#[test]
#[cfg(unix)]
fn resume_cwd_validates_canonically_but_returns_the_agent_spelling() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let base = std::env::temp_dir().join(format!("agui-resume-cwd-{unique}"));
    let real = base.join("real");
    let nested = real.join("nested/project");
    std::fs::create_dir_all(&nested).expect("create nested cwd");
    let link = base.join("link");
    std::os::unix::fs::symlink(&real, &link).expect("symlinked prefix");

    let canonical_root = std::fs::canonicalize(&real).expect("canonical root");

    // The agent persisted its cwd spelled through the symlink.
    let reported = link.join("nested/project");
    let chosen = resume_cwd(&reported, &canonical_root).expect("nested cwd is allowed");
    assert_eq!(
        chosen, reported,
        "must hand back the spelling the agent persisted"
    );
    assert_ne!(
        chosen,
        std::fs::canonicalize(&reported).expect("canonical nested"),
        "returning the canonical spelling is the bug this test guards"
    );

    // Containment still resolves symlinks: a link inside the root that
    // points outside it must be rejected, not laundered.
    let escape = link.join("escape");
    std::os::unix::fs::symlink(std::env::temp_dir(), &escape).expect("escape symlink");
    assert!(
        resume_cwd(&escape, &canonical_root).is_err(),
        "a symlink escaping the bridge root must be rejected"
    );

    // Plain outside-root and relative paths stay rejected.
    assert!(resume_cwd(&nested, Path::new("/definitely/not/the/root")).is_err());
    assert!(resume_cwd(Path::new("relative/path"), &canonical_root).is_err());

    std::fs::remove_dir_all(&base).ok();
}
