//! Build script: stamp a build identifier into `MITOS_BUILD_SHA`.
//!
//! The workspace crate version is a static `0.0.1`, so it isn't a
//! meaningful "what's actually deployed" identifier. `GET
//! /_admin/status` reports this stamp via `env!("MITOS_BUILD_SHA")`.
//!
//! Resolution order:
//! 1. `MITOS_BUILD_SHA` env var — set by `scripts/deploy.sh`, which
//!    rsyncs the working tree **without** `.git`, so on-box `git`
//!    can't resolve a SHA. The deploy injects the local tree's
//!    `git describe` here so the deployed binary reports the truth.
//! 2. Local `git describe --always --dirty` — dev builds in a checkout.
//! 3. `"unknown"` — tarball / vendored builds with neither.

use std::path::Path;
use std::process::Command;

fn main() {
    let sha = std::env::var("MITOS_BUILD_SHA")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(git_describe)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=MITOS_BUILD_SHA={sha}");
    // Restamp when the deploy-injected override changes.
    println!("cargo:rerun-if-env-changed=MITOS_BUILD_SHA");

    // For the local `git` fallback: re-run when HEAD (or the branch
    // ref it points at) moves, so a fresh SHA is stamped after a
    // commit without forcing this big crate to rebuild every time.
    // The crate lives at `crates/mitos-platform`, so the repo root is
    // two levels up.
    let git_dir = Path::new("../../.git");
    let head = git_dir.join("HEAD");
    if head.exists() {
        println!("cargo:rerun-if-changed={}", head.display());
        if let Ok(content) = std::fs::read_to_string(&head)
            && let Some(ref_rel) = content.strip_prefix("ref:").map(str::trim)
        {
            println!("cargo:rerun-if-changed={}", git_dir.join(ref_rel).display());
        }
        let packed = git_dir.join("packed-refs");
        if packed.exists() {
            println!("cargo:rerun-if-changed={}", packed.display());
        }
    }
}

/// Local-checkout build identifier: short commit SHA with a `-dirty`
/// suffix when the tree has uncommitted changes. `None` when `git`
/// isn't available or this isn't a checkout.
fn git_describe() -> Option<String> {
    Command::new("git")
        .args(["describe", "--always", "--dirty", "--abbrev=12"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
