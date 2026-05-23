//! Build script: stamp the git short SHA into `MITOS_BUILD_SHA`.
//!
//! The workspace crate version is a static `0.0.1`, so it isn't a
//! meaningful "what's actually deployed" identifier — and the deploy
//! path is build-on-box (`git pull` + `cargo build`), where the git
//! SHA is the honest answer. `GET /_admin/status` reports this via
//! `env!("MITOS_BUILD_SHA")`.
//!
//! Best-effort: if `git` isn't available or this isn't a checkout
//! (tarball / vendored build), the stamp falls back to `unknown`.

use std::path::Path;
use std::process::Command;

fn main() {
    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=MITOS_BUILD_SHA={sha}");

    // Re-run only when HEAD (or the branch ref it points at) moves,
    // so a fresh SHA is stamped after a commit without forcing this
    // big crate to rebuild on every `cargo build`. The crate lives at
    // `crates/mitos-platform`, so the repo root is two levels up.
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
