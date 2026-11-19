//! Does this toolchain's `wasm32-wasip2` std speak the `wasi:cli` version this
//! host links?
//!
//! The answer is load-bearing and normally invisible. `mitos-platform` embeds a
//! `wasmtime-wasi` that implements exactly one `wasi:cli@0.2.x`, and every
//! module's std imports whatever version its compiler ships. When the two
//! disagree no module instantiates at all — a CI run failed with
//!
//! ```text
//! component imports instance `wasi:cli/exit@0.2.12`,
//! but a matching implementation was not found in the linker
//! ```
//!
//! once for every community module, several minutes in.
//!
//! Wasmtime resolves those imports by exact name, then by semver-compatible
//! version, then typechecks the interface — so what breaks is a version whose
//! interfaces *differ*, not a version string.
//!
//! `fixtures/wasip2-probe` is the smallest artifact that shows the toolchain's
//! choice — its only dependency is std — so the same failure surfaces here, by
//! name, in the fast `test` job. Both CI (`.github/workflows/ci.yaml`) and the
//! Nix devshell install the target, so the probe only skips on a bare rustup
//! checkout that has not.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The probe is a standalone crate — it declares its own `[workspace]` so that
/// nothing links it into this one.
fn probe_manifest() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wasip2-probe/Cargo.toml")
}

/// Is the `wasm32-wasip2` std present for the compiler running this test?
///
/// Asked of the sysroot rather than of `rustup`, so it answers the same way in
/// the Nix devshell, where there is no rustup at all.
fn wasip2_std_installed() -> bool {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let Ok(out) = Command::new(rustc).args(["--print", "sysroot"]).output() else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    let sysroot = String::from_utf8_lossy(&out.stdout);
    Path::new(sysroot.trim())
        .join("lib/rustlib/wasm32-wasip2")
        .is_dir()
}

/// Build the probe and return it with the temporary target directory that must
/// outlive it.
fn build_probe() -> Option<(tempfile::TempDir, PathBuf)> {
    if !wasip2_std_installed() {
        eprintln!(
            "skipped: the wasm32-wasip2 std is not installed for this compiler, so the \
             toolchain/WASI linkage cannot be checked. Install it (`rustup target add \
             wasm32-wasip2`, or use the devshell) — CI always has it."
        );
        return None;
    }

    let target_dir = tempfile::tempdir().expect("a temp dir for the probe build");
    let out = Command::new(env!("CARGO"))
        .args([
            "build",
            "--release",
            "--target",
            "wasm32-wasip2",
            "--manifest-path",
        ])
        .arg(probe_manifest())
        .arg("--target-dir")
        .arg(target_dir.path())
        .output()
        .expect("could not run cargo to build the probe");
    assert!(
        out.status.success(),
        "the wasip2 probe did not build:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let wasm = target_dir
        .path()
        .join("wasm32-wasip2/release/wasip2-probe.wasm");
    assert!(
        wasm.is_file(),
        "cargo reported success but {} is not there",
        wasm.display()
    );
    Some((target_dir, wasm))
}

#[tokio::test]
async fn the_toolchain_std_links_against_this_host() {
    let Some((_build_dir, wasm)) = build_probe() else {
        return;
    };

    let imports = mitos_platform::inspect::link_probe(&wasm)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "a std-only wasm32-wasip2 component does not instantiate against this host's \
                 linker.\n\n{e}\n\nThis means the toolchain's `wasi:cli` and the one this host \
                 implements are no longer interchangeable — see rust-toolchain.toml."
            )
        });

    // The probe earns its place only while it imports the interface the modules
    // do. A probe that stopped would pass here and prove nothing.
    assert!(
        imports
            .iter()
            .any(|name| name.starts_with("wasi:cli/exit@")),
        "the probe no longer imports wasi:cli/exit, so it cannot stand in for a module's std. \
         Imports seen: {imports:?}"
    );
}
