//! Community-module auto-load.
//!
//! Walks a `community-modules/<name>/` directory tree and activates
//! any module whose pre-built artifact (`build/<name>.wasm` +
//! `build/manifest.toml`) is present and differs from the currently-
//! activated artifact under `<modules_dir>/<name>/`.
//!
//! Idempotent: re-running with the same artifacts is a no-op. Skips
//! modules without a pre-built artifact (operator hasn't run
//! `mitos-build` yet, or the module is source-only). Logs and
//! continues on per-module errors so a single bad module can't
//! abort host startup.
//!
//! See `docs/strategy/COMMUNITY_MODULES.md` for the design.

use std::path::Path;

use mitos_platform::manifest::Manifest;
use mitos_platform::storage::ModuleStorage;
use tracing::{error, info, warn};

/// Read every `community-modules/<name>/build/` artifact and
/// activate it into `storage` if its sha differs from what's
/// already on disk. Returns the names of modules touched (newly
/// activated or refreshed); skipped modules don't appear.
pub fn auto_load(community_modules_dir: &Path, storage: &ModuleStorage) -> Vec<String> {
    if !community_modules_dir.exists() {
        info!(
            dir = %community_modules_dir.display(),
            "community-modules dir absent; skipping auto-load"
        );
        return Vec::new();
    }

    let entries = match std::fs::read_dir(community_modules_dir) {
        Ok(it) => it,
        Err(e) => {
            error!(
                dir = %community_modules_dir.display(),
                error = %e,
                "community-modules read_dir failed; skipping auto-load"
            );
            return Vec::new();
        }
    };

    let mut activated = Vec::new();
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if !ft.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Skip non-module dirs (e.g. README.md if someone makes a
        // dir for notes). A module dir must contain `<name>.rs` —
        // even if the operator hasn't built it yet.
        let source_rs = entry.path().join(format!("{name}.rs"));
        if !source_rs.exists() {
            continue;
        }

        match load_one(&entry.path(), &name, storage) {
            Ok(touched) => {
                if touched {
                    activated.push(name);
                }
            }
            Err(e) => {
                error!(
                    module = %name,
                    error = %e,
                    "community module auto-load failed; skipping"
                );
            }
        }
    }
    activated
}

fn load_one(module_dir: &Path, name: &str, storage: &ModuleStorage) -> anyhow::Result<bool> {
    let build_dir = module_dir.join("build");
    let manifest_path = build_dir.join("manifest.toml");
    let wasm_path = build_dir.join(format!("{name}.wasm"));

    if !manifest_path.exists() || !wasm_path.exists() {
        warn!(
            module = %name,
            "no pre-built artifact at {}; run `mitos-build --module {}` to produce one",
            build_dir.display(),
            module_dir.join(format!("{name}.rs")).display()
        );
        return Ok(false);
    }

    let manifest_str = std::fs::read_to_string(&manifest_path)?;
    let manifest = Manifest::parse(&manifest_str)?;
    if manifest.module.id != name {
        anyhow::bail!(
            "manifest id `{}` doesn't match dir name `{}`",
            manifest.module.id,
            name
        );
    }
    let wasm_bytes = std::fs::read(&wasm_path)?;

    // Idempotent: skip if storage already has this exact sha.
    if let Ok(Some(existing)) = storage.read_manifest(name)
        && existing.module.sha256 == manifest.module.sha256
    {
        info!(
            module = %name,
            sha = %manifest.module.sha256,
            "community module already active; skipping"
        );
        return Ok(false);
    }

    storage.activate(&manifest, &wasm_bytes)?;
    info!(
        module = %name,
        sha = %manifest.module.sha256,
        size = wasm_bytes.len(),
        "community module activated"
    );

    // CBOR config alongside the wasm — same convention mitos-build
    // emits. Optional; modules without runtime config get an empty
    // init call.
    let config_path = build_dir.join("config.cbor");
    if config_path.exists() {
        let bytes = std::fs::read(&config_path)?;
        storage.write_config(name, &bytes)?;
        info!(
            module = %name,
            bytes = bytes.len(),
            "community module config.cbor written"
        );
    }

    Ok(true)
}
