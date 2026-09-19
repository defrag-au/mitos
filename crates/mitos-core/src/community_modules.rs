//! Community-module auto-load.
//!
//! Walks a `community-modules/<name>/` directory tree and activates
//! any module whose pre-built artifact is present and differs from
//! the currently-activated artifact under `<modules_dir>/<name>/`.
//! The artifact lives at `target/mitos/<name>/<name>.wasm` paired
//! with `target/mitos/<name>/manifest.toml` — that path matches
//! the default `--out` of `mitos-build`, so a deploy that runs
//! `mitos-build --module <path>` for each community module on the
//! box drops the artifacts straight where auto-load reads them.
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

/// What auto-load did with one module dir.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadOutcome {
    /// Artifact differed from what storage holds; now activated.
    Activated,
    /// Artifact matches the activated one — nothing to do.
    AlreadyActive,
    /// No pre-built artifact; operator hasn't run `mitos-build`.
    NoArtifact,
}

/// Per-outcome tally from one auto-load pass.
///
/// Four named buckets rather than one "touched" count, because the
/// interesting states are the ones that aren't activation: a run
/// reporting `activated=0` is perfectly healthy when the other 19
/// are `already_active`, and alarming when they're `refused`. The
/// single count couldn't tell those apart.
#[derive(Debug, Default)]
pub struct AutoLoadSummary {
    /// Newly activated or refreshed.
    pub activated: Vec<String>,
    /// Already on this exact sha.
    pub already_active: Vec<String>,
    /// Module dir with source but no built artifact.
    pub no_artifact: Vec<String>,
    /// Failed validation or errored — NOT activated. Non-empty here
    /// means the deploy did not fully land.
    pub refused: Vec<String>,
}

/// Read every `community-modules/<name>/build/` artifact and
/// activate it into `storage` if its sha differs from what's
/// already on disk.
pub fn auto_load(community_modules_dir: &Path, storage: &ModuleStorage) -> AutoLoadSummary {
    if !community_modules_dir.exists() {
        info!(
            dir = %community_modules_dir.display(),
            "community-modules dir absent; skipping auto-load"
        );
        return AutoLoadSummary::default();
    }

    let entries = match std::fs::read_dir(community_modules_dir) {
        Ok(it) => it,
        Err(e) => {
            error!(
                dir = %community_modules_dir.display(),
                error = %e,
                "community-modules read_dir failed; skipping auto-load"
            );
            return AutoLoadSummary::default();
        }
    };

    let mut summary = AutoLoadSummary::default();
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
        // even if the operator hasn't built it yet. Rust filenames
        // can't contain hyphens, so the source file uses
        // underscores even when the module id uses hyphens (e.g.
        // `jpg-co/jpg_co.rs`).
        let source_stem = name.replace('-', "_");
        let source_rs = entry.path().join(format!("{source_stem}.rs"));
        if !source_rs.exists() {
            continue;
        }

        match load_one(&entry.path(), &name, storage) {
            Ok(LoadOutcome::Activated) => summary.activated.push(name),
            Ok(LoadOutcome::AlreadyActive) => summary.already_active.push(name),
            Ok(LoadOutcome::NoArtifact) => summary.no_artifact.push(name),
            Err(e) => {
                error!(
                    module = %name,
                    error = %e,
                    "community module REFUSED; not activated"
                );
                summary.refused.push(name);
            }
        }
    }
    summary
}

fn load_one(module_dir: &Path, name: &str, storage: &ModuleStorage) -> anyhow::Result<LoadOutcome> {
    // `mitos-build --module <name>.rs` writes its artifact to
    // `<workspace>/target/mitos/<module-id>/` by default. When the
    // workspace is the per-module dir (single-file shape), that's
    // `community-modules/<name>/target/mitos/<name>/`.
    let build_dir = module_dir.join("target").join("mitos").join(name);
    let manifest_path = build_dir.join("manifest.toml");
    let wasm_path = build_dir.join(format!("{name}.wasm"));

    if !manifest_path.exists() || !wasm_path.exists() {
        let source_stem = name.replace('-', "_");
        warn!(
            module = %name,
            "no pre-built artifact at {}; run `mitos-build --module {}` to produce one",
            build_dir.display(),
            module_dir.join(format!("{source_stem}.rs")).display()
        );
        return Ok(LoadOutcome::NoArtifact);
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

    // Same validation the admin upload path applies
    // (`mitos_platform::admin`): module id, trap strategy, ABI major
    // + wit world + wit revision, and the wasm's own sha/size against
    // what the manifest claims.
    //
    // This runs BEFORE the idempotency check on purpose. A stale
    // artifact has the same sha on both sides, so checking "already
    // active" first would report it as a cheerful skip — which is
    // precisely how an ABI-skewed module stays live across a deploy.
    // Validate first and the skew surfaces every boot.
    manifest.validate_against_host(
        &wasm_bytes,
        &[(
            mitos_platform::registry_v2::HOST_ABI_MAJOR_V2,
            "mitos:platform-v2/mitos-module-v2",
        )],
    )?;

    // Idempotent: skip if storage already has this exact sha.
    if let Ok(Some(existing)) = storage.read_manifest(name)
        && existing.module.sha256 == manifest.module.sha256
    {
        info!(
            module = %name,
            sha = %manifest.module.sha256,
            "community module already active; skipping"
        );
        return Ok(LoadOutcome::AlreadyActive);
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

    Ok(LoadOutcome::Activated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitos_platform::manifest::{
        AbiSection, BuildSection, Manifest, ModuleSection, TrapPolicySection, host_wit_sha,
        sha256_hex,
    };

    fn sample_manifest(id: &str, wasm: &[u8]) -> Manifest {
        Manifest {
            module: ModuleSection {
                id: id.to_owned(),
                sha256: sha256_hex(wasm),
                size_bytes: wasm.len() as u64,
            },
            abi: AbiSection {
                version_major: 2,
                version_minor: 0,
                wit_package: "mitos:platform-v2".to_owned(),
                wit_world: "mitos-module-v2".to_owned(),
                wit_sha: Some(host_wit_sha().to_owned()),
            },
            trap_policy: TrapPolicySection {
                strategy: "replay".to_owned(),
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
            build: BuildSection {
                rust_version: "1.94.1".to_owned(),
                target: "wasm32-wasip2".to_owned(),
                profile: "release".to_owned(),
                build_id: "2026-05-11T00:00:00Z".to_owned(),
                git_sha: None,
                crate_version: "0.0.0".to_owned(),
            },
            interest: Default::default(),
        }
    }

    fn write_module(
        community_dir: &Path,
        dir_name: &str,
        source_stem: &str,
        wasm: &[u8],
    ) -> std::path::PathBuf {
        let module_dir = community_dir.join(dir_name);
        let build_dir = module_dir.join("target").join("mitos").join(dir_name);
        std::fs::create_dir_all(&build_dir).unwrap();
        // Source file presence is what auto-load uses to skip
        // non-module dirs.
        std::fs::write(module_dir.join(format!("{source_stem}.rs")), "").unwrap();
        let manifest = sample_manifest(dir_name, wasm);
        std::fs::write(module_dir.join(format!("{source_stem}.toml")), "").unwrap();
        std::fs::write(build_dir.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
        std::fs::write(build_dir.join(format!("{dir_name}.wasm")), wasm).unwrap();
        module_dir
    }

    #[test]
    fn skips_when_dir_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&tmp.path().join("does-not-exist"), &storage);
        assert!(summary.activated.is_empty());
        assert!(summary.refused.is_empty());
    }

    #[test]
    fn hyphen_dir_with_underscore_source_activates() {
        // The canonical case: directory is hyphen-cased (`jpg-co`)
        // because module ids must be `[a-z0-9-]+`; source filename
        // is underscore-cased (`jpg_co.rs`) because Rust filenames
        // can't contain hyphens.
        let tmp = tempfile::tempdir().unwrap();
        let community_dir = tmp.path().join("community-modules");
        std::fs::create_dir_all(&community_dir).unwrap();
        write_module(&community_dir, "jpg-co", "jpg_co", b"fake wasm bytes");

        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&community_dir, &storage);
        assert_eq!(summary.activated, vec!["jpg-co".to_owned()]);
        // Re-running is idempotent — sha matches, no re-activation.
        // It lands in `already_active`, NOT in a bare "nothing
        // happened": that distinction is the point of the buckets.
        let summary2 = auto_load(&community_dir, &storage);
        assert!(summary2.activated.is_empty());
        assert_eq!(summary2.already_active, vec!["jpg-co".to_owned()]);
        assert!(summary2.refused.is_empty());
    }

    #[test]
    fn refuses_module_built_against_a_different_wit() {
        // The skew that shipped stale modules: everything matches
        // except the WIT revision the bindings came from.
        let tmp = tempfile::tempdir().unwrap();
        let community_dir = tmp.path().join("community-modules");
        std::fs::create_dir_all(&community_dir).unwrap();
        let wasm = b"fake wasm bytes";
        let module_dir = community_dir.join("jpg-co");
        let build_dir = module_dir.join("target").join("mitos").join("jpg-co");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(module_dir.join("jpg_co.rs"), "").unwrap();
        let mut manifest = sample_manifest("jpg-co", wasm);
        manifest.abi.wit_sha = Some("11".repeat(32));
        std::fs::write(build_dir.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
        std::fs::write(build_dir.join("jpg-co.wasm"), wasm).unwrap();

        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&community_dir, &storage);
        assert!(
            summary.activated.is_empty(),
            "a module built against another WIT must not activate"
        );
        assert_eq!(summary.refused, vec!["jpg-co".to_owned()]);
    }

    #[test]
    fn refuses_wasm_that_does_not_match_its_manifest() {
        // Auto-load used to activate whatever bytes were on disk
        // without checking them against the manifest — the admin
        // upload path validated, this one didn't.
        let tmp = tempfile::tempdir().unwrap();
        let community_dir = tmp.path().join("community-modules");
        std::fs::create_dir_all(&community_dir).unwrap();
        let module_dir = community_dir.join("jpg-co");
        let build_dir = module_dir.join("target").join("mitos").join("jpg-co");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(module_dir.join("jpg_co.rs"), "").unwrap();
        // Manifest describes one set of bytes; the wasm is another.
        let manifest = sample_manifest("jpg-co", b"the bytes the manifest describes");
        std::fs::write(build_dir.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
        std::fs::write(build_dir.join("jpg-co.wasm"), b"different bytes entirely").unwrap();

        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&community_dir, &storage);
        assert!(summary.activated.is_empty());
        assert_eq!(summary.refused, vec!["jpg-co".to_owned()]);
    }

    #[test]
    fn skips_dir_without_source_file() {
        let tmp = tempfile::tempdir().unwrap();
        let community_dir = tmp.path().join("community-modules");
        std::fs::create_dir_all(community_dir.join("scratch")).unwrap();
        // No `<name>.rs` — auto-load treats this as a non-module
        // directory (e.g. README, notes).

        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&community_dir, &storage);
        assert!(summary.activated.is_empty());
        // A non-module dir isn't a refusal — it never had a claim.
        assert!(summary.refused.is_empty());
    }

    #[test]
    fn rejects_manifest_id_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let community_dir = tmp.path().join("community-modules");
        std::fs::create_dir_all(&community_dir).unwrap();
        // Write a module dir named `foo-bar` but inject a manifest
        // claiming id `wrong-id`. Auto-load should skip (and log)
        // rather than activate under the wrong id.
        let wasm = b"fake wasm";
        let module_dir = community_dir.join("foo-bar");
        let build_dir = module_dir.join("target").join("mitos").join("foo-bar");
        std::fs::create_dir_all(&build_dir).unwrap();
        std::fs::write(module_dir.join("foo_bar.rs"), "").unwrap();
        let manifest = sample_manifest("wrong-id", wasm);
        std::fs::write(build_dir.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
        std::fs::write(build_dir.join("foo-bar.wasm"), wasm).unwrap();

        let storage = ModuleStorage::new(tmp.path().join("modules"));
        let summary = auto_load(&community_dir, &storage);
        assert!(
            summary.activated.is_empty(),
            "mismatched manifest id must not activate"
        );
        assert_eq!(summary.refused, vec!["foo-bar".to_owned()]);
    }
}
