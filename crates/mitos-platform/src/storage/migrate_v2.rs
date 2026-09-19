//! One-shot migration of the v2 on-disk layout into v3's instance-keyed one.
//!
//! v2 keyed every byte by the bare module id:
//!
//! ```text
//! <root>/<module_id>/
//!   current.wasm -> <sha>.wasm     manifest.toml     config.cbor
//!   <sha>.wasm  <old-sha>.wasm ... (≈20 "rollback targets")
//!   kv.redb  cursor.redb  watched.redb  emissions.redb  last-trap.toml
//!   companions/<client_id>/<key>.cbor
//! ```
//!
//! v3 splits that into content-addressed artifacts and an instance
//! directory — see the [`super`] module docs for the target shape.
//!
//! # The two properties that matter
//!
//! **It is a data move, not a judgement.** v2 kept old shas as rollback
//! targets but only ever kept the CURRENT manifest, so those artifacts can
//! neither be validated nor started. They are moved aside to
//! `_cas/orphaned-v2/`, not deleted. Reclaiming them is GC's job.
//!
//! **It converges from any crash.** Every step tolerates "already done"
//! (source missing AND destination present ⇒ skip), and `current.toml` is the
//! commit point — writing it is what makes a directory stop being v2. A
//! module that is interrupted anywhere is finished by the next boot, and a
//! module that fails outright is logged and skipped so it cannot hold up the
//! others.
//!
//! The state files move by `rename`, which is instant even for the 2.1G
//! `emissions.redb` on the production box. A rename that would cross a
//! filesystem is FATAL rather than falling back to a copy: a half-copied redb
//! is worse than an unmigrated one.

use std::path::{Path, PathBuf};

use mitos_lifecycle::ids::{InstanceKey, ModuleId, Sha256Hex};

use super::{CURRENT_POINTER, ModuleStorage, StorageError};
use crate::manifest::{Manifest, sha256_hex};

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("manifest: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),
    #[error("id: {0}")]
    Id(#[from] mitos_lifecycle::ids::IdError),
    #[error("{env}=`{got}` is not a mode — expected `apply` or `plan`")]
    UnknownMode { env: &'static str, got: String },
    #[error("directory `{dir}` holds a manifest for module `{claimed}` — refusing to migrate it")]
    IdMismatch { dir: String, claimed: String },
    #[error(
        "renaming {from} to {to} crosses a filesystem boundary; a half-copied redb is worse than \
         an unmigrated one, so this is fatal — move the storage root onto one filesystem first"
    )]
    CrossDevice { from: String, to: String },
    #[error("{what} is missing from both the module directory and the content store")]
    NothingToMigrate { what: String },
    #[error("{path} exists at both the source and the destination — refusing to pick one")]
    AmbiguousState { path: String },
    /// Only ever produced by the resumability test's fault injection.
    #[cfg(test)]
    #[error("injected fault after {0:?}")]
    InjectedFault(Step),
}

/// How much of the migration actually runs.
///
/// A named mode rather than a `--dry-run` boolean because `plan` has a
/// different OUTCOME, not a quieter one: the host refuses to serve afterwards,
/// so the operator reads the log and then decides. That is the whole point of
/// running it on the box before the real deploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    Apply,
    Plan,
}

impl MigrationMode {
    pub const ENV: &'static str = "MITOS_STORAGE_MIGRATION";

    pub fn from_env() -> Result<Self, MigrationError> {
        match std::env::var(Self::ENV) {
            Err(_) => Ok(Self::Apply),
            Ok(raw) => Self::parse(raw.trim()),
        }
    }

    fn parse(value: &str) -> Result<Self, MigrationError> {
        match value {
            "" | "apply" => Ok(Self::Apply),
            "plan" => Ok(Self::Plan),
            other => Err(MigrationError::UnknownMode {
                env: Self::ENV,
                got: other.to_owned(),
            }),
        }
    }
}

/// What one pass did, logged by the host as a single structured line.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Modules whose layout this pass moved (including ones it merely
    /// finished after an earlier interrupted pass).
    pub migrated: Vec<String>,
    /// Modules already in the v3 layout — nothing to do.
    pub already_v3: Vec<String>,
    /// Modules that FAILED to migrate and were skipped. Non-empty means the
    /// boot did not fully land: a module with no `current.toml` reads as
    /// unregistered through the facade, so it is simply NOT RUNNING. Its own
    /// bucket for the reason `AutoLoadSummary` has four — `migrated = 19`
    /// reads as healthy whether or not a twentieth was silently dropped.
    pub failed: Vec<String>,
    /// Old `<sha>.wasm` rollback targets moved to `_cas/orphaned-v2/`.
    pub orphaned_artifacts: usize,
}

/// Where the fault-injection hook can fire. One variant per step of the
/// sequence below; the resumability test runs the migration with a fault
/// after each in turn and asserts a clean re-run still converges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Step {
    /// Read `manifest.toml` (and any `config.cbor`) into an `InstanceKey`.
    /// Touches nothing.
    ReadKey,
    /// Copy the current wasm, its manifest and its config into `_cas/`.
    CopyToCas,
    /// Move every OTHER `<sha>.wasm` to `_cas/orphaned-v2/<module>/`.
    MoveOrphans,
    /// Move the redb files and `last-trap.toml` into the instance directory.
    MoveState,
    /// Write `instance.toml`, then `current.toml`. The commit point.
    Commit,
    /// Remove the v2 originals, `manifest.toml` last.
    RemoveOriginals,
}

/// Bring every module directory under `storage`'s root into the v3 layout.
pub fn run(
    storage: &ModuleStorage,
    mode: MigrationMode,
) -> Result<MigrationReport, MigrationError> {
    run_with_hook(storage, mode, &|_| Ok(()))
}

pub(crate) fn run_with_hook(
    storage: &ModuleStorage,
    mode: MigrationMode,
    after_step: &dyn Fn(Step) -> Result<(), MigrationError>,
) -> Result<MigrationReport, MigrationError> {
    let root = storage.root().to_path_buf();
    let mut report = MigrationReport::default();
    if !root.exists() {
        return Ok(report);
    }

    let mut dirs: Vec<(String, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        // Reserved directories (`_cas`) can never be module ids.
        if name.starts_with('_') {
            continue;
        }
        dirs.push((name, entry.path()));
    }
    dirs.sort();

    let mover = Mover { mode };
    for (name, dir) in dirs {
        match classify(&dir) {
            Shape::NotAModule => continue,
            Shape::AlreadyV3 => report.already_v3.push(name),
            Shape::NeedsMigration => {
                match migrate_module(storage, &mover, &dir, &name, after_step) {
                    Ok(orphaned) => {
                        report.orphaned_artifacts += orphaned;
                        report.migrated.push(name);
                    }
                    // Deliberately not fatal. One unreadable module must not
                    // keep the rest of the host's modules on the old layout,
                    // and because every step is idempotent the failed one is
                    // either untouched or resumable on the next boot.
                    Err(e) => {
                        tracing::error!(
                            module = %name,
                            error = %e,
                            "storage migration failed for this module; skipping it — the other \
                             modules still migrate and a re-run resumes this one",
                        );
                        report.failed.push(name);
                    }
                }
            }
        }
    }
    Ok(report)
}

enum Shape {
    NotAModule,
    AlreadyV3,
    NeedsMigration,
}

/// A top-level `manifest.toml` is what makes a directory v2. It survives
/// until the last step, so a directory that has BOTH it and a `current.toml`
/// is a migration that was interrupted after the commit point — finishing it
/// is the same code path.
fn classify(dir: &Path) -> Shape {
    if dir.join("manifest.toml").is_file() {
        Shape::NeedsMigration
    } else if dir.join(CURRENT_POINTER).is_file() {
        Shape::AlreadyV3
    } else {
        Shape::NotAModule
    }
}

/// The five per-instance state files, in the order the design doc lists them.
const STATE_FILES: [&str; 5] = [
    "kv.redb",
    "cursor.redb",
    "watched.redb",
    "emissions.redb",
    "last-trap.toml",
];

fn migrate_module(
    storage: &ModuleStorage,
    mover: &Mover,
    dir: &Path,
    name: &str,
    after_step: &dyn Fn(Step) -> Result<(), MigrationError>,
) -> Result<usize, MigrationError> {
    // --- Step 1: identity -------------------------------------------------
    let manifest_src = dir.join("manifest.toml");
    let manifest = Manifest::parse(&std::fs::read_to_string(&manifest_src)?)?;
    if manifest.module.id != name {
        return Err(MigrationError::IdMismatch {
            dir: name.to_owned(),
            claimed: manifest.module.id.clone(),
        });
    }
    let module = ModuleId::parse(name)?;
    let wasm_sha = Sha256Hex::parse(&manifest.module.sha256)?;
    let config_src = dir.join("config.cbor");

    // A pointer already on disk is the record of what step 5 committed, and
    // it wins. Without this a re-run that happens after step 6 has removed
    // `config.cbor` would derive a config-less key and build a SECOND
    // instance directory beside the one it already migrated.
    let committed = read_pointer(dir, name)?;
    let key = match committed {
        Some(key) => key,
        None => {
            let config = if config_src.is_file() {
                Some(Sha256Hex::parse(&sha256_hex(&std::fs::read(&config_src)?))?)
            } else {
                None
            };
            InstanceKey {
                module,
                wasm: wasm_sha.clone(),
                config,
            }
        }
    };
    after_step(Step::ReadKey)?;

    // --- Step 2: copy the live artifact + manifest + config into the CAS --
    // COPY, not move: the originals stay as the fallback until step 6, so an
    // interruption here leaves a directory that is still a working v2 module.
    let wasm_src = dir.join(format!("{wasm_sha}.wasm"));
    mover.copy(&wasm_src, &storage.artifact_path(&key.wasm))?;
    mover.copy(&manifest_src, &storage.artifact_manifest_path(&key.wasm))?;
    if let Some(config_sha) = &key.config {
        mover.copy(&config_src, &storage.config_bytes_path(config_sha))?;
    }
    after_step(Step::CopyToCas)?;

    // --- Step 3: move the old rollback shas aside -------------------------
    let orphan_dir = storage
        .root()
        .join(super::CAS_DIR)
        .join("orphaned-v2")
        .join(name);
    let mut orphaned = 0;
    for old in old_artifacts(dir, &wasm_src)? {
        let dest = orphan_dir.join(old.file_name().expect("artifact file name"));
        mover.rename(&old, &dest)?;
        orphaned += 1;
    }
    after_step(Step::MoveOrphans)?;

    // --- Step 4: move the state files into the instance directory ---------
    let instance_dir = storage.instance_dir(&key);
    mover.mkdir(&instance_dir)?;
    for file in STATE_FILES {
        mover.rename(&dir.join(file), &instance_dir.join(file))?;
    }
    after_step(Step::MoveState)?;

    // --- Step 5: commit ---------------------------------------------------
    mover.write_marker(storage, &key)?;
    mover.set_current(storage, &key)?;
    after_step(Step::Commit)?;

    // --- Step 6: drop the originals, `manifest.toml` LAST -----------------
    // The manifest is what `classify` keys off, so removing it last means a
    // crash anywhere in this step leaves the directory classified as
    // "needs migration" and the next pass finishes the removals.
    mover.remove(&dir.join("current.wasm"))?;
    mover.remove(&wasm_src)?;
    mover.remove(&config_src)?;
    mover.remove(&manifest_src)?;
    after_step(Step::RemoveOriginals)?;

    Ok(orphaned)
}

fn read_pointer(dir: &Path, name: &str) -> Result<Option<InstanceKey>, MigrationError> {
    let path = dir.join(CURRENT_POINTER);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let key = toml::from_str(&raw).map_err(|e| StorageError::MalformedPointer {
        module: name.to_owned(),
        file: CURRENT_POINTER,
        why: e.to_string(),
    })?;
    Ok(Some(key))
}

/// Every `<sha>.wasm` in the module directory except the live one. Skips
/// `current.wasm` — it ends in `.wasm` but is the symlink, removed in step 6.
fn old_artifacts(dir: &Path, live: &Path) -> Result<Vec<PathBuf>, MigrationError> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != "wasm") {
            continue;
        }
        if path == live || entry.file_name() == "current.wasm" {
            continue;
        }
        // A symlink to a sha artifact is v2's `current.wasm` under another
        // name; only real files are rollback targets.
        if !entry.file_type()?.is_file() {
            continue;
        }
        out.push(path);
    }
    out.sort();
    Ok(out)
}

/// Every filesystem mutation the migration makes goes through this.
///
/// `plan` is therefore the SAME traversal making the SAME decisions — it just
/// logs instead of acting. A separate dry-run walk would be a second
/// implementation of the migration, and the two would drift.
struct Mover {
    mode: MigrationMode,
}

impl Mover {
    fn planning(&self) -> bool {
        self.mode == MigrationMode::Plan
    }

    /// Copy, skipping when the destination is already there. A missing source
    /// with the destination present is a resumed run; missing on both sides
    /// means the data is simply gone, which is an error we must not paper
    /// over.
    fn copy(&self, from: &Path, to: &Path) -> Result<(), MigrationError> {
        if to.exists() {
            return Ok(());
        }
        if !from.is_file() {
            return Err(MigrationError::NothingToMigrate {
                what: from.display().to_string(),
            });
        }
        tracing::info!(from = %from.display(), to = %to.display(), "storage migration: copy");
        if self.planning() {
            return Ok(());
        }
        std::fs::create_dir_all(to.parent().expect("destination parent"))?;
        // Copy to a sibling and rename into place. The skip above trusts
        // "destination exists" to mean "destination is COMPLETE", and a crash
        // halfway through a direct copy would break that: the next pass would
        // skip a truncated `<sha>.wasm` — a content-addressed file holding the
        // wrong content, which nothing downstream re-hashes before running.
        let mut partial = to.as_os_str().to_owned();
        partial.push(".partial");
        let partial = PathBuf::from(partial);
        std::fs::copy(from, &partial)?;
        std::fs::rename(&partial, to)?;
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<(), MigrationError> {
        let from_there = std::fs::symlink_metadata(from).is_ok();
        if !from_there {
            return Ok(());
        }
        if to.exists() {
            return Err(MigrationError::AmbiguousState {
                path: to.display().to_string(),
            });
        }
        tracing::info!(from = %from.display(), to = %to.display(), "storage migration: move");
        if self.planning() {
            return Ok(());
        }
        std::fs::create_dir_all(to.parent().expect("destination parent"))?;
        std::fs::rename(from, to).map_err(|e| {
            // EXDEV. Falling back to a copy here would be the one way to end
            // up with a torn 2.1G redb file.
            if e.raw_os_error() == Some(EXDEV) {
                MigrationError::CrossDevice {
                    from: from.display().to_string(),
                    to: to.display().to_string(),
                }
            } else {
                MigrationError::Io(e)
            }
        })
    }

    fn mkdir(&self, dir: &Path) -> Result<(), MigrationError> {
        if dir.is_dir() {
            return Ok(());
        }
        tracing::info!(dir = %dir.display(), "storage migration: create instance directory");
        if self.planning() {
            return Ok(());
        }
        std::fs::create_dir_all(dir)?;
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<(), MigrationError> {
        // `exists()` follows symlinks, and `current.wasm` is dangling by the
        // time this runs.
        if std::fs::symlink_metadata(path).is_err() {
            return Ok(());
        }
        tracing::info!(path = %path.display(), "storage migration: remove v2 original");
        if self.planning() {
            return Ok(());
        }
        std::fs::remove_file(path)?;
        Ok(())
    }

    fn write_marker(
        &self,
        storage: &ModuleStorage,
        key: &InstanceKey,
    ) -> Result<(), MigrationError> {
        tracing::info!(instance = %key, "storage migration: write instance.toml");
        if self.planning() {
            return Ok(());
        }
        storage.write_instance_marker(key)?;
        Ok(())
    }

    fn set_current(
        &self,
        storage: &ModuleStorage,
        key: &InstanceKey,
    ) -> Result<(), MigrationError> {
        tracing::info!(instance = %key, "storage migration: write current.toml (commit)");
        if self.planning() {
            return Ok(());
        }
        storage.set_current(key)?;
        Ok(())
    }
}

/// "Invalid cross-device link" — 18 on both Linux and macOS. Not exposed by
/// `std`, and pulling `libc` in for one integer is not worth it.
const EXDEV: i32 = 18;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        AbiSection, BuildSection, ModuleSection, TrapPolicySection, host_wit_sha,
    };

    const MODULE: &str = "ownership-indexer";

    fn manifest_for(wasm: &[u8]) -> Manifest {
        Manifest {
            module: ModuleSection {
                id: MODULE.to_owned(),
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
                rust_version: "1.95.0".to_owned(),
                target: "wasm32-wasip2".to_owned(),
                profile: "release".to_owned(),
                build_id: "2026-05-03T12:34:00Z".to_owned(),
                git_sha: None,
                crate_version: "0.1.0".to_owned(),
            },
            interest: crate::manifest::InterestSection::default(),
            lifecycle: Default::default(),
        }
    }

    const LIVE_WASM: &[u8] = b"the live artifact bytes";
    const CONFIG: &[u8] = b"\xa1\x64conf\x01";

    /// A realistic v2 module directory: the live sha, three old rollback
    /// shas, the `current.wasm` symlink, all four redb files, a config, a
    /// last-trap fixture and a companion registration.
    fn v2_module(root: &Path) -> (PathBuf, Manifest) {
        let dir = root.join(MODULE);
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = manifest_for(LIVE_WASM);
        let live = format!("{}.wasm", manifest.module.sha256);
        std::fs::write(dir.join(&live), LIVE_WASM).unwrap();
        std::fs::write(dir.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
        std::fs::write(dir.join("config.cbor"), CONFIG).unwrap();
        std::os::unix::fs::symlink(&live, dir.join("current.wasm")).unwrap();

        for old in ["aa", "bb", "cc"] {
            std::fs::write(dir.join(format!("{}.wasm", old.repeat(32))), old).unwrap();
        }
        for (file, body) in [
            ("kv.redb", "kv-bytes"),
            ("cursor.redb", "cursor-bytes"),
            ("watched.redb", "watched-bytes"),
            ("emissions.redb", "emissions-bytes"),
            ("last-trap.toml", "trap-bytes"),
        ] {
            std::fs::write(dir.join(file), body).unwrap();
        }

        let companion = dir.join("companions").join("hooks.epochify.space");
        std::fs::create_dir_all(&companion).unwrap();
        std::fs::write(companion.join("customer_42.cbor"), b"sub").unwrap();

        (dir, manifest)
    }

    fn expected_key(manifest: &Manifest) -> InstanceKey {
        InstanceKey {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: Sha256Hex::parse(&manifest.module.sha256).unwrap(),
            config: Some(Sha256Hex::parse(&sha256_hex(CONFIG)).unwrap()),
        }
    }

    /// Every path under `root` with its size, relative and sorted — enough to
    /// compare two layouts file by file.
    fn tree(root: &Path) -> Vec<(String, u64)> {
        fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, u64)>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let rel = path
                    .strip_prefix(base)
                    .expect("under base")
                    .display()
                    .to_string();
                let meta = std::fs::symlink_metadata(&path).expect("metadata");
                if meta.is_dir() {
                    out.push((format!("{rel}/"), 0));
                    walk(&path, base, out);
                } else {
                    out.push((rel, meta.len()));
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out.sort();
        out
    }

    #[test]
    fn migrates_a_v2_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (dir, manifest) = v2_module(root);
        let storage = ModuleStorage::new(root);

        let report = run(&storage, MigrationMode::Apply).unwrap();
        assert_eq!(report.migrated, vec![MODULE.to_owned()]);
        assert!(report.already_v3.is_empty());
        assert_eq!(report.orphaned_artifacts, 3);

        let key = expected_key(&manifest);

        // Artifacts are content-addressed, with the manifest beside them.
        assert_eq!(
            std::fs::read(storage.artifact_path(&key.wasm)).unwrap(),
            LIVE_WASM
        );
        assert_eq!(
            storage
                .read_artifact_manifest(&key.wasm)
                .unwrap()
                .expect("manifest in cas"),
            manifest
        );
        assert_eq!(
            std::fs::read(storage.config_bytes_path(key.config.as_ref().unwrap())).unwrap(),
            CONFIG
        );

        // State lives in the instance directory, and only there.
        let instance = storage.instance_dir(&key);
        for file in STATE_FILES {
            assert!(instance.join(file).is_file(), "{file} not in instance dir");
            assert!(!dir.join(file).exists(), "{file} left in module dir");
        }
        assert!(instance.join("instance.toml").is_file());
        assert_eq!(
            storage.current_instance(MODULE).unwrap().unwrap(),
            key,
            "current.toml must name the migrated instance"
        );
        assert_eq!(storage.list_instances(&key.module).unwrap(), vec![key]);

        // Companions belong to the MODULE, not to one instance of it.
        assert!(
            dir.join("companions")
                .join("hooks.epochify.space")
                .join("customer_42.cbor")
                .is_file(),
        );

        // Old rollback shas moved aside; none deleted.
        let orphans = root.join("_cas").join("orphaned-v2").join(MODULE);
        for old in ["aa", "bb", "cc"] {
            assert!(orphans.join(format!("{}.wasm", old.repeat(32))).is_file());
        }

        // v2 originals are gone.
        assert!(!dir.join("manifest.toml").exists());
        assert!(!dir.join("config.cbor").exists());
        assert!(std::fs::symlink_metadata(dir.join("current.wasm")).is_err());
        assert!(
            !dir.join(format!("{}.wasm", manifest.module.sha256))
                .exists()
        );

        // And the cursor reads back through the facade, byte for byte.
        assert_eq!(
            std::fs::read(storage.cursor_path(MODULE).unwrap()).unwrap(),
            b"cursor-bytes"
        );
    }

    #[test]
    fn migration_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let (_dir, _manifest) = v2_module(tmp.path());
        let storage = ModuleStorage::new(tmp.path());

        run(&storage, MigrationMode::Apply).unwrap();
        let after_first = tree(tmp.path());

        let second = run(&storage, MigrationMode::Apply).unwrap();
        assert!(second.migrated.is_empty());
        assert_eq!(second.already_v3, vec![MODULE.to_owned()]);
        assert_eq!(second.orphaned_artifacts, 0);
        assert_eq!(
            tree(tmp.path()),
            after_first,
            "a second pass changed the tree"
        );
    }

    #[test]
    fn migration_is_resumable_from_every_step() {
        let reference = {
            let tmp = tempfile::tempdir().unwrap();
            v2_module(tmp.path());
            let storage = ModuleStorage::new(tmp.path());
            run(&storage, MigrationMode::Apply).unwrap();
            tree(tmp.path())
        };

        for fault in [
            Step::ReadKey,
            Step::CopyToCas,
            Step::MoveOrphans,
            Step::MoveState,
            Step::Commit,
            Step::RemoveOriginals,
        ] {
            let tmp = tempfile::tempdir().unwrap();
            v2_module(tmp.path());
            let storage = ModuleStorage::new(tmp.path());

            // The interrupted pass. The per-module error is swallowed by
            // design (one bad module must not stop the others), so the proof
            // that the fault landed is the report: nothing was migrated.
            let interrupted = run_with_hook(&storage, MigrationMode::Apply, &|step| {
                if step == fault {
                    Err(MigrationError::InjectedFault(step))
                } else {
                    Ok(())
                }
            })
            .unwrap();
            assert!(
                interrupted.migrated.is_empty(),
                "fault after {fault:?} should have aborted the module"
            );

            // The restart.
            let resumed = ModuleStorage::new(tmp.path());
            run(&resumed, MigrationMode::Apply).unwrap();
            assert_eq!(
                tree(tmp.path()),
                reference,
                "a crash after {fault:?} did not converge on the uninterrupted layout"
            );
        }
    }

    #[test]
    fn plan_mode_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        v2_module(tmp.path());
        let storage = ModuleStorage::new(tmp.path());

        let before = tree(tmp.path());
        let report = run(&storage, MigrationMode::Plan).unwrap();
        assert_eq!(report.migrated, vec![MODULE.to_owned()]);
        assert_eq!(report.orphaned_artifacts, 3);
        assert_eq!(tree(tmp.path()), before, "plan mode touched the tree");
    }

    #[test]
    fn a_module_that_fails_to_migrate_does_not_stop_the_others() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        v2_module(root);

        // A second module whose manifest is not parseable.
        let broken = root.join("broken-indexer");
        std::fs::create_dir_all(&broken).unwrap();
        std::fs::write(broken.join("manifest.toml"), "this is not toml = = =").unwrap();
        std::fs::write(broken.join("cursor.redb"), "cursor-bytes").unwrap();
        let broken_before = tree(&broken);

        let storage = ModuleStorage::new(root);
        let report = run(&storage, MigrationMode::Apply).unwrap();

        assert_eq!(report.migrated, vec![MODULE.to_owned()]);
        assert_eq!(
            storage.list_modules().unwrap(),
            vec![MODULE.to_owned()],
            "the broken module must not become registered"
        );
        assert_eq!(
            tree(&broken),
            broken_before,
            "the failed module must be left untouched, not half-migrated"
        );
    }

    #[test]
    fn a_v2_module_without_a_config_migrates_to_a_configless_instance() {
        let tmp = tempfile::tempdir().unwrap();
        let (dir, manifest) = v2_module(tmp.path());
        std::fs::remove_file(dir.join("config.cbor")).unwrap();
        let storage = ModuleStorage::new(tmp.path());

        run(&storage, MigrationMode::Apply).unwrap();

        let key = storage.current_instance(MODULE).unwrap().unwrap();
        assert_eq!(key.config, None);
        assert_eq!(key.wasm.as_str(), manifest.module.sha256);
        assert!(storage.read_config(MODULE).unwrap().is_none());
    }

    #[test]
    fn the_migration_mode_is_a_named_enum_not_a_flag() {
        assert_eq!(MigrationMode::parse("").unwrap(), MigrationMode::Apply);
        assert_eq!(MigrationMode::parse("apply").unwrap(), MigrationMode::Apply);
        assert_eq!(MigrationMode::parse("plan").unwrap(), MigrationMode::Plan);
        assert!(matches!(
            MigrationMode::parse("dry-run"),
            Err(MigrationError::UnknownMode { .. })
        ));
    }
}
