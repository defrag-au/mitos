//! Module artifact storage.
//!
//! Layout per `MITOS_PLATFORM_DEPLOYMENT.md` §"Resolved design
//! questions" #1:
//!
//! ```text
//! <storage_root>/<module_id>/
//!   current.wasm           -> <active-sha>.wasm  (symlink)
//!   <sha-1>.wasm           (rollback target)
//!   <sha-active>.wasm      (current)
//!   manifest.toml          (matches current symlink target)
//!   .upload.lock           (per-module write lock; see #4)
//! ```
//!
//! Atomic activation: write `<new-sha>.wasm`, write
//! `current.wasm.tmp` symlink, `rename(current.wasm.tmp →
//! current.wasm)`. Readers see either old target or new target,
//! never a missing or partial symlink.

use std::path::{Path, PathBuf};

use crate::manifest::Manifest;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),
    #[error("upload in progress for module {0}")]
    UploadInProgress(String),
}

/// Owns the storage root path. Cheap to clone; no internal state.
#[derive(Clone)]
pub struct ModuleStorage {
    root: PathBuf,
}

impl ModuleStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn module_dir(&self, id: &str) -> PathBuf {
        self.root.join(id)
    }

    fn artifact_path(&self, id: &str, sha: &str) -> PathBuf {
        self.module_dir(id).join(format!("{sha}.wasm"))
    }

    fn current_symlink(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("current.wasm")
    }

    fn manifest_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("manifest.toml")
    }

    fn lock_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join(".upload.lock")
    }

    /// Best-effort per-module lock — creates a lockfile, returns
    /// a guard that removes it on drop. Returns
    /// `UploadInProgress` if the lockfile already exists and is
    /// fresh (within 5 minutes); reaps stale locks (older) as
    /// part of acquisition. Per §"Resolved design questions" #4.
    pub fn acquire_upload_lock(&self, id: &str) -> Result<UploadLockGuard, StorageError> {
        std::fs::create_dir_all(self.module_dir(id))?;
        let lock = self.lock_path(id);
        if lock.exists() {
            let mtime = std::fs::metadata(&lock)?.modified()?;
            let age = std::time::SystemTime::now()
                .duration_since(mtime)
                .unwrap_or(std::time::Duration::ZERO);
            if age < std::time::Duration::from_secs(300) {
                return Err(StorageError::UploadInProgress(id.to_owned()));
            }
            // Stale: reap.
            tracing::warn!(?lock, "reaping stale upload lock");
            let _ = std::fs::remove_file(&lock);
        }
        std::fs::write(
            &lock,
            format!("{}", std::process::id()).as_bytes(),
        )?;
        Ok(UploadLockGuard { path: lock })
    }

    /// Write a wasm artifact + manifest atomically:
    ///
    /// 1. Write `<sha>.wasm` (overwrite if exists — same content
    ///    by sha so this is idempotent).
    /// 2. Write `manifest.toml.new` then rename to `manifest.toml`.
    /// 3. Write `current.wasm.tmp` symlink, rename to
    ///    `current.wasm`.
    ///
    /// On error mid-flight we don't try to roll back; readers
    /// either see the old `current.wasm` (if step 3 didn't run)
    /// or the new (if it did). The orphaned `<sha>.wasm` from a
    /// failed activation gets cleaned up by future
    /// `prune-modules`; for v1 it's harmless dead weight.
    pub fn activate(&self, manifest: &Manifest, wasm_bytes: &[u8]) -> Result<(), StorageError> {
        let id = &manifest.module.id;
        let sha = &manifest.module.sha256;
        std::fs::create_dir_all(self.module_dir(id))?;

        // Step 1: wasm artifact.
        let wasm_path = self.artifact_path(id, sha);
        std::fs::write(&wasm_path, wasm_bytes)?;

        // Step 2: manifest (write-then-rename for atomicity).
        let manifest_toml = manifest.to_toml()?;
        let manifest_path = self.manifest_path(id);
        let manifest_tmp = manifest_path.with_extension("toml.new");
        std::fs::write(&manifest_tmp, manifest_toml.as_bytes())?;
        std::fs::rename(&manifest_tmp, &manifest_path)?;

        // Step 3: symlink swap.
        let symlink_target = format!("{sha}.wasm");
        let symlink_path = self.current_symlink(id);
        let symlink_tmp = symlink_path.with_file_name("current.wasm.tmp");
        // Remove any leftover tmp from a previously-aborted swap.
        let _ = std::fs::remove_file(&symlink_tmp);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&symlink_target, &symlink_tmp)?;
        #[cfg(not(unix))]
        compile_error!("ModuleStorage::activate requires symlink support; v1 is unix-only");
        std::fs::rename(&symlink_tmp, &symlink_path)?;

        Ok(())
    }

    /// Return the manifest of the currently-active module, or None
    /// if no module is registered under this id.
    pub fn read_manifest(&self, id: &str) -> Result<Option<Manifest>, StorageError> {
        let path = self.manifest_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let s = std::fs::read_to_string(path)?;
        Ok(Some(Manifest::parse(&s)?))
    }

    /// Read the wasm bytes the current symlink points at.
    pub fn read_current_wasm(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.current_symlink(id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(path)?))
    }

    /// Resolve `current.wasm` symlink to its absolute target path.
    /// Useful for callers that want to hand the path to wasmtime
    /// `Component::from_file` rather than allocating the bytes.
    pub fn current_wasm_path(&self, id: &str) -> Result<Option<PathBuf>, StorageError> {
        let symlink = self.current_symlink(id);
        if !symlink.exists() {
            return Ok(None);
        }
        let target = std::fs::read_link(&symlink)?;
        // Symlink targets are stored relative to the symlink's
        // dir; resolve to absolute for caller convenience.
        Ok(Some(self.module_dir(id).join(target)))
    }

    /// List registered module ids by directory enumeration.
    pub fn list_modules(&self) -> Result<Vec<String>, StorageError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
                && self.manifest_path(name).exists()
            {
                out.push(name.to_owned());
            }
        }
        out.sort();
        Ok(out)
    }
}

/// RAII guard for the per-module upload lock.
#[derive(Debug)]
pub struct UploadLockGuard {
    path: PathBuf,
}

impl Drop for UploadLockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        sha256_hex, AbiSection, BuildSection, ModuleSection, TrapPolicySection,
    };

    fn tempdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "mitos-platform-storage-test-{}-{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample_manifest(wasm: &[u8]) -> Manifest {
        Manifest {
            module: ModuleSection {
                id: "test-module".to_owned(),
                sha256: sha256_hex(wasm),
                size_bytes: wasm.len() as u64,
            },
            abi: AbiSection {
                version_major: 1,
                version_minor: 0,
                wit_package: "mitos:platform".to_owned(),
                wit_world: "mitos-module".to_owned(),
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
        }
    }

    #[test]
    fn activate_writes_artifact_and_symlink() {
        let dir = tempdir("activate");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"fake wasm bytes 1";
        let manifest = sample_manifest(wasm);

        storage.activate(&manifest, wasm).unwrap();

        let read = storage.read_current_wasm("test-module").unwrap().unwrap();
        assert_eq!(read, wasm);

        let read_manifest = storage
            .read_manifest("test-module")
            .unwrap()
            .unwrap();
        assert_eq!(read_manifest, manifest);

        let path = storage
            .current_wasm_path("test-module")
            .unwrap()
            .unwrap();
        assert!(path.exists());
        assert!(path.to_string_lossy().ends_with(".wasm"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn activate_replaces_symlink_atomically() {
        let dir = tempdir("replace");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"version A";
        let mut manifest_a = sample_manifest(wasm_a);
        manifest_a.module.sha256 = sha256_hex(wasm_a);
        manifest_a.module.size_bytes = wasm_a.len() as u64;
        storage.activate(&manifest_a, wasm_a).unwrap();
        assert_eq!(
            storage.read_current_wasm("test-module").unwrap().unwrap(),
            wasm_a
        );

        let wasm_b = b"version B different";
        let mut manifest_b = sample_manifest(wasm_b);
        manifest_b.module.sha256 = sha256_hex(wasm_b);
        manifest_b.module.size_bytes = wasm_b.len() as u64;
        storage.activate(&manifest_b, wasm_b).unwrap();
        assert_eq!(
            storage.read_current_wasm("test-module").unwrap().unwrap(),
            wasm_b
        );

        // Both shas should still exist on disk (rollback target).
        assert!(storage
            .artifact_path("test-module", &sha256_hex(wasm_a))
            .exists());
        assert!(storage
            .artifact_path("test-module", &sha256_hex(wasm_b))
            .exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn list_modules_only_returns_registered() {
        let dir = tempdir("list");
        let storage = ModuleStorage::new(&dir);

        // Empty storage.
        assert!(storage.list_modules().unwrap().is_empty());

        // Activate one.
        let wasm = b"hello";
        let manifest = sample_manifest(wasm);
        storage.activate(&manifest, wasm).unwrap();
        assert_eq!(storage.list_modules().unwrap(), vec!["test-module"]);

        // Subdir without a manifest doesn't count.
        std::fs::create_dir_all(dir.join("orphan")).unwrap();
        assert_eq!(storage.list_modules().unwrap(), vec!["test-module"]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn upload_lock_blocks_concurrent() {
        let dir = tempdir("lock");
        let storage = ModuleStorage::new(&dir);

        let _guard = storage.acquire_upload_lock("contested").unwrap();
        let err = storage.acquire_upload_lock("contested").unwrap_err();
        assert!(matches!(err, StorageError::UploadInProgress(_)));

        // Different module: no contention.
        let _other = storage.acquire_upload_lock("other").unwrap();

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn upload_lock_released_on_drop() {
        let dir = tempdir("lock-drop");
        let storage = ModuleStorage::new(&dir);
        {
            let _guard = storage.acquire_upload_lock("released").unwrap();
        }
        // Should be re-acquirable.
        let _again = storage.acquire_upload_lock("released").unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_returns_none_for_absent_module() {
        let dir = tempdir("absent");
        let storage = ModuleStorage::new(&dir);
        assert!(storage.read_manifest("nope").unwrap().is_none());
        assert!(storage.read_current_wasm("nope").unwrap().is_none());
        assert!(storage.current_wasm_path("nope").unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }
}
