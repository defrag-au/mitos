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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dolos_core::ChainPoint;

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

/// Owns the storage root path + a cache of open per-module
/// `CursorStore` handles. The cache is load-bearing for write
/// throughput: cursor commits fire on every applied block;
/// reopening redb per write costs ~100-500ms (file-format check +
/// possible repair pass), capping throughput at ~2-10 blocks/sec.
/// Holding the database open drops per-write overhead to ~5ms,
/// which moves the bottleneck off cursor I/O and onto wasmtime
/// dispatch (the irreducible per-block work).
///
/// Thread-safe + cheap to clone (Arc-wrapped cache).
#[derive(Clone)]
pub struct ModuleStorage {
    root: PathBuf,
    cursor_stores: Arc<Mutex<HashMap<String, Arc<CursorStore>>>>,
}

impl ModuleStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursor_stores: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get-or-open the cursor store for a module. First call
    /// per module pays the redb open cost (~hundreds of ms);
    /// subsequent calls are O(1) HashMap lookup + Arc clone.
    fn cursor_store(&self, id: &str) -> Result<Arc<CursorStore>, StorageError> {
        let mut cache = self.cursor_stores.lock().expect("cursor_stores mutex");
        if let Some(s) = cache.get(id) {
            return Ok(s.clone());
        }
        std::fs::create_dir_all(self.module_dir(id))?;
        let store = Arc::new(CursorStore::open(self.cursor_path(id))?);
        cache.insert(id.to_owned(), store.clone());
        Ok(store)
    }

    /// Drop the cached cursor store for a module. Used by
    /// `host::stop` so a follower restart re-opens the database
    /// (ensuring no stale handle outlives a Driver replacement).
    pub fn close_cursor(&self, id: &str) {
        let mut cache = self.cursor_stores.lock().expect("cursor_stores mutex");
        cache.remove(id);
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn module_dir(&self, id: &str) -> PathBuf {
        self.root.join(id)
    }

    /// Per-module companions registration directory.
    /// `<storage_root>/<id>/companions/<companion_key>.cbor` is
    /// where each registered companion's `SubscribeRequest` lives.
    pub fn module_dir_for_companions(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("companions")
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
        std::fs::write(&lock, format!("{}", std::process::id()).as_bytes())?;
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

    /// Per-module crash-safe cursor file (redb).
    pub fn cursor_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("cursor.redb")
    }

    /// Per-module config bytes (CBOR'd typed config from the
    /// dApp's `mitos.toml`). Plain file because the host reads
    /// it once at follower start and passes through to the
    /// module's `init`.
    pub fn config_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("config.cbor")
    }

    /// Write module config bytes atomically (write-then-rename).
    pub fn write_config(&self, id: &str, bytes: &[u8]) -> Result<(), StorageError> {
        std::fs::create_dir_all(self.module_dir(id))?;
        let final_path = self.config_path(id);
        let tmp = final_path.with_extension("cbor.new");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    /// Read module config bytes, if any. `Ok(None)` means no
    /// config has been uploaded — host calls `init(&[])`.
    pub fn read_config(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.config_path(id);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(path)?))
    }

    /// Per-module crash-safe KV file (redb, via the vendored
    /// `RedbKv`). Bundle's KV factory points at this.
    pub fn kv_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("kv.redb")
    }

    /// Persist the driver's last-applied cursor atomically.
    /// Reuses the open `CursorStore` per module — one redb open
    /// per module lifetime, one redb commit per block. See
    /// `ModuleStorage` doc comment for why this matters.
    pub fn write_cursor(&self, id: &str, cursor: &ChainPoint) -> Result<(), StorageError> {
        self.cursor_store(id)?.write(cursor)
    }

    /// Read the persisted cursor, if any. `Ok(None)` means the
    /// module has no checkpoint yet; caller should start from
    /// `ChainPoint::Origin` (or whatever the configured start
    /// point is).
    pub fn read_cursor(&self, id: &str) -> Result<Option<ChainPoint>, StorageError> {
        let path = self.cursor_path(id);
        if !path.exists() {
            return Ok(None);
        }
        self.cursor_store(id)?.read()
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

/// Crash-safe per-module cursor store. One redb file per
/// module containing a single-row `cursor` table. Opened
/// per-call rather than kept hot — cursor writes are infrequent
/// (one per applied block) and the open overhead is dominated
/// by the per-block wasm dispatch cost.
///
/// Why not vendor Balius's `store.rs`: their `Store` is a WAL
/// for replay-safe worker restart (`CURSORS` → `WAL` tables,
/// workers replay `LogEntry`s forward from their recorded
/// `LogSeq`). Mitos has a different replay model — dolos's
/// archive *is* our WAL; on module restart we re-subscribe
/// from the persisted `ChainPoint` and the chain follower
/// re-fetches blocks from there. A host-side WAL would be
/// dead weight; a focused single-row cursor store is what we
/// actually need.
struct CursorStore {
    db: redb::Database,
}

const CURSOR_TABLE: redb::TableDefinition<'_, &str, &[u8]> = redb::TableDefinition::new("cursor");
const CURSOR_ROW: &str = "current";

impl CursorStore {
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StorageError> {
        let db = redb::Database::builder()
            .create(path)
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        // Initialize the table on first open so reads don't
        // racy-fail on a not-yet-created table.
        let wx = db
            .begin_write()
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        wx.open_table(CURSOR_TABLE)
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        wx.commit()
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        Ok(Self { db })
    }

    fn write(&self, cursor: &ChainPoint) -> Result<(), StorageError> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(cursor, &mut buf)
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        let wx = self
            .db
            .begin_write()
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        {
            let mut table = wx
                .open_table(CURSOR_TABLE)
                .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
            table
                .insert(CURSOR_ROW, buf.as_slice())
                .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        }
        wx.commit()
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    fn read(&self) -> Result<Option<ChainPoint>, StorageError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        let table = rx
            .open_table(CURSOR_TABLE)
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        let entry = table
            .get(CURSOR_ROW)
            .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
        match entry {
            Some(v) => {
                let cursor: ChainPoint = ciborium::de::from_reader(v.value())
                    .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;
                Ok(Some(cursor))
            }
            None => Ok(None),
        }
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
    use crate::manifest::{AbiSection, BuildSection, ModuleSection, TrapPolicySection, sha256_hex};

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

        let read_manifest = storage.read_manifest("test-module").unwrap().unwrap();
        assert_eq!(read_manifest, manifest);

        let path = storage.current_wasm_path("test-module").unwrap().unwrap();
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
        assert!(
            storage
                .artifact_path("test-module", &sha256_hex(wasm_a))
                .exists()
        );
        assert!(
            storage
                .artifact_path("test-module", &sha256_hex(wasm_b))
                .exists()
        );

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
    fn cursor_round_trip() {
        let dir = tempdir("cursor");
        let storage = ModuleStorage::new(&dir);

        // No cursor yet.
        assert!(storage.read_cursor("test-module").unwrap().is_none());

        // Write + read.
        let cursor = ChainPoint::Slot(186_000_000);
        storage.write_cursor("test-module", &cursor).unwrap();
        let read = storage.read_cursor("test-module").unwrap().unwrap();
        assert_eq!(read.slot(), 186_000_000);

        // Overwrite — last write wins.
        let cursor2 = ChainPoint::Slot(186_001_000);
        storage.write_cursor("test-module", &cursor2).unwrap();
        let read2 = storage.read_cursor("test-module").unwrap().unwrap();
        assert_eq!(read2.slot(), 186_001_000);

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
