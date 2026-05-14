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

use mitos_data_plane::ChainPoint;

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
/// redb store handles (`CursorStore`, `EmissionsStore`,
/// `RedbKv`). One file = one cached handle for the lifetime of
/// the process — the canonical pattern for redb, which is
/// single-writer-per-process and rejects a second
/// `Database::open` of the same file with `Database already
/// open. Cannot acquire lock.`
///
/// **All redb opens for module-scoped files MUST go through
/// `ModuleStorage`.** Direct calls to `redb::Database::create`
/// outside this module are a bug — see `clippy.toml`'s
/// `disallowed_methods` for the lint.
///
/// Mirrors the dolos pattern (`StateStore`, `RedbWalStore`):
/// each typed store wraps a private `Arc<Database>`, derives
/// `Clone`, and exposes only typed read/write methods. There
/// is no public way to obtain `&redb::Database` — by design.
///
/// Cache is load-bearing for write throughput: cursor commits
/// fire on every applied block; reopening redb per write costs
/// ~100-500ms (file-format check + possible repair pass),
/// capping throughput at ~2-10 blocks/sec. Holding databases
/// open drops per-write overhead to ~5ms, moving the
/// bottleneck onto wasmtime dispatch (the irreducible
/// per-block work).
///
/// Thread-safe + cheap to clone (Arc-wrapped caches).
#[derive(Clone)]
pub struct ModuleStorage {
    root: PathBuf,
    cursor_stores: Arc<Mutex<HashMap<String, CursorStore>>>,
    emissions_stores: Arc<Mutex<HashMap<String, crate::emissions::EmissionsStore>>>,
    kv_stores: Arc<Mutex<HashMap<String, crate::vendored::balius::kv::RedbKv>>>,
    /// Platform-wide aux-data cache. One handle shared across all
    /// module followers — `None` until first `aux_data_cache()` call.
    aux_data_cache: Arc<Mutex<Option<crate::aux_data_cache::AuxDataCache>>>,
}

impl ModuleStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursor_stores: Arc::new(Mutex::new(HashMap::new())),
            emissions_stores: Arc::new(Mutex::new(HashMap::new())),
            kv_stores: Arc::new(Mutex::new(HashMap::new())),
            aux_data_cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Get-or-open the per-module KV store. Idempotent; first
    /// call per module pays the redb open cost, subsequent
    /// calls return the cached `RedbKv` (cheap to clone — one
    /// `Arc<Database>` inside).
    ///
    /// Caller surface: the bundle's `KvFactory` invokes this
    /// once per module follower instantiation. Trap-replay
    /// outcomes that re-instantiate within the same process
    /// re-call the factory; without this cache, the second
    /// open hits the single-writer lock and fails.
    ///
    /// `cache_size` is forwarded to balius's redb cache config
    /// (MiB). Defaults applied internally if `None`.
    pub fn kv_store(
        &self,
        id: &str,
        cache_size: Option<usize>,
    ) -> Result<crate::vendored::balius::kv::RedbKv, crate::vendored::balius::kv::KvError> {
        let mut cache = self.kv_stores.lock().expect("kv_stores mutex");
        if let Some(s) = cache.get(id) {
            return Ok(s.clone());
        }
        std::fs::create_dir_all(self.module_dir(id))
            .map_err(|e| crate::vendored::balius::kv::KvError::Internal(e.to_string()))?;
        let store = crate::vendored::balius::kv::RedbKv::try_new(self.kv_path(id), cache_size)?;
        cache.insert(id.to_owned(), store.clone());
        Ok(store)
    }

    /// Drop the cached KV handle for a module. Mirror of
    /// `close_cursor` / `close_emissions`. Called from
    /// `host::stop` so a follower restart re-opens redb cleanly.
    pub fn close_kv(&self, id: &str) {
        let mut cache = self.kv_stores.lock().expect("kv_stores mutex");
        cache.remove(id);
    }

    /// Get-or-open the platform-wide aux-data cache. Stored at
    /// `<storage_root>/aux_data.redb` (not per-module — aux_data
    /// is a chain fact shared across all modules). Idempotent;
    /// first call opens the redb file, subsequent calls return
    /// the cached handle (cheap clone of the internal Arc).
    pub fn aux_data_cache(
        &self,
    ) -> Result<crate::aux_data_cache::AuxDataCache, crate::aux_data_cache::AuxDataCacheError> {
        let mut lock = self.aux_data_cache.lock().expect("aux_data_cache mutex");
        if let Some(cache) = lock.as_ref() {
            return Ok(cache.clone());
        }
        let path = self.root.join("aux_data.redb");
        let cache = crate::aux_data_cache::AuxDataCache::open(path)?;
        *lock = Some(cache.clone());
        Ok(cache)
    }

    /// Get-or-open the emissions store for a module. Idempotent;
    /// first call per module pays the redb open cost, subsequent
    /// calls return the cached handle. Cloning the returned
    /// `EmissionsStore` is cheap (Arc-wrapped redb handle inside).
    pub fn emissions_store(
        &self,
        id: &str,
    ) -> Result<crate::emissions::EmissionsStore, crate::emissions::EmissionsError> {
        let mut cache = self
            .emissions_stores
            .lock()
            .expect("emissions_stores mutex");
        if let Some(s) = cache.get(id) {
            return Ok(s.clone());
        }
        let store = crate::emissions::EmissionsStore::open(self.emissions_path(id))?;
        cache.insert(id.to_owned(), store.clone());
        Ok(store)
    }

    /// Drop the cached emissions store handle for a module.
    /// Mirror of `close_cursor` — used during follower stop so
    /// the next start re-opens redb cleanly.
    pub fn close_emissions(&self, id: &str) {
        let mut cache = self
            .emissions_stores
            .lock()
            .expect("emissions_stores mutex");
        cache.remove(id);
    }

    /// Get-or-open the cursor store for a module. First call
    /// per module pays the redb open cost (~hundreds of ms);
    /// subsequent calls are O(1) HashMap lookup + cheap clone
    /// (`CursorStore` holds `Arc<redb::Database>` internally).
    fn cursor_store(&self, id: &str) -> Result<CursorStore, StorageError> {
        let mut cache = self.cursor_stores.lock().expect("cursor_stores mutex");
        if let Some(s) = cache.get(id) {
            return Ok(s.clone());
        }
        std::fs::create_dir_all(self.module_dir(id))?;
        let store = CursorStore::open(self.cursor_path(id))?;
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

    /// Remove a module's entire artifact directory
    /// (`<storage_root>/<id>/`) — wasm, manifest, config.cbor,
    /// emissions.redb, kv.redb, cursor.redb, companions/.
    /// Used by the admin **evict** path (`POST
    /// /_admin/modules/{id}/evict`) to fully retire a module
    /// rather than just stopping its slot.
    ///
    /// **Caller MUST close cached DB handles first** — call
    /// `close_kv(id)`, `close_emissions(id)`, `close_cursor(id)`
    /// (or equivalent host-side teardown) before this method.
    /// Open redb handles hold an OS file lock that prevents
    /// `remove_dir_all` from cleaning the files. The DELETE-slot
    /// → drop-dialer-state → close-DB-handles → remove-dir order
    /// is what the evict handler orchestrates.
    ///
    /// Idempotent: a missing dir returns `Ok(())`.
    pub fn remove_module_dir(&self, id: &str) -> Result<(), StorageError> {
        let dir = self.module_dir(id);
        if !dir.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(&dir).map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "remove module dir {}: {e}",
                dir.display()
            )))
        })?;
        Ok(())
    }

    /// Per-module last-trap fixture path. The host's
    /// `TrapContextLogger` writes here on every `init` /
    /// `handle_event` failure; the admin endpoint
    /// `GET /_admin/modules/<id>/last-trap` reads it back. Always
    /// the most recent trap — older ones are overwritten,
    /// matching the "pull → debug → push fix" iteration loop
    /// the operator runs.
    pub fn last_trap_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("last-trap.toml")
    }

    /// Per-module companions registration directory.
    /// `<storage_root>/<id>/companions/<companion_key>.cbor` is
    /// where each registered companion's `SubscribeRequest` lives.
    pub fn module_dir_for_companions(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("companions")
    }

    /// Per-module emissions log path. Single redb file at
    /// `<storage_root>/<id>/emissions.redb` — feeds the companion
    /// dialer's outbound Apply stream with per-row delivery state.
    ///
    /// Crate-private — callers outside `mitos-platform` MUST go
    /// through `emissions_store(id)` so the cached
    /// `Arc<Database>` handle is shared. Direct path access
    /// would let callers `Database::open` themselves and trip
    /// the single-writer lock.
    pub(crate) fn emissions_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("emissions.redb")
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
    ///
    /// Crate-private — callers go through `cursor_store(id)`.
    /// See `emissions_path` for the rationale.
    pub(crate) fn cursor_path(&self, id: &str) -> PathBuf {
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
    ///
    /// Crate-private — callers go through `kv_store(id)`.
    /// See `emissions_path` for the rationale.
    pub(crate) fn kv_path(&self, id: &str) -> PathBuf {
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
/// module containing a single-row `cursor` table. Opened once
/// per process per module, cached in `ModuleStorage::cursor_stores`,
/// shared via cheap `Clone` (the inner `Arc<redb::Database>`
/// makes clones share one open handle).
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
#[derive(Clone)]
struct CursorStore {
    db: Arc<redb::Database>,
}

const CURSOR_TABLE: redb::TableDefinition<'_, &str, &[u8]> = redb::TableDefinition::new("cursor");
const CURSOR_ROW: &str = "current";

impl CursorStore {
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self, StorageError> {
        // Sole open site for cursor.redb. Routed exclusively
        // through `ModuleStorage::cursor_store` which caches by
        // path; see clippy.toml for the workspace lint.
        #[allow(clippy::disallowed_methods)]
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
        Ok(Self { db: Arc::new(db) })
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
            interest: crate::manifest::InterestSection::default(),
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
