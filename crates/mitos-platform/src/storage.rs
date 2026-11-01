//! Module artifact storage — platform v3's instance-keyed layout.
//!
//! ```text
//! <storage_root>/
//!   indexer_data.redb                       platform-wide chain-fact cache
//!   _cas/                                   `_` cannot begin a module id
//!     wasm/<sha256>.wasm                    immutable artifact
//!     wasm/<sha256>.manifest.toml           its manifest, beside it
//!     config/<sha256>.cbor                  immutable config bytes
//!     orphaned-v2/<module_id>/<sha>.wasm    old rollback shas, see `migrate_v2`
//!   <module_id>/
//!     .upload.lock                          per-module upload lock
//!     current.toml                          a serialised `InstanceKey`
//!     companions/<client_id>/<key>.cbor     module-scoped, NOT instance-scoped
//!     instances/<InstanceKey::dir_name()>/
//!       instance.toml                       the full key (dir names truncate)
//!       kv.redb cursor.redb watched.redb emissions.redb last-trap.toml
//! ```
//!
//! # Why the split is where it is
//!
//! The unit of execution is the INSTANCE — `(module, wasm sha, config sha)`.
//! Artifacts are content-addressed because identical bytes are shared by every
//! instance that runs them, so forking an instance copies no wasm. Config is
//! content-addressed too, because it is part of instance IDENTITY: two clients
//! announcing the same wasm with different `config.cbor` must not share state.
//! `companions/` stays module-scoped on purpose — a subscription follows its
//! client row between instances, it is not one instance's property.
//!
//! # The `&str` facade
//!
//! v2 runs exactly one instance per module, so every existing `&str`-taking
//! method resolves "that module's CURRENT instance" through `current.toml` and
//! behaves exactly as it did. Callers migrate off the facade one subsystem at
//! a time as each becomes instance-aware; until then this is the only
//! translation layer and there is no second layout to keep in sync.

pub mod migrate_v2;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use mitos_data_plane::ChainPoint;
use mitos_lifecycle::client_move::ClientRow;
use mitos_lifecycle::handoff::Handoffs;
use mitos_lifecycle::ids::{ClientId, InstanceKey, ModuleId, Sha256Hex};
use mitos_lifecycle::instance::LifecycleRecord;

use crate::manifest::Manifest;

/// `_` cannot begin a module id (ids are `[a-z0-9-]+`), so the content-
/// addressed store can live beside module directories without a collision
/// check.
const CAS_DIR: &str = "_cas";
/// The facade's pointer file: which instance `&str` methods resolve to.
const CURRENT_POINTER: &str = "current.toml";
/// The full `InstanceKey` for one instance directory. `dir_name()` truncates
/// both digests, so the directory name alone cannot reconstruct the key —
/// this file is the source of truth and `list_instances` reads it.
const INSTANCE_MARKER: &str = "instance.toml";
const INSTANCES_DIR: &str = "instances";
/// The instance's `LifecycleRecord` — see [`ModuleStorage::read_lifecycle`].
const LIFECYCLE_RECORD: &str = "lifecycle.toml";
/// One instance's hand-off watermarks — see [`ModuleStorage::read_handoffs_at`].
const HANDOFFS: &str = "handoffs.toml";
/// What an instance directory is called while it is still being built. Not a
/// valid `dir_name()` — those are two hex digests joined by `-` — so nothing
/// that resolves a key can ever name one.
const PARTIAL_SUFFIX: &str = ".partial";
/// A client's row — see [`ModuleStorage::read_row`].
const CLIENT_ROW: &str = "rows/<client>.toml";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),
    #[error("upload in progress for module {0}")]
    UploadInProgress(String),
    /// An id read off disk (or handed in by a caller) that isn't one. Every
    /// id here ends up in a `join()`, so this is refused rather than
    /// sanitised.
    #[error("id: {0}")]
    Id(#[from] mitos_lifecycle::ids::IdError),
    #[error("{file} for module {module} is not a readable instance key: {why}")]
    MalformedPointer {
        module: String,
        file: &'static str,
        why: String,
    },
    /// `current.toml` names an instance directory that is not there, and the
    /// module does not have exactly one other instance to repair the pointer
    /// to. Refusing to guess: picking wrong here resumes a follower on another
    /// instance's cursor and state.
    #[error(
        "module {module}: current.toml names instance `{instance}`, which does not exist, and \
         the module has {found} instance directories — refusing to guess which one is current"
    )]
    NoSuchInstance {
        module: String,
        instance: String,
        found: usize,
    },
    /// A config upload for a module that was never activated. There is no
    /// wasm sha, so there is no instance key to attach the config to.
    #[error("module {0} has never been activated, so there is no instance to configure")]
    NotActivated(String),
    /// A repoint would have to rename one instance directory onto another.
    /// Nothing in this layout creates a second instance yet, so this means
    /// the tree was edited underneath the host.
    #[error(
        "module {module}: instance directory `{instance}` already exists; refusing to rename onto it"
    )]
    InstanceExists { module: String, instance: String },
    #[error("artifact sha mismatch: manifest claims {manifest}, bytes hash to {computed}")]
    ArtifactShaMismatch { manifest: String, computed: String },
    #[error("serialising {what}: {why}")]
    Serialise { what: &'static str, why: String },
}

/// How the caller of a `*_locked` method holds the `repoint` lock. A named
/// value rather than a `bool` because the two cases permit different things:
/// only `Write` may persist anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    Read,
    Write,
}

/// What a cached redb handle belongs to.
///
/// Handles were keyed by module id, which is exactly why a second instance of
/// a module could not run: both would resolve to one `kv.redb` handle and
/// overwrite each other's cursor. Keyed by INSTANCE they are separate, and
/// the `&str` API keeps working by resolving the module's current instance
/// and addressing that one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StateAddr {
    Instance(InstanceKey),
    /// Never activated: no `current.toml`, so the state lives in the module
    /// directory where v2 put it. Tests and a few tools write a cursor to
    /// such a module and it must keep working — see `state_dir`.
    Unactivated(String),
}

/// How a caller names the state it wants: the way every v2 call site does, or
/// exactly.
enum Target<'a> {
    /// Whatever `current.toml` resolves to — the `&str` API.
    Current(&'a str),
    Exact(&'a InstanceKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IfAbsent {
    Create,
    Leave,
}

/// Write-tmp-then-rename. The tmp sits beside the target so the rename never
/// crosses a filesystem, which is what makes it atomic.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| {
            StorageError::Io(std::io::Error::other(format!(
                "{} has no file name",
                path.display()
            )))
        })?
        .to_owned();
    tmp_name.push(".new");
    let tmp = path.with_file_name(tmp_name);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// One of an instance's state files, by name.
///
/// An enum rather than four string literals because a client move COPIES
/// three of them from its parent, and "the files the copy carries" and "the
/// files the stores open" must be the same set. `mitos_lifecycle::state_prep`
/// decides WHICH — [`CopySet`](mitos_lifecycle::state_prep::CopySet) has one
/// field per copyable file — and this is how the host spells them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateFile {
    Kv,
    Cursor,
    /// The watched-UTxO index. May be dropped on its own (`reset-watched`)
    /// but is never carried without `Kv` and `Cursor` — it indexes UTxOs a
    /// fresh KV would not know.
    Watched,
    /// The emissions log. NOT in a `CopySet` and never copied: it is the
    /// largest file there is and every row in it is addressed to a client
    /// that is still being served by the parent (D5).
    Emissions,
}

impl StateFile {
    pub fn file_name(self) -> &'static str {
        match self {
            StateFile::Kv => "kv.redb",
            StateFile::Cursor => "cursor.redb",
            StateFile::Watched => "watched.redb",
            StateFile::Emissions => "emissions.redb",
        }
    }
}

fn write_lifecycle_to(path: &Path, record: &LifecycleRecord) -> Result<(), StorageError> {
    let toml = toml::to_string(record).map_err(|e| StorageError::Serialise {
        what: "lifecycle record",
        why: e.to_string(),
    })?;
    write_atomic(path, toml.as_bytes())
}

fn instance_key_toml(key: &InstanceKey) -> Result<String, StorageError> {
    toml::to_string(key).map_err(|e| StorageError::Serialise {
        what: "instance key",
        why: e.to_string(),
    })
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
    cursor_stores: Arc<Mutex<HashMap<StateAddr, CursorStore>>>,
    emissions_stores: Arc<Mutex<HashMap<StateAddr, crate::emissions::EmissionsStore>>>,
    kv_stores: Arc<Mutex<HashMap<StateAddr, crate::vendored::balius::kv::RedbKv>>>,
    /// Per-instance watched-UTxO index handles (`watched.redb`). Shared
    /// `Arc` per instance so the follower's plane (reader) and driver
    /// (writer) observe the same one. See `crate::watched_ref_index`.
    watched_stores: Arc<Mutex<HashMap<StateAddr, Arc<crate::watched_ref_index::WatchedRefIndex>>>>,
    /// Platform-wide indexer-data cache (aux_data + datums). One
    /// handle shared across all module followers — `None` until
    /// first `indexer_data_cache()` call.
    indexer_data_cache: Arc<Mutex<Option<crate::indexer_data_cache::IndexerDataCache>>>,
    /// Makes instance RESOLUTION and instance REPOINTING exclude each other.
    ///
    /// `activate` renames a live module's instance directory while its
    /// follower is running, then repoints `current.toml` at the new name.
    /// Anything resolving `&str → InstanceKey` holds this for READ across
    /// BOTH the resolve and the path build, so it can never observe the
    /// half-second where the pointer names the old directory and the old
    /// directory is gone — and, more importantly, can never re-create the
    /// old path behind the rename.
    ///
    /// One lock for the whole store rather than one per module: a repoint is
    /// rare and takes microseconds, and a single lock cannot deadlock.
    repoint: Arc<RwLock<()>>,
    /// Fires inside the repoint's critical section, between the rename and
    /// the pointer write. Exists so a test can hold that window open and
    /// prove a concurrent writer blocks rather than resurrecting the old
    /// directory; nothing outside `cfg(test)` can install one.
    #[cfg(test)]
    repoint_hook: Arc<Mutex<Option<RepointHook>>>,
    /// Fires in `cursor_store_for` between locating the state and looking it
    /// up in the cache — the window a repoint must not be able to enter.
    #[cfg(test)]
    located_hook: Arc<Mutex<Option<RepointHook>>>,
}

#[cfg(test)]
type RepointHook = Arc<dyn Fn() + Send + Sync>;

impl ModuleStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursor_stores: Arc::new(Mutex::new(HashMap::new())),
            emissions_stores: Arc::new(Mutex::new(HashMap::new())),
            kv_stores: Arc::new(Mutex::new(HashMap::new())),
            watched_stores: Arc::new(Mutex::new(HashMap::new())),
            indexer_data_cache: Arc::new(Mutex::new(None)),
            repoint: Arc::new(RwLock::new(())),
            #[cfg(test)]
            repoint_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            located_hook: Arc::new(Mutex::new(None)),
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
        self.kv_store_for(&Target::Current(id), cache_size)
    }

    /// The KV of ONE instance. Two instances of a module have two.
    pub fn kv_store_at(
        &self,
        key: &InstanceKey,
        cache_size: Option<usize>,
    ) -> Result<crate::vendored::balius::kv::RedbKv, crate::vendored::balius::kv::KvError> {
        self.kv_store_for(&Target::Exact(key), cache_size)
    }

    fn kv_store_for(
        &self,
        target: &Target<'_>,
        cache_size: Option<usize>,
    ) -> Result<crate::vendored::balius::kv::RedbKv, crate::vendored::balius::kv::KvError> {
        // Held across locate, open AND insert — see `locate_held`.
        let _guard = self.repoint.read().expect("repoint lock");
        let (addr, dir) = self
            .locate_held(target)
            .map_err(|e| crate::vendored::balius::kv::KvError::Internal(e.to_string()))?;
        let mut cache = self.kv_stores.lock().expect("kv_stores mutex");
        if let Some(s) = cache.get(&addr) {
            return Ok(s.clone());
        }
        let store = crate::vendored::balius::kv::RedbKv::try_new(
            dir.join(StateFile::Kv.file_name()),
            cache_size,
        )?;
        cache.insert(addr, store.clone());
        Ok(store)
    }

    /// Drop the cached KV handle. Mirror of
    /// `close_cursor` / `close_emissions`. Called from
    /// `host::stop` so a follower restart re-opens redb cleanly.
    pub fn close_kv(&self, id: &str) {
        if let Ok(addr) = self.addr_of(id) {
            self.kv_stores
                .lock()
                .expect("kv_stores mutex")
                .remove(&addr);
        }
    }

    pub fn close_kv_at(&self, key: &InstanceKey) {
        self.kv_stores
            .lock()
            .expect("kv_stores mutex")
            .remove(&StateAddr::Instance(key.clone()));
    }

    /// Get-or-open the per-module watched-UTxO index. Idempotent;
    /// first call per module pays the redb open + mirror-load cost,
    /// subsequent calls return the shared `Arc`. On open failure the
    /// index degrades to in-memory (logs loudly; the set re-seeds at
    /// the next bootstrap) rather than taking the follower down —
    /// same posture as `kv_store`.
    pub fn watched_index(&self, id: &str) -> Arc<crate::watched_ref_index::WatchedRefIndex> {
        self.watched_index_for(&Target::Current(id), id)
    }

    /// The watched set of ONE instance.
    ///
    /// Keeps `watched_index`'s posture of degrading to in-memory rather than
    /// taking a follower down: the set re-seeds at the next bootstrap, and a
    /// dead follower is worse than a cold watched index.
    pub fn watched_index_at(
        &self,
        key: &InstanceKey,
    ) -> Arc<crate::watched_ref_index::WatchedRefIndex> {
        self.watched_index_for(&Target::Exact(key), key.module.as_str())
    }

    fn watched_index_for(
        &self,
        target: &Target<'_>,
        what: &str,
    ) -> Arc<crate::watched_ref_index::WatchedRefIndex> {
        // Held across locate, open AND insert — see `locate_held`.
        let _guard = self.repoint.read().expect("repoint lock");
        let (addr, dir) = match self.locate_held(target) {
            Ok(located) => located,
            // NOT cached: there is no address to file it under, and an
            // instance that does not exist must not acquire one.
            Err(e) => return Self::watched_fallback(what, &e.to_string()),
        };
        let mut cache = self.watched_stores.lock().expect("watched_stores mutex");
        if let Some(idx) = cache.get(&addr) {
            return idx.clone();
        }
        let idx = match crate::watched_ref_index::WatchedRefIndex::open(
            dir.join(StateFile::Watched.file_name()),
        ) {
            Ok(idx) => Arc::new(idx),
            Err(e) => Self::watched_fallback(what, &e.to_string()),
        };
        cache.insert(addr, idx.clone());
        idx
    }

    fn watched_fallback(what: &str, error: &str) -> Arc<crate::watched_ref_index::WatchedRefIndex> {
        tracing::error!(
            instance = %what,
            error = %error,
            "watched.redb open failed; falling back to in-memory watched index"
        );
        Arc::new(crate::watched_ref_index::WatchedRefIndex::in_memory())
    }

    /// Drop the cached watched-index handle. Mirror of
    /// `close_kv`; release the redb file lock before `remove_module_dir`.
    pub fn close_watched(&self, id: &str) {
        if let Ok(addr) = self.addr_of(id) {
            self.watched_stores
                .lock()
                .expect("watched_stores mutex")
                .remove(&addr);
        }
    }

    pub fn close_watched_at(&self, key: &InstanceKey) {
        self.watched_stores
            .lock()
            .expect("watched_stores mutex")
            .remove(&StateAddr::Instance(key.clone()));
    }

    /// Get-or-open the platform-wide indexer-data cache holding
    /// aux_data and datums. Stored at
    /// `<storage_root>/indexer_data.redb` (not per-module — these
    /// are chain facts shared across all modules). Idempotent;
    /// first call opens the redb file, subsequent calls return the
    /// cached handle (cheap clone of the internal Arc).
    pub fn indexer_data_cache(
        &self,
    ) -> Result<
        crate::indexer_data_cache::IndexerDataCache,
        crate::indexer_data_cache::IndexerDataCacheError,
    > {
        let mut lock = self
            .indexer_data_cache
            .lock()
            .expect("indexer_data_cache mutex");
        if let Some(cache) = lock.as_ref() {
            return Ok(cache.clone());
        }
        let path = self.root.join("indexer_data.redb");
        // One-time migration: the cache was previously named
        // `aux_data.redb` (aux_data only). Rename it in place so an
        // existing deployment keeps its warm aux_data entries
        // instead of cold-starting; the `datums` table is created
        // lazily on open. Best-effort — a failed rename just means
        // a cold start, not an error.
        let legacy = self.root.join("aux_data.redb");
        if !path.exists() && legacy.exists() {
            if let Err(e) = std::fs::rename(&legacy, &path) {
                tracing::warn!(
                    error = %e,
                    "indexer_data_cache: failed to migrate aux_data.redb; starting cold",
                );
            } else {
                tracing::info!("indexer_data_cache: migrated aux_data.redb -> indexer_data.redb");
            }
        }
        let cache = crate::indexer_data_cache::IndexerDataCache::open(path)?;
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
        self.emissions_store_for(&Target::Current(id))
    }

    /// The emissions log of ONE instance. Each instance keeps its own, and
    /// which one a client's companions are served from is
    /// `mitos_lifecycle::routing::fan_out`'s decision.
    pub fn emissions_store_at(
        &self,
        key: &InstanceKey,
    ) -> Result<crate::emissions::EmissionsStore, crate::emissions::EmissionsError> {
        self.emissions_store_for(&Target::Exact(key))
    }

    fn emissions_store_for(
        &self,
        target: &Target<'_>,
    ) -> Result<crate::emissions::EmissionsStore, crate::emissions::EmissionsError> {
        // Held across locate, open AND insert — see `locate_held`.
        let _guard = self.repoint.read().expect("repoint lock");
        let (addr, dir) = self.locate_held(target).map_err(|e| {
            crate::emissions::EmissionsError::Io(std::io::Error::other(e.to_string()))
        })?;
        let mut cache = self
            .emissions_stores
            .lock()
            .expect("emissions_stores mutex");
        if let Some(s) = cache.get(&addr) {
            return Ok(s.clone());
        }
        let store =
            crate::emissions::EmissionsStore::open(dir.join(StateFile::Emissions.file_name()))?;
        cache.insert(addr, store.clone());
        Ok(store)
    }

    /// Drop the cached emissions store handle.
    /// Mirror of `close_cursor` — used during follower stop so
    /// the next start re-opens redb cleanly.
    pub fn close_emissions(&self, id: &str) {
        if let Ok(addr) = self.addr_of(id) {
            self.emissions_stores
                .lock()
                .expect("emissions_stores mutex")
                .remove(&addr);
        }
    }

    pub fn close_emissions_at(&self, key: &InstanceKey) {
        self.emissions_stores
            .lock()
            .expect("emissions_stores mutex")
            .remove(&StateAddr::Instance(key.clone()));
    }

    /// Get-or-open the cursor store for a module. First call
    /// per module pays the redb open cost (~hundreds of ms);
    /// subsequent calls are O(1) HashMap lookup + cheap clone
    /// (`CursorStore` holds `Arc<redb::Database>` internally).
    ///
    /// `Ok(None)` only under [`IfAbsent::Leave`]: a READ of a cursor that was
    /// never written must not create `cursor.redb` on the way to saying so.
    fn cursor_store_for(
        &self,
        target: &Target<'_>,
        if_absent: IfAbsent,
    ) -> Result<Option<CursorStore>, StorageError> {
        // Held across locate, open AND insert — see `locate_held`.
        let _guard = self.repoint.read().expect("repoint lock");
        let (addr, dir) = self.locate_held(target)?;
        #[cfg(test)]
        {
            let hook = self
                .located_hook
                .lock()
                .expect("located_hook mutex")
                .clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        let mut cache = self.cursor_stores.lock().expect("cursor_stores mutex");
        if let Some(s) = cache.get(&addr) {
            return Ok(Some(s.clone()));
        }
        let path = dir.join(StateFile::Cursor.file_name());
        if if_absent == IfAbsent::Leave && !path.exists() {
            return Ok(None);
        }
        let store = CursorStore::open(path)?;
        cache.insert(addr, store.clone());
        Ok(Some(store))
    }

    fn write_cursor_for(
        &self,
        target: &Target<'_>,
        cursor: &ChainPoint,
    ) -> Result<(), StorageError> {
        self.cursor_store_for(target, IfAbsent::Create)?
            .expect("IfAbsent::Create always yields a store")
            .write(cursor)
    }

    fn read_cursor_for(&self, target: &Target<'_>) -> Result<Option<ChainPoint>, StorageError> {
        match self.cursor_store_for(target, IfAbsent::Leave)? {
            Some(store) => store.read(),
            None => Ok(None),
        }
    }

    /// Drop the cached cursor store. Used by `host::stop` so a follower
    /// restart re-opens the database (ensuring no stale handle outlives a
    /// Driver replacement).
    pub fn close_cursor(&self, id: &str) {
        if let Ok(addr) = self.addr_of(id) {
            self.cursor_stores
                .lock()
                .expect("cursor_stores mutex")
                .remove(&addr);
        }
    }

    pub fn close_cursor_at(&self, key: &InstanceKey) {
        self.cursor_stores
            .lock()
            .expect("cursor_stores mutex")
            .remove(&StateAddr::Instance(key.clone()));
    }

    /// Persist ONE instance's cursor. Two instances of a module follow
    /// independently and each advances its own.
    pub fn write_cursor_at(
        &self,
        key: &InstanceKey,
        cursor: &ChainPoint,
    ) -> Result<(), StorageError> {
        self.write_cursor_for(&Target::Exact(key), cursor)
    }

    pub fn read_cursor_at(&self, key: &InstanceKey) -> Result<Option<ChainPoint>, StorageError> {
        self.read_cursor_for(&Target::Exact(key))
    }

    /// Where ONE instance's trap fixture goes.
    pub fn last_trap_path_at(&self, key: &InstanceKey) -> Result<PathBuf, StorageError> {
        Ok(self.instance_state_dir(key)?.join("last-trap.toml"))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn module_dir(&self, id: &str) -> PathBuf {
        self.root.join(id)
    }

    // -----------------------------------------------------------------
    // Content-addressed artifact + config store
    // -----------------------------------------------------------------

    fn cas_wasm_dir(&self) -> PathBuf {
        self.root.join(CAS_DIR).join("wasm")
    }

    fn cas_config_dir(&self) -> PathBuf {
        self.root.join(CAS_DIR).join("config")
    }

    fn cas_manifest_dir(&self) -> PathBuf {
        self.root.join(CAS_DIR).join("manifest")
    }

    /// A manifest keyed by its OWN digest, holding the bytes exactly as
    /// fetched.
    ///
    /// Distinct from the copy beside the wasm (`_cas/wasm/<wasm>.manifest.toml`,
    /// keyed by the WASM's sha, which the facade still reads). A manifest's
    /// bytes change on every build — `build_id` is in them — while the wasm's
    /// may not, so two announcements can legitimately pin two different
    /// manifests over one wasm, and keyed by the wasm they would overwrite
    /// each other. Keyed by their own digest they cannot.
    pub fn manifest_by_digest_path(&self, sha: &Sha256Hex) -> PathBuf {
        self.cas_manifest_dir().join(format!("{sha}.toml"))
    }

    /// Store a manifest's RAW bytes under their own digest. A second put of
    /// the same digest is a no-op, as everywhere else in the CAS.
    pub fn put_manifest(&self, bytes: &[u8]) -> Result<Sha256Hex, StorageError> {
        let sha = Sha256Hex::parse(&crate::manifest::sha256_hex(bytes))?;
        let path = self.manifest_by_digest_path(&sha);
        if path.exists() {
            return Ok(sha);
        }
        std::fs::create_dir_all(self.cas_manifest_dir())?;
        write_atomic(&path, bytes)?;
        Ok(sha)
    }

    pub fn read_manifest_by_digest(
        &self,
        sha: &Sha256Hex,
    ) -> Result<Option<Manifest>, StorageError> {
        let path = self.manifest_by_digest_path(sha);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Manifest::parse(&std::fs::read_to_string(path)?)?))
    }

    /// Where the wasm bytes for a digest live. Shared by every instance and
    /// namespace that runs them — promoting or forking an instance copies no
    /// wasm because there is only ever one copy.
    pub fn artifact_path(&self, sha: &Sha256Hex) -> PathBuf {
        self.cas_wasm_dir().join(format!("{sha}.wasm"))
    }

    fn artifact_manifest_path(&self, sha: &Sha256Hex) -> PathBuf {
        self.cas_wasm_dir().join(format!("{sha}.manifest.toml"))
    }

    /// Where a config digest's CBOR bytes live.
    pub fn config_bytes_path(&self, sha: &Sha256Hex) -> PathBuf {
        self.cas_config_dir().join(format!("{sha}.cbor"))
    }

    /// Store a wasm artifact and its manifest, keyed by the wasm's digest.
    ///
    /// Refuses bytes that do not hash to what the manifest claims: the
    /// digest is half of every instance's identity, so a manifest that lies
    /// about it would give two different artifacts one identity.
    ///
    /// A second put of the same digest is a NO-OP, not an overwrite — that
    /// is what content-addressed means, and it is what lets two instances
    /// reference one artifact without either owning it.
    pub fn put_artifact(
        &self,
        manifest: &Manifest,
        wasm_bytes: &[u8],
    ) -> Result<Sha256Hex, StorageError> {
        let computed = crate::manifest::sha256_hex(wasm_bytes);
        if computed != manifest.module.sha256 {
            return Err(StorageError::ArtifactShaMismatch {
                manifest: manifest.module.sha256.clone(),
                computed,
            });
        }
        let sha = Sha256Hex::parse(&computed)?;
        let wasm = self.artifact_path(&sha);
        let manifest_path = self.artifact_manifest_path(&sha);
        if wasm.exists() && manifest_path.exists() {
            return Ok(sha);
        }
        std::fs::create_dir_all(self.cas_wasm_dir())?;
        if !wasm.exists() {
            write_atomic(&wasm, wasm_bytes)?;
        }
        if !manifest_path.exists() {
            write_atomic(&manifest_path, manifest.to_toml()?.as_bytes())?;
        }
        Ok(sha)
    }

    /// Store config bytes keyed by their digest. Same no-op-on-repeat rule as
    /// [`Self::put_artifact`], and for the same reason: once an instance key
    /// references a config digest those bytes are immutable.
    pub fn put_config(&self, bytes: &[u8]) -> Result<Sha256Hex, StorageError> {
        let sha = Sha256Hex::parse(&crate::manifest::sha256_hex(bytes))?;
        let path = self.config_bytes_path(&sha);
        if path.exists() {
            return Ok(sha);
        }
        std::fs::create_dir_all(self.cas_config_dir())?;
        write_atomic(&path, bytes)?;
        Ok(sha)
    }

    /// The manifest stored beside an artifact. `Ok(None)` if the digest was
    /// never put.
    pub fn read_artifact_manifest(
        &self,
        sha: &Sha256Hex,
    ) -> Result<Option<Manifest>, StorageError> {
        let path = self.artifact_manifest_path(sha);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Manifest::parse(&std::fs::read_to_string(path)?)?))
    }

    /// Config bytes for a digest. `Ok(None)` if the digest was never put.
    pub fn read_config_bytes(&self, sha: &Sha256Hex) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.config_bytes_path(sha);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(path)?))
    }

    // -----------------------------------------------------------------
    // Instance-keyed paths
    // -----------------------------------------------------------------

    /// `<root>/<module>/instances/<key.dir_name()>` — everything that makes
    /// "two versions of one module side by side" mean anything: kv, cursor,
    /// watched set, emissions and the last trap fixture.
    pub fn instance_dir(&self, key: &InstanceKey) -> PathBuf {
        self.root
            .join(key.module.as_str())
            .join(INSTANCES_DIR)
            .join(key.dir_name())
    }

    fn instances_root(&self, module: &ModuleId) -> PathBuf {
        self.root.join(module.as_str()).join(INSTANCES_DIR)
    }

    /// Create an instance directory and record its full key in
    /// `instance.toml`.
    ///
    /// **This is the ONLY place an instance directory is created** (the
    /// migration calls it too). Every other open requires the directory to
    /// already exist and fails with [`StorageError::NoSuchInstance`]
    /// otherwise — because `activate` renames a live instance directory, and
    /// anything that could implicitly re-create the old path would leave the
    /// module with two of them and no way to tell which is current.
    pub fn write_instance_marker(&self, key: &InstanceKey) -> Result<(), StorageError> {
        let dir = self.instance_dir(key);
        std::fs::create_dir_all(&dir)?;
        write_atomic(
            &dir.join(INSTANCE_MARKER),
            instance_key_toml(key)?.as_bytes(),
        )
    }

    /// Begin building a NEW instance directory, beside where it will live.
    ///
    /// The whole of it — the state files carried from a parent, the lifecycle
    /// record, the marker — is written under `<dir_name>.partial` and RENAMED
    /// into place by [`PartialInstance::commit`]. Two reasons, and both are
    /// the README's "destination exists must mean destination is complete":
    /// a `.partial` suffix is not a valid instance directory name, and
    /// `list_instances` skips a directory with no `instance.toml` anyway — so
    /// until the rename nothing on the host can see it, not the drain's store
    /// scan, not the admin route, not `boot`.
    ///
    /// A leftover partial from a crash is REMOVED rather than resumed: half a
    /// copied `kv.redb` is exactly the thing that must never be mistaken for
    /// a finished one. `boot` sweeps them too ([`Self::sweep_partial_instances`]).
    pub fn begin_instance(&self, key: &InstanceKey) -> Result<PartialInstance, StorageError> {
        let dir = self.partial_instance_dir(key);
        if dir.exists() {
            tracing::warn!(
                module = %key.module,
                instance = %key.dir_name(),
                "a partial instance directory was left behind by a crash; rebuilding it",
            );
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        Ok(PartialInstance {
            storage: self.clone(),
            key: key.clone(),
            dir,
        })
    }

    fn partial_instance_dir(&self, key: &InstanceKey) -> PathBuf {
        self.instances_root(&key.module)
            .join(format!("{}{PARTIAL_SUFFIX}", key.dir_name()))
    }

    /// Delete every unfinished instance build under a module. Called at boot:
    /// a partial is by construction a directory whose writer is gone.
    ///
    /// Returns how many were removed. One that will not delete is logged and
    /// SKIPPED rather than failing the sweep — one stuck directory must not
    /// stop the module from booting.
    pub fn sweep_partial_instances(&self, module: &ModuleId) -> usize {
        let Ok(entries) = std::fs::read_dir(self.instances_root(module)) else {
            return 0;
        };
        let mut swept = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                || !entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(PARTIAL_SUFFIX)
            {
                continue;
            }
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {
                    swept += 1;
                    tracing::warn!(
                        module = %module,
                        partial = %path.display(),
                        "removed an unfinished instance build left by a crash",
                    );
                }
                Err(e) => tracing::warn!(
                    module = %module,
                    partial = %path.display(),
                    error = %e,
                    "an unfinished instance build could not be removed; leaving it",
                ),
            }
        }
        swept
    }

    /// Every instance of a module, read from the markers. Directories without
    /// a readable marker are skipped — the name alone carries truncated
    /// digests and cannot reconstruct a key.
    pub fn list_instances(&self, module: &ModuleId) -> Result<Vec<InstanceKey>, StorageError> {
        let root = self.instances_root(module);
        let Ok(entries) = std::fs::read_dir(&root) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let marker = entry.path().join(INSTANCE_MARKER);
            let Ok(raw) = std::fs::read_to_string(&marker) else {
                continue;
            };
            match toml::from_str::<InstanceKey>(&raw) {
                // The marker must agree with the directory it sits in. A key
                // whose `dir_name()` is not this directory's name would send
                // every caller to a path that does not exist, so it is not an
                // instance this listing can vouch for. (`repoint_locked`
                // writes the marker before renaming, so the one legitimate
                // disagreement — crashed before the rename — is a directory
                // `current.toml` still resolves directly, never via here.)
                Ok(key) if entry.file_name().to_str() == Some(key.dir_name().as_str()) => {
                    out.push(key);
                }
                Ok(key) => tracing::warn!(
                    marker = %marker.display(),
                    claims = %key.dir_name(),
                    "instance marker disagrees with its directory name; ignoring this directory",
                ),
                Err(e) => tracing::warn!(
                    marker = %marker.display(),
                    error = %e,
                    "instance marker is unreadable; ignoring this instance directory",
                ),
            }
        }
        out.sort_by_key(|k| k.dir_name());
        Ok(out)
    }

    // -----------------------------------------------------------------
    // The `&str` compatibility facade
    // -----------------------------------------------------------------

    fn current_pointer_path(&self, id: &str) -> PathBuf {
        self.module_dir(id).join(CURRENT_POINTER)
    }

    /// Which instance the `&str`-taking methods resolve to. `Ok(None)` means
    /// the module was never activated, which every facade method treats the
    /// way it treated an unregistered module in v2.
    pub fn current_instance(&self, module_id: &str) -> Result<Option<InstanceKey>, StorageError> {
        let _guard = self.repoint.read().expect("repoint lock");
        self.current_instance_locked(module_id, Held::Read)
    }

    /// Point a module's facade at an instance. Write-tmp-then-rename, so a
    /// reader sees the old key or the new one, never a torn file.
    pub fn set_current(&self, key: &InstanceKey) -> Result<(), StorageError> {
        let _guard = self.repoint.write().expect("repoint lock");
        self.set_current_locked(key)
    }

    fn set_current_locked(&self, key: &InstanceKey) -> Result<(), StorageError> {
        let dir = self.module_dir(key.module.as_str());
        std::fs::create_dir_all(&dir)?;
        write_atomic(
            &dir.join(CURRENT_POINTER),
            instance_key_toml(key)?.as_bytes(),
        )
    }

    /// Caller must already hold `repoint`, and says HOW — because the crash
    /// repair below may only be PERSISTED under the write lock.
    ///
    /// A read lock is shared. After a crash the follower, the dialer and the
    /// compactor all resolve the same module within moments of boot; if each
    /// persisted the repair they would race on one `current.toml.new`, and the
    /// loser's rename would fail with `ENOENT` — turning a repairable crash
    /// into a module that does not start, at the one moment that matters.
    /// Under `Held::Read` the repaired key is therefore derived and RETURNED
    /// but not written: every reader derives the same answer from the same
    /// directory listing, so it is idempotent, and the next writer persists it.
    fn current_instance_locked(
        &self,
        module_id: &str,
        held: Held,
    ) -> Result<Option<InstanceKey>, StorageError> {
        let pointer = self.current_pointer_path(module_id);
        let raw = match std::fs::read_to_string(&pointer) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let key: InstanceKey =
            toml::from_str(&raw).map_err(|e| StorageError::MalformedPointer {
                module: module_id.to_owned(),
                file: CURRENT_POINTER,
                why: e.to_string(),
            })?;
        if self.instance_dir(&key).is_dir() {
            return Ok(Some(key));
        }

        // Crash repair, and ONLY for the shape a crash can actually produce:
        // `activate` renames the instance directory first and rewrites the
        // pointer last, so a crash in between leaves the pointer naming a
        // directory that is gone while exactly one instance directory exists.
        // Anything else — zero instances, or several — is a tree we did not
        // write, and guessing which one is current would resume a follower on
        // someone else's cursor.
        let mut found = self.list_instances(&key.module)?;
        if found.len() == 1 {
            let repaired = found.remove(0);
            tracing::warn!(
                module = %module_id,
                stale = %key.dir_name(),
                repaired = %repaired.dir_name(),
                "current.toml named a missing instance directory; repairing it to the module's \
                 only instance (a crash between activate's rename and its repoint)",
            );
            if held == Held::Write {
                self.set_current_locked(&repaired)?;
            }
            return Ok(Some(repaired));
        }
        Err(StorageError::NoSuchInstance {
            module: module_id.to_owned(),
            instance: key.dir_name(),
            found: found.len(),
        })
    }

    /// Where a module's instance-scoped state files live, resolved and built
    /// under one read guard so a concurrent repoint cannot slip between the
    /// two steps.
    ///
    /// A module with no `current.toml` has no instance identity — it was
    /// never activated. Its state stays in the module directory, which is
    /// exactly where v2 put it, so the facade keeps behaving as it did for a
    /// module that only ever had a cursor written to it.
    fn state_dir(&self, module_id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.dir_of(&self.addr_of(module_id)?))
    }

    /// Move every cached handle from one instance key to another, after the
    /// directory holding their files has been renamed.
    ///
    /// Caller holds `repoint` for WRITE. All four caches, together — a
    /// partial re-key would leave one store reachable and another not, and
    /// the follower would be half-alive.
    fn rekey_caches(&self, from: &InstanceKey, to: &InstanceKey) {
        fn rekey<T>(
            cache: &Mutex<HashMap<StateAddr, T>>,
            from: &StateAddr,
            to: StateAddr,
            what: &'static str,
        ) {
            let mut cache = cache.lock().unwrap_or_else(|_| panic!("{what} mutex"));
            if let Some(handle) = cache.remove(from) {
                cache.insert(to, handle);
            }
        }
        let from = StateAddr::Instance(from.clone());
        let to = StateAddr::Instance(to.clone());
        rekey(&self.cursor_stores, &from, to.clone(), "cursor_stores");
        rekey(
            &self.emissions_stores,
            &from,
            to.clone(),
            "emissions_stores",
        );
        rekey(&self.kv_stores, &from, to.clone(), "kv_stores");
        rekey(&self.watched_stores, &from, to, "watched_stores");
    }

    /// Which state a `&str` names: the module's current instance, or the
    /// module directory when it was never activated.
    ///
    /// Resolve and decide under ONE read guard, so a concurrent repoint
    /// cannot slip between them — the same reason `state_dir` held it.
    fn addr_of(&self, module_id: &str) -> Result<StateAddr, StorageError> {
        let _guard = self.repoint.read().expect("repoint lock");
        Ok(match self.current_instance_locked(module_id, Held::Read)? {
            Some(key) => StateAddr::Instance(key),
            None => StateAddr::Unactivated(module_id.to_owned()),
        })
    }

    /// Resolve a target to its cache address and directory. **Caller holds
    /// `repoint` for READ, and keeps holding it until the handle it opens is
    /// IN the cache.**
    ///
    /// Resolving under the guard and looking up after it is not enough. While
    /// the caches were keyed by module id, a `&str` caller whose handle was
    /// already cached could not fail: the key never changed. Keyed by
    /// instance, it can — resolve `K1`, release, an upload renames `K1 → K2`
    /// and re-keys the caches, and the lookup for `K1` now MISSES; the
    /// fallback open of `K1/…` then fails, because that directory is gone. A
    /// transient error, but on paths that treat an open failure as final: the
    /// dialer's drain task logs it and EXITS. Holding the guard makes the
    /// resolution and the lookup one step — before the rename both say `K1`,
    /// after it both say `K2`.
    ///
    /// (It is NOT what keeps a late opener from filing a handle under a stale
    /// key. Each `*_for` holds its cache mutex across the open, and
    /// `rekey_caches` takes the same mutex, so the re-key already waits for
    /// the insert. Keep that too.)
    fn locate_held(&self, target: &Target<'_>) -> Result<(StateAddr, PathBuf), StorageError> {
        match target {
            Target::Current(id) => match self.current_instance_locked(id, Held::Read)? {
                Some(key) => {
                    let dir = self.instance_dir(&key);
                    Ok((StateAddr::Instance(key), dir))
                }
                None => {
                    let addr = StateAddr::Unactivated((*id).to_owned());
                    let dir = self.dir_of(&addr);
                    Ok((addr, dir))
                }
            },
            // Never created here — see `instance_state_dir`.
            Target::Exact(key) => {
                let dir = self.instance_dir(key);
                if !dir.is_dir() {
                    return Err(StorageError::NoSuchInstance {
                        module: key.module.to_string(),
                        instance: key.dir_name(),
                        found: 0,
                    });
                }
                Ok((StateAddr::Instance((*key).clone()), dir))
            }
        }
    }

    /// The directory an address's state files live in.
    ///
    /// The `Unactivated` arm CREATES it, because that is what v2 did for a
    /// module nobody ever activated and `cursor_round_trip` depends on it.
    /// The `Instance` arm never does — see `instance_state_dir`.
    fn dir_of(&self, addr: &StateAddr) -> PathBuf {
        match addr {
            StateAddr::Instance(key) => self.instance_dir(key),
            StateAddr::Unactivated(module) => {
                let dir = self.module_dir(module);
                let _ = std::fs::create_dir_all(&dir);
                dir
            }
        }
    }

    /// The directory of an instance that MUST already exist.
    ///
    /// Every `*_at` method goes through this, and none of them creates
    /// anything. WO-01's review is why: an implicit `create_dir_all` behind
    /// `activate`'s rename resurrects the old directory, and a follower then
    /// resumes on an empty cursor inside it. Instance directories are created
    /// in exactly the two places they are today — `write_instance_marker`,
    /// reached from `activate` and from the migration.
    fn instance_state_dir(&self, key: &InstanceKey) -> Result<PathBuf, StorageError> {
        let _guard = self.repoint.read().expect("repoint lock");
        let dir = self.instance_dir(key);
        if !dir.is_dir() {
            return Err(StorageError::NoSuchInstance {
                module: key.module.to_string(),
                instance: key.dir_name(),
                found: 0,
            });
        }
        Ok(dir)
    }

    /// Rename the module's instance directory to `next`'s name and point
    /// `current.toml` at it. Caller holds `repoint` for WRITE.
    ///
    /// This preserves v2's replace semantics exactly: uploading a new sha
    /// resumed the new code on the OLD state and cursor, and a rename is how
    /// that reads in a layout where the sha is part of the directory name.
    /// A proper client move replaces this later; it is not this change.
    fn repoint_locked(
        &self,
        current: Option<&InstanceKey>,
        next: &InstanceKey,
    ) -> Result<(), StorageError> {
        if let Some(cur) = current
            && cur != next
        {
            let from = self.instance_dir(cur);
            let to = self.instance_dir(next);
            if from.is_dir() {
                if to.exists() {
                    return Err(StorageError::InstanceExists {
                        module: next.module.to_string(),
                        instance: next.dir_name(),
                    });
                }
                // The marker for `next` goes into the directory BEFORE the
                // rename, while it still has its old name. The other order
                // has a window — renamed, marker not yet rewritten — that
                // leaves a directory NAMED for `next` whose marker still says
                // `cur`; the crash repair would then "repair" the pointer to
                // `cur`, whose directory no longer exists, and the module
                // could not start. Marker-first has no such shape: crash
                // before the rename and the pointer still resolves (the
                // directory is where it says); crash after it and directory
                // name, marker and repair all agree on `next`.
                write_atomic(
                    &from.join(INSTANCE_MARKER),
                    instance_key_toml(next)?.as_bytes(),
                )?;
                // The module's FOLLOWER is not running across this:
                // `LifecycleManager::upload` stops it first, because a
                // follower holding the old key can neither write its cursor
                // nor have its emissions fanned out. But handles are still
                // open — the dialer's drain keeps `emissions.redb` for the
                // life of the process, and an admin read may have any of the
                // others. That is safe on unix: a cached redb handle holds a
                // file descriptor, which names an inode and not a path, and
                // redb never reopens by path once `ModuleStorage` has cached
                // the handle.
                std::fs::rename(&from, &to)?;
                // …but the caches are keyed by InstanceKey, so those surviving
                // handles are now filed under a key whose directory is gone.
                // The next `kv_store_at(next)` would miss the cache and open
                // the same file a second time — redb refuses that, and the
                // module cannot start. Re-key inside the same WRITE guard,
                // right after the rename.
                //
                // In memory only, so it opens no crash window. It lives here
                // rather than in the callers because there are two of them
                // (`activate` and `write_config`, since config is half of
                // instance identity) and an upload carrying a config renames
                // TWICE — K1 → K2 → K3. Neither caller can forget it.
                self.rekey_caches(cur, next);
            }
        }
        self.write_instance_marker(next)?;
        #[cfg(test)]
        self.run_repoint_hook();
        // The pointer goes LAST. A crash before this leaves it naming a
        // directory that no longer exists, which is the one shape
        // `current_instance_locked` repairs.
        self.set_current_locked(next)
    }

    #[cfg(test)]
    fn run_repoint_hook(&self) {
        let hook = self
            .repoint_hook
            .lock()
            .expect("repoint_hook mutex")
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Install a callback that runs inside the repoint's critical section,
    /// after the rename and before the pointer write.
    #[cfg(test)]
    fn set_repoint_hook(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.repoint_hook.lock().expect("repoint_hook mutex") = Some(Arc::new(hook));
    }

    /// Remove a module's entire directory (`<storage_root>/<id>/`) — its
    /// pointer, every instance directory under it, and `companions/`. The
    /// content-addressed artifacts it referenced stay in `_cas/`; reclaiming
    /// those is GC's job, not evict's.
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
    pub fn last_trap_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("last-trap.toml"))
    }

    /// The instance's persisted [`LifecycleRecord`] — what an operator WANTS
    /// and what the machine has decided, beside the state it governs.
    ///
    /// Deliberately holds no [`InstanceKey`]: an upload RENAMES the instance
    /// directory to a new key and this file moves with it untouched, so there
    /// is no window in which the two could disagree. The key is read from
    /// `instance.toml`.
    fn lifecycle_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("lifecycle.toml"))
    }

    /// `Ok(None)` means the instance predates lifecycle state — every module
    /// on the first v3 boot — and must be ADOPTED rather than defaulted.
    pub fn read_lifecycle(&self, id: &str) -> Result<Option<LifecycleRecord>, StorageError> {
        self.read_lifecycle_from(&self.lifecycle_path(id)?, id)
    }

    pub fn write_lifecycle(&self, id: &str, record: &LifecycleRecord) -> Result<(), StorageError> {
        write_lifecycle_to(&self.lifecycle_path(id)?, record)
    }

    /// ONE instance's record. Like every other `*_at`, the directory must
    /// already exist — WO-08 creates an instance and writes its first record
    /// in one place, and nothing here may conjure a directory `activate` has
    /// just renamed away.
    fn lifecycle_path_at(&self, key: &InstanceKey) -> Result<PathBuf, StorageError> {
        Ok(self.instance_state_dir(key)?.join(LIFECYCLE_RECORD))
    }

    /// `Ok(None)` means this instance has no record. For the module's CURRENT
    /// instance that means "predates the lifecycle, adopt it"; for any OTHER
    /// instance it means `Unmanaged` — see `LifecycleManager::instances`.
    pub fn read_lifecycle_at(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<LifecycleRecord>, StorageError> {
        self.read_lifecycle_from(&self.lifecycle_path_at(key)?, key.module.as_str())
    }

    pub fn write_lifecycle_at(
        &self,
        key: &InstanceKey,
        record: &LifecycleRecord,
    ) -> Result<(), StorageError> {
        write_lifecycle_to(&self.lifecycle_path_at(key)?, record)
    }

    fn handoffs_path_at(&self, key: &InstanceKey) -> Result<PathBuf, StorageError> {
        Ok(self.instance_state_dir(key)?.join(HANDOFFS))
    }

    /// One instance's hand-off watermarks: for each client it took over, the
    /// slot its predecessor had delivered up to.
    ///
    /// Absent ⇒ [`Handoffs::default`], which admits everything: an instance
    /// nobody has been handed over to withholds nothing. A file that will NOT
    /// PARSE is an error here, and its two callers answer it differently —
    /// the commit refuses (overwriting would lose another client's watermark
    /// and deliver its blocks twice), the drain delivers (see
    /// `run_emit_drain`).
    ///
    /// **Exactly two writers, and they can never overlap.** The lifecycle
    /// manager writes it at the commit, and only with that instance's
    /// follower STOPPED; that follower's own emission drain writes it while
    /// it runs. `stop_instance` awaits the drain task, so there is no instant
    /// at which both could. Nothing else writes it — in particular no admin
    /// route.
    pub fn read_handoffs_at(&self, key: &InstanceKey) -> Result<Handoffs, StorageError> {
        let path = self.handoffs_path_at(key)?;
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Handoffs::default());
            }
            Err(e) => return Err(e.into()),
        };
        toml::from_str(&raw).map_err(|e| StorageError::MalformedPointer {
            module: key.module.to_string(),
            file: HANDOFFS,
            why: e.to_string(),
        })
    }

    pub fn write_handoffs_at(
        &self,
        key: &InstanceKey,
        handoffs: &Handoffs,
    ) -> Result<(), StorageError> {
        let path = self.handoffs_path_at(key)?;
        let toml = toml::to_string(handoffs).map_err(|e| StorageError::Serialise {
            what: "hand-off watermarks",
            why: e.to_string(),
        })?;
        write_atomic(&path, toml.as_bytes())
    }

    fn read_lifecycle_from(
        &self,
        path: &Path,
        module: &str,
    ) -> Result<Option<LifecycleRecord>, StorageError> {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        toml::from_str(&raw)
            .map(Some)
            .map_err(|e| StorageError::MalformedPointer {
                module: module.to_owned(),
                file: LIFECYCLE_RECORD,
                why: e.to_string(),
            })
    }

    /// Remove ONE instance directory, leaving the module — its pointer, its
    /// `companions/`, its `rows/` and its siblings — alone.
    ///
    /// The instance-scoped half of [`Self::remove_module_dir`], and with the
    /// same precondition: **close this instance's cached redb handles first**
    /// (`close_kv_at`, `close_emissions_at`, `close_cursor_at`,
    /// `close_watched_at`), because an open handle holds an OS file lock and
    /// unlinking files underneath one leaves a writer addressing an inode
    /// nothing can reach.
    ///
    /// Idempotent: a missing directory returns `Ok(())`.
    pub fn remove_instance_dir(&self, key: &InstanceKey) -> Result<(), StorageError> {
        // Under the repoint lock: `activate` renames this very directory, and
        // removing it from under a rename would take the module's live state
        // with it.
        let _guard = self.repoint.write().expect("repoint lock");
        let dir = self.instance_dir(key);
        if !dir.exists() {
            return Ok(());
        }
        std::fs::remove_dir_all(&dir).map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "remove instance dir {}: {e}",
                dir.display()
            )))
        })
    }

    /// Per-module companions registration directory.
    /// `<storage_root>/<id>/companions/<companion_key>.cbor` is
    /// where each registered companion's `SubscribeRequest` lives.
    pub fn module_dir_for_companions(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("companions")
    }

    /// Per-module CLIENT ROW directory — `<module>/rows/<client_id>.toml`.
    ///
    /// Module-level, like `companions/`, and for the same reason turned up a
    /// level: a row is the unit of VERSIONING, so it outlives any one
    /// instance and must not live inside one. A client id belongs to at most
    /// one namespace (WO-02), which is what makes it a sufficient file name.
    pub fn module_dir_for_rows(&self, id: &str) -> PathBuf {
        self.module_dir(id).join("rows")
    }

    fn row_path(&self, module: &str, client: &ClientId) -> PathBuf {
        self.module_dir_for_rows(module)
            .join(format!("{client}.toml"))
    }

    /// `Ok(None)` when this client has never announced for this module.
    pub fn read_row(
        &self,
        module: &str,
        client: &ClientId,
    ) -> Result<Option<ClientRow>, StorageError> {
        let path = self.row_path(module, client);
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        toml::from_str(&raw)
            .map(Some)
            .map_err(|e| StorageError::MalformedPointer {
                module: module.to_owned(),
                file: CLIENT_ROW,
                why: e.to_string(),
            })
    }

    pub fn write_row(&self, module: &str, row: &ClientRow) -> Result<(), StorageError> {
        let dir = self.module_dir_for_rows(module);
        std::fs::create_dir_all(&dir)?;
        let toml = toml::to_string(row).map_err(|e| StorageError::Serialise {
            what: "client row",
            why: e.to_string(),
        })?;
        write_atomic(&self.row_path(module, &row.client.client), toml.as_bytes())
    }

    /// Idempotent: a missing row is `Ok(())`.
    pub fn remove_row(&self, module: &str, client: &ClientId) -> Result<(), StorageError> {
        match std::fs::remove_file(self.row_path(module, client)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Every row for a module. A row that will not parse is logged and
    /// skipped rather than failing the whole listing — one unreadable row
    /// must not hide the others from `GET …/rows`.
    pub fn list_rows(&self, module: &str) -> Result<Vec<ClientRow>, StorageError> {
        let dir = self.module_dir_for_rows(module);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "toml") {
                continue;
            }
            match std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|raw| toml::from_str::<ClientRow>(&raw).map_err(|e| e.to_string()))
            {
                Ok(row) => out.push(row),
                Err(e) => tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "client row is unreadable; skipping it",
                ),
            }
        }
        out.sort_by(|a, b| a.client.client.as_str().cmp(b.client.client.as_str()));
        Ok(out)
    }

    /// Count registered companions for a module across the two-level
    /// `<client_id>/<companion_key>.cbor` layout. Best-effort: a
    /// missing companions root (no subscribers yet) counts as 0, and
    /// metadata dirs (`.unreachable/`, etc.) are skipped — mirroring
    /// the walk in `companions::*`. Feeds the `/_admin/status`
    /// per-module summary.
    pub fn count_companions(&self, id: &str) -> usize {
        let root = self.module_dir_for_companions(id);
        let Ok(clients) = std::fs::read_dir(&root) else {
            return 0;
        };
        let mut count = 0;
        for client in clients.flatten() {
            if !client.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            if client.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if let Ok(files) = std::fs::read_dir(client.path()) {
                count += files
                    .flatten()
                    .filter(|f| f.path().extension().is_some_and(|ext| ext == "cbor"))
                    .count();
            }
        }
        count
    }

    /// Per-instance emissions log path, resolved through the module's
    /// current pointer.
    ///
    /// Test-only since the data plane became instance-addressed: production
    /// reaches emissions through `emissions_store` / `emissions_store_at`,
    /// which share one cached `Arc<Database>` per instance (redb is
    /// single-writer). It stays because
    /// `opening_state_for_a_missing_instance_is_an_error_not_a_mkdir` proves
    /// the refusal per PATH, which is a stronger statement than proving it
    /// per store.
    #[cfg(test)]
    pub(crate) fn emissions_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("emissions.redb"))
    }

    /// Per-instance crash-safe KV file (redb). Test-only — see
    /// [`Self::emissions_path`].
    #[cfg(test)]
    pub(crate) fn kv_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("kv.redb"))
    }

    /// Per-instance watched-UTxO index file (redb). Test-only — see
    /// [`Self::emissions_path`].
    #[cfg(test)]
    pub(crate) fn watched_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("watched.redb"))
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

    /// Put the artifact in the CAS and make it the module's current
    /// instance.
    ///
    /// A new sha is a new [`InstanceKey`], hence a new directory name, so
    /// this RENAMES the module's existing instance directory rather than
    /// creating a second one — see [`Self::repoint_locked`] for why that is
    /// both the v2-preserving behaviour and safe to do under a live
    /// follower. The config half of the key is carried over untouched: an
    /// upload without a config field leaves the config as it was.
    pub fn activate(&self, manifest: &Manifest, wasm_bytes: &[u8]) -> Result<(), StorageError> {
        let module = ModuleId::parse(&manifest.module.id)?;
        // Outside the write lock on purpose: a CAS put is content-addressed
        // and touches nothing any resolution can observe.
        let wasm = self.put_artifact(manifest, wasm_bytes)?;

        let _guard = self.repoint.write().expect("repoint lock");
        let current = self.current_instance_locked(module.as_str(), Held::Write)?;
        let next = InstanceKey {
            module,
            wasm,
            config: current.as_ref().and_then(|c| c.config.clone()),
        };
        self.repoint_locked(current.as_ref(), &next)
    }

    /// Return the manifest of the currently-active instance, or None
    /// if no module is registered under this id.
    pub fn read_manifest(&self, id: &str) -> Result<Option<Manifest>, StorageError> {
        let Some(key) = self.current_instance(id)? else {
            return Ok(None);
        };
        self.read_artifact_manifest(&key.wasm)
    }

    /// Read the wasm bytes of the currently-active instance.
    pub fn read_current_wasm(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(key) = self.current_instance(id)? else {
            return Ok(None);
        };
        let path = self.artifact_path(&key.wasm);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(std::fs::read(path)?))
    }

    /// The current instance's wasm path, for callers that hand it to
    /// wasmtime `Component::from_file` rather than allocating the bytes.
    pub fn current_wasm_path(&self, id: &str) -> Result<Option<PathBuf>, StorageError> {
        let Some(key) = self.current_instance(id)? else {
            return Ok(None);
        };
        let path = self.artifact_path(&key.wasm);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(path))
    }

    /// Per-instance crash-safe cursor file (redb).
    ///
    /// Test-only, like its three siblings: production reaches the cursor
    /// through `cursor_store_for`, which resolves and opens under one guard.
    #[cfg(test)]
    pub(crate) fn cursor_path(&self, id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.state_dir(id)?.join("cursor.redb"))
    }

    /// The CAS path of the current instance's config bytes, or `Ok(None)`
    /// when the module is unregistered or runs with no config at all.
    pub fn config_path(&self, id: &str) -> Result<Option<PathBuf>, StorageError> {
        let Some(key) = self.current_instance(id)? else {
            return Ok(None);
        };
        Ok(key.config.map(|sha| self.config_bytes_path(&sha)))
    }

    /// Store config bytes and make them part of the module's instance
    /// identity. Same rename-and-repoint as [`Self::activate`], because a
    /// different config is a different instance.
    pub fn write_config(&self, id: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let config = self.put_config(bytes)?;

        let _guard = self.repoint.write().expect("repoint lock");
        let current = self
            .current_instance_locked(id, Held::Write)?
            .ok_or_else(|| StorageError::NotActivated(id.to_owned()))?;
        let next = InstanceKey {
            module: current.module.clone(),
            wasm: current.wasm.clone(),
            config: Some(config),
        };
        self.repoint_locked(Some(&current), &next)
    }

    /// Read the current instance's config bytes, if any. `Ok(None)` means
    /// the instance runs with no config — the host calls `init(&[])`, which
    /// it distinguishes from an empty config.
    pub fn read_config(&self, id: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(key) = self.current_instance(id)? else {
            return Ok(None);
        };
        let Some(sha) = key.config else {
            return Ok(None);
        };
        self.read_config_bytes(&sha)
    }

    /// Persist the driver's last-applied cursor atomically.
    /// Reuses the open `CursorStore` per module — one redb open
    /// per module lifetime, one redb commit per block. See
    /// `ModuleStorage` doc comment for why this matters.
    pub fn write_cursor(&self, id: &str, cursor: &ChainPoint) -> Result<(), StorageError> {
        self.write_cursor_for(&Target::Current(id), cursor)
    }

    /// Read the persisted cursor, if any. `Ok(None)` means the
    /// module has no checkpoint yet; caller should start from
    /// `ChainPoint::Origin` (or whatever the configured start
    /// point is).
    pub fn read_cursor(&self, id: &str) -> Result<Option<ChainPoint>, StorageError> {
        self.read_cursor_for(&Target::Current(id))
    }

    /// List registered module ids — the directories that have a
    /// `current.toml`, i.e. that have an instance the facade can resolve.
    pub fn list_modules(&self) -> Result<Vec<String>, StorageError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
                && self.current_pointer_path(name).exists()
            {
                out.push(name.to_owned());
            }
        }
        out.sort();
        Ok(out)
    }
}

/// A new instance directory under construction. See
/// [`ModuleStorage::begin_instance`].
///
/// Dropped without [`Self::commit`], the partial stays on disk for `boot` to
/// sweep — deleting it in `Drop` would mean a best-effort filesystem
/// operation running on a panic unwind, which is how one half-removed
/// directory becomes two.
#[derive(Clone)]
pub struct PartialInstance {
    storage: ModuleStorage,
    key: InstanceKey,
    dir: PathBuf,
}

impl PartialInstance {
    /// Throw the build away. For a build that FAILED while the host is still
    /// up: boot's sweep would get it eventually, but until then it is up to a
    /// parent's worth of disk (1.3 GB for the largest module) that nothing
    /// will ever use. Best-effort — the sweep is still there behind it.
    pub fn discard(self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            tracing::warn!(
                module = %self.key.module,
                instance = %self.key.dir_name(),
                error = %e,
                "discarding a failed instance build failed; boot will sweep it",
            );
        }
    }

    pub fn key(&self) -> &InstanceKey {
        &self.key
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Carry one state file across from `parent`. Returns the bytes copied.
    ///
    /// **The parent's follower must be stopped**, so that `kv`, `cursor` and
    /// `watched` all describe the same block and redb has flushed — see
    /// `mitos_lifecycle::state_prep`'s rule 1. This does not check it: only
    /// the lifecycle manager, holding the module lock, can know.
    ///
    /// A file the parent never created is `Ok(0)`: starting without it is
    /// exactly what starting with an empty one means, and a `watched.redb`
    /// that was never opened is the ordinary case.
    pub fn carry(&self, parent: &InstanceKey, file: StateFile) -> Result<u64, StorageError> {
        let from = self.storage.instance_dir(parent).join(file.file_name());
        if !from.is_file() {
            return Ok(0);
        }
        let to = self.dir.join(file.file_name());
        // `reflink_or_copy` — a metadata clone where the filesystem has one,
        // a byte copy where it does not. NOT `std::fs::copy`: identical on
        // today's ext4, and the difference between seconds and nothing on the
        // next box's XFS, which is chosen for this.
        reflink_copy::reflink_or_copy(&from, &to).map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "copy {} → {}: {e}",
                from.display(),
                to.display()
            )))
        })?;
        Ok(std::fs::metadata(&to).map(|m| m.len()).unwrap_or(0))
    }

    pub fn write_lifecycle(&self, record: &LifecycleRecord) -> Result<(), StorageError> {
        write_lifecycle_to(&self.dir.join(LIFECYCLE_RECORD), record)
    }

    /// Write the marker LAST, then rename the whole directory into place.
    ///
    /// The marker last because it is what makes a directory an instance:
    /// `list_instances` skips one without it. So a crash anywhere before this
    /// line leaves something nothing can resolve, and a crash after the
    /// rename leaves an instance that is complete.
    ///
    /// The rename never crosses a filesystem — the partial is a sibling of
    /// its destination — which is what makes it atomic. Under the repoint
    /// WRITE lock, because `activate`'s rename shares this parent directory.
    pub fn commit(self) -> Result<(), StorageError> {
        write_atomic(
            &self.dir.join(INSTANCE_MARKER),
            instance_key_toml(&self.key)?.as_bytes(),
        )?;
        let _guard = self.storage.repoint.write().expect("repoint lock");
        let to = self.storage.instance_dir(&self.key);
        if to.exists() {
            return Err(StorageError::InstanceExists {
                module: self.key.module.to_string(),
                instance: self.key.dir_name(),
            });
        }
        std::fs::rename(&self.dir, &to)?;
        Ok(())
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
                wit_sha: Some(crate::manifest::host_wit_sha().to_owned()),
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

    fn sha(bytes: &[u8]) -> Sha256Hex {
        Sha256Hex::parse(&sha256_hex(bytes)).unwrap()
    }

    fn module(id: &str) -> ModuleId {
        ModuleId::parse(id).unwrap()
    }

    /// The instance directories a module has, by name.
    fn instance_dirs(root: &Path, id: &str) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(root.join(id).join("instances"))
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    #[test]
    fn activate_puts_the_artifact_in_the_cas_and_points_current_at_it() {
        let dir = tempdir("activate");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"fake wasm bytes 1";
        let manifest = sample_manifest(wasm);

        storage.activate(&manifest, wasm).unwrap();

        // The artifact is content-addressed, with its manifest beside it.
        let wasm_sha = sha(wasm);
        assert_eq!(
            std::fs::read(storage.artifact_path(&wasm_sha)).unwrap(),
            wasm
        );
        assert!(
            dir.join("_cas")
                .join("wasm")
                .join(format!("{wasm_sha}.manifest.toml"))
                .is_file()
        );

        // The facade resolves through `current.toml` to exactly that.
        let read = storage.read_current_wasm("test-module").unwrap().unwrap();
        assert_eq!(read, wasm);
        let read_manifest = storage.read_manifest("test-module").unwrap().unwrap();
        assert_eq!(read_manifest, manifest);
        let path = storage.current_wasm_path("test-module").unwrap().unwrap();
        assert_eq!(path, storage.artifact_path(&wasm_sha));

        // And the instance exists, with a marker carrying the untruncated key.
        let key = InstanceKey {
            module: module("test-module"),
            wasm: wasm_sha,
            config: None,
        };
        assert_eq!(
            storage.current_instance("test-module").unwrap(),
            Some(key.clone())
        );
        assert!(dir.join("test-module").join(CURRENT_POINTER).is_file());
        assert!(storage.instance_dir(&key).join(INSTANCE_MARKER).is_file());
        assert_eq!(storage.list_instances(&key.module).unwrap(), vec![key]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn activating_a_second_sha_renames_the_one_instance_directory() {
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
        // Something in the instance directory, so the rename is observable.
        storage
            .write_cursor("test-module", &ChainPoint::Slot(42))
            .unwrap();

        let wasm_b = b"version B different";
        let mut manifest_b = sample_manifest(wasm_b);
        manifest_b.module.sha256 = sha256_hex(wasm_b);
        manifest_b.module.size_bytes = wasm_b.len() as u64;
        storage.activate(&manifest_b, wasm_b).unwrap();
        assert_eq!(
            storage.read_current_wasm("test-module").unwrap().unwrap(),
            wasm_b
        );

        // ONE instance directory, renamed — not two. v2 replaced in place and
        // the new code resumed on the old state; a rename is how that reads
        // when the sha is part of the directory name.
        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm_b),
            config: None,
        };
        assert_eq!(instance_dirs(&dir, "test-module"), vec![key_b.dir_name()]);
        assert_eq!(
            storage.current_instance("test-module").unwrap(),
            Some(key_b)
        );
        assert!(storage.read_cursor("test-module").unwrap().is_some());

        // Both artifacts stay in the CAS — identical bytes, one copy each,
        // owned by no instance.
        assert!(storage.artifact_path(&sha(wasm_a)).exists());
        assert!(storage.artifact_path(&sha(wasm_b)).exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_artifact_refuses_bytes_that_do_not_match_the_manifest() {
        let dir = tempdir("cas-mismatch");
        let storage = ModuleStorage::new(&dir);
        let manifest = sample_manifest(b"the bytes the manifest describes");

        let err = storage
            .put_artifact(&manifest, b"some entirely other bytes")
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ArtifactShaMismatch { .. }),
            "{err}"
        );
        // Nothing was written under a digest that does not describe it.
        assert!(!dir.join("_cas").exists());

        // And the same refusal through the facade.
        let err = storage
            .activate(&manifest, b"some entirely other bytes")
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ArtifactShaMismatch { .. }),
            "{err}"
        );
        assert!(storage.current_instance("test-module").unwrap().is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn put_artifact_twice_is_a_no_op() {
        let dir = tempdir("cas-idempotent");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"content addressed bytes";
        let manifest = sample_manifest(wasm);

        let first = storage.put_artifact(&manifest, wasm).unwrap();
        let path = storage.artifact_path(&first);
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Coarse filesystem timestamps would hide a rewrite otherwise.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let second = storage.put_artifact(&manifest, wasm).unwrap();

        assert_eq!(first, second);
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);
        assert_eq!(std::fs::read(&path).unwrap(), wasm);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_configs_of_one_wasm_are_two_instance_dirs() {
        let dir = tempdir("two-configs");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"one artifact, two deployments";
        let manifest = sample_manifest(wasm);

        let wasm_sha = storage.put_artifact(&manifest, wasm).unwrap();
        let left = storage.put_config(b"config for the left one").unwrap();
        let right = storage.put_config(b"config for the right one").unwrap();
        assert_ne!(left, right);

        let key_left = InstanceKey {
            module: module("test-module"),
            wasm: wasm_sha.clone(),
            config: Some(left),
        };
        let key_right = InstanceKey {
            module: module("test-module"),
            wasm: wasm_sha,
            config: Some(right),
        };
        storage.write_instance_marker(&key_left).unwrap();
        storage.write_instance_marker(&key_right).unwrap();

        assert_ne!(
            storage.instance_dir(&key_left),
            storage.instance_dir(&key_right)
        );
        assert_eq!(instance_dirs(&dir, "test-module").len(), 2);
        let mut expected = vec![key_left, key_right];
        expected.sort_by_key(|k| k.dir_name());
        assert_eq!(
            storage.list_instances(&module("test-module")).unwrap(),
            expected
        );
        // One wasm, one copy of it.
        assert_eq!(
            std::fs::read_dir(dir.join("_cas").join("wasm"))
                .unwrap()
                .count(),
            2, // the artifact and its manifest
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn activating_a_new_sha_carries_state_forward() {
        let dir = tempdir("carry-forward");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"carry A";
        let mut manifest_a = sample_manifest(wasm_a);
        manifest_a.module.sha256 = sha256_hex(wasm_a);
        manifest_a.module.size_bytes = wasm_a.len() as u64;
        storage.activate(&manifest_a, wasm_a).unwrap();
        storage
            .write_cursor("test-module", &ChainPoint::Slot(186_000_000))
            .unwrap();

        let wasm_b = b"carry B";
        let mut manifest_b = sample_manifest(wasm_b);
        manifest_b.module.sha256 = sha256_hex(wasm_b);
        manifest_b.module.size_bytes = wasm_b.len() as u64;
        storage.activate(&manifest_b, wasm_b).unwrap();

        // Through the live handle, whose fd survived the rename.
        assert_eq!(
            storage.read_cursor("test-module").unwrap().unwrap().slot(),
            186_000_000
        );
        // And the file itself moved with the directory, so a cold host
        // resolves it by name too. (Opening it here would trip redb's
        // single-writer lock — the handle above still holds the file.)
        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm_b),
            config: None,
        };
        assert_eq!(
            storage.cursor_path("test-module").unwrap(),
            storage.instance_dir(&key_b).join("cursor.redb"),
        );
        assert!(storage.instance_dir(&key_b).join("cursor.redb").is_file());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_crash_between_rename_and_repoint_is_repaired_on_read() {
        let dir = tempdir("repair");
        let storage = ModuleStorage::new(&dir);
        let wasm_a = b"repair A";
        let mut manifest_a = sample_manifest(wasm_a);
        manifest_a.module.sha256 = sha256_hex(wasm_a);
        manifest_a.module.size_bytes = wasm_a.len() as u64;
        storage.activate(&manifest_a, wasm_a).unwrap();

        let key_a = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm_a),
            config: None,
        };
        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(b"repair B"),
            config: None,
        };

        // Exactly what a crash after `activate`'s rename leaves behind: the
        // directory is renamed and its marker rewritten, `current.toml` still
        // names the old one.
        std::fs::rename(storage.instance_dir(&key_a), storage.instance_dir(&key_b)).unwrap();
        std::fs::write(
            storage.instance_dir(&key_b).join(INSTANCE_MARKER),
            instance_key_toml(&key_b).unwrap(),
        )
        .unwrap();

        assert_eq!(
            storage.current_instance("test-module").unwrap(),
            Some(key_b.clone()),
        );
        // A READ derives the repair and does not persist it: the read lock is
        // shared, and concurrent readers racing on one `current.toml.new`
        // would turn a repairable crash into a module that cannot start.
        let pointer_path = dir.join("test-module").join(CURRENT_POINTER);
        let pointer = std::fs::read_to_string(&pointer_path).unwrap();
        assert_eq!(toml::from_str::<InstanceKey>(&pointer).unwrap(), key_a);
        // …and every reader derives the same answer, so that is safe.
        assert_eq!(
            storage.current_instance("test-module").unwrap(),
            Some(key_b.clone()),
        );

        // The next WRITER persists it. Re-activating the same artifact is the
        // smallest such write.
        let wasm_b = b"repair B";
        let mut manifest_b = sample_manifest(wasm_b);
        manifest_b.module.sha256 = sha256_hex(wasm_b);
        manifest_b.module.size_bytes = wasm_b.len() as u64;
        storage.activate(&manifest_b, wasm_b).unwrap();
        let pointer = std::fs::read_to_string(&pointer_path).unwrap();
        assert_eq!(toml::from_str::<InstanceKey>(&pointer).unwrap(), key_b);
        assert_eq!(instance_dirs(&dir, "test-module").len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The window `repoint_locked`'s marker-first ordering closes. With the
    /// marker written AFTER the rename, a crash between the two left a
    /// directory named for B whose marker said A — and the repair pointed the
    /// module at A, which no longer existed.
    #[test]
    fn a_crash_before_the_rename_still_resolves_to_the_old_instance() {
        let dir = tempdir("crash-before-rename");
        let storage = ModuleStorage::new(&dir);
        let wasm_a = b"before rename A";
        let mut manifest_a = sample_manifest(wasm_a);
        manifest_a.module.sha256 = sha256_hex(wasm_a);
        manifest_a.module.size_bytes = wasm_a.len() as u64;
        storage.activate(&manifest_a, wasm_a).unwrap();

        let key_a = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm_a),
            config: None,
        };
        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(b"before rename B"),
            config: None,
        };
        // Marker rewritten for B, crash before the rename: the directory
        // still has A's name and `current.toml` still says A.
        std::fs::write(
            storage.instance_dir(&key_a).join(INSTANCE_MARKER),
            instance_key_toml(&key_b).unwrap(),
        )
        .unwrap();

        assert_eq!(
            storage.current_instance("test-module").unwrap(),
            Some(key_a),
            "the pointer names a directory that exists, so it wins outright"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_marker_that_disagrees_with_its_directory_is_not_listed() {
        let dir = tempdir("marker-mismatch");
        let storage = ModuleStorage::new(&dir);
        let key_a = InstanceKey {
            module: module("test-module"),
            wasm: sha(b"mismatch A"),
            config: None,
        };
        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(b"mismatch B"),
            config: None,
        };
        storage.write_instance_marker(&key_a).unwrap();
        assert_eq!(
            storage.list_instances(&key_a.module).unwrap(),
            vec![key_a.clone()]
        );

        std::fs::write(
            storage.instance_dir(&key_a).join(INSTANCE_MARKER),
            instance_key_toml(&key_b).unwrap(),
        )
        .unwrap();
        assert!(
            storage.list_instances(&key_a.module).unwrap().is_empty(),
            "a key whose dir_name() is not this directory would send callers to a missing path"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_dangling_pointer_with_no_instance_is_an_error_not_a_guess() {
        let dir = tempdir("dangling");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"dangling";
        let manifest = sample_manifest(wasm);
        storage.activate(&manifest, wasm).unwrap();

        std::fs::remove_dir_all(dir.join("test-module").join("instances")).unwrap();

        let err = storage.current_instance("test-module").unwrap_err();
        assert!(
            matches!(err, StorageError::NoSuchInstance { found: 0, .. }),
            "{err}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn opening_state_for_a_missing_instance_is_an_error_not_a_mkdir() {
        let dir = tempdir("no-mkdir");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"no mkdir";
        let manifest = sample_manifest(wasm);
        storage.activate(&manifest, wasm).unwrap();

        let instances = dir.join("test-module").join("instances");
        std::fs::remove_dir_all(&instances).unwrap();

        // Every instance-scoped path refuses, by name, rather than quietly
        // re-creating the directory `activate` just renamed away.
        type Probe<'a> = Box<dyn Fn() -> Result<PathBuf, StorageError> + 'a>;
        let probes: [(&str, Probe<'_>); 5] = [
            ("kv", Box::new(|| storage.kv_path("test-module"))),
            ("cursor", Box::new(|| storage.cursor_path("test-module"))),
            ("watched", Box::new(|| storage.watched_path("test-module"))),
            (
                "emissions",
                Box::new(|| storage.emissions_path("test-module")),
            ),
            (
                "last-trap",
                Box::new(|| storage.last_trap_path("test-module")),
            ),
        ];
        for (what, probe) in probes {
            let err = probe().unwrap_err();
            assert!(
                matches!(err, StorageError::NoSuchInstance { .. }),
                "{what}: {err}"
            );
            assert!(!instances.exists(), "{what} re-created the instances dir");
        }

        // The typed stores refuse too, rather than opening redb somewhere new.
        assert!(storage.kv_store("test-module", None).is_err());
        assert!(storage.emissions_store("test-module").is_err());
        assert!(storage.read_cursor("test-module").is_err());
        assert!(
            !instances.exists(),
            "a store open re-created the instances dir"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_trap_written_during_activate_lands_in_the_new_directory() {
        use std::sync::mpsc;

        let dir = tempdir("trap-during-activate");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"trap A";
        let mut manifest_a = sample_manifest(wasm_a);
        manifest_a.module.sha256 = sha256_hex(wasm_a);
        manifest_a.module.size_bytes = wasm_a.len() as u64;
        storage.activate(&manifest_a, wasm_a).unwrap();

        // The hook holds the repoint's critical section open long enough for
        // the other thread to reach `last_trap_path` and block on the read
        // guard. The sleep only widens the window — if the writer arrives
        // late it resolves the new pointer instead, which is the same result.
        let (mid_repoint, wait_for_repoint) = mpsc::channel();
        storage.set_repoint_hook(move || {
            mid_repoint.send(()).expect("test receiver alive");
            std::thread::sleep(std::time::Duration::from_millis(250));
        });

        let writer_storage = storage.clone();
        let writer = std::thread::spawn(move || {
            wait_for_repoint.recv().expect("repoint started");
            let path = writer_storage.last_trap_path("test-module").unwrap();
            std::fs::write(&path, "trapped").unwrap();
        });

        let wasm_b = b"trap B";
        let mut manifest_b = sample_manifest(wasm_b);
        manifest_b.module.sha256 = sha256_hex(wasm_b);
        manifest_b.module.size_bytes = wasm_b.len() as u64;
        storage.activate(&manifest_b, wasm_b).unwrap();
        writer.join().expect("writer thread");

        let key_b = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm_b),
            config: None,
        };
        assert_eq!(
            instance_dirs(&dir, "test-module"),
            vec![key_b.dir_name()],
            "the trap write resurrected the old instance directory",
        );
        assert_eq!(
            std::fs::read_to_string(storage.instance_dir(&key_b).join("last-trap.toml")).unwrap(),
            "trapped",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `&str` caller whose handle is ALREADY cached must not be able to
    /// fail because an upload landed mid-call. Keyed by module id it could
    /// not; keyed by instance it can, unless resolving and looking up are one
    /// step: resolve `K1`, the upload renames and re-keys, the lookup for `K1`
    /// misses, and the fallback open of `K1/cursor.redb` finds no directory.
    #[test]
    fn a_cached_handle_survives_an_upload_that_lands_mid_call() {
        use std::sync::mpsc;

        let dir = tempdir("upload-mid-call");
        let storage = ModuleStorage::new(&dir);
        let wasm_a = b"mid-call A";
        storage.activate(&sample_manifest(wasm_a), wasm_a).unwrap();
        let cursor = ChainPoint::Slot(42);
        // Cached from here on.
        storage.write_cursor("test-module", &cursor).unwrap();

        // Park the reader between "located" and "looked up", and start the
        // upload while it is parked.
        let (located, wait_until_located) = mpsc::channel();
        storage
            .located_hook
            .lock()
            .unwrap()
            .replace(Arc::new(move || {
                // Only the first call parks; later ones must not block.
                if located.send(()).is_ok() {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                }
            }));

        let reader_storage = storage.clone();
        let reader = std::thread::spawn(move || reader_storage.read_cursor("test-module"));

        wait_until_located.recv().expect("reader reached the hook");
        drop(wait_until_located);
        let wasm_b = b"mid-call B";
        storage.activate(&sample_manifest(wasm_b), wasm_b).unwrap();

        assert_eq!(
            reader.join().expect("reader thread").unwrap(),
            Some(cursor.clone()),
            "a cached read failed because an upload renamed the instance mid-call",
        );
        // …and the handle followed the rename.
        assert_eq!(storage.read_cursor("test-module").unwrap(), Some(cursor));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writing_a_config_makes_it_part_of_the_instance_identity() {
        let dir = tempdir("config-identity");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"configured";
        let manifest = sample_manifest(wasm);
        storage.activate(&manifest, wasm).unwrap();
        assert!(storage.read_config("test-module").unwrap().is_none());

        storage.write_config("test-module", b"cbor bytes").unwrap();

        let key = storage.current_instance("test-module").unwrap().unwrap();
        assert_eq!(key.config, Some(sha(b"cbor bytes")));
        assert_eq!(
            storage.read_config("test-module").unwrap().unwrap(),
            b"cbor bytes"
        );
        assert_eq!(instance_dirs(&dir, "test-module"), vec![key.dir_name()]);

        // A later wasm upload carries the config half of the key over — an
        // upload without a config field leaves the config as it was.
        let wasm_next = b"configured, rebuilt";
        let mut next = sample_manifest(wasm_next);
        next.module.sha256 = sha256_hex(wasm_next);
        next.module.size_bytes = wasm_next.len() as u64;
        storage.activate(&next, wasm_next).unwrap();
        assert_eq!(
            storage.read_config("test-module").unwrap().unwrap(),
            b"cbor bytes"
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

    // ---------------------------------------------------------------------
    // Instance-addressed handles. Until these, every cache was keyed by
    // module id, which is exactly why a second instance could not run: both
    // would share one `kv.redb` handle and overwrite each other's cursor.
    // ---------------------------------------------------------------------

    /// An instance directory built the only way one can be outside
    /// `activate` — the marker write, which is also how the host tests make
    /// a second instance.
    fn second_instance(storage: &ModuleStorage, wasm: &[u8]) -> InstanceKey {
        let manifest = sample_manifest(wasm);
        storage.put_artifact(&manifest, wasm).unwrap();
        let key = InstanceKey {
            module: module("test-module"),
            wasm: sha(wasm),
            config: None,
        };
        storage.write_instance_marker(&key).unwrap();
        key
    }

    #[test]
    fn two_instances_of_one_module_have_separate_handles() {
        let dir = tempdir("two-instances");
        let storage = ModuleStorage::new(&dir);
        let wasm_a = b"instance A";
        storage.activate(&sample_manifest(wasm_a), wasm_a).unwrap();
        let a = storage.current_instance("test-module").unwrap().unwrap();
        let b = second_instance(&storage, b"instance B");
        assert_ne!(a, b);

        // Two cursors, advancing independently.
        storage.write_cursor_at(&a, &ChainPoint::Slot(100)).unwrap();
        storage.write_cursor_at(&b, &ChainPoint::Slot(900)).unwrap();
        assert_eq!(storage.read_cursor_at(&a).unwrap().unwrap().slot(), 100);
        assert_eq!(storage.read_cursor_at(&b).unwrap().unwrap().slot(), 900);

        // Two KVs. Opening both at once also proves redb's single-writer
        // lock is not being tripped — one file each, not one shared.
        let kv_a = storage.kv_store_at(&a, None).unwrap();
        let kv_b = storage.kv_store_at(&b, None).unwrap();
        kv_a.set_value("test-module", "k", b"from A".to_vec())
            .unwrap();
        kv_b.set_value("test-module", "k", b"from B".to_vec())
            .unwrap();
        assert_eq!(kv_a.get_value("test-module", "k").unwrap(), b"from A");
        assert_eq!(kv_b.get_value("test-module", "k").unwrap(), b"from B");

        // And two emissions logs.
        assert!(storage.emissions_store_at(&a).is_ok());
        assert!(storage.emissions_store_at(&b).is_ok());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Every `*_at` method refuses a missing instance and creates nothing.
    /// WO-01's review is why: an implicit `create_dir_all` behind
    /// `activate`'s rename resurrects the old directory, and a follower then
    /// resumes on an empty cursor inside it.
    #[test]
    fn an_at_call_on_a_missing_instance_is_an_error_and_creates_nothing() {
        let dir = tempdir("at-missing");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"at missing";
        storage.activate(&sample_manifest(wasm), wasm).unwrap();
        let before = instance_dirs(&dir, "test-module");

        // A key whose directory was never created.
        let ghost = InstanceKey {
            module: module("test-module"),
            wasm: sha(b"never written"),
            config: None,
        };

        assert!(storage.kv_store_at(&ghost, None).is_err());
        assert!(storage.emissions_store_at(&ghost).is_err());
        assert!(storage.read_cursor_at(&ghost).is_err());
        assert!(
            storage
                .write_cursor_at(&ghost, &ChainPoint::Slot(1))
                .is_err()
        );
        assert!(storage.last_trap_path_at(&ghost).is_err());
        // `watched_index_at` degrades to in-memory rather than failing —
        // its established posture — but must still write nothing.
        let _ = storage.watched_index_at(&ghost);

        assert_eq!(
            instance_dirs(&dir, "test-module"),
            before,
            "an `*_at` call created an instance directory",
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `activate` RENAMES the live instance directory, and the open redb
    /// handles survive it because they hold fds. But the caches are keyed by
    /// `InstanceKey`, so without a re-key the next `*_at(K2)` would miss and
    /// open the same file twice — which redb refuses.
    #[test]
    fn activate_carries_open_handles_to_the_new_key() {
        let dir = tempdir("carry-handles");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"handles A";
        storage.activate(&sample_manifest(wasm_a), wasm_a).unwrap();
        let k1 = storage.current_instance("test-module").unwrap().unwrap();

        // All four open, with something written through each.
        let kv = storage.kv_store_at(&k1, None).unwrap();
        kv.set_value("test-module", "carried", b"yes".to_vec())
            .unwrap();
        storage
            .write_cursor_at(&k1, &ChainPoint::Slot(777))
            .unwrap();
        let _emissions = storage.emissions_store_at(&k1).unwrap();
        let _watched = storage.watched_index_at(&k1);

        let wasm_b = b"handles B";
        storage.activate(&sample_manifest(wasm_b), wasm_b).unwrap();
        let k2 = storage.current_instance("test-module").unwrap().unwrap();
        assert_ne!(k1, k2);

        // The SAME handles, reachable under the new key — and no second
        // redb open, which would fail with "Database already open".
        assert_eq!(
            storage
                .kv_store_at(&k2, None)
                .unwrap()
                .get_value("test-module", "carried")
                .unwrap(),
            b"yes",
        );
        assert_eq!(storage.read_cursor_at(&k2).unwrap().unwrap().slot(), 777);
        assert!(storage.emissions_store_at(&k2).is_ok());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// An upload carrying a config renames TWICE — K1 → K2 (new wasm), then
    /// K2 → K3 (new config, which is half of instance identity). The re-key
    /// lives in `repoint_locked` so neither caller can forget one.
    #[test]
    fn an_upload_with_a_config_carries_them_across_both_renames() {
        let dir = tempdir("carry-two-renames");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"two renames A";
        storage.activate(&sample_manifest(wasm_a), wasm_a).unwrap();
        let k1 = storage.current_instance("test-module").unwrap().unwrap();
        let kv = storage.kv_store_at(&k1, None).unwrap();
        kv.set_value("test-module", "carried", b"twice".to_vec())
            .unwrap();
        storage
            .write_cursor_at(&k1, &ChainPoint::Slot(4242))
            .unwrap();

        let wasm_b = b"two renames B";
        storage.activate(&sample_manifest(wasm_b), wasm_b).unwrap();
        storage.write_config("test-module", b"a config").unwrap();
        let k3 = storage.current_instance("test-module").unwrap().unwrap();
        assert!(k3.config.is_some());

        assert_eq!(
            storage
                .kv_store_at(&k3, None)
                .unwrap()
                .get_value("test-module", "carried")
                .unwrap(),
            b"twice",
        );
        assert_eq!(storage.read_cursor_at(&k3).unwrap().unwrap().slot(), 4242);
        assert_eq!(
            instance_dirs(&dir, "test-module"),
            vec![k3.dir_name()],
            "two renames must leave ONE directory, not three",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Between `activate`'s rename and the upload handler's `replace`, the
    /// running follower still holds K1. Its cursor write must fail — and
    /// resurrect nothing. `flush_cursor` already logs and carries on; the
    /// follower is stopped milliseconds later and the restart replays at
    /// most one block, which is the at-least-once the cursor always had.
    #[test]
    fn a_cursor_write_to_the_old_key_after_a_rename_fails_and_resurrects_nothing() {
        let dir = tempdir("stale-cursor-write");
        let storage = ModuleStorage::new(&dir);

        let wasm_a = b"stale A";
        storage.activate(&sample_manifest(wasm_a), wasm_a).unwrap();
        let k1 = storage.current_instance("test-module").unwrap().unwrap();
        storage.write_cursor_at(&k1, &ChainPoint::Slot(1)).unwrap();

        let wasm_b = b"stale B";
        storage.activate(&sample_manifest(wasm_b), wasm_b).unwrap();
        let k2 = storage.current_instance("test-module").unwrap().unwrap();

        let err = storage
            .write_cursor_at(&k1, &ChainPoint::Slot(2))
            .unwrap_err();
        assert!(matches!(err, StorageError::NoSuchInstance { .. }), "{err}");
        assert_eq!(
            instance_dirs(&dir, "test-module"),
            vec![k2.dir_name()],
            "the stale write resurrected the old instance directory",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The `Unactivated` path: a module that was never activated keeps its
    /// state in the module directory, exactly where v2 put it.
    #[test]
    fn an_unactivated_module_still_takes_a_cursor() {
        let dir = tempdir("unactivated");
        let storage = ModuleStorage::new(&dir);

        assert!(storage.read_cursor("never-activated").unwrap().is_none());
        storage
            .write_cursor("never-activated", &ChainPoint::Slot(5))
            .unwrap();
        assert_eq!(
            storage
                .read_cursor("never-activated")
                .unwrap()
                .unwrap()
                .slot(),
            5,
        );
        assert!(
            dir.join("never-activated").join("cursor.redb").is_file(),
            "an unactivated module's state belongs in the module directory",
        );
        assert!(
            !dir.join("never-activated").join("instances").exists(),
            "and must not have invented an instance",
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
