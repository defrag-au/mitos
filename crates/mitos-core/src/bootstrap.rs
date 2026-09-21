//! `mitos bootstrap` — one Mithril fetch, one chunk store, no second binary.
//!
//! The plan is a pure function. Fetch and import sit behind traits so the
//! tests never touch the network or invent an immutable-DB format.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use mitos_config::{InstallPhase, InstallState};
use tracing::{info, warn};

use crate::atomic::{atomic_write, write_unless_edited};

/// 15% headroom on top of the aggregator's snapshot size. Written down so
/// an upgrade of a library default cannot move it. Same figure
/// `snapshot-bootstrap.sh` used.
pub const DISK_HEADROOM_PERCENT: u64 = 15;

const SCRATCH_SUFFIX: &str = ".incoming";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    Empty,
    HasChunks { newest: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataDir {
    Fresh,
    PartlyImported { cursor_slot: u64 },
    AlreadyBootstrapped { cursor_slot: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fetch {
    IfEmpty,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    FetchAndVerify,
    Import,
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    #[error(
        "chunk store is empty and --fetch never; pass --fetch if-empty or put chunks at --chunk-store first"
    )]
    StoreEmptyAndFetchForbidden,
    #[error(
        "not enough disk: need {needed} bytes (snapshot + {headroom}% headroom), have {available}"
    )]
    TooLittleDisk {
        needed: u64,
        available: u64,
        headroom: u64,
    },
    #[error("data dir is in use — is mitos running? {detail}")]
    DataDirInUse { detail: String },
    #[error("--chunk-store is required (the directory that contains immutable/; no default)")]
    ChunkStoreRequired,
    #[error("missing mithril config (aggregator + genesis_key) on this network")]
    MissingMithril,
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Refused(#[from] Refused),
    #[error("fetch: {0}")]
    Fetch(String),
    #[error("import: {0}")]
    Import(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("install state: {0}")]
    State(String),
    #[error(
        "a file that was under the chunk store is gone ({path}); mitos bootstrap never deletes the store"
    )]
    StoreMutated { path: PathBuf },
}

pub fn plan(store: Store, data: DataDir, fetch: Fetch) -> Result<Vec<Step>, Refused> {
    match data {
        DataDir::AlreadyBootstrapped { .. } => return Ok(vec![Step::Nothing]),
        DataDir::Fresh | DataDir::PartlyImported { .. } => {}
    }
    match store {
        Store::HasChunks { .. } => Ok(vec![Step::Import]),
        Store::Empty => match fetch {
            Fetch::Never => Err(Refused::StoreEmptyAndFetchForbidden),
            Fetch::IfEmpty => Ok(vec![Step::FetchAndVerify, Step::Import]),
        },
    }
}

pub fn inspect_store(chunk_store: &Path) -> Store {
    let imm = chunk_store.join("immutable");
    let Ok(entries) = fs::read_dir(&imm) else {
        return Store::Empty;
    };
    let mut newest: Option<u32> = None;
    for ent in entries.flatten() {
        let name = ent.file_name();
        let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(".chunk")) else {
            continue;
        };
        if let Ok(n) = stem.parse::<u32>() {
            newest = Some(newest.map_or(n, |m| m.max(n)));
        }
    }
    match newest {
        Some(newest) => Store::HasChunks { newest },
        None => Store::Empty,
    }
}

pub fn classify_data_dir(cursor_slot: Option<u64>, store_tip: Option<u64>) -> DataDir {
    match cursor_slot {
        None => DataDir::Fresh,
        Some(cursor) => match store_tip {
            Some(tip) if cursor >= tip && tip > 0 => DataDir::AlreadyBootstrapped {
                cursor_slot: cursor,
            },
            _ => DataDir::PartlyImported {
                cursor_slot: cursor,
            },
        },
    }
}

pub fn scratch_dir(chunk_store: &Path) -> PathBuf {
    let mut raw = chunk_store.as_os_str().to_owned();
    raw.push(SCRATCH_SUFFIX);
    PathBuf::from(raw)
}

pub fn snapshot_tree(root: &Path) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    fn walk(dir: &Path, out: &mut BTreeSet<PathBuf>) {
        let Ok(rd) = fs::read_dir(dir) else {
            return;
        };
        for ent in rd.flatten() {
            let p = ent.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.insert(p);
            }
        }
    }
    walk(root, &mut out);
    out
}

pub fn store_was_not_shrunk(
    before: &BTreeSet<PathBuf>,
    after: &BTreeSet<PathBuf>,
) -> Result<(), BootstrapError> {
    for p in before {
        if !after.contains(p) {
            return Err(BootstrapError::StoreMutated { path: p.clone() });
        }
    }
    Ok(())
}

/// Size of a snapshot plus written-down headroom.
pub fn needed_bytes(snapshot_bytes: u64) -> u64 {
    snapshot_bytes + snapshot_bytes.saturating_mul(DISK_HEADROOM_PERCENT) / 100
}

pub trait Disk {
    fn available_bytes(&self, on: &Path) -> Result<u64, BootstrapError>;
}

pub trait SnapshotFetcher {
    fn snapshot_bytes(&self) -> Result<u64, BootstrapError>;
    /// Unpack a verified snapshot into `dest` (empty directory). Must not
    /// touch `chunk_store`.
    fn fetch_and_verify(&self, dest: &Path) -> Result<(), BootstrapError>;
}

pub trait Importer {
    fn inspect(&self) -> Result<DataDir, BootstrapError>;
    /// Import from `{chunk_store}/immutable`. Returns the cursor slot after.
    fn import(&self, chunk_store: &Path, from_cursor: Option<u64>) -> Result<u64, BootstrapError>;
}

pub struct BootstrapPaths<'a> {
    pub chunk_store: &'a Path,
    pub state_file: &'a Path,
    pub marker: &'a Path,
}

pub fn run(
    paths: BootstrapPaths<'_>,
    fetch: Fetch,
    disk: &dyn Disk,
    fetcher: &dyn SnapshotFetcher,
    importer: &dyn Importer,
) -> Result<(), BootstrapError> {
    let store_root = paths.chunk_store;
    let before = if store_root.exists() {
        snapshot_tree(store_root)
    } else {
        BTreeSet::new()
    };
    let result = run_inner(paths, fetch, disk, fetcher, importer);
    if store_root.exists() {
        let after = snapshot_tree(store_root);
        store_was_not_shrunk(&before, &after)?;
    }
    result
}

fn run_inner(
    paths: BootstrapPaths<'_>,
    fetch: Fetch,
    disk: &dyn Disk,
    fetcher: &dyn SnapshotFetcher,
    importer: &dyn Importer,
) -> Result<(), BootstrapError> {
    let store = inspect_store(paths.chunk_store);
    let data = importer.inspect()?;
    let steps = plan(store, data, fetch)?;

    if matches!(steps.as_slice(), [Step::Nothing]) {
        write_marker(paths.marker)?;
        persist(
            paths.state_file,
            &InstallState::Bootstrapped {
                tip_slot: match data {
                    DataDir::AlreadyBootstrapped { cursor_slot } => cursor_slot,
                    DataDir::PartlyImported { cursor_slot } => cursor_slot,
                    DataDir::Fresh => 0,
                },
            },
        )?;
        info!("bootstrap: already complete");
        return Ok(());
    }

    for step in steps {
        match step {
            Step::Nothing => {}
            Step::FetchAndVerify => {
                persist(paths.state_file, &InstallState::Fetching)?;
                fetch_into_store(paths.chunk_store, disk, fetcher)?;
            }
            Step::Import => {
                let cursor = match importer.inspect()? {
                    DataDir::Fresh => None,
                    DataDir::PartlyImported { cursor_slot }
                    | DataDir::AlreadyBootstrapped { cursor_slot } => Some(cursor_slot),
                };
                persist(
                    paths.state_file,
                    &InstallState::Importing {
                        cursor: cursor.unwrap_or(0),
                    },
                )?;
                let tip = importer
                    .import(paths.chunk_store, cursor)
                    .inspect_err(|e| {
                        let _ = persist(
                            paths.state_file,
                            &InstallState::Failed {
                                phase: InstallPhase::Importing,
                                reason: e.to_string(),
                            },
                        );
                    })?;
                persist(
                    paths.state_file,
                    &InstallState::Bootstrapped { tip_slot: tip },
                )?;
            }
        }
    }

    // Marker LAST. A crash between Bootstrapped and this write is recovered
    // as WriteMarker — not a re-import.
    write_marker(paths.marker)?;
    Ok(())
}

fn fetch_into_store(
    chunk_store: &Path,
    disk: &dyn Disk,
    fetcher: &dyn SnapshotFetcher,
) -> Result<(), BootstrapError> {
    let size = fetcher.snapshot_bytes()?;
    let needed = needed_bytes(size);
    let avail_on = chunk_store
        .parent()
        .filter(|p| p.exists())
        .unwrap_or(chunk_store);
    let available = disk.available_bytes(avail_on)?;
    if available < needed {
        return Err(Refused::TooLittleDisk {
            needed,
            available,
            headroom: DISK_HEADROOM_PERCENT,
        }
        .into());
    }

    let scratch = scratch_dir(chunk_store);
    if scratch.exists() {
        warn!(
            path = %scratch.display(),
            "clearing leftover bootstrap scratch directory (not trusted)"
        );
        fs::remove_dir_all(&scratch)?;
    }
    fs::create_dir_all(&scratch)?;
    fetcher.fetch_and_verify(&scratch)?;
    publish_scratch(&scratch, chunk_store)?;
    Ok(())
}

fn publish_scratch(scratch: &Path, chunk_store: &Path) -> Result<(), BootstrapError> {
    if !chunk_store.exists() {
        fs::rename(scratch, chunk_store)?;
        return Ok(());
    }
    // Store dir exists (possibly empty). Move children; never remove the store.
    for ent in fs::read_dir(scratch)? {
        let ent = ent?;
        let dest = chunk_store.join(ent.file_name());
        if dest.exists() {
            return Err(BootstrapError::Fetch(format!(
                "refusing to overwrite {} (chunk store already has this entry)",
                dest.display()
            )));
        }
        fs::rename(ent.path(), dest)?;
    }
    fs::remove_dir_all(scratch)?;
    Ok(())
}

fn persist(path: &Path, state: &InstallState) -> Result<(), BootstrapError> {
    let bytes =
        serde_json::to_vec_pretty(state).map_err(|e| BootstrapError::State(e.to_string()))?;
    atomic_write(path, &bytes, Some(0o644))?;
    Ok(())
}

fn write_marker(path: &Path) -> Result<(), BootstrapError> {
    write_unless_edited(path, b"ok\n", Some(0o644))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FakeDisk {
        available: u64,
    }
    impl Disk for FakeDisk {
        fn available_bytes(&self, _: &Path) -> Result<u64, BootstrapError> {
            Ok(self.available)
        }
    }

    struct FakeFetch {
        size: u64,
        fetches: Mutex<u32>,
        fail: bool,
    }
    impl SnapshotFetcher for FakeFetch {
        fn snapshot_bytes(&self) -> Result<u64, BootstrapError> {
            Ok(self.size)
        }
        fn fetch_and_verify(&self, dest: &Path) -> Result<(), BootstrapError> {
            if self.fail {
                return Err(BootstrapError::Fetch("forced".into()));
            }
            *self.fetches.lock().unwrap() += 1;
            let imm = dest.join("immutable");
            fs::create_dir_all(&imm)?;
            fs::write(imm.join("00001.chunk"), b"chunk")?;
            Ok(())
        }
    }

    struct FakeImport {
        cursor: Mutex<Option<u64>>,
        tip: u64,
        imports: Mutex<u32>,
        in_use: bool,
        imported_from: Mutex<Vec<Option<u64>>>,
    }
    impl Importer for FakeImport {
        fn inspect(&self) -> Result<DataDir, BootstrapError> {
            if self.in_use {
                return Err(Refused::DataDirInUse {
                    detail: "lock held".into(),
                }
                .into());
            }
            Ok(classify_data_dir(
                *self.cursor.lock().unwrap(),
                Some(self.tip),
            ))
        }
        fn import(
            &self,
            chunk_store: &Path,
            from_cursor: Option<u64>,
        ) -> Result<u64, BootstrapError> {
            assert!(
                chunk_store.join("immutable").exists(),
                "import must see the store"
            );
            self.imported_from.lock().unwrap().push(from_cursor);
            *self.imports.lock().unwrap() += 1;
            *self.cursor.lock().unwrap() = Some(self.tip);
            Ok(self.tip)
        }
    }

    fn whole_table_case(store: Store, data: DataDir, fetch: Fetch) -> Result<Vec<Step>, Refused> {
        plan(store, data, fetch)
    }

    #[test]
    fn the_whole_plan_table() {
        use DataDir::*;
        use Fetch::*;
        use Step::*;
        use Store::*;

        assert_eq!(
            whole_table_case(Empty, Fresh, IfEmpty).unwrap(),
            vec![FetchAndVerify, Import]
        );
        assert!(matches!(
            whole_table_case(Empty, Fresh, Never),
            Err(Refused::StoreEmptyAndFetchForbidden)
        ));
        assert_eq!(
            whole_table_case(Empty, PartlyImported { cursor_slot: 9 }, IfEmpty).unwrap(),
            vec![FetchAndVerify, Import]
        );
        assert!(matches!(
            whole_table_case(Empty, PartlyImported { cursor_slot: 9 }, Never),
            Err(Refused::StoreEmptyAndFetchForbidden)
        ));
        assert_eq!(
            whole_table_case(HasChunks { newest: 3 }, Fresh, IfEmpty).unwrap(),
            vec![Import]
        );
        assert_eq!(
            whole_table_case(HasChunks { newest: 3 }, Fresh, Never).unwrap(),
            vec![Import]
        );
        assert_eq!(
            whole_table_case(
                HasChunks { newest: 3 },
                PartlyImported { cursor_slot: 1 },
                Never
            )
            .unwrap(),
            vec![Import]
        );
        assert_eq!(
            whole_table_case(
                HasChunks { newest: 3 },
                AlreadyBootstrapped { cursor_slot: 99 },
                IfEmpty
            )
            .unwrap(),
            vec![Nothing]
        );
        assert_eq!(
            whole_table_case(Empty, AlreadyBootstrapped { cursor_slot: 99 }, IfEmpty).unwrap(),
            vec![Nothing]
        );
    }

    fn layout(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
        (
            dir.join("db"),
            dir.join("install-state.json"),
            dir.join(".bootstrapped"),
        )
    }

    #[test]
    fn an_existing_store_is_imported_from_and_never_fetched_over() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        fs::create_dir_all(store.join("immutable")).unwrap();
        fs::write(store.join("immutable/00007.chunk"), b"x").unwrap();
        let fetch = FakeFetch {
            size: 100,
            fetches: Mutex::new(0),
            fail: false,
        };
        let import = FakeImport {
            cursor: Mutex::new(None),
            tip: 50,
            imports: Mutex::new(0),
            in_use: false,
            imported_from: Mutex::new(Vec::new()),
        };
        run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::IfEmpty,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        )
        .unwrap();
        assert_eq!(*fetch.fetches.lock().unwrap(), 0);
        assert_eq!(*import.imports.lock().unwrap(), 1);
        assert!(marker.exists());
    }

    #[test]
    fn nothing_under_the_chunk_store_is_ever_removed() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        fs::create_dir_all(store.join("immutable")).unwrap();
        let keep = store.join("immutable/00007.chunk");
        fs::write(&keep, b"keep-me").unwrap();
        let import = FakeImport {
            cursor: Mutex::new(None),
            tip: 50,
            imports: Mutex::new(0),
            in_use: false,
            imported_from: Mutex::new(Vec::new()),
        };
        let fetch = FakeFetch {
            size: 100,
            fetches: Mutex::new(0),
            fail: true,
        };
        let _ = run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        );
        assert_eq!(fs::read(&keep).unwrap(), b"keep-me");
        // failed import must not delete either
        let import_fail = FakeImport {
            cursor: Mutex::new(None),
            tip: 50,
            imports: Mutex::new(0),
            in_use: true,
            imported_from: Mutex::new(Vec::new()),
        };
        let err = run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import_fail,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BootstrapError::Refused(Refused::DataDirInUse { .. })
        ));
        assert_eq!(fs::read(&keep).unwrap(), b"keep-me");
    }

    #[test]
    fn a_scratch_directory_left_by_a_crash_is_cleared_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        let scratch = scratch_dir(&store);
        fs::create_dir_all(scratch.join("immutable")).unwrap();
        fs::write(scratch.join("immutable/stale.chunk"), b"stale").unwrap();
        let fetch = FakeFetch {
            size: 100,
            fetches: Mutex::new(0),
            fail: false,
        };
        let import = FakeImport {
            cursor: Mutex::new(None),
            tip: 50,
            imports: Mutex::new(0),
            in_use: false,
            imported_from: Mutex::new(Vec::new()),
        };
        run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::IfEmpty,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        )
        .unwrap();
        assert!(!scratch.exists());
        assert!(!store.join("immutable/stale.chunk").exists());
        assert!(store.join("immutable/00001.chunk").exists());
    }

    #[test]
    fn an_import_resumes_from_the_cursor_and_does_not_import_a_block_twice() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        fs::create_dir_all(store.join("immutable")).unwrap();
        fs::write(store.join("immutable/00007.chunk"), b"x").unwrap();
        let fetch = FakeFetch {
            size: 100,
            fetches: Mutex::new(0),
            fail: false,
        };
        let import = FakeImport {
            cursor: Mutex::new(Some(12)),
            tip: 50,
            imports: Mutex::new(0),
            in_use: false,
            imported_from: Mutex::new(Vec::new()),
        };
        run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        )
        .unwrap();
        assert_eq!(*import.imported_from.lock().unwrap(), vec![Some(12)]);
    }

    #[test]
    fn bootstrapping_twice_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        fs::create_dir_all(store.join("immutable")).unwrap();
        fs::write(store.join("immutable/00007.chunk"), b"x").unwrap();
        let fetch = FakeFetch {
            size: 100,
            fetches: Mutex::new(0),
            fail: false,
        };
        let import = FakeImport {
            cursor: Mutex::new(Some(50)),
            tip: 50,
            imports: Mutex::new(0),
            in_use: false,
            imported_from: Mutex::new(Vec::new()),
        };
        let paths = BootstrapPaths {
            chunk_store: &store,
            state_file: &state,
            marker: &marker,
        };
        run(
            paths,
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        )
        .unwrap();
        run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &fetch,
            &import,
        )
        .unwrap();
        assert_eq!(*import.imports.lock().unwrap(), 0);
        assert!(marker.exists());
    }

    #[test]
    fn a_data_dir_that_is_in_use_says_so_in_plain_words() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        fs::create_dir_all(store.join("immutable")).unwrap();
        fs::write(store.join("immutable/00007.chunk"), b"x").unwrap();
        let err = run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::Never,
            &FakeDisk {
                available: 1_000_000,
            },
            &FakeFetch {
                size: 100,
                fetches: Mutex::new(0),
                fail: false,
            },
            &FakeImport {
                cursor: Mutex::new(None),
                tip: 50,
                imports: Mutex::new(0),
                in_use: true,
                imported_from: Mutex::new(Vec::new()),
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("is mitos running"), "{msg}");
    }

    #[test]
    fn too_little_disk_is_refused_before_a_byte_is_fetched() {
        let dir = tempfile::tempdir().unwrap();
        let (store, state, marker) = layout(dir.path());
        let fetch = FakeFetch {
            size: 1_000,
            fetches: Mutex::new(0),
            fail: false,
        };
        let err = run(
            BootstrapPaths {
                chunk_store: &store,
                state_file: &state,
                marker: &marker,
            },
            Fetch::IfEmpty,
            &FakeDisk { available: 10 },
            &fetch,
            &FakeImport {
                cursor: Mutex::new(None),
                tip: 50,
                imports: Mutex::new(0),
                in_use: false,
                imported_from: Mutex::new(Vec::new()),
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BootstrapError::Refused(Refused::TooLittleDisk { .. })
        ));
        assert_eq!(*fetch.fetches.lock().unwrap(), 0);
        assert!(!store.join("immutable").exists());
    }
}
