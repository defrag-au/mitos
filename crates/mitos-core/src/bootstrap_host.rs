//! Real disk, Mithril fetch, and dolos import. Tests in `bootstrap.rs` use
//! fakes; this module is what `mitos bootstrap` calls.

use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use dolos::adapters::DomainAdapter;
use dolos_core::config::{MithrilConfig, RootConfig};
use dolos_core::{ChainPoint, Domain, ImportExt, StateStore};
use mithril_client::{ClientBuilder, MessageBuilder};
use pallas_hardano::storage::immutable::{self, Point};
use tracing::{info, warn};

use crate::bootstrap::{
    BootstrapError, DataDir, Disk, Importer, Refused, SnapshotFetcher, classify_data_dir,
    needed_bytes,
};
use crate::domain::setup_domain;

pub struct UnixDisk;

impl Disk for UnixDisk {
    fn available_bytes(&self, on: &Path) -> Result<u64, BootstrapError> {
        let probe = if on.exists() {
            on
        } else {
            on.parent().unwrap_or(on)
        };
        let cpath = CString::new(std::os::unix::ffi::OsStrExt::as_bytes(probe.as_os_str()))
            .map_err(|_| {
                BootstrapError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "path for statvfs",
                ))
            })?;
        unsafe {
            let mut vfs = std::mem::zeroed::<libc::statvfs>();
            if libc::statvfs(cpath.as_ptr(), &mut vfs) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(vfs.f_bavail as u64 * vfs.f_frsize as u64)
        }
    }
}

pub struct MithrilFetcher {
    pub config: MithrilConfig,
}

impl SnapshotFetcher for MithrilFetcher {
    fn snapshot_bytes(&self) -> Result<u64, BootstrapError> {
        block_on(async {
            let client = mithril_client(&self.config)?;
            let snapshots = client
                .cardano_database()
                .list()
                .await
                .map_err(|e| BootstrapError::Fetch(e.to_string()))?;
            let snap = snapshots
                .first()
                .ok_or_else(|| BootstrapError::Fetch("no snapshot available".into()))?;
            Ok(snap.size)
        })
    }

    fn fetch_and_verify(&self, dest: &Path) -> Result<(), BootstrapError> {
        block_on(fetch_and_verify(&self.config, dest))
    }
}

fn mithril_client(config: &MithrilConfig) -> Result<mithril_client::Client, BootstrapError> {
    let mut b = ClientBuilder::aggregator(&config.aggregator, &config.genesis_key);
    if let Some(key) = &config.ancillary_key {
        b = b.set_ancillary_verification_key(key.clone());
    }
    b.build().map_err(|e| BootstrapError::Fetch(e.to_string()))
}

async fn fetch_and_verify(config: &MithrilConfig, dest: &Path) -> Result<(), BootstrapError> {
    let client = mithril_client(config)?;
    let snapshots = client
        .cardano_database()
        .list()
        .await
        .map_err(|e| BootstrapError::Fetch(e.to_string()))?;
    let last_digest = snapshots
        .first()
        .ok_or_else(|| BootstrapError::Fetch("no snapshot available".into()))?
        .digest
        .as_ref();
    let snapshot = client
        .cardano_database()
        .get(last_digest)
        .await
        .map_err(|e| BootstrapError::Fetch(e.to_string()))?
        .ok_or_else(|| BootstrapError::Fetch("no snapshot available".into()))?;

    let needed = needed_bytes(snapshot.size);
    info!(
        snapshot_bytes = snapshot.size,
        needed_bytes = needed,
        dest = %dest.display(),
        "mithril snapshot selected"
    );

    let certificate = client
        .certificate()
        .verify_chain(&snapshot.certificate_hash)
        .await
        .map_err(|e| BootstrapError::Fetch(format!("certificate chain: {e}")))?;

    std::fs::create_dir_all(dest)?;
    client
        .cardano_database()
        .download_unpack(&snapshot, dest)
        .await
        .map_err(|e| BootstrapError::Fetch(e.to_string()))?;

    if let Err(e) = client.cardano_database().add_statistics(&snapshot).await {
        warn!("failed incrementing snapshot download statistics: {e:?}");
    }

    // Validate by default. Upstream inverts this condition and skips the
    // file check; we do not.
    let message = MessageBuilder::new()
        .compute_snapshot_message(&certificate, dest)
        .await
        .map_err(|e| BootstrapError::Fetch(format!("compute snapshot message: {e}")))?;
    if !certificate.match_message(&message) {
        return Err(BootstrapError::Fetch(
            "downloaded files do not match the Mithril certificate".into(),
        ));
    }
    Ok(())
}

fn block_on<T>(fut: impl std::future::Future<Output = T>) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => tokio::runtime::Runtime::new()
            .expect("tokio runtime")
            .block_on(fut),
    }
}

pub struct DolosImporter {
    pub config: RootConfig,
    pub chunk_size: usize,
    pub chunk_store: PathBuf,
}

impl Importer for DolosImporter {
    fn inspect(&self) -> Result<DataDir, BootstrapError> {
        let tip = store_tip_slot(&self.chunk_store)?;
        let domain = open_domain(&self.config)?;
        let data = classify_from_domain(&domain, tip)?;
        drop(domain);
        Ok(data)
    }

    fn import(&self, chunk_store: &Path, from_cursor: Option<u64>) -> Result<u64, BootstrapError> {
        let immutable = chunk_store.join("immutable");
        let domain = open_domain(&self.config)?;
        let result = do_import(&domain, &immutable, from_cursor, self.chunk_size);
        drop(domain);
        result
    }
}

fn open_domain(config: &RootConfig) -> Result<DomainAdapter, BootstrapError> {
    setup_domain(config).map_err(|e| map_open_error(e.to_string()))
}

fn map_open_error(msg: String) -> BootstrapError {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("lock")
        || lower.contains("in use")
        || lower.contains("resource temporarily unavailable")
        || lower.contains("already open")
    {
        Refused::DataDirInUse { detail: msg }.into()
    } else {
        BootstrapError::Import(msg)
    }
}

fn classify_from_domain(
    domain: &DomainAdapter,
    store_tip: Option<u64>,
) -> Result<DataDir, BootstrapError> {
    let cursor = domain
        .state()
        .read_cursor()
        .map_err(|e| map_open_error(e.to_string()))?;
    let slot = match cursor {
        None | Some(ChainPoint::Origin) => None,
        Some(ChainPoint::Specific(slot, _)) | Some(ChainPoint::Slot(slot)) => Some(slot),
    };
    Ok(classify_data_dir(slot, store_tip))
}

fn do_import(
    domain: &DomainAdapter,
    immutable: &Path,
    from_cursor: Option<u64>,
    chunk_size: usize,
) -> Result<u64, BootstrapError> {
    let tip = immutable::get_tip(immutable)
        .map_err(|e| BootstrapError::Import(format!("reading immutable db tip: {e:?}")))?
        .ok_or_else(|| BootstrapError::Import("immutable db has no tip".into()))?;
    let tip_slot = point_slot(&tip);

    let cursor = match from_cursor {
        Some(slot) if slot > 0 => Point::Specific(slot, Vec::new()),
        _ => match domain.state().read_cursor() {
            Ok(Some(ChainPoint::Specific(slot, hash))) => Point::Specific(slot, hash.to_vec()),
            Ok(Some(ChainPoint::Slot(slot))) => Point::Specific(slot, Vec::new()),
            _ => Point::Origin,
        },
    };

    let mut iter = immutable::read_blocks_from_point(immutable, cursor.clone())
        .map_err(|e| BootstrapError::Import(format!("reading immutable db: {e:?}")))?;

    if cursor != Point::Origin {
        let _ = iter.next();
    }

    info!(tip_slot, chunk_size, "importing immutable db");
    let started = Instant::now();
    let mut last_log = Instant::now();
    let mut last_slot = point_slot(&cursor);
    let mut batch: Vec<Arc<Vec<u8>>> = Vec::with_capacity(chunk_size);

    let flush =
        |batch: &mut Vec<Arc<Vec<u8>>>, last_slot: &mut u64| -> Result<(), BootstrapError> {
            if batch.is_empty() {
                return Ok(());
            }
            let take = std::mem::take(batch);
            *last_slot = domain
                .import_blocks(take)
                .map_err(|e| BootstrapError::Import(e.to_string()))?;
            Ok(())
        };

    for item in iter {
        let raw = item.map_err(|e| BootstrapError::Import(format!("reading block: {e:?}")))?;
        batch.push(Arc::new(raw));
        if batch.len() >= chunk_size {
            flush(&mut batch, &mut last_slot)?;
            if last_log.elapsed().as_secs() >= 30 {
                let pct = last_slot
                    .saturating_mul(100)
                    .checked_div(tip_slot)
                    .unwrap_or(100);
                info!(
                    slot = last_slot,
                    tip_slot,
                    percent = pct,
                    elapsed_s = started.elapsed().as_secs(),
                    "import progress"
                );
                last_log = Instant::now();
            }
        }
    }
    flush(&mut batch, &mut last_slot)?;
    info!(slot = last_slot, "immutable db import complete");
    Ok(last_slot)
}

fn point_slot(p: &Point) -> u64 {
    match p {
        Point::Origin => 0,
        Point::Specific(slot, _) => *slot,
    }
}

/// Inspect the immutable DB tip without opening dolos stores.
pub fn store_tip_slot(chunk_store: &Path) -> Result<Option<u64>, BootstrapError> {
    let immutable = chunk_store.join("immutable");
    match immutable::get_tip(&immutable) {
        Ok(Some(p)) => Ok(Some(point_slot(&p))),
        Ok(None) => Ok(None),
        Err(_) => Ok(None),
    }
}

pub fn mithril_from_root(config: &RootConfig) -> Result<MithrilConfig, BootstrapError> {
    let m = config.mithril.as_ref().ok_or(Refused::MissingMithril)?;
    Ok(MithrilConfig {
        aggregator: m.aggregator.clone(),
        genesis_key: m.genesis_key.clone(),
        ancillary_key: m.ancillary_key.clone(),
    })
}

pub fn paths_for(node_root: &Path, chunk_store: &Path) -> (PathBuf, PathBuf, PathBuf) {
    (
        chunk_store.to_path_buf(),
        node_root.join("install-state.json"),
        node_root.join(".bootstrapped"),
    )
}
