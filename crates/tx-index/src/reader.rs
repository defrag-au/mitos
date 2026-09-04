//! The read side: base + tail segments → located body → verified, decoded.
//!
//! Lookup order is tail segments newest-first, then the base. The tail is
//! whatever landed since the last compaction; after a normal refresh cycle
//! it is empty, and even when it is not, its segments are a megabyte each
//! and searched by binary search.
//!
//! Every hit is VERIFIED: the body is read from the chunk and re-hashed
//! against the full 32-byte hash before anything is decoded. The index
//! stores an 8-byte prefix, so a collision is a wasted 16 KB read at most —
//! never a wrong answer.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use pallas_crypto::hash::Hasher;
use pallas_traverse::Era;

use crate::base::{BaseFile, base_path};
use crate::decode;
use crate::extract::chunk_path;
use crate::format::{Location, era_from_u8, prefix_of};
use crate::segment::{SegmentFile, list_segments, segment_path};
use crate::wire::ResolvedOutput;

/// A candidate hit: where a body with the requested prefix lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Located {
    pub loc: Location,
    pub era: Era,
}

/// A verified body: the bytes whose blake2b-256 IS the requested hash.
#[derive(Clone, Debug)]
pub struct TxBody {
    pub hash: [u8; 32],
    pub era: Era,
    pub loc: Location,
    pub cbor: Vec<u8>,
}

impl TxBody {
    pub fn outputs(&self) -> Result<Vec<ResolvedOutput>> {
        decode::outputs(self.era, &self.cbor)
    }
}

/// What an outref lookup came back with.
#[derive(Clone, Debug)]
pub enum Resolution {
    Found { body: TxBody, output: ResolvedOutput },
    /// The tx exists with `outputs` outputs; the index asked for is past them.
    NoSuchOutput { body: TxBody, outputs: usize },
    UnknownTx,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub base_first_chunk: Option<u16>,
    pub base_last_chunk: Option<u16>,
    pub base_entries: u64,
    pub tail_segments: usize,
    pub tail_entries: u64,
    pub newest_chunk: Option<u16>,
}

pub struct Index {
    immutable: PathBuf,
    base: Option<BaseFile>,
    /// Newest first.
    tail: Vec<SegmentFile>,
}

impl Index {
    /// Map whatever is on disk: an optional base plus every segment newer
    /// than it. With no base at all, every segment is tail.
    pub fn open(index_dir: &Path, immutable: &Path) -> Result<Index> {
        let bp = base_path(index_dir);
        let base = if bp.exists() {
            Some(BaseFile::open(&bp)?)
        } else {
            None
        };
        let floor = base.as_ref().map(|b| b.header.last_chunk);
        let mut tail_chunks: Vec<u16> = list_segments(index_dir)?
            .into_iter()
            .filter(|c| floor.is_none_or(|f| *c > f))
            .collect();
        tail_chunks.sort_unstable_by(|a, b| b.cmp(a));
        let tail = tail_chunks
            .iter()
            .map(|c| SegmentFile::open(&segment_path(index_dir, *c)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Index {
            immutable: immutable.to_path_buf(),
            base,
            tail,
        })
    }

    pub fn coverage(&self) -> Coverage {
        let tail_entries = self.tail.iter().map(|s| s.len() as u64).sum();
        let newest_tail = self.tail.first().map(|s| s.header.chunk);
        let base_last = self.base.as_ref().map(|b| b.header.last_chunk);
        Coverage {
            base_first_chunk: self.base.as_ref().map(|b| b.header.first_chunk),
            base_last_chunk: base_last,
            base_entries: self.base.as_ref().map_or(0, BaseFile::len),
            tail_segments: self.tail.len(),
            tail_entries,
            newest_chunk: newest_tail.max(base_last),
        }
    }

    /// Every candidate for `hash`, tail (newest first) then base. Usually
    /// zero or one; more means a prefix collision the caller must verify.
    pub fn locate(&self, hash: &[u8; 32]) -> Result<Vec<Located>> {
        let prefix = prefix_of(hash);
        let mut out = Vec::new();
        for s in &self.tail {
            let era = era_from_u8(s.header.era)?;
            out.extend(s.find(prefix).into_iter().map(|loc| Located { loc, era }));
        }
        if let Some(b) = &self.base {
            for loc in b.find(prefix) {
                let era_byte = b
                    .era_of(loc.chunk)
                    .ok_or_else(|| anyhow!("base has no era for chunk {}", loc.chunk))?;
                out.push(Located {
                    loc,
                    era: era_from_u8(era_byte)?,
                });
            }
        }
        Ok(out)
    }

    /// Read the body bytes at a location.
    pub fn read_body(&self, loc: Location) -> Result<Vec<u8>> {
        let path = chunk_path(&self.immutable, loc.chunk);
        let f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let mut buf = vec![0u8; usize::from(loc.len)];
        f.read_exact_at(&mut buf, u64::from(loc.offset))
            .with_context(|| {
                format!(
                    "reading {} bytes at {} of {}",
                    loc.len,
                    loc.offset,
                    path.display()
                )
            })?;
        Ok(buf)
    }

    /// The verified body for `hash`, if any completed chunk holds it.
    pub fn tx(&self, hash: &[u8; 32]) -> Result<Option<TxBody>> {
        for cand in self.locate(hash)? {
            let cbor = self.read_body(cand.loc)?;
            let got = Hasher::<256>::hash(&cbor);
            if got.as_ref() == hash {
                return Ok(Some(TxBody {
                    hash: *hash,
                    era: cand.era,
                    loc: cand.loc,
                    cbor,
                }));
            }
            tracing::debug!(
                chunk = cand.loc.chunk,
                offset = cand.loc.offset,
                "prefix collision: body hash mismatch, trying next candidate"
            );
        }
        Ok(None)
    }

    /// Resolve an outref to its producing output.
    pub fn resolve(&self, hash: &[u8; 32], index: u32) -> Result<Resolution> {
        let Some(body) = self.tx(hash)? else {
            return Ok(Resolution::UnknownTx);
        };
        let mut outputs = body.outputs()?;
        let n = outputs.len();
        if (index as usize) < n {
            let output = outputs.swap_remove(index as usize);
            Ok(Resolution::Found { body, output })
        } else {
            Ok(Resolution::NoSuchOutput { body, outputs: n })
        }
    }
}

/// What on disk would make a mapped `Index` stale.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    base: Option<(u64, SystemTime)>,
    segments: Vec<u16>,
}

impl Fingerprint {
    fn take(index_dir: &Path) -> Result<Fingerprint> {
        let bp = base_path(index_dir);
        let base = match std::fs::metadata(&bp) {
            Ok(m) => Some((m.len(), m.modified()?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).with_context(|| format!("stat {}", bp.display())),
        };
        Ok(Fingerprint {
            base,
            segments: list_segments(index_dir)?,
        })
    }
}

/// A hot-swappable `Index` for long-running processes: `get()` hands out the
/// current mapping, `reload_if_changed()` re-opens when a compaction or new
/// segment has landed. Old mappings live until their last holder drops them.
pub struct IndexHandle {
    index_dir: PathBuf,
    immutable: PathBuf,
    current: RwLock<Arc<Index>>,
    fingerprint: Mutex<Fingerprint>,
}

impl IndexHandle {
    pub fn open(index_dir: &Path, immutable: &Path) -> Result<IndexHandle> {
        let fp = Fingerprint::take(index_dir)?;
        let idx = Index::open(index_dir, immutable)?;
        Ok(IndexHandle {
            index_dir: index_dir.to_path_buf(),
            immutable: immutable.to_path_buf(),
            current: RwLock::new(Arc::new(idx)),
            fingerprint: Mutex::new(fp),
        })
    }

    pub fn get(&self) -> Arc<Index> {
        self.current.read().expect("index lock").clone()
    }

    /// Re-open if the on-disk fingerprint moved. Returns whether it did.
    pub fn reload_if_changed(&self) -> Result<bool> {
        let fresh = Fingerprint::take(&self.index_dir)?;
        let mut held = self.fingerprint.lock().expect("fingerprint lock");
        if *held == fresh {
            return Ok(false);
        }
        let idx = Index::open(&self.index_dir, &self.immutable)?;
        *self.current.write().expect("index lock") = Arc::new(idx);
        *held = fresh;
        Ok(true)
    }
}
