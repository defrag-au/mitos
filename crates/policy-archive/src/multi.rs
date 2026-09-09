//! A whole policy archive — every file the manifest names — read over a
//! caller-supplied fetch, so the same code serves a Worker against R2 and a
//! test against a `Vec<u8>`.
//!
//! Async but runtime-free: the reader hands the caller `(key, range)` wants
//! through a closure that returns a future, and never touches a socket
//! itself. `wasm-bindgen-futures` drives it in a Worker, `tokio` on a box.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;

use bytes::Bytes;

use crate::density::DensityBucket;
use crate::manifest::FileKind;
use crate::reader::{Archive, FOOTER_HINT, SparseBytes, footer_length, footer_request};
use crate::schema::Movement;
use crate::{Error, Result};

/// What the reader wants from an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    /// The last `n` bytes — the first footer request, before the object's
    /// size is known.
    Tail(u64),
    Range {
        start: u64,
        len: u64,
    },
}

/// What a fetch returned. `total_len` is the object's full size, which a
/// tail request learns as a by-product and every later range needs.
#[derive(Debug, Clone)]
pub struct Got {
    pub total_len: u64,
    pub bytes: Bytes,
}

struct Opened {
    key: String,
    kind: FileKind,
    bytes: SparseBytes,
    archive: Archive,
    blooms_loaded: bool,
    requests: usize,
    fetched: u64,
}

impl Opened {
    fn from_footer(key: String, kind: FileKind, f: FetchedFooter) -> Result<Self> {
        let mut bytes = SparseBytes::new(f.total_len);
        bytes.insert(f.start, f.bytes);
        let archive = Archive::open(&bytes)?;
        Ok(Self {
            key,
            kind,
            bytes,
            archive,
            blooms_loaded: false,
            requests: f.requests,
            fetched: f.fetched,
        })
    }
}

/// A file's footer as fetched — enough to open the file without touching it
/// again, and what a [`crate::Bundle`] carries per file.
#[derive(Debug, Clone)]
pub struct FetchedFooter {
    pub total_len: u64,
    /// Offset of `bytes` in the file.
    pub start: u64,
    /// Exactly the footer — metadata, its length and the magic.
    pub bytes: Bytes,
    /// What it cost, for the caller's accounting.
    pub requests: usize,
    pub fetched: u64,
}

/// The footer protocol against one object: the last [`FOOTER_HINT`] bytes,
/// then exactly the footer if that fell short.
pub async fn fetch_footer<F, Fut>(key: &str, fetch: &mut F) -> Result<FetchedFooter>
where
    F: FnMut(String, Want) -> Fut,
    Fut: Future<Output = Result<Got>>,
{
    let tail = fetch(key.to_string(), Want::Tail(FOOTER_HINT)).await?;
    let total = tail.total_len;
    let got = tail.bytes.len() as u64;
    let need = footer_length(&tail.bytes)?;
    if need > got {
        // The second request covers the whole footer, tail included.
        let (start, len) = footer_request(total, need);
        let more = fetch(key.to_string(), Want::Range { start, len }).await?;
        return Ok(FetchedFooter {
            total_len: total,
            start,
            fetched: got + more.bytes.len() as u64,
            bytes: more.bytes,
            requests: 2,
        });
    }
    // Exactly the footer: a tail that overshot into the data pages would
    // otherwise ride along into every bundle.
    Ok(FetchedFooter {
        total_len: total,
        start: total - need,
        bytes: tail.bytes.slice((got - need) as usize..),
        requests: 1,
        fetched: got,
    })
}

/// The coverage the files themselves state, from their footers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Extent {
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    /// Transactions, from the movement files' footers.
    pub total_txs: u64,
}

/// Every file of one policy, opened to its footers.
pub struct MultiArchive {
    files: Vec<Opened>,
}

impl MultiArchive {
    /// Open each file's footer. Nothing else is fetched.
    pub async fn open<F, Fut>(files: Vec<(String, FileKind)>, fetch: &mut F) -> Result<Self>
    where
        F: FnMut(String, Want) -> Fut,
        Fut: Future<Output = Result<Got>>,
    {
        let mut opened = Vec::with_capacity(files.len());
        for (key, kind) in files {
            let footer = fetch_footer(&key, fetch).await?;
            opened.push(Opened::from_footer(key, kind, footer)?);
        }
        Ok(Self { files: opened })
    }

    /// Open from footers already in hand — a [`crate::Bundle`] out of a
    /// key-value store. No fetch at all until a page or a lookup.
    pub fn from_footers(files: Vec<(String, FileKind, FetchedFooter)>) -> Result<Self> {
        let files = files
            .into_iter()
            .map(|(key, kind, f)| Opened::from_footer(key, kind, f))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { files })
    }

    pub fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Requests and bytes so far, across every file.
    pub fn fetched(&self) -> (usize, u64) {
        self.files
            .iter()
            .fold((0, 0), |(r, b), f| (r + f.requests, b + f.fetched))
    }

    pub fn extent(&self) -> Extent {
        let ranges = self
            .files
            .iter()
            .filter(|f| f.kind == FileKind::Movements)
            .flat_map(|f| (0..f.archive.num_groups()).filter_map(|g| f.archive.slot_range(g)));
        Extent {
            first_slot: ranges.clone().map(|(lo, _)| lo).min(),
            last_slot: ranges.map(|(_, hi)| hi).max(),
            total_txs: self
                .files
                .iter()
                .filter(|f| f.kind == FileKind::Movements)
                .flat_map(|f| f.archive.groups().iter().map(|g| g.txs))
                .sum(),
        }
    }

    /// The density tier, from footers alone: transactions, mints and burns
    /// from the movement files; movements from those AND the corrections.
    /// Buckets coarser than the files' own are summed up; finer ones fall
    /// back to the files' resolution.
    pub fn density(&self, bucket_secs: u64) -> Vec<DensityBucket> {
        let mut by: BTreeMap<u64, DensityBucket> = BTreeMap::new();
        for f in &self.files {
            let native = f.archive.bucket_secs();
            let width = bucket_secs.max(native);
            for b in f.archive.density() {
                let from = b.from_unix / width * width;
                let slot = by.entry(from).or_insert(DensityBucket {
                    from_unix: from,
                    to_unix: from + width,
                    ..DensityBucket::default()
                });
                slot.movements += b.movements;
                if f.kind == FileKind::Movements {
                    slot.txs += b.txs;
                    slot.mints += b.mints;
                    slot.burns += b.burns;
                }
            }
        }
        by.into_values().collect()
    }

    async fn fetch_group<F, Fut>(&mut self, i: usize, g: usize, fetch: &mut F) -> Result<()>
    where
        F: FnMut(String, Want) -> Fut,
        Fut: Future<Output = Result<Got>>,
    {
        let f = &mut self.files[i];
        let (start, len) = f.archive.group_range(g);
        if f.bytes.has(start, len) {
            return Ok(());
        }
        let got = fetch(f.key.clone(), Want::Range { start, len }).await?;
        f.requests += 1;
        f.fetched += got.bytes.len() as u64;
        f.bytes.insert(start, got.bytes);
        Ok(())
    }

    /// The UNFOLDED rows behind a page: the newest `limit` transactions'
    /// movements below `before`, plus every correction to them. Fold with
    /// [`crate::feed::fold_rows`].
    pub async fn page<F, Fut>(
        &mut self,
        limit: usize,
        before: Option<u64>,
        fetch: &mut F,
    ) -> Result<Vec<Movement>>
    where
        F: FnMut(String, Want) -> Fut,
        Fut: Future<Output = Result<Got>>,
    {
        let limit = limit.clamp(1, 5_000);
        let before = before.unwrap_or(u64::MAX);

        // Every movement group that could hold a row below `before`, newest
        // first by its top slot.
        let mut candidates: Vec<(usize, usize, u64)> = Vec::new();
        for (i, f) in self.files.iter().enumerate() {
            if f.kind != FileKind::Movements {
                continue;
            }
            for g in 0..f.archive.num_groups() {
                if let Some((lo, hi)) = f.archive.slot_range(g)
                    && lo < before
                {
                    candidates.push((i, g, hi));
                }
            }
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.2));

        let mut rows: Vec<Movement> = Vec::new();
        let mut txs: HashSet<Vec<u8>> = HashSet::new();
        for (i, g, _) in candidates {
            if txs.len() >= limit {
                break;
            }
            self.fetch_group(i, g, fetch).await?;
            let f = &self.files[i];
            for m in f.archive.read_group(&f.bytes, g)? {
                if m.slot < before {
                    txs.insert(m.tx_hash.clone());
                    rows.push(m);
                }
            }
        }
        self.attach_corrections(&mut rows, &txs, fetch).await?;
        Ok(rows)
    }

    /// Every row of one transaction, unfolded, through the bloom filters.
    pub async fn find_tx<F, Fut>(&mut self, tx_hash: &[u8], fetch: &mut F) -> Result<Vec<Movement>>
    where
        F: FnMut(String, Want) -> Fut,
        Fut: Future<Output = Result<Got>>,
    {
        let mut rows = Vec::new();
        for i in 0..self.files.len() {
            if !self.files[i].blooms_loaded {
                let ranges: Vec<(u64, u64)> = (0..self.files[i].archive.num_groups())
                    .filter_map(|g| self.files[i].archive.bloom_range(g))
                    .collect();
                for (start, len) in ranges {
                    let f = &mut self.files[i];
                    if f.bytes.has(start, len) {
                        continue;
                    }
                    let got = fetch(f.key.clone(), Want::Range { start, len }).await?;
                    f.requests += 1;
                    f.fetched += got.bytes.len() as u64;
                    f.bytes.insert(start, got.bytes);
                }
                self.files[i].blooms_loaded = true;
            }
            let candidates = {
                let f = &self.files[i];
                f.archive.candidate_groups(&f.bytes, tx_hash)?
            };
            for g in candidates {
                self.fetch_group(i, g, fetch).await?;
            }
            let f = &self.files[i];
            rows.extend(f.archive.find_tx(&f.bytes, tx_hash)?);
        }
        Ok(rows)
    }

    async fn attach_corrections<F, Fut>(
        &mut self,
        rows: &mut Vec<Movement>,
        txs: &HashSet<Vec<u8>>,
        fetch: &mut F,
    ) -> Result<()>
    where
        F: FnMut(String, Want) -> Fut,
        Fut: Future<Output = Result<Got>>,
    {
        let (Some(lo), Some(hi)) = (
            rows.iter().map(|m| m.slot).min(),
            rows.iter().map(|m| m.slot).max(),
        ) else {
            return Ok(());
        };
        for i in 0..self.files.len() {
            if self.files[i].kind != FileKind::Corrections {
                continue;
            }
            let groups = self.files[i].archive.groups_overlapping(lo, hi);
            for g in groups {
                self.fetch_group(i, g, fetch).await?;
                let f = &self.files[i];
                rows.extend(
                    f.archive
                        .read_group(&f.bytes, g)?
                        .into_iter()
                        .filter(|m| txs.contains(&m.tx_hash)),
                );
            }
        }
        Ok(())
    }
}

/// A fetch over in-memory objects — tests, and the shape a real transport
/// mirrors.
pub fn memory_fetch(
    objects: std::collections::HashMap<String, Bytes>,
) -> impl FnMut(String, Want) -> std::future::Ready<Result<Got>> {
    move |key, want| {
        let Some(obj) = objects.get(&key) else {
            return std::future::ready(Err(Error::Fetch(format!("no object {key}"))));
        };
        let total = obj.len() as u64;
        let (start, len) = match want {
            Want::Tail(n) => (total.saturating_sub(n), n.min(total)),
            Want::Range { start, len } => (start, len.min(total.saturating_sub(start))),
        };
        std::future::ready(Ok(Got {
            total_len: total,
            bytes: obj.slice(start as usize..(start + len) as usize),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::fold_rows;
    use crate::groups::GroupPolicy;
    use crate::schema::{Completeness, Stamp};
    use crate::writer::ArchiveWriter;
    use crate::writer::fixtures::{hash, policy};

    fn sealed(rows: &[Movement]) -> Bytes {
        let stamp = Stamp {
            policy_hex: "ef".repeat(28),
            completeness: Completeness::Complete,
            walk_from: Some(0),
            walk_to: Some(1),
            covered_from: 0,
            covered_to: 1,
            sealed_unix: 0,
        };
        let mut sink = Vec::new();
        let mut w = ArchiveWriter::new(
            &mut sink,
            &stamp,
            GroupPolicy {
                min_rows: 64,
                max_rows: 1_000,
                ..GroupPolicy::DAILY
            },
        )
        .unwrap();
        for r in rows {
            w.push(r.clone()).unwrap();
        }
        w.finish().unwrap();
        sink.into()
    }

    /// Two movement files and a corrections file, read the way a Worker
    /// would: footers, density, a page with its corrections folded, a hash.
    #[test]
    fn a_whole_archive_reads_over_a_fetch() {
        let older = policy(1_700_000_000, 30, |d| if d == 0 { 200 } else { 6 });
        let newer = policy(1_700_000_000 + 40 * 86_400, 20, |_| 6);
        // A correction to one of the older rows: alice loses what bob got.
        let target = older[10].clone();
        let mut corr = target.clone();
        corr.address = "addr1correctedsource".into();
        corr.amount = -1;
        let objects = std::collections::HashMap::from([
            ("p/archive-0000.parquet".to_string(), sealed(&older)),
            ("p/pass-0003/movements.parquet".to_string(), sealed(&newer)),
            (
                "p/pass-0003/corrections.parquet".to_string(),
                sealed(&[corr.clone()]),
            ),
        ]);
        let mut fetch = memory_fetch(objects);

        let files = vec![
            ("p/archive-0000.parquet".to_string(), FileKind::Movements),
            (
                "p/pass-0003/movements.parquet".to_string(),
                FileKind::Movements,
            ),
            (
                "p/pass-0003/corrections.parquet".to_string(),
                FileKind::Corrections,
            ),
        ];
        let mut a =
            futures_executor::block_on(MultiArchive::open(files.clone(), &mut fetch)).unwrap();
        assert_eq!(a.num_files(), 3);
        let (reqs, _) = a.fetched();
        assert_eq!(reqs, 3, "one tail per file opens the footers");

        let ext = a.extent();
        assert_eq!(ext.total_txs, (200 + 29 * 6 + 1) + (20 * 6 + 1));
        let density = a.density(86_400);
        assert_eq!(density.iter().map(|b| b.txs).sum::<u64>(), ext.total_txs);

        // The newest page comes from the newer file; paging below it
        // crosses into the older one, and the correction lands on its row.
        // Whole groups come back — at least the limit — and the caller
        // truncates after folding, as the API does.
        let page = futures_executor::block_on(a.page(5, None, &mut fetch)).unwrap();
        let folded = fold_rows(page);
        assert!(folded.len() >= 5);
        assert!(folded[0].slot >= newer.last().unwrap().slot);

        let below =
            futures_executor::block_on(a.page(300, Some(newer[0].slot), &mut fetch)).unwrap();
        let folded = fold_rows(below);
        let corrected = folded
            .iter()
            .find(|r| r.tx_hash == target.tx_hash)
            .expect("the corrected transaction is on the page");
        let unit = corrected
            .units
            .iter()
            .find(|u| u.name == target.unit_name)
            .unwrap();
        assert!(
            unit.parties
                .iter()
                .any(|p| p.address == "addr1correctedsource" && p.amount == -1)
        );

        // A hash lookup goes through every file's filters.
        let rows = futures_executor::block_on(a.find_tx(&target.tx_hash, &mut fetch)).unwrap();
        assert!(rows.iter().any(|m| m.address == "addr1correctedsource"));
        assert!(
            futures_executor::block_on(a.find_tx(&hash(999_999), &mut fetch))
                .unwrap()
                .is_empty()
        );

        // THE BUNDLE: the same footers, gathered once, carried through the
        // encoding, and opened with no fetch. Density and a page agree with
        // the fetched open, and the page costs exactly its row groups.
        let bundle = crate::Bundle {
            format: crate::BUNDLE_FORMAT,
            manifest: b"{}".to_vec(),
            files: files
                .iter()
                .map(|(key, _)| {
                    let f = futures_executor::block_on(fetch_footer(key, &mut fetch)).unwrap();
                    crate::BundledFooter {
                        file: key.trim_start_matches("p/").to_string(),
                        total_len: f.total_len,
                        footer_start: f.start,
                        footer: f.bytes.to_vec(),
                    }
                })
                .collect(),
        };
        let bytes = bundle.encode().unwrap();
        let back = crate::Bundle::decode(&bytes).unwrap();
        assert_eq!(back, bundle);
        let mut b = MultiArchive::from_footers(back.footers(|rel| format!("p/{rel}"))).unwrap();
        assert_eq!(b.fetched(), (0, 0), "a bundle opens for free");
        assert_eq!(b.extent(), a.extent());
        assert_eq!(b.density(86_400), a.density(86_400));
        let page = futures_executor::block_on(b.page(5, None, &mut fetch)).unwrap();
        assert!(fold_rows(page).len() >= 5);
        let (reqs, _) = b.fetched();
        assert!((1..=3).contains(&reqs), "row groups only: {reqs}");
        // Kind comes from the file name inside a pass directory too.
        assert!(
            crate::Bundle::decode(&bytes)
                .unwrap()
                .footers(|r| r.to_string())
                .iter()
                .any(|(_, kind, _)| *kind == FileKind::Corrections)
        );
    }
}
