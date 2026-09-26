//! Where the bytes come from: a directory, or the object-store mirror.
//!
//! One trait, two implementations. The trait is deliberately object-safe and
//! knows nothing but names and byte ranges, so the same readers run over a local
//! snapshot, the R2 mirror, and — when there is one — a Durable Object or a
//! browser fetch without any of them being special-cased.
//!
//! Names are file base names (`09161.chunk`, `09161.primary`) because that is
//! what both a directory and an object store address the file by. There is no
//! path: a chunk is a flat name in the bucket, which is what the mirror is.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::multipart;

/// The largest single range this crate will ask for.
///
/// A mainnet chunk reaches ~128 MiB (measured: none does — 96% are ≤64 MiB), so a
/// whole-chunk read goes through the windowed readers in [`chunk`](crate::chunk),
/// never through one `read_range` call. This cap is what makes that structural
/// rather than a convention.
pub const MAX_RANGE: u64 = 64 * 1024 * 1024;

/// The most ranges one `multipart/byteranges` request may carry.
///
/// Cloudflare answers 2–300 ascending, non-overlapping ranges. **Above 300 it
/// ignores `Range` and returns the whole object as `200`** — a silent 128 MiB
/// transfer — so the cap is 299 and nothing is left to the boundary case.
pub const MAX_BATCH_RANGES: usize = 299;

/// The most bytes one batched request may ask for.
///
/// Without it, 299 spans of a MiB each would land in memory at once. A single span
/// larger than this is still requested on its own — the alternative is failing.
pub const MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// One immutable file's bytes, addressed by name and byte range.
///
/// Object-safe on purpose: the readers take `Arc<dyn RangeSource>` so a caller can
/// choose the source at runtime (a CLI flag, a config value) without the reader
/// being generic over it.
pub trait RangeSource: Send + Sync {
    /// Total length of `name`.
    fn len(&self, name: &str) -> Result<u64>;

    /// Exactly `len` bytes of `name` starting at `offset`.
    ///
    /// A short read is an error, never a truncated buffer — the window logic
    /// relies on the length being the one it asked for.
    fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// The whole of `name`. Refuses a file past [`MAX_RANGE`], which is the point:
    /// a chunk must be walked, not swallowed.
    fn read_all(&self, name: &str) -> Result<Vec<u8>> {
        let n = self.len(name)?;
        if n > MAX_RANGE {
            bail!("{name} is {n} bytes — past the {MAX_RANGE}-byte single-range cap");
        }
        self.read_range(name, 0, n)
    }

    /// Many spans of one object, in as few requests as the transport allows.
    ///
    /// **Spans must ascend by offset and not overlap.** That is not a preference:
    /// it is the contract Cloudflare's multi-range support imposes, and the answer
    /// to violating it is a silent whole-object `200` rather than an error — so it
    /// is checked here rather than assumed. Results come back in the order asked
    /// for, one per span.
    ///
    /// A caller whose spans are in no particular order, or which may repeat,
    /// wants [`Chunk::read_spans`](crate::Chunk::read_spans) — it merges first.
    fn read_ranges(&self, name: &str, spans: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        spans
            .iter()
            .map(|(offset, len)| self.read_range(name, *offset, *len))
            .collect()
    }
}

/// A directory of `NNNNN.chunk` / `.primary` / `.secondary` — a local snapshot,
/// the shape [`mithril-snapshot`](../../mithril-snapshot) validates.
pub struct LocalDir {
    dir: PathBuf,
}

impl LocalDir {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }
}

impl RangeSource for LocalDir {
    fn len(&self, name: &str) -> Result<u64> {
        let p = self.dir.join(name);
        let m = std::fs::metadata(&p).with_context(|| format!("stat {}", p.display()))?;
        Ok(m.len())
    }

    fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;

        let p = self.dir.join(name);
        let f = std::fs::File::open(&p).with_context(|| format!("opening {}", p.display()))?;
        let mut buf = vec![0u8; usize::try_from(len).context("range length exceeds usize")?];
        f.read_exact_at(&mut buf, offset)
            .with_context(|| format!("reading {len} bytes at {offset} of {}", p.display()))?;
        Ok(buf)
    }
}

/// An object-store mirror of the immutable directory, over HTTP.
///
/// One request per [`read_range`](RangeSource::read_range). The mirror is expected
/// to answer `206 Partial Content`; Cloudflare does, and with **Origin Range
/// Requests** enabled it fetches whole 1 MiB cache lines from R2, so a small read
/// costs one MiB of origin traffic and consecutive reads share that line. That is
/// why [`chunk`](crate::chunk) aligns its window and its blocks fall inside it.
///
/// `len` is probed with `bytes=0-0` and cached. A `HEAD` would answer too, but R2's
/// version-qualified `HEAD` is the request that 501s under rclone, and the ranged
/// `GET` also warms the object's first cache line.
pub struct HttpMirror {
    base: String,
    client: reqwest::blocking::Client,
    lengths: RwLock<HashMap<String, u64>>,
    /// Every HTTP request this mirror has made. Counted so a caller can assert a
    /// batch really was one request rather than trust that it was.
    requests: AtomicU64,
}

impl HttpMirror {
    /// `base` is the directory prefix, e.g. `https://mithril.defrag.cc/mainnet`.
    pub fn new(base: impl Into<String>) -> Result<Self> {
        let base = base.into();
        let base = base.trim_end_matches('/').to_string();
        if !(base.starts_with("http://") || base.starts_with("https://")) {
            bail!("mirror base {base} is not an http(s) URL");
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            base,
            client,
            lengths: RwLock::new(HashMap::new()),
            requests: AtomicU64::new(0),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// How many HTTP requests this mirror has made.
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    fn url(&self, name: &str) -> String {
        format!("{}/{}", self.base, name)
    }

    /// One request carrying every span, answered as `multipart/byteranges`.
    fn fetch_multipart(&self, name: &str, spans: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        let header = spans
            .iter()
            .map(|(offset, len)| format!("{offset}-{}", offset + len - 1))
            .collect::<Vec<_>>()
            .join(",");

        self.requests.fetch_add(1, Ordering::Relaxed);
        let resp = self
            .client
            .get(self.url(name))
            .header("range", format!("bytes={header}"))
            .send()
            .with_context(|| format!("reading {} ranges of {name}", spans.len()))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("{name}: HTTP {status} for {} ranges", spans.len());
        }
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = resp
            .bytes()
            .with_context(|| format!("reading the body of {name}"))?;

        let boundary = multipart::boundary_of(&content_type).with_context(|| {
            format!(
                "{name}: {} ranges answered with content-type {content_type:?}",
                spans.len()
            )
        })?;
        let parts = multipart::parse(&body, boundary)
            .with_context(|| format!("{name}: in a {}-byte multipart body", body.len()))?;

        let mut by_range: HashMap<(u64, u64), Vec<u8>> = HashMap::with_capacity(parts.len());
        for part in parts {
            if by_range.insert((part.start, part.end), part.data).is_some() {
                bail!(
                    "{name}: the mirror returned two parts for bytes {}-{}",
                    part.start,
                    part.end
                );
            }
        }
        let mut returned: Vec<(u64, u64)> = by_range.keys().copied().collect();
        returned.sort_unstable();

        let mut out = Vec::with_capacity(spans.len());
        for (offset, len) in spans {
            let end = offset + len - 1;
            let data = by_range.remove(&(*offset, end)).with_context(|| {
                format!("{name}: no part for bytes {offset}-{end}; the body carried {returned:?}")
            })?;
            if data.len() as u64 != *len {
                bail!(
                    "{name}: the part for bytes {offset}-{end} is {} bytes, not {len}",
                    data.len()
                );
            }
            out.push(data);
        }
        if !by_range.is_empty() {
            let mut extra: Vec<(u64, u64)> = by_range.keys().copied().collect();
            extra.sort_unstable();
            bail!("{name}: the mirror returned ranges that were not asked for: {extra:?}");
        }
        Ok(out)
    }
}

impl RangeSource for HttpMirror {
    fn len(&self, name: &str) -> Result<u64> {
        if let Some(n) = self.lengths.read().expect("lengths lock").get(name) {
            return Ok(*n);
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        let resp = self
            .client
            .get(self.url(name))
            .header("range", "bytes=0-0")
            .send()
            .with_context(|| format!("probing {name}"))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("{name}: HTTP {status} sizing the object");
        }
        let total = resp
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .with_context(|| format!("{name}: no usable content-range on a 206"))?;
        self.lengths
            .write()
            .expect("lengths lock")
            .insert(name.to_string(), total);
        Ok(total)
    }

    fn read_range(&self, name: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        if len > MAX_RANGE {
            bail!("range of {len} bytes exceeds the {MAX_RANGE}-byte cap");
        }
        let end = offset.checked_add(len).context("range end overflow")? - 1;
        self.requests.fetch_add(1, Ordering::Relaxed);
        let resp = self
            .client
            .get(self.url(name))
            .header("range", format!("bytes={offset}-{end}"))
            .send()
            .with_context(|| format!("reading {len} bytes at {offset} of {name}"))?;
        let status = resp.status();
        if !status.is_success() {
            bail!("{name}: HTTP {status} for bytes {offset}-{end}");
        }
        let body = resp
            .bytes()
            .with_context(|| format!("reading the body of {name}"))?;
        if body.len() as u64 != len {
            // A 200 carrying the whole object is the shape this catches: the mirror
            // ignored the Range header, and a caller that trusted it would slice
            // offsets into the wrong bytes.
            bail!(
                "{name}: asked for {len} bytes at {offset}, got {} — the mirror did not honour the range",
                body.len()
            );
        }
        Ok(body.to_vec())
    }

    fn read_ranges(&self, name: &str, spans: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        if spans.len() < 2 {
            return spans
                .iter()
                .map(|(offset, len)| self.read_range(name, *offset, *len))
                .collect();
        }
        validate_batch(spans)?;

        let mut out = Vec::with_capacity(spans.len());
        for batch in batches(spans) {
            if let [(offset, len)] = batch {
                // A lone span in a batch means it alone is past MAX_BATCH_BYTES, and
                // one range is answered as a plain 206 rather than multipart.
                out.push(self.read_range(name, *offset, *len)?);
                continue;
            }
            out.extend(self.fetch_multipart(name, batch)?);
        }
        Ok(out)
    }
}

/// Refuse a span set Cloudflare would answer with a whole object instead.
fn validate_batch(spans: &[(u64, u64)]) -> Result<()> {
    for (offset, len) in spans {
        if *len == 0 {
            bail!("a zero-length range cannot be requested (at {offset})");
        }
        if *len > MAX_RANGE {
            bail!("a {len}-byte range exceeds the {MAX_RANGE}-byte cap");
        }
        offset.checked_add(*len).context("range end overflow")?;
    }
    for w in spans.windows(2) {
        let (offset, len) = w[0];
        let next = w[1].0;
        if offset + len > next {
            bail!(
                "ranges must ascend and not overlap: {}..{} overlaps {next}",
                offset,
                offset + len
            );
        }
    }
    Ok(())
}

/// Split into the runs that fit one request, never producing an empty or oversized
/// one. A single span past [`MAX_BATCH_BYTES`] becomes a batch of its own.
fn batches(spans: &[(u64, u64)]) -> Vec<&[(u64, u64)]> {
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < spans.len() {
        let mut end = start;
        let mut bytes = 0u64;
        while end < spans.len() {
            let next = spans[end].1;
            if end > start && (end - start >= MAX_BATCH_RANGES || bytes + next > MAX_BATCH_BYTES) {
                break;
            }
            bytes += next;
            end += 1;
        }
        out.push(&spans[start..end]);
        start = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_stops_short_of_300_ranges() {
        // 600 spans of 10 bytes. Cloudflare answers ≤300 and silently returns the
        // WHOLE object above that, so the cap has to be enforced here.
        let spans: Vec<(u64, u64)> = (0..600u64).map(|i| (i * 100, 10)).collect();
        let split = batches(&spans);
        assert_eq!(
            split.iter().map(|b| b.len()).collect::<Vec<_>>(),
            vec![MAX_BATCH_RANGES, MAX_BATCH_RANGES, 2]
        );
        assert!(split.iter().all(|b| b.len() <= MAX_BATCH_RANGES));
        let flat: Vec<(u64, u64)> = split.iter().flat_map(|b| b.iter().copied()).collect();
        assert_eq!(flat, spans, "every span appears exactly once, in order");
    }

    #[test]
    fn a_batch_stops_at_the_byte_budget() {
        let two_mib = 2 * 1024 * 1024;
        let spans: Vec<(u64, u64)> = (0..5u64).map(|i| (i * 2 * two_mib, two_mib)).collect();
        let split = batches(&spans);
        assert_eq!(
            split.iter().map(|b| b.len()).collect::<Vec<_>>(),
            vec![4, 1]
        );
    }

    #[test]
    fn a_single_span_past_the_byte_budget_is_still_its_own_batch() {
        // The alternative would be an empty request, or dropping the span.
        let spans = vec![(0u64, MAX_BATCH_BYTES * 2)];
        let split = batches(&spans);
        assert_eq!(split.len(), 1);
        assert_eq!(split[0], [(0, MAX_BATCH_BYTES * 2)]);
    }

    #[test]
    fn spans_that_are_not_ascending_or_are_overlapping_are_refused() {
        assert!(
            validate_batch(&[(0, 16), (8, 16)])
                .unwrap_err()
                .to_string()
                .contains("overlap")
        );
        assert!(
            validate_batch(&[(100, 16), (0, 16)])
                .unwrap_err()
                .to_string()
                .contains("ascend")
        );
        assert!(
            validate_batch(&[(0, 0)])
                .unwrap_err()
                .to_string()
                .contains("zero-length")
        );
        assert!(
            validate_batch(&[(0, MAX_RANGE + 1)])
                .unwrap_err()
                .to_string()
                .contains("cap")
        );
        assert!(
            validate_batch(&[(0, 16), (16, 16)]).is_ok(),
            "adjacent is legal"
        );
    }
}
