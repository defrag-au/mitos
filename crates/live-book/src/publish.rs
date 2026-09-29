//! Laying a book out for a remote reader: uniform parts, and an attestation.
//!
//! # No credentials, no network
//!
//! This writes a directory. Uploading it is somebody else's job — `rclone`,
//! `wrangler`, `cf-publish` — and keeping the two apart means the layout is
//! inspectable before it is anywhere, and that nothing here can leak a key. It
//! also means the same output serves R2, a second region, or a file server.
//!
//! # Why parts at all, and why the size is a parameter
//!
//! Measured 2026-09-25 against never-read objects
//! (`cnft.dev-workers/docs/design/EDGE_PROBE_PLAN.md`): a cold read through
//! Cloudflare costs a **~650 ms floor plus ~35 ms per MiB of depth** to the byte
//! asked for. So the cost of a read is set by how deep into an object it reaches,
//! and splitting a 1.69 GiB arena into smaller objects caps that depth. That
//! argument chose 1 MiB, and it is now [`DEFAULT_PART`] rather than a constant of
//! the format.
//!
//! ⚠️ **It stopped being the whole story once the reader started batching.**
//! Multi-range works only *within* one object, so part size decides whether
//! [`crate::plan`] has anything to coalesce: a 1,055-row wallet query over the
//! mainnet arena plans to **1,061 requests at 1 MiB and 27 at 64 MiB**, for the
//! same 8.6 MB. `MITOS_AT_THE_EDGE.md` §4.8f has the table. Larger parts buy fewer
//! requests and pay cold depth; which way that trades depends on the substrate,
//! which is exactly why `publish` takes the size rather than owning it.
//!
//! **Uniform** is not negotiable, whatever the size: it makes
//! `offset -> (part, offset within part)` arithmetic, where a mixed layout would
//! need a table consulted on every read.
//!
//! ⚠️ **A run can straddle a boundary** — a 332 KB clustered read does so about a
//! third of the time at 1 MiB — so a reader must be able to stitch two parts.
//! [`crate::plan::Located`] carries the segments in order for exactly this.
//!
//! # The attestation, and what it is not
//!
//! `Base::open` validates that each directory partitions its entry array: two
//! passes over 2^21 buckets plus a probe per non-empty bucket. That is 73 ms over
//! a mapping and roughly 2.1 M round trips over a network — not slow, impossible.
//! So the publisher runs the check and records the result, and a remote reader
//! trusts it.
//!
//! ⚠️ That is a real transfer of trust and it is narrow on purpose: the
//! attestation carries `book.meta`'s fields, each file's length and part count,
//! and the validator's verdict. It is **not** a content hash of the bytes — see
//! `ARTIFACT_SHARDING.md` §8 for why integrity is a separate mechanism — and a
//! reader still confirms every hit against the row it lands on, exactly as the
//! local reader does. The narrow-key-plus-confirm discipline is untouched.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
// The digest a content-addressed datum object's key is made of. Blake2b-256, the
// same hash a datum's own key is, so the layout's integrity check and the chain's
// are one computation.
use pallas_crypto::hash::Hasher;

use crate::format::{
    self, BaseFingerprint, DIRECTORY_ADDR, DIRECTORY_OUTREF, DIRECTORY_POLICY, META_FILE, Meta,
    POLICY_FILE, ROWS_FILE, TABLE_ADDR, TABLE_OUTREF, TABLE_POLICY,
};
use crate::plan::PartSize;

/// The part size a publisher uses unless told otherwise.
///
/// ⚠️ **A default, not a constant of the format.** A reader takes the real value
/// from the attestation's `part_size`, so an artifact can be published at any size
/// without a reader change — which `ARTIFACT_SHARDING.md` §6 calls for, because the
/// right size is a property of the *substrate*, not of the book.
///
/// See the module header for why 1 MiB was the first answer and §4.8f of
/// `MITOS_AT_THE_EDGE.md` for why it is no longer obviously the right one: at 1 MiB
/// a wallet's rows land one per object, and multi-range — which works only *within*
/// an object — has nothing to coalesce.
pub const DEFAULT_PART: PartSize = PartSize(1 << 20);

/// The manifest's object key.
///
/// ⚠️ **Carries [`PART_EXT`] for exactly the same reason the parts do**, and it was
/// missed when they gained it: Cloudflare's cache eligibility is extension-based, so
/// a key of plain `attestation` returns `cf-cache-status: DYNAMIC` forever and every
/// read of it is an origin round trip. Measured 2026-09-25 — it was the **most
/// expensive hop** in a client-direct query, not an average one: taking it off the
/// critical path saved 266 ms where the other hops cost ~110 ms each.
///
/// A zone Cache Rule over `book/*` would fix this and any future file more
/// generally; naming is what a publisher can do on its own.
pub const ATTESTATION_FILE: &str = "attestation.bin";

/// The extension every part key carries.
///
/// ⚠️ **Not cosmetic — it is what makes a part CDN-cacheable.** Measured
/// 2026-09-25: Cloudflare's default cache eligibility is **extension-based**, so a
/// key like `rows.bin/00000` returns `cf-cache-status: DYNAMIC` forever and **a
/// multi-range request against it fails with 400**, because multi-range is served
/// off the cache path. The identical object named `…/00000.bin` caches and serves
/// multipart. One control, one variable, and it cost the whole client-direct design
/// until it was found.
pub const PART_EXT: &str = "bin";

/// The files a published book is made of, in the order a reader needs them.
///
/// `book.meta` is not here: it is carried inside the attestation instead, so a
/// reader learns the artifact's shape and its parts from one read rather than two.
pub const PARTED: [&str; 8] = [
    ROWS_FILE,
    TABLE_OUTREF,
    TABLE_ADDR,
    TABLE_POLICY,
    DIRECTORY_OUTREF,
    DIRECTORY_ADDR,
    DIRECTORY_POLICY,
    POLICY_FILE,
];

/// One file's layout in the published form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartedFile {
    pub name: String,
    /// Total logical length. A reader checks this against what `book.meta` implies
    /// before trusting any offset arithmetic.
    pub len: u64,
    /// `ceil(len / PART)`. The last part is short unless `len` divides exactly.
    pub parts: u64,
}

/// What a remote reader is asked to take on trust, and nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attestation {
    pub fingerprint: BaseFingerprint,
    pub part_size: PartSize,
    pub files: Vec<PartedFile>,
    /// `book.meta`'s own text, carried verbatim rather than re-rendered — the
    /// publisher must not reword what the builder wrote.
    pub meta: String,
    /// Whether the publisher ran the directory-partition check and it passed.
    /// A reader that finds `false` here should refuse rather than proceed.
    pub validated: bool,
}

/// The version [`Attestation::render`] writes and [`Attestation::parse`] accepts.
///
/// Separate from `LAYOUT_VERSION`: this describes the *manifest*, and the manifest
/// can gain a field without a single byte of the artifact changing.
pub const ATTESTATION_VERSION: u32 = 1;

impl Attestation {
    /// Read back what [`Attestation::render`] wrote.
    ///
    /// The remote reader's **first** read, and the only one it makes before it
    /// knows the artifact's shape — so every failure here has to be loud. An
    /// unparseable manifest means the reader cannot do offset arithmetic at all,
    /// which is a refusal rather than a degraded mode.
    ///
    /// ⚠️ `validated` is parsed, not enforced. Whether `false` is fatal is the
    /// *reader's* call — this function reports what the file says.
    pub fn parse(text: &str) -> Result<Attestation> {
        let mut version = None;
        let mut fingerprint = None;
        let mut part_size = None;
        let mut validated = None;
        let mut files = Vec::new();
        let mut meta = String::new();
        let mut in_meta = false;
        let mut meta_closed = false;

        for (n, line) in text.lines().enumerate() {
            let lineno = n + 1;
            if in_meta {
                if line == "meta_end" {
                    in_meta = false;
                    meta_closed = true;
                    continue;
                }
                // `render` indents by exactly two spaces. Strip that and nothing
                // else: a meta line's own leading whitespace would be its content.
                meta.push_str(line.strip_prefix("  ").unwrap_or(line));
                meta.push('\n');
                continue;
            }
            let Some((key, rest)) = line.split_once(' ').or(Some((line, ""))) else {
                continue;
            };
            match key {
                "" => continue,
                "attestation_version" => {
                    version = Some(
                        rest.parse::<u32>()
                            .with_context(|| format!("line {lineno}: attestation_version"))?,
                    )
                }
                "fingerprint" => {
                    fingerprint = Some(
                        rest.parse::<BaseFingerprint>()
                            .with_context(|| format!("line {lineno}: fingerprint"))?,
                    )
                }
                "part_size" => {
                    part_size = Some(PartSize(
                        rest.parse::<u64>()
                            .with_context(|| format!("line {lineno}: part_size"))?,
                    ))
                }
                "validated" => match rest {
                    "true" => validated = Some(true),
                    "false" => validated = Some(false),
                    other => bail!("line {lineno}: validated is {other:?}, not true or false"),
                },
                "file" => {
                    let mut it = rest.split(' ');
                    let (Some(name), Some(len), Some(parts), None) =
                        (it.next(), it.next(), it.next(), it.next())
                    else {
                        bail!("line {lineno}: expected `file <name> <len> <parts>`, got {rest:?}");
                    };
                    let len: u64 = len
                        .parse()
                        .with_context(|| format!("line {lineno}: {name} length"))?;
                    let parts: u64 = parts
                        .parse()
                        .with_context(|| format!("line {lineno}: {name} part count"))?;
                    files.push(PartedFile {
                        name: name.to_owned(),
                        len,
                        parts,
                    });
                }
                "meta_begin" => in_meta = true,
                "meta_end" => bail!("line {lineno}: meta_end without meta_begin"),
                other => bail!("line {lineno}: unknown key {other:?}"),
            }
        }

        if in_meta {
            bail!("meta_begin was never closed by meta_end");
        }
        let version = version.context("attestation_version is missing")?;
        if version != ATTESTATION_VERSION {
            bail!("attestation is version {version}; this build reads {ATTESTATION_VERSION}");
        }
        let part_size = part_size.context("part_size is missing")?;
        if files.is_empty() {
            bail!("attestation lists no files");
        }
        if !meta_closed {
            bail!("attestation carries no meta block");
        }

        // The part count is what every offset calculation depends on, so check it
        // against the length rather than trusting two numbers that can disagree.
        // A manifest written by a publisher with a different `PART` is the failure
        // this catches, and it would otherwise read as scrambled rows.
        for f in &files {
            let want = part_count(f.len, part_size);
            if f.parts != want {
                bail!(
                    "{}: {} B at {} B implies {want} parts, manifest says {}",
                    f.name,
                    f.len,
                    part_size.get(),
                    f.parts
                );
            }
        }

        Ok(Attestation {
            fingerprint: fingerprint.context("fingerprint is missing")?,
            part_size,
            files,
            meta,
            validated: validated.context("validated is missing")?,
        })
    }

    /// One file's entry, or `None` if the manifest does not carry it.
    pub fn file(&self, name: &str) -> Option<&PartedFile> {
        self.files.iter().find(|f| f.name == name)
    }

    /// Render as the line-keyed text `book.meta` already uses, because a format a
    /// person can diff is worth more here than a compact one — this is the file
    /// someone reads when a reader and a publisher disagree.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("attestation_version {ATTESTATION_VERSION}\n"));
        out.push_str(&format!("fingerprint {}\n", self.fingerprint));
        out.push_str(&format!("part_size {}\n", self.part_size.get()));
        out.push_str(&format!("validated {}\n", self.validated));
        for f in &self.files {
            out.push_str(&format!("file {} {} {}\n", f.name, f.len, f.parts));
        }
        out.push_str("meta_begin\n");
        for line in self.meta.lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
        out.push_str("meta_end\n");
        out
    }
}

/// How many parts a file of `len` bytes becomes at `part_size`.
///
/// Both arguments are byte counts, which is why the second one is a [`PartSize`]
/// and not a `u64`: `part_count(part_size, len)` used to compile and return a
/// plausible-looking number.
pub fn part_count(len: u64, part_size: PartSize) -> u64 {
    len.div_ceil(part_size.get())
}

/// The part a logical offset falls in, and where it sits inside that part.
///
/// The arithmetic uniform parts exist for. A reader resolving a row offset calls
/// this and fetches one object; if the row runs past the part's end it stitches
/// the next one (see the module header's straddling note).
pub fn locate(offset: u64, part_size: PartSize) -> (u64, u64) {
    (offset / part_size.get(), offset % part_size.get())
}

/// The object key for one part — zero-padded so a lexical listing is also the
/// numeric order, which is what makes a prune or a diff legible, and suffixed with
/// [`PART_EXT`] because that is what makes it cacheable.
///
/// ⚠️ **The one definition.** A reader that builds this string itself is a second
/// copy of the key format, and the extension is exactly the kind of detail that
/// gets left out of the copy.
pub fn part_key(name: &str, part: u64) -> String {
    format!("{name}/{part:05}.{PART_EXT}")
}

/// Split one file into `part_size` objects under `out/<name>/NNNNN.bin`.
fn split_file(src: &Path, out: &Path, name: &str, part_size: PartSize) -> Result<PartedFile> {
    let mut f = File::open(src).with_context(|| format!("opening {}", src.display()))?;
    let len = f
        .metadata()
        .with_context(|| format!("statting {}", src.display()))?
        .len();
    let dir = out.join(name);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut buf = vec![0u8; part_size.get() as usize];
    let mut part = 0u64;
    let mut written = 0u64;
    while written < len {
        let want = (len - written).min(part_size.get()) as usize;
        f.read_exact(&mut buf[..want])
            .with_context(|| format!("reading {} at {written}", src.display()))?;
        // Through `part_key` so the layout on disk is the layout in the bucket —
        // including the extension, which is load-bearing.
        let key = out.join(part_key(name, part));
        let mut w = BufWriter::new(
            File::create(&key).with_context(|| format!("creating {}", key.display()))?,
        );
        w.write_all(&buf[..want])?;
        w.flush()?;
        written += want as u64;
        part += 1;
    }

    // Belt and braces: the reader's offset arithmetic depends on this identity, so
    // assert it here rather than discover a short file over a network.
    if part != part_count(len, part_size) {
        bail!(
            "{name} split into {part} parts; {len} B at {} B implies {}",
            part_size.get(),
            part_count(len, part_size)
        );
    }
    Ok(PartedFile {
        name: name.to_owned(),
        len,
        parts: part,
    })
}

/// Lay a book out for upload: `out/<file>/NNNNN.bin` plus the manifest.
///
/// `validated` should be the result of actually opening the book locally — the
/// caller does that, because opening it is what proves the directories partition
/// their arrays and this module must not claim a check it did not run.
///
/// `part_size` is a **publish-time parameter** ([`DEFAULT_PART`] is only a default):
/// the right size depends on how the artifact will be read, not on what it
/// contains. A reader takes it from the attestation.
pub fn publish(
    book: &Path,
    out: &Path,
    validated: bool,
    part_size: PartSize,
) -> Result<Attestation> {
    if !part_size.get().is_power_of_two() {
        bail!(
            "part_size {} is not a power of two; offset arithmetic assumes it is",
            part_size.get()
        );
    }
    let meta_path = book.join(META_FILE);
    let meta_text = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("reading {}", meta_path.display()))?;
    let meta = Meta::parse(&meta_text)?;
    if meta.layout_version != format::LAYOUT_VERSION {
        bail!(
            "book.meta is layout_version {}; this build publishes {}",
            meta.layout_version,
            format::LAYOUT_VERSION
        );
    }

    std::fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
    let mut files = Vec::with_capacity(PARTED.len());
    for name in PARTED {
        files.push(split_file(&book.join(name), out, name, part_size)?);
    }

    // The one cross-check worth making here: the row arena's published length must
    // match what the builder recorded, or every offset in every index is wrong.
    let rows = files
        .iter()
        .find(|f| f.name == ROWS_FILE)
        .expect("ROWS_FILE is in PARTED");
    if rows.len != meta.rows_bytes {
        bail!(
            "{ROWS_FILE} is {} B but book.meta claims {}",
            rows.len,
            meta.rows_bytes
        );
    }

    let att = Attestation {
        fingerprint: meta.fingerprint(),
        part_size,
        files,
        meta: meta_text,
        validated,
    };
    let path = out.join(ATTESTATION_FILE);
    std::fs::write(&path, att.render()).with_context(|| format!("writing {}", path.display()))?;
    Ok(att)
}

/// Every object key a published book consists of, for an uploader to walk.
pub fn keys(att: &Attestation) -> Vec<PathBuf> {
    let mut out = vec![PathBuf::from(ATTESTATION_FILE)];
    for f in &att.files {
        for p in 0..f.parts {
            out.push(PathBuf::from(part_key(&f.name, p)));
        }
    }
    out
}

// ── The datum set: `hash → preimage`, published additively ─────────────────────────
//
// Design: `docs/design/DATUM_RESOLVER_FORMAT.md`. This is the artifact a
// hash-attached output needs: the output carries no payload, and the bytes exist
// only in the witness set of some transaction — which is a *capture*, not somewhere
// a reader can walk to, because a block keeps its bodies and its witness sets in
// separate arrays (design §2, measured).
//
// The unit is one **closed window pair** — `datum.tbl` + `datum.bin`, exactly as the
// builder wrote them — plus one catalogue naming every published window:
//
//     datum/v1/w-<first_slot>-<last_slot>-<digest>.tbl
//     datum/v1/w-<first_slot>-<last_slot>-<digest>.bin
//     datum/v1/current
//
// ⚠️ **A window pair is content-addressed.** The digest in the key is the file's own
// blake2b-256, so a re-capture that finds the same datums lands on the same keys and
// uploads nothing, and a reader can check what it fetched against the key it asked
// for. The catalogue is the ONE object that is ever rewritten — which is also why it
// is the one object that must be uploaded with a short TTL, never with the windows'
// cache posture.
//
// ⚠️ **No parts, unlike [`PARTED`].** A lookup ranges into the entry array and then
// into the blob, and multi-range works only *within* one object, so splitting a
// window would cost every lookup its batching. A table big enough to want splitting
// is split by hash prefix instead — one pair per prefix — which is the design's
// second tier (§6.4), and then each object is small again.

/// Every published datum object lives under this prefix.
pub const DATUM_PREFIX: &str = "datum/v1";

/// The catalogue — the one mutable object in the set.
pub const DATUM_CATALOGUE_FILE: &str = "current";

/// The catalogue's own layout, so a reader can refuse one it does not understand
/// before it has fetched a table.
pub const DATUM_CATALOGUE_VERSION: u32 = 1;

/// The catalogue's object key.
pub fn datum_catalogue_key() -> String {
    format!("{DATUM_PREFIX}/{DATUM_CATALOGUE_FILE}")
}

/// The key of one window's entry array.
///
/// ⚠️ **The one definition.** Both sides of this — the writer naming an object and a
/// reader deriving where to range — call these, because a second copy of the key
/// format is how an extension or a separator silently diverges.
pub fn datum_table_key(first_slot: u64, last_slot: u64, digest: &str) -> String {
    format!("{DATUM_PREFIX}/w-{first_slot}-{last_slot}-{digest}.tbl")
}

/// The key of one window's payload blob.
pub fn datum_blob_key(first_slot: u64, last_slot: u64, digest: &str) -> String {
    format!("{DATUM_PREFIX}/w-{first_slot}-{last_slot}-{digest}.bin")
}

/// The walk a capture covered, in the walker's own coordinate.
///
/// ⚠️ The slot range and the entry count are deliberately NOT here: they come from
/// the table's own header, so a name or a catalogue line cannot describe a table
/// that is not the one on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatumWindow {
    pub first_chunk: u16,
    pub last_chunk: u16,
}

/// One object in the set, as the catalogue records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatumObject {
    pub key: String,
    pub len: u64,
    /// Blake2b-256 of the object's bytes, lower-case hex — the component of `key`
    /// that makes the object rewrite-proof.
    pub digest: String,
}

/// One window, as the catalogue records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatumEntry {
    pub first_chunk: u16,
    pub last_chunk: u16,
    pub first_slot: u64,
    pub last_slot: u64,
    pub entries: u64,
    /// The table format the pair is written in — a reader refuses a version it does
    /// not read rather than ranging into offsets it cannot interpret.
    pub layout_version: u32,
    pub table: DatumObject,
    pub blob: DatumObject,
}

/// Every window published so far.
///
/// Flat text, one line per window, in the same doctrine as `book.meta`: every field
/// is required, a missing or extra one is an error rather than a default, and the
/// rendering is byte-stable so re-publishing the same set is not a diff.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatumCatalogue {
    /// Sorted by `(first_chunk, last_chunk)` — the order a chunk-anchored reader
    /// walks, and the order the rendering is pinned to.
    windows: Vec<DatumEntry>,
}

impl DatumCatalogue {
    pub fn windows(&self) -> &[DatumEntry] {
        &self.windows
    }

    /// The window covering `chunk`, if one is published. The reader's anchor is a
    /// chunk — that is what the index answers a transaction with.
    pub fn covering(&self, chunk: u16) -> Option<&DatumEntry> {
        self.covering_index(chunk).map(|at| &self.windows[at])
    }

    /// Where that window sits in [`windows`](Self::windows).
    ///
    /// ⚠️ **The predicate lives here and nowhere else.** A caller that needs to *name* the
    /// window — a reader that asks for an array once and files the answers under it — would
    /// otherwise write the interval test a second time, and two copies of "is this chunk in
    /// this window" is how a lookup lands on the wrong array.
    pub fn covering_index(&self, chunk: u16) -> Option<usize> {
        self.windows
            .iter()
            .position(|w| chunk >= w.first_chunk && chunk <= w.last_chunk)
    }

    pub fn parse(text: &str) -> Result<DatumCatalogue> {
        let mut lines = text.lines();
        let first = lines
            .next()
            .context("an empty catalogue: the version line is missing")?;
        let mut version = first.split_whitespace();
        let (Some("catalogue_version"), Some(v), None) =
            (version.next(), version.next(), version.next())
        else {
            bail!("line 1: expected `catalogue_version <n>`, got {first:?}");
        };
        let v: u32 = v.parse().context("line 1: catalogue_version")?;
        if v != DATUM_CATALOGUE_VERSION {
            bail!("the catalogue is version {v}; this build reads {DATUM_CATALOGUE_VERSION}");
        }

        let mut windows = Vec::new();
        for (at, line) in lines.enumerate() {
            let lineno = at + 2;
            if line.trim().is_empty() {
                continue;
            }
            let f: Vec<&str> = line.split_whitespace().collect();
            let ["window", fc, lc, fs, ls, n, lv, tl, td, bl, bd] = f.as_slice() else {
                bail!(
                    "line {lineno}: a window is `window <first_chunk> <last_chunk> \
                     <first_slot> <last_slot> <entries> <layout_version> <tbl_len> \
                     <tbl_digest> <bin_len> <bin_digest>`, got {line:?}"
                );
            };
            let num = |s: &str, what: &str| -> Result<u64> {
                s.parse().with_context(|| format!("line {lineno}: {what}"))
            };
            let first_slot = num(fs, "first_slot")?;
            let last_slot = num(ls, "last_slot")?;
            windows.push(DatumEntry {
                first_chunk: num(fc, "first_chunk")? as u16,
                last_chunk: num(lc, "last_chunk")? as u16,
                first_slot,
                last_slot,
                entries: num(n, "entries")?,
                layout_version: num(lv, "layout_version")? as u32,
                table: DatumObject {
                    key: datum_table_key(first_slot, last_slot, td),
                    len: num(tl, "table len")?,
                    digest: td.to_string(),
                },
                blob: DatumObject {
                    key: datum_blob_key(first_slot, last_slot, bd),
                    len: num(bl, "blob len")?,
                    digest: bd.to_string(),
                },
            });
        }
        Ok(DatumCatalogue { windows })
    }

    pub fn render(&self) -> String {
        let mut out = format!("catalogue_version {DATUM_CATALOGUE_VERSION}\n");
        for w in &self.windows {
            out.push_str(&format!(
                "window {} {} {} {} {} {} {} {} {} {}\n",
                w.first_chunk,
                w.last_chunk,
                w.first_slot,
                w.last_slot,
                w.entries,
                w.layout_version,
                w.table.len,
                w.table.digest,
                w.blob.len,
                w.blob.digest,
            ));
        }
        out
    }

    /// The catalogue on disk, or an empty one — absent is a first publish, not an
    /// error.
    pub fn load(path: &Path) -> Result<DatumCatalogue> {
        match std::fs::read_to_string(path) {
            Ok(text) => DatumCatalogue::parse(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(DatumCatalogue::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    pub fn store(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        std::fs::write(path, self.render()).with_context(|| format!("writing {}", path.display()))
    }

    /// Add a window, replacing any entry for the same chunk range — a re-capture of
    /// a window supersedes its own entry and leaves every other window alone.
    /// Superseded objects stay in the layout (and in the bucket): correctness never
    /// depends on deleting them, only on which catalogue a reader was handed.
    pub fn upsert(&mut self, entry: DatumEntry) {
        let at = self
            .windows
            .iter()
            .position(|w| w.first_chunk == entry.first_chunk && w.last_chunk == entry.last_chunk);
        match at {
            Some(i) => self.windows[i] = entry,
            None => self.windows.push(entry),
        }
        self.windows
            .sort_by_key(|w| (w.first_chunk, w.last_chunk, w.first_slot));
    }
}

/// Lay one window's capture out for upload, and merge it into the catalogue.
///
/// Reads the window's slot range and entry count from `dir/datum.tbl`'s own header —
/// the file is the authority — checks the pair before it can be published (see
/// [`check_datum_pair`]), copies both files to their content-addressed keys under
/// `out`, and updates `out/datum/v1/current`.
///
/// Returns the entry, so a caller can print or assert the keys it just laid out.
pub fn publish_datums(dir: &Path, out: &Path, window: DatumWindow) -> Result<DatumEntry> {
    let table_src = dir.join(crate::format::DATUM_FILE);
    let blob_src = dir.join(crate::format::DATUM_BLOB);

    let table =
        std::fs::read(&table_src).with_context(|| format!("reading {}", table_src.display()))?;
    let header = crate::format::parse_datum_header(&table, &table_src.display().to_string())?;
    let blob_len = std::fs::metadata(&blob_src)
        .with_context(|| format!("statting {}", blob_src.display()))?
        .len();
    let entries = check_datum_pair(&table, blob_len)?;
    if entries != header.entries {
        bail!(
            "{} has {} entries; the pair tiles to {entries}",
            table_src.display(),
            header.entries
        );
    }

    let (tbl_digest, tbl_len) = digest_file(&table_src)?;
    let (bin_digest, bin_len) = digest_file(&blob_src)?;
    let entry = DatumEntry {
        first_chunk: window.first_chunk,
        last_chunk: window.last_chunk,
        first_slot: header.first_slot,
        last_slot: header.last_slot,
        entries,
        layout_version: header.layout_version,
        table: DatumObject {
            key: datum_table_key(header.first_slot, header.last_slot, &tbl_digest),
            len: tbl_len,
            digest: tbl_digest,
        },
        blob: DatumObject {
            key: datum_blob_key(header.first_slot, header.last_slot, &bin_digest),
            len: bin_len,
            digest: bin_digest,
        },
    };

    for (src, object) in [(&table_src, &entry.table), (&blob_src, &entry.blob)] {
        let dst = out.join(&object.key);
        let wrote = copy_object(src, &dst)?;
        if wrote != object.len {
            bail!(
                "{} copied {wrote} B; it is {} B — the object does not describe itself",
                src.display(),
                object.len
            );
        }
    }

    let catalogue_path = out.join(datum_catalogue_key());
    let mut catalogue = DatumCatalogue::load(&catalogue_path)?;
    catalogue.upsert(entry.clone());
    catalogue.store(&catalogue_path)?;
    Ok(entry)
}

/// A file's blake2b-256, lower-case hex, and its length.
///
/// Streamed: the largest legitimate pair is `u32`-addressed (under 4 GiB, see
/// [`crate::format::write_datum_table`]) and a whole-chain capture is many windows
/// rather than one large one, but materialising a window to hash it is a cost with
/// no reader waiting on it.
fn digest_file(path: &Path) -> Result<(String, u64)> {
    let mut f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Hasher::<256>::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut len = 0u64;
    loop {
        let n = f
            .read(&mut buf)
            .with_context(|| format!("hashing {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.input(&buf[..n]);
        len += n as u64;
    }
    Ok((hasher.finalize().to_string(), len))
}

/// Copy one object into the layout, creating its directory, and report the bytes
/// written.
fn copy_object(src: &Path, dst: &Path) -> Result<u64> {
    if let Some(dir) = dst.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let mut from = File::open(src).with_context(|| format!("opening {}", src.display()))?;
    let mut to = File::create(dst).with_context(|| format!("creating {}", dst.display()))?;
    let wrote = std::io::copy(&mut from, &mut to)
        .with_context(|| format!("copying {} to {}", src.display(), dst.display()))?;
    to.flush()
        .with_context(|| format!("flushing {}", dst.display()))?;
    Ok(wrote)
}

/// The invariant [`crate::format::DatumTable::open`] enforces, applied *before* a
/// publish.
///
/// ⚠️ Both halves matter and neither is optional. A pair that does not tile would
/// answer with **wrong bytes** instead of failing — the worst shape of error a
/// reader can meet — and a pair whose entries are not strictly increasing by hash
/// makes the reader's binary search return the wrong row for a *correct* key.
/// Returns the entry count.
fn check_datum_pair(table: &[u8], blob_len: u64) -> Result<u64> {
    let header = crate::format::parse_datum_header(table, "datum.tbl")?;
    let entries = header.entries;
    let want =
        crate::format::DATUM_HEADER as u64 + crate::format::DATUM_ENTRY_BYTES as u64 * entries;
    if table.len() as u64 != want {
        bail!(
            "datum.tbl is {} B; {entries} entries need {want}",
            table.len()
        );
    }
    let mut expect = 0u64;
    let mut previous: Option<[u8; 32]> = None;
    for i in 0..entries {
        let at = crate::format::DATUM_HEADER + crate::format::DATUM_ENTRY_BYTES * i as usize;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&table[at..at + 32]);
        if let Some(prev) = previous
            && hash <= prev
        {
            bail!("datum.tbl entry {i} is not above its predecessor; the sort is broken");
        }
        previous = Some(hash);
        let off = u32::from_le_bytes(table[at + 32..at + 36].try_into().expect("4 bytes"));
        let len = u32::from_le_bytes(table[at + 36..at + 40].try_into().expect("4 bytes"));
        if u64::from(off) != expect {
            bail!(
                "datum.tbl entry {i} starts at {off} in datum.bin; {expect} expected — the \
                 pair is not from one capture"
            );
        }
        expect += u64::from(len);
    }
    if expect != blob_len {
        bail!(
            "datum.tbl's {entries} entries address {expect} B; datum.bin is {blob_len} B — the \
             pair is not from one capture"
        );
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PART: PartSize = DEFAULT_PART;
    /// The same number as a plain byte count, for the offsets a test computes.
    const P: u64 = PART.get();

    /// A directory of its own per test, as `format.rs`'s datum tests do: these read
    /// and write real files, and a published key is the thing under test.
    fn datum_scratch(what: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("live-book-publish-{}-{what}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create");
        dir
    }

    /// A capture directory with two datums in it, written by the real writer.
    fn captured(dir: &Path, coverage: (u64, u64)) {
        let a = ([0x11u8; 32], vec![0xA1u8, 0x01]);
        let b = ([0x22u8; 32], vec![0xA1u8, 0x02, 0x03]);
        crate::format::write_datum_table(dir, coverage, &[a, b]).expect("write datum table");
    }

    #[test]
    fn a_datum_window_names_its_objects_by_slot_range_and_digest() {
        let dir = datum_scratch("naming");
        let out = dir.join("publish");
        captured(&dir, (100, 200));

        let e = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect("publish");
        assert_eq!((e.first_chunk, e.last_chunk), (7, 9));
        assert_eq!((e.first_slot, e.last_slot), (100, 200));
        assert_eq!(e.entries, 2);
        assert_eq!(e.layout_version, crate::format::LAYOUT_VERSION);
        // The digest is in the key, and it is the FILE's own — so the key a reader
        // asked for is the check it can run on what came back.
        assert_eq!(e.table.key, datum_table_key(100, 200, &e.table.digest),);
        assert_eq!(e.blob.key, datum_blob_key(100, 200, &e.blob.digest));
        assert!(e.table.key.ends_with(".tbl"), "{} ", e.table.key);
        assert!(e.blob.key.ends_with(".bin"), "{} ", e.blob.key);
        // Both files are on disk at exactly those keys, with exactly those bytes.
        for object in [&e.table, &e.blob] {
            let bytes = std::fs::read(out.join(&object.key)).expect("the object is at its key");
            assert_eq!(bytes.len() as u64, object.len);
            assert_eq!(
                digest_file(&out.join(&object.key)).unwrap().0,
                object.digest
            );
        }
        // And the catalogue a reader fetches first describes that window.
        let c = DatumCatalogue::load(&out.join(datum_catalogue_key())).expect("catalogue");
        assert_eq!(c.windows(), &[e]);
    }

    /// Re-publishing the same window is not a diff and not a second object: the
    /// digest names the bytes, so a re-capture that found the same datums lands on
    /// the same keys. This is what makes the set safe to publish additively.
    #[test]
    fn re_publishing_a_window_lands_on_the_same_keys() {
        let dir = datum_scratch("idempotent");
        let out = dir.join("publish");
        captured(&dir, (100, 200));
        let first = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect("first publish");
        let before = std::fs::read(out.join(datum_catalogue_key())).expect("catalogue");

        let after = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect("second publish");
        let again = std::fs::read(out.join(datum_catalogue_key())).expect("catalogue");
        assert_eq!(first, after);
        assert_eq!(before, again, "the catalogue is byte-stable for one window");
        assert_eq!(
            DatumCatalogue::load(&out.join(datum_catalogue_key()))
                .expect("catalogue")
                .windows()
                .len(),
            1,
            "a re-publish of one window is one entry, not two"
        );
    }

    /// Windows accumulate, and a reader anchored on a chunk finds the one that
    /// covers it — the whole point of carrying the chunk range rather than only the
    /// slot range.
    #[test]
    fn windows_accumulate_and_a_chunk_finds_its_window() {
        let dir = datum_scratch("accumulate");
        let out = dir.join("publish");
        captured(&dir, (100, 200));
        let a = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect("a");
        captured(&dir, (300, 400));
        let b = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 10,
                last_chunk: 12,
            },
        )
        .expect("b");

        let c = DatumCatalogue::load(&out.join(datum_catalogue_key())).expect("catalogue");
        assert_eq!(c.windows(), &[a, b]);
        assert_eq!(c.covering(8).map(|w| w.first_slot), Some(100));
        assert_eq!(c.covering(11).map(|w| w.first_slot), Some(300));
        assert_eq!(
            c.covering(6),
            None,
            "a chunk no window covers is a scoped miss"
        );
        // ⚠️ **The index and the entry must agree**, because a reader that files an array's
        // answers under a window asks for the index and then reads the entry at it. Two
        // copies of the interval test is how those two would come apart.
        assert_eq!(c.covering_index(8), Some(0));
        assert_eq!(c.covering_index(11), Some(1));
        assert_eq!(c.covering_index(6), None);
        for chunk in [7u16, 9, 10, 12] {
            let at = c.covering_index(chunk).expect("a covered chunk");
            assert_eq!(c.covering(chunk), Some(&c.windows()[at]));
        }
        // A second window's capture rewrote datum.tbl/datum.bin, and the FIRST
        // window's objects are still on disk — nothing about adding is a rewrite.
        assert!(out.join(&c.windows()[0].table.key).exists());
        assert!(out.join(&c.windows()[1].table.key).exists());
    }

    /// A catalogue survives a round trip through its own text, and refuses one that
    /// does not say what it means — the `book.meta` doctrine, where a missing field
    /// is an error rather than a default.
    #[test]
    fn a_catalogue_round_trips_and_refuses_a_broken_line() {
        let dir = datum_scratch("catalogue-text");
        let out = dir.join("publish");
        captured(&dir, (100, 200));
        let e = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect("publish");
        let text = DatumCatalogue::load(&out.join(datum_catalogue_key()))
            .expect("catalogue")
            .render();
        let back = DatumCatalogue::parse(&text).expect("parses");
        assert_eq!(back.windows(), &[e]);
        assert_eq!(back.render(), text, "rendering is byte-stable");

        let short = text.replace(" 2 3 ", " 2 ");
        assert!(
            DatumCatalogue::parse(&short).is_err(),
            "a window line missing its layout version must not parse"
        );
        assert!(DatumCatalogue::parse("").is_err(), "no version line");
        assert!(DatumCatalogue::parse("catalogue_version 99\n").is_err());
    }

    /// The pair check runs before anything is published: a blob that belongs to a
    /// different capture would answer with WRONG BYTES rather than fail, so it must
    /// not reach a bucket. Both halves of the invariant are exercised.
    #[test]
    fn a_pair_that_does_not_tile_is_refused_before_it_is_published() {
        let dir = datum_scratch("tiling");
        let out = dir.join("publish");
        captured(&dir, (100, 200));
        let blob = dir.join(crate::format::DATUM_BLOB);
        let good = std::fs::read(&blob).expect("blob");

        // A blob one byte too long: every offset still resolves, and the last one
        // now points past the data a reader would get.
        std::fs::write(&blob, [&good[..], &[0u8]].concat()).expect("write");
        let err = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect_err("a non-tiling pair must be refused")
        .to_string();
        assert!(err.contains("address"), "{err}");
        assert!(
            !out.join(datum_catalogue_key()).exists(),
            "nothing is published when the pair does not check out"
        );
    }

    /// A table whose entries are out of order is refused too: a binary search over
    /// it can return a row that is not the key asked for, which reads as data.
    #[test]
    fn a_table_out_of_hash_order_is_refused() {
        let dir = datum_scratch("order");
        let out = dir.join("publish");
        captured(&dir, (100, 200));
        let tbl = dir.join(crate::format::DATUM_FILE);
        let mut bytes = std::fs::read(&tbl).expect("table");
        // Swap the two entries' HASHES and nothing else: tiling still holds (their
        // offsets and lengths stay where they are), so this isolates the ordering
        // rule from the pairing one — the pair the tiling check alone would pass.
        let (a, b) = (bytes[28..60].to_vec(), bytes[68..100].to_vec());
        bytes[28..60].copy_from_slice(&b);
        bytes[68..100].copy_from_slice(&a);
        std::fs::write(&tbl, &bytes).expect("write");

        let err = publish_datums(
            &dir,
            &out,
            DatumWindow {
                first_chunk: 7,
                last_chunk: 9,
            },
        )
        .expect_err("an unsorted table must be refused")
        .to_string();
        assert!(err.contains("sort"), "{err}");
        assert!(!out.join(datum_catalogue_key()).exists());
    }

    #[test]
    fn locate_is_the_inverse_of_part_layout() {
        assert_eq!(locate(0, PART), (0, 0));
        assert_eq!(locate(P - 1, PART), (0, P - 1));
        assert_eq!(locate(P, PART), (1, 0));
        assert_eq!(locate(P * 3 + 17, PART), (3, 17));
    }

    #[test]
    fn locate_follows_the_part_size_it_is_given() {
        // The property that makes part size a publish-time parameter: the same
        // offset resolves differently under a different layout, and a reader that
        // used a compiled-in size against an artifact published at another would
        // read the right bytes from the wrong object.
        let big = PartSize(64 << 20);
        assert_eq!(locate(P, big), (0, P));
        assert_eq!(locate(big.get() + 17, big), (1, 17));
    }

    #[test]
    fn part_count_is_ceiling_not_floor() {
        // A file one byte over a boundary needs another part; getting this wrong
        // truncates the last row of the arena.
        assert_eq!(part_count(0, PART), 0);
        assert_eq!(part_count(1, PART), 1);
        assert_eq!(part_count(P, PART), 1);
        assert_eq!(part_count(P + 1, PART), 2);
    }

    #[test]
    fn keys_are_zero_padded_so_lexical_order_is_numeric() {
        let mut ks = vec![part_key("rows.bin", 10), part_key("rows.bin", 9)];
        ks.sort();
        assert_eq!(ks, vec!["rows.bin/00009.bin", "rows.bin/00010.bin"]);
    }

    #[test]
    fn a_part_key_carries_the_extension_that_makes_it_cacheable() {
        // Not cosmetic: without an extension Cloudflare never caches the object,
        // and a multi-range request against an uncached object is a 400. This
        // assertion is the guard on a property nothing else in the type system
        // expresses.
        assert!(part_key("rows.bin", 0).ends_with(".bin"));
        assert!(
            keys(&sample())
                .iter()
                .skip(1)
                .all(|k| k.to_string_lossy().ends_with(".bin"))
        );
    }

    #[test]
    fn a_rendered_attestation_is_diffable_and_carries_meta_verbatim() {
        let att = Attestation {
            fingerprint: Meta::default().fingerprint(),
            part_size: PART,
            files: vec![PartedFile {
                name: "rows.bin".into(),
                len: P + 10,
                parts: 2,
            }],
            meta: "layout_version 3\nrows 7\n".into(),
            validated: true,
        };
        let text = att.render();
        assert!(text.contains("file rows.bin 1048586 2"));
        assert!(text.contains("validated true"));
        // Indented rather than reworded: a publisher must not paraphrase what the
        // builder wrote, because the two disagreeing is the thing this file exists
        // to make visible.
        assert!(text.contains("  layout_version 3"));
        assert!(text.contains("  rows 7"));
    }

    /// The real manifest shape, as `fold-probe publish` writes it.
    fn sample() -> Attestation {
        Attestation {
            fingerprint: "59c0554061419b7e00f607cb77c69f80"
                .parse()
                .expect("32 hex digits"),
            part_size: PART,
            files: vec![
                PartedFile {
                    name: ROWS_FILE.into(),
                    len: 21_924_039,
                    parts: 21,
                },
                PartedFile {
                    name: POLICY_FILE.into(),
                    len: 372_664,
                    parts: 1,
                },
            ],
            meta: "layout_version 3\nrows 128567\nbase_slot 198590382\n".into(),
            validated: true,
        }
    }

    #[test]
    fn render_and_parse_are_inverses() {
        let att = sample();
        let back = Attestation::parse(&att.render()).expect("round trip");
        assert_eq!(back, att);
        // The meta block specifically: a parser that kept the two-space indent
        // would hand `Meta::parse` lines it rejects.
        assert!(back.meta.starts_with("layout_version 3\n"));
    }

    #[test]
    fn the_fingerprint_survives_the_text_form() {
        let att = sample();
        let back = Attestation::parse(&att.render()).expect("round trip");
        assert_eq!(
            back.fingerprint.to_string(),
            "59c0554061419b7e00f607cb77c69f80"
        );
        assert_eq!(back.fingerprint, att.fingerprint);
    }

    #[test]
    fn a_part_count_that_contradicts_the_length_is_refused() {
        // The failure this check exists for: a manifest written by a publisher
        // using a different PART. Every offset would be wrong, and the rows would
        // read as scrambled rather than as an error.
        let mut att = sample();
        att.files[0].parts = 6;
        let err = Attestation::parse(&att.render()).expect_err("must refuse");
        assert!(
            err.to_string().contains("implies 21 parts"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn a_truncated_or_unknown_manifest_is_refused_rather_than_guessed() {
        // Each of these is a manifest a reader cannot do arithmetic from, so each
        // has to be a refusal rather than a default.
        for (what, text) in [
            ("no version", "part_size 1048576\nvalidated true\n"),
            (
                "no files",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nmeta_begin\nmeta_end\n",
            ),
            (
                "no meta block",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\n",
            ),
            (
                "unclosed meta",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\nmeta_begin\n  rows 1\n",
            ),
            (
                "a key from a future version",
                "attestation_version 1\nfingerprint 59c0554061419b7e00f607cb77c69f80\n\
                 part_size 1048576\nvalidated true\nfile rows.bin 10 1\ncontent_hash abc\n\
                 meta_begin\nmeta_end\n",
            ),
            (
                "a short fingerprint",
                "attestation_version 1\nfingerprint 59c0\npart_size 1048576\n\
                 validated true\nfile rows.bin 10 1\nmeta_begin\nmeta_end\n",
            ),
        ] {
            assert!(
                Attestation::parse(text).is_err(),
                "{what} must be refused, not guessed past"
            );
        }
    }

    #[test]
    fn validated_false_parses_and_is_left_for_the_reader_to_judge() {
        // Reported, not enforced: whether an unvalidated book is usable is the
        // reader's decision, and a parser that refused here would hide it.
        let mut att = sample();
        att.validated = false;
        let back = Attestation::parse(&att.render()).expect("parses");
        assert!(!back.validated);
    }
}
