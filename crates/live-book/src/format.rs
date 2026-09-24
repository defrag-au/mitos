//! The artifact's files: the version they share, the entries they hold, the
//! directories over them, and the two side tables.
//!
//! One ordering is one table plus one directory, and every ordering uses the
//! shape written here — 12-byte `{u64 prefix, u32 row}` entries in canonical
//! order, under a `2^DIR_BITS`-bucket directory that partitions them. Three
//! callers today (`by_outref`, `by_policy`, `by_addr`), one writer, because the
//! last time this was written twice in one function the second copy was a
//! duplicate of the first.
//!
//! # Every file is swapped, never written in place
//!
//! A published artifact is read through a mapping that outlives a rebuild
//! (`reader::Base`), so a writer must leave the old inode intact: each file is
//! built under a temporary name and moved into place with `rename`, which is
//! atomic within a filesystem. Writing in place would truncate under a live
//! reader's mapping, and a read past the new end is a `SIGBUS` rather than an
//! error. `book.meta` goes last for the same reason the builder writes it last —
//! its presence means everything before it succeeded.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// `B3`: 2^21 buckets, ~8 MB, ~5 entries per bucket at 10.9 M rows.
pub const DIR_BITS: u32 = 21;

/// The only layout this code writes or reads, in every file that has one.
///
/// 2: the build key folds the output index into its tail (version 1 omitted it,
/// so a version-1 artifact has the outputs of each transaction merged).
/// 3: `addr_len` is `u16`, not `u8` (version 2 could not hold a Byron address:
/// real mainnet data falsifies the `u8` at chunk 81 of 9,181 with a 513-byte
/// address). The row header is still 56 bytes — the byte came out of the spare.
pub const LAYOUT_VERSION: u32 = 3;

/// The value a bucket holds when no entry falls in it. A real entry index never
/// reaches it: the entry count is bounded by the row count, which `u16`
/// addressing of `rows.bin` already caps far below `u32::MAX`.
pub const DIRECTORY_EMPTY: u32 = u32::MAX;

/// A policy id, and the width `policy.tbl` stores them at.
pub const POLICY_ID_BYTES: usize = 28;

pub const TABLE_OUTREF: &str = "by_outref.idx";
pub const TABLE_POLICY: &str = "by_policy.idx";
pub const TABLE_ADDR: &str = "by_addr.idx";
pub const DIRECTORY_OUTREF: &str = "dir.idx";
pub const DIRECTORY_POLICY: &str = "policy.dir.idx";
pub const DIRECTORY_ADDR: &str = "addr.dir.idx";
pub const ROWS_FILE: &str = "rows.bin";
pub const POLICY_FILE: &str = "policy.tbl";
pub const META_FILE: &str = "book.meta";

/// One entry of a permutation: the 64-bit prefix a lookup matches, and the row
/// it points at.
///
/// `tail` is the build's business — it is what makes the build index's key 96
/// bits wide and therefore confirm-free — and is deliberately **not** written to
/// the artifact, where 64 bits plus a confirm against the row is the trade `B3`
/// makes. That asymmetry is why this is a type rather than two tuples: every
/// ordering hands the writer the same shape, so the entry width, the canonical
/// order and the directory fill are written in one place.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Perm {
    pub prefix: u64,
    pub tail: u32,
    pub row: u32,
}

/// Write one ordering: its `{u64 prefix, u32 row}` permutations in canonical
/// order, plus a `2^DIR_BITS`-bucket directory over them.
///
/// Every ordering goes through here so they cannot disagree about the format, the
/// canonical order, or the fill — and the fill is the part that was wrong. An
/// empty bucket must carry the index of the next one that is not; filling with the
/// entry count instead makes an empty bucket *precede* a non-empty one, so
/// `buckets[b] > buckets[b + 1]`, the array stops being the partition `B5` claims,
/// and a consumer reading the range as `end − start` underflows. `verify` catches
/// it, and a book round-trips through it in the probe's tests.
pub fn write_ordering(dir: &Path, table: &str, directory: &str, entries: &[Perm]) -> Result<()> {
    let mut w = BufWriter::with_capacity(4 << 20, temp_for(dir, table)?);
    w.write_all(&LAYOUT_VERSION.to_le_bytes())?;
    w.write_all(&(entries.len() as u64).to_le_bytes())?;
    for e in entries {
        w.write_all(&e.prefix.to_le_bytes())?;
        w.write_all(&e.row.to_le_bytes())?;
    }
    w.flush()?;
    drop(w);
    swap_in(dir, table)?;

    write_directory(
        dir,
        directory,
        entries.iter().map(|e| e.prefix),
        entries.len(),
    )
}

/// The `2^DIR_BITS`-bucket directory over an ordering's prefixes, which must
/// arrive in canonical order.
///
/// Split out of `write_ordering` so `dir.idx` can be rebuilt on its own, from the
/// prefixes `by_outref.idx` already holds, **without rewriting the entry array** —
/// reconstructing that array could not reproduce the builder's canonical order,
/// because `tail` is not stored and two rows sharing a prefix could come back in
/// the other order. A repair must not invent a second ordering.
pub fn write_directory(
    dir: &Path,
    name: &str,
    prefixes: impl Iterator<Item = u64>,
    n: usize,
) -> Result<()> {
    let n_buckets = 1usize << DIR_BITS;
    let mut buckets = vec![DIRECTORY_EMPTY; n_buckets];
    for (i, prefix) in prefixes.enumerate() {
        let b = (prefix >> (64 - DIR_BITS)) as usize;
        if buckets[b] == DIRECTORY_EMPTY {
            buckets[b] = i as u32;
        }
    }
    // Backward fill: see `write_ordering`.
    let mut next = n as u32;
    for b in (0..n_buckets).rev() {
        if buckets[b] == DIRECTORY_EMPTY {
            buckets[b] = next;
        } else {
            next = buckets[b];
        }
    }
    let mut d = BufWriter::with_capacity(1 << 20, temp_for(dir, name)?);
    d.write_all(&LAYOUT_VERSION.to_le_bytes())?;
    d.write_all(&DIR_BITS.to_le_bytes())?;
    for b in &buckets {
        d.write_all(&b.to_le_bytes())?;
    }
    d.flush()?;
    drop(d);
    swap_in(dir, name)
}

/// Write `policy.tbl`: the interning table, `id -> 28-byte policy`, in the order
/// the builder assigned the ids.
pub fn write_policy_tbl(dir: &Path, policies: &[[u8; 28]]) -> Result<()> {
    let mut p = BufWriter::with_capacity(1 << 20, temp_for(dir, POLICY_FILE)?);
    p.write_all(&LAYOUT_VERSION.to_le_bytes())?;
    p.write_all(&(policies.len() as u64).to_le_bytes())?;
    for pol in policies {
        p.write_all(pol)?;
    }
    p.flush()?;
    drop(p);
    swap_in(dir, POLICY_FILE)
}

/// The interning table as a read-only mapping: `id -> 28-byte policy`, resolved
/// without an allocation, which is what lets a row's asset records be handed over
/// borrowed.
pub struct PolicyTable {
    mmap: memmap2::Mmap,
    n: usize,
}

impl PolicyTable {
    pub fn open(dir: &Path) -> Result<PolicyTable> {
        let path = dir.join(POLICY_FILE);
        let mmap = map(&path)?;
        if mmap.len() < 12 {
            bail!("{} is too short to hold a header", path.display());
        }
        let pl = u32::from_le_bytes(mmap[0..4].try_into().expect("4 bytes"));
        if pl != LAYOUT_VERSION {
            bail!(
                "{} is layout_version {pl}; this build reads {LAYOUT_VERSION}",
                path.display()
            );
        }
        let n = u64::from_le_bytes(mmap[4..12].try_into().expect("8 bytes")) as usize;
        if mmap.len() != 12 + 28 * n {
            bail!(
                "{} is {} B; {n} policies need {}",
                path.display(),
                mmap.len(),
                12 + 28 * n
            );
        }
        Ok(PolicyTable { mmap, n })
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn get(&self, id: u32) -> Option<&[u8; 28]> {
        let id = id as usize;
        if id >= self.n {
            return None;
        }
        self.mmap[12 + 28 * id..12 + 28 * id + 28].try_into().ok()
    }

    pub fn ids(&self) -> impl Iterator<Item = &[u8; 28]> {
        let bytes = &self.mmap;
        (0..self.n).map(move |i| {
            bytes[12 + 28 * i..12 + 28 * i + 28]
                .try_into()
                .expect("28 bytes")
        })
    }
}

/// The builder's live set, read back: every `(prefix, row)` a scan of `rows.bin`
/// produced, keyed by the interned policy id the row holds.
///
/// One decision, one entry. A row holding two assets of one policy yields one
/// entry, which is what makes an enumeration return a ref once rather than once
/// per name.
pub fn policy_perms(pairs: &[(u32, u32)], policies: &PolicyTable) -> Result<Vec<Perm>> {
    let mut entries: Vec<Perm> = Vec::with_capacity(pairs.len());
    for (id, row) in pairs {
        let policy = policies
            .get(*id)
            .with_context(|| format!("policy id {id} is not in the table"))?;
        let mut p = [0u8; 8];
        p.copy_from_slice(&policy[0..8]);
        entries.push(Perm {
            prefix: u64::from_le_bytes(p),
            tail: 0,
            row: *row,
        });
    }
    entries.sort_unstable();
    Ok(entries)
}

/// The book's 96-bit key for a live row: 8 bytes of the outref's hash, then 4
/// more mixed with the outref's output index.
///
/// The index has to be in the key. An outref is `(hash, index)`, a transaction's
/// outputs all share its hash, and most transactions have more than one output —
/// so a key taken from the hash alone collapses a whole transaction onto one
/// entry, and the second output silences the first.
pub fn split(h: &[u8; 32], index: u32) -> (u64, u32) {
    let mut p = [0u8; 8];
    p.copy_from_slice(&h[0..8]);
    let mut t = [0u8; 4];
    t.copy_from_slice(&h[8..12]);
    // Mixed rather than added or `xor`ed raw: consecutive indices would
    // otherwise probe adjacent slots — index 0 and index 1 differ by one in the
    // low bit — which is the clustering a linear probe is worst at.
    (u64::from_le_bytes(p), u32::from_le_bytes(t) ^ mix32(index))
}

/// A 32-bit finaliser for the output index. See `split`.
pub fn mix32(mut x: u32) -> u32 {
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^ (x >> 16)
}

/// Position of the key in a build-time table. Not a security boundary: the key is
/// a chain hash and the table is builder-local. FxHash-shaped, as `DECODE_SHAPE`
/// §2.7 measured SipHash to be worth ~3 s over the whole chain.
pub fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// `book.meta` — the artifact describing itself.
///
/// Every field is required, and a missing one is an error rather than a default:
/// the file is written whole, so a field that is absent is a field that was never
/// written, which means the writer and this reader disagree about the format. A
/// change to this set is a change to `layout_version`, like every other file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    pub layout_version: u32,
    pub rows: u64,
    pub created: u64,
    pub spent_hits: u64,
    pub spent_misses: u64,
    /// Σ lovelace over live rows, for the one check that has an outside oracle.
    pub live_lovelace: u128,
    pub live_assets: u64,
    pub rows_bytes: u64,
    pub by_outref_entries: u64,
    pub policies: u64,
    pub compactions: u64,
    pub rehashes: u64,
    pub distinct_prefixes: u64,
    pub by_policy_entries: u64,
    pub max_addr_len: u32,
    pub addrs_over_255: u64,
    pub by_addr_entries: u64,
    pub addr_payment: u64,
    pub addr_stake: u64,
    pub addr_bootstrap: u64,
    pub addr_unkeyed: u64,
    /// The slot of the last block the build applied — where the artifact *is*.
    ///
    /// **The one optional field, and the exception is the point.** Every other field
    /// is required because an absent one means the writer and this reader disagree
    /// about the format. This one is absent for a different reason: a book built
    /// before the field existed is a valid book that does not know where it sits,
    /// and "unknown" has to be a state a reader can see rather than one to guess
    /// past. A plane asked for a `tip` refuses instead of inventing one.
    ///
    /// Its absence is **not** a `layout_version` change: the rows and the three
    /// orderings are the same bytes stamped or unstamped, which is what that version
    /// describes. `fold-probe stamp-boundary` fills it in for an artifact built
    /// before it existed, and a build writes it from the walk.
    pub base_slot: Option<u64>,
}

/// Which base an artifact *is*, for deciding whether something else was built
/// against it.
///
/// # Pairing, never integrity
///
/// A follower's delta is only meaningful against one base: its spend bitmap is
/// indexed by the row's entry in `by_outref` (`Base::outref_entry`), so the same
/// bit means a different output under a different base. Today `Delta` borrows
/// its `&Base` and the borrow checker settles it. **A delta that is written down
/// has no borrow**, and nothing else in the format can tell that a delta and a
/// base belong together — the failure is not loud, it is a live output reported
/// absent, which reads as data rather than corruption.
///
/// So this exists to answer one question: *is this the base that was there when
/// the delta was built?* It is **not** a content hash and must never be used as
/// one — it covers the metadata, not the 2.23 GB of rows and orderings beneath
/// it. Integrity is a publish-time concern over the bytes themselves
/// (`ARTIFACT_SHARDING.md` §8), and it is a different mechanism for a different
/// threat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BaseFingerprint(u64, u64);

impl std::fmt::Display for BaseFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let BaseFingerprint(a, b) = self;
        write!(f, "{a:016x}{b:016x}")
    }
}

impl Meta {
    /// Fields describing **how the builder ran** rather than what it produced,
    /// and therefore excluded from [`Meta::fingerprint`].
    ///
    /// `LIVE_BOOK.md` §Decided proves `rows.bin`, `by_outref.idx`, `dir.idx` and
    /// `policy.tbl` are **byte-identical across runs at 0, 1 and 7
    /// compactions** — the artifact does not depend on how the builder used its
    /// memory. So two identical artifacts can carry different `compactions`
    /// here, and a fingerprint including it would call them different bases.
    /// `spent_hits` / `spent_misses` describe the fold's resolution path for the
    /// same reason.
    const BUILD_PATH_KEYS: [&'static str; 4] =
        ["compactions", "rehashes", "spent_hits", "spent_misses"];

    /// The lines a repair recomputes, and the only ones it is allowed to rewrite.
    pub const DERIVED_KEYS: [&'static str; 6] = [
        "by_policy_entries",
        "by_addr_entries",
        "addr_payment",
        "addr_stake",
        "addr_bootstrap",
        "addr_unkeyed",
    ];

    pub fn parse(text: &str) -> Result<Meta> {
        let field = |k: &str| -> Result<&str> {
            text.lines()
                .find_map(|l| l.strip_prefix(k).and_then(|r| r.strip_prefix(' ')))
                .with_context(|| format!("book.meta has no `{k}`"))
        };
        let num = |k: &str| -> Result<u64> {
            field(k)?
                .trim()
                .parse()
                .with_context(|| format!("book.meta `{k}` is not a number"))
        };
        // The one field allowed to be absent. An absent key is `None`; a key that is
        // present and will not parse is still an error, so a typo cannot read as
        // "unknown".
        let opt_num = |k: &str| -> Result<Option<u64>> {
            match text
                .lines()
                .find_map(|l| l.strip_prefix(k).and_then(|r| r.strip_prefix(' ')))
            {
                None => Ok(None),
                Some(v) => v
                    .trim()
                    .parse()
                    .map(Some)
                    .with_context(|| format!("book.meta `{k}` is not a number")),
            }
        };
        Ok(Meta {
            layout_version: num("layout_version")? as u32,
            rows: num("rows")?,
            created: num("created")?,
            spent_hits: num("spent_hits")?,
            spent_misses: num("spent_misses")?,
            live_lovelace: field("live_lovelace")?
                .trim()
                .parse()
                .context("book.meta `live_lovelace` is not a number")?,
            live_assets: num("live_assets")?,
            rows_bytes: num("rows_bytes")?,
            by_outref_entries: num("by_outref_entries")?,
            policies: num("policies")?,
            compactions: num("compactions")?,
            rehashes: num("rehashes")?,
            distinct_prefixes: num("distinct_prefixes")?,
            by_policy_entries: num("by_policy_entries")?,
            max_addr_len: num("max_addr_len")? as u32,
            addrs_over_255: num("addrs_over_255")?,
            by_addr_entries: num("by_addr_entries")?,
            addr_payment: num("addr_payment")?,
            addr_stake: num("addr_stake")?,
            addr_bootstrap: num("addr_bootstrap")?,
            addr_unkeyed: num("addr_unkeyed")?,
            base_slot: opt_num("base_slot")?,
        })
    }

    pub fn open(dir: &Path) -> Result<Meta> {
        let path = dir.join(META_FILE);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        Meta::parse(&text)
    }

    /// Written last, and swapped in — its presence means every write before it
    /// succeeded, and a reader that maps it sees a whole file or none.
    pub fn write(&self, dir: &Path) -> Result<()> {
        let m = self;
        let mut out = String::new();
        {
            let mut line = |k: &str, v: &dyn std::fmt::Display| {
                out.push_str(&format!("{k} {v}\n"));
            };
            line("layout_version", &m.layout_version);
            line("rows", &m.rows);
            line("created", &m.created);
            line("spent_hits", &m.spent_hits);
            line("spent_misses", &m.spent_misses);
            line("live_lovelace", &m.live_lovelace);
            line("live_assets", &m.live_assets);
            line("rows_bytes", &m.rows_bytes);
            line("by_outref_entries", &m.by_outref_entries);
            line("policies", &m.policies);
            line("compactions", &m.compactions);
            line("rehashes", &m.rehashes);
            line("distinct_prefixes", &m.distinct_prefixes);
            line("by_policy_entries", &m.by_policy_entries);
            line("max_addr_len", &m.max_addr_len);
            line("addrs_over_255", &m.addrs_over_255);
            line("by_addr_entries", &m.by_addr_entries);
            line("addr_payment", &m.addr_payment);
            line("addr_stake", &m.addr_stake);
            line("addr_bootstrap", &m.addr_bootstrap);
            line("addr_unkeyed", &m.addr_unkeyed);
            if let Some(slot) = m.base_slot {
                line("base_slot", &slot);
            }
        }
        let mut tmp = temp_for(dir, META_FILE)?;
        tmp.write_all(out.as_bytes())?;
        tmp.flush()?;
        drop(tmp);
        swap_in(dir, META_FILE)
    }

    /// The lines the fingerprint is taken over — what the artifact **is**.
    ///
    /// Rendered rather than hashed field-by-field so the input is inspectable:
    /// a fingerprint that disagrees can be explained by diffing two of these,
    /// which is the difference between a debuggable mismatch and a mystery.
    ///
    /// Derived from [`Meta::write`]'s own rendering by dropping
    /// [`Meta::DERIVED_KEYS`] (a repair may rewrite them without the rows
    /// changing) and [`Meta::BUILD_PATH_KEYS`] (they record how the build ran).
    /// An unknown `base_slot` is rendered explicitly rather than omitted — a
    /// book that does not know where it sits is a different thing from one at
    /// slot 0, and the fingerprint must not collapse them.
    pub fn identity_text(&self) -> String {
        let mut rendered = String::new();
        {
            let m = self;
            let mut tmp = Vec::new();
            let mut line = |k: &str, v: &dyn std::fmt::Display| {
                tmp.push(format!("{k} {v}"));
            };
            line("layout_version", &m.layout_version);
            line("rows", &m.rows);
            line("created", &m.created);
            line("live_lovelace", &m.live_lovelace);
            line("live_assets", &m.live_assets);
            line("rows_bytes", &m.rows_bytes);
            line("by_outref_entries", &m.by_outref_entries);
            line("policies", &m.policies);
            line("distinct_prefixes", &m.distinct_prefixes);
            line("max_addr_len", &m.max_addr_len);
            line("addrs_over_255", &m.addrs_over_255);
            match m.base_slot {
                Some(slot) => line("base_slot", &slot),
                None => line("base_slot", &"unknown"),
            }
            for l in tmp {
                rendered.push_str(&l);
                rendered.push('\n');
            }
        }
        debug_assert!(
            Self::DERIVED_KEYS
                .iter()
                .chain(Self::BUILD_PATH_KEYS.iter())
                .all(|k| !rendered
                    .lines()
                    .any(|l| l.strip_prefix(k).is_some_and(|r| r.starts_with(' ')))),
            "a repairable or build-path key reached the identity text"
        );
        rendered
    }

    /// A stable identity for this base — see [`BaseFingerprint`].
    pub fn fingerprint(&self) -> BaseFingerprint {
        let text = self.identity_text();
        // Two lanes with different seeds over the same bytes. `mix` is the
        // finaliser the build index already uses; this is a fingerprint for
        // pairing, so a non-cryptographic mixer is the honest choice and keeps
        // this crate's dependency list at three.
        let mut a: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut b: u64 = 0xc2b2_ae3d_27d4_eb4f;
        for byte in text.as_bytes() {
            a = mix(a ^ u64::from(*byte));
            b = mix(b.rotate_left(23) ^ a);
        }
        BaseFingerprint(a, b)
    }

    /// Record where the artifact sits on chain, as a line edit.
    ///
    /// An **observed** field rather than a derived one — it is not in
    /// [`Meta::DERIVED_KEYS`], because no pass over the rows can recover it — but it
    /// is written the same way, because a stamping run is a repair over an artifact
    /// this binary may be older than and must not re-render.
    pub fn stamp_base_slot(dir: &Path, slot: u64) -> Result<()> {
        let path = dir.join(META_FILE);
        let old = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut out: String = old
            .lines()
            .filter(|l| {
                !l.strip_prefix("base_slot")
                    .is_some_and(|r| r.starts_with(' '))
            })
            .map(|l| format!("{l}\n"))
            .collect();
        out.push_str(&format!("base_slot {slot}\n"));
        let mut tmp = temp_for(dir, META_FILE)?;
        tmp.write_all(out.as_bytes())?;
        tmp.flush()?;
        drop(tmp);
        swap_in(dir, META_FILE)
    }

    /// Rewrite only the lines this tool computes, carrying every other line
    /// through verbatim.
    ///
    /// **`counts` must name every key in [`Meta::DERIVED_KEYS`].** The derived set is
    /// *replaced*, not merged: a key that is not handed in has its line dropped, and
    /// the resulting file fails to open (`has no \`addr_payment\``) rather than
    /// reading a stale one. That is the safer of the two failures and it is still a
    /// silent one at the call site, so `reindex` — the only caller — passes all six.
    ///
    /// `book.meta`'s presence means "everything before me passed", and a reindex
    /// is a pass over the same rows — it derives the same counts from the same
    /// bytes and asserts the same permutation identity, so it is entitled to
    /// update them. What it must not do is leave the file claiming an artifact
    /// that no longer exists, or drop a field written by a newer builder that
    /// this binary does not know about. Hence a line edit and not a re-render.
    pub fn rewrite_derived(dir: &Path, counts: &[(&str, u64)]) -> Result<()> {
        let path = dir.join(META_FILE);
        let old = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut out: String = old
            .lines()
            .filter(|l| {
                !Self::DERIVED_KEYS
                    .iter()
                    .any(|k| l.strip_prefix(k).is_some_and(|r| r.starts_with(' ')))
            })
            .map(|l| format!("{l}\n"))
            .collect();
        for (k, v) in counts {
            out.push_str(&format!("{k} {v}\n"));
        }
        let mut tmp = temp_for(dir, META_FILE)?;
        tmp.write_all(out.as_bytes())?;
        tmp.flush()?;
        drop(tmp);
        swap_in(dir, META_FILE)
    }
}

/// The policy ids an ordering's directory addresses: the number of buckets is
/// `2^DIR_BITS`, and a directory file is `8 + 4 × n_buckets` bytes.
pub fn directory_bytes() -> usize {
    8 + (1usize << DIR_BITS) * 4
}

/// Map a file read-only.
///
/// # Safety
///
/// The unsafety is `Mmap::map`'s: the file must not be mutated for the life of
/// the mapping. Everything this crate writes satisfies that by swapping rather
/// than truncating (`swap_in`), and the base is published by `rename`, so a
/// mapping held across a rebuild keeps addressing the inode it opened.
pub fn map(path: &Path) -> Result<memmap2::Mmap> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    // SAFETY: see above — every writer in this crate swaps, never edits in place.
    unsafe { memmap2::Mmap::map(&f) }.with_context(|| format!("mapping {}", path.display()))
}

/// The temporary a writer fills before swapping it into place.
fn temp_for(dir: &Path, name: &str) -> Result<BufWriter<File>> {
    let path = temp_path(dir, name);
    let f = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
    Ok(BufWriter::with_capacity(4 << 20, f))
}

fn temp_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.new"))
}

/// Move a filled temporary onto its published name.
fn swap_in(dir: &Path, name: &str) -> Result<()> {
    let tmp = temp_path(dir, name);
    let dst = dir.join(name);
    std::fs::rename(&tmp, &dst)
        .with_context(|| format!("renaming {} onto {}", tmp.display(), dst.display()))
}

/// Read a whole file, with the path in the error. Used by the verifier and the
/// repair path, which want random access over the bytes rather than a mapping.
pub fn read_file(dir: &Path, name: &str) -> Result<Vec<u8>> {
    let path = dir.join(name);
    let mut f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(buf)
}

/// The fields of a fixed-width, `LAYOUT_VERSION`-stamped array file: the 4-byte
/// version and the 8-byte count every one of them starts with.
pub const ARRAY_HEADER: usize = 12;

/// Read and check a table's 12-byte header, returning `(count, stride)`, where
/// `stride` is 12 for an index table and 28 for the policy table.
pub fn check_array_header(bytes: &[u8], what: &str, stride: usize) -> Result<u64> {
    if bytes.len() < ARRAY_HEADER {
        bail!("{what} is too short to hold a header");
    }
    let v = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
    if v != LAYOUT_VERSION {
        bail!("{what} is layout_version {v}; this build reads {LAYOUT_VERSION}");
    }
    let n = u64::from_le_bytes(bytes[4..12].try_into().expect("8 bytes"));
    let want = ARRAY_HEADER + stride * n as usize;
    if bytes.len() != want {
        bail!("{what} is {} B; {n} entries need {want}", bytes.len());
    }
    Ok(n)
}

/// A `{u64 prefix, u32 row}` entry read out of a mapped table, by index.
pub fn entry_at(bytes: &[u8], i: usize) -> (u64, u32) {
    let b = &bytes[ARRAY_HEADER + 12 * i..];
    (
        u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
        u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
    )
}

/// The bucket boundaries of a directory: `buckets[b]` is the first entry index
/// whose prefix falls in bucket `b`, and the array is a partition (see
/// `write_ordering`).
pub fn bucket_at(bytes: &[u8], b: usize) -> u32 {
    u32::from_le_bytes(bytes[8 + 4 * b..8 + 4 * b + 4].try_into().expect("4 bytes"))
}

/// A directory as a sorted array of entry indices, validated once at open.
///
/// Read `bucket(b)` through the mapping rather than copied into a `Vec<u32>`:
/// the array is 8 MB, it is the hot path of every lookup, and the page cache
/// holds the one or two pages a lookup touches.
pub struct Directory {
    bytes: memmap2::Mmap,
    n_buckets: usize,
}

impl Directory {
    pub fn open(dir: &Path, name: &str) -> Result<Directory> {
        let path = dir.join(name);
        let bytes = map(&path)?;
        if bytes.len() < 8 {
            bail!("{name} is too short to hold a header");
        }
        let v = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
        let dbits = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
        if v != LAYOUT_VERSION || dbits != DIR_BITS {
            bail!("{name} has layout {v} / DIR_BITS {dbits}");
        }
        if bytes.len() != directory_bytes() {
            bail!("{name} is {} B, not {}", bytes.len(), directory_bytes());
        }
        Ok(Directory {
            bytes,
            n_buckets: 1usize << DIR_BITS,
        })
    }

    pub fn bucket(&self, b: usize) -> u32 {
        bucket_at(&self.bytes, b)
    }

    /// Check what `B5` claims: the buckets are monotone, never address past the
    /// end, and a non-empty bucket starts on an entry carrying its top bits.
    ///
    /// Run at open rather than only by `verify`, because every lookup indexes
    /// through this array — a directory that is not a partition would otherwise
    /// be read as a silently wrong range instead of a refusal.
    ///
    /// This **bails on the first violation** and `verify` **counts them**, which
    /// is deliberate rather than duplicated logic: one protects a serving path
    /// and only needs to refuse, the other audits an artifact and wants a number
    /// to report. They share `bucket_at` and nothing else needs to.
    pub fn validate(&self, prefixes: impl Fn(usize) -> u64, entries: u64) -> Result<()> {
        let mut prev = 0u32;
        for b in 0..self.n_buckets {
            let v = self.bucket(b);
            if v < prev || u64::from(v) > entries {
                bail!("directory is not a partition: bucket {b} is {v}, previous {prev}");
            }
            prev = v;
        }
        for b in 0..self.n_buckets {
            let start = self.bucket(b) as usize;
            let end = if b + 1 < self.n_buckets {
                self.bucket(b + 1) as usize
            } else {
                entries as usize
            };
            if start >= end {
                continue;
            }
            if (prefixes(start) >> (64 - DIR_BITS)) as usize != b {
                bail!("directory bucket {b} starts at entry {start}, whose prefix is not in it");
            }
        }
        Ok(())
    }

    /// `[start, end)` for the bucket a prefix falls in.
    pub fn range_for(&self, prefix: u64) -> std::ops::Range<usize> {
        let b = (prefix >> (64 - DIR_BITS)) as usize;
        let start = self.bucket(b) as usize;
        let end = if b + 1 < self.n_buckets {
            self.bucket(b + 1) as usize
        } else {
            usize::MAX
        };
        start..end
    }
}
