//! The delta: everything created after a book's boundary, held in memory.
//!
//! A published [`Base`] is the live set at **one** chunk boundary. A follower at
//! the tip is asked about transactions that landed after it, and those
//! transactions spend outputs that were created after it too — so the book has no
//! row for them and cannot answer. Measured on the ARM box over chunks 1000..1006
//! against a from-genesis base, that is **15.82% of all inputs** the book cannot
//! reach, and at lag 101 it is **57.38%** (`resolve-inputs`).
//!
//! This is the structure that closes it, and it is deliberately **the same shape
//! as the artifact**: a byte-addressed arena of rows in `row`'s encoding, one
//! probe from outref to a creation ordinal. A delta is therefore a small book,
//! and a book is a large delta — the second consumer of *writing* one layout,
//! which is what `docs/design/DELTA_CHAIN.md` argues for.
//!
//! # Three things a delta must hold, and why each is where it is
//!
//! | holds | why |
//! |---|---|
//! | the **rows** created since the boundary | an input is answered with the output it spent, not with a yes |
//! | a **spent bit per creation** | a follower's live set is `base ∪ delta − spent`, and a module that asks for an address must not be handed a spent output |
//! | a **spent bit per base row** | the base is a *snapshot*, so it does not know what the followed chunks spent. One bit per row: **1.36 MB** over mainnet's 10.9 M rows |
//!
//! The base bitmap is why [`Base::outref_entry`] exists: it is a dense id for a
//! base row, and a byte offset into a variable-length arena is not.
//!
//! # What one delta entry costs
//!
//! Measured rather than assumed — `follow` reports both halves. The arena is the
//! row's own length in `row`'s encoding, the probe is `size_of::<([u8;36], u32)>`
//! per bucket plus hashbrown's control byte, accounted the way the book's own
//! `accounted_bytes` accounts its table so the two numbers are comparable.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::{Result, bail};
use live_book::row::{self, Datum, ROW_HEADER, Row, RowRead};
use live_book::{Base, BaseFingerprint};

use crate::{FxHasher, OutRef, outref_key};

/// Where one input was found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    /// In the published book — live at the boundary and not spent before this chunk.
    Base,
    /// Created after the boundary and before this chunk: the delta's own rows.
    Delta,
    /// No transaction in the log ever created it — Byron's genesis distribution,
    /// redeem-style inputs. Not a gap: an archive could not answer it either.
    Uncreated,
}

/// What the delta did, in the units the report wants.
#[derive(Clone, Copy, Default, Debug)]
pub struct Stat {
    pub created: u64,
    pub arena_bytes: u64,
    /// Inputs answered from the base, from the delta, and from neither.
    pub spend_base: u64,
    pub spend_delta: u64,
    pub spend_uncreated: u64,
    /// Base rows and delta rows a followed chunk spent, counted once each.
    pub base_rows_spent: u64,
    pub delta_rows_spent: u64,
    /// Nanoseconds in the delta's own probe, excluding the base lookup it falls
    /// back to. Divided by `spend_delta` this is the marginal cost of having the
    /// row in memory rather than in the book.
    pub lookup_nanos: u64,
    /// Nanoseconds building a delta row: intern, encode, append, probe.
    pub create_nanos: u64,
    /// Policies the base never saw. Their ids continue from the base's own count,
    /// so a delta row can be read with the base's `policy.tbl` and a small tail.
    pub policies_new: u64,
}

/// The live set since a book's boundary.
pub struct Delta<'a> {
    base: &'a Base,
    /// Which base this was built against.
    ///
    /// Redundant *here* — `base` is a borrow, so the compiler already settles
    /// it — and recorded anyway, because the moment a delta is written down the
    /// borrow is gone and nothing else in the format can tell a delta and a base
    /// apart. `base_spent` is indexed by `Base::outref_entry`, so under the wrong
    /// base the same bit means a different output and the symptom is a live
    /// output reported **absent**: data, not corruption.
    ///
    /// Capturing it at construction is what makes that a check a writer can
    /// emit and a reader can enforce, rather than an invariant living in a
    /// design doc. See `MITOS_AT_THE_EDGE.md` §7.4.
    base_fingerprint: BaseFingerprint,
    /// The row arena, byte-addressed, in `row`'s encoding.
    arena: Vec<u8>,
    /// Creation ordinal → the row's offset in `arena`. An ordinal is dense and so
    /// is the bitmap over it; an offset is neither.
    offsets: Vec<u32>,
    /// outref → creation ordinal.
    by_outref: HashMap<[u8; 36], u32, BuildHasherDefault<FxHasher>>,
    /// One bit per creation, set when a later chunk spends it.
    spent: Vec<u64>,
    /// One bit per **base** row, over `Base::outref_entry`.
    base_spent: Vec<u64>,
    /// policy id → policy, extended past the base's own table.
    policies: HashMap<[u8; 28], u32, BuildHasherDefault<FxHasher>>,
    /// The policies behind ids `base.policies().len()..` — the tail a reader of a
    /// delta row needs on top of the base's `policy.tbl`.
    policy_tail: Vec<[u8; 28]>,
    /// The asset-record scratch for [`Delta::create_output`], reused rather than
    /// reallocated per row. One `malloc`/`free` pair per *created output* is the
    /// kind of cost that hides inside a per-row figure measured at ~900 ns.
    body: Vec<u8>,
    stat: Stat,
}

impl<'a> Delta<'a> {
    /// A delta over `base`'s boundary. Allocates the base spent-bitmap up front —
    /// one bit per base row, 1.36 MB over mainnet — so no lookup has to grow it.
    pub fn new(base: &'a Base) -> Result<Delta<'a>> {
        // A reader has to resolve a policy to the id the base interned, and the
        // artifact publishes the table only in the other direction. Rebuilt here
        // and *not* counted as part of the delta: a production delta is written by
        // the process that wrote the base, which holds this map already.
        let mut policies = HashMap::with_capacity_and_hasher(
            base.policies().len(),
            BuildHasherDefault::<FxHasher>::default(),
        );
        for (id, policy) in base.policies().ids().enumerate() {
            policies.insert(*policy, id as u32);
        }
        Ok(Delta {
            base,
            base_fingerprint: base.meta().fingerprint(),
            arena: Vec::new(),
            offsets: Vec::new(),
            by_outref: HashMap::default(),
            spent: Vec::new(),
            base_spent: vec![0u64; (base.rows() as usize).div_ceil(64)],
            policies,
            policy_tail: Vec::new(),
            body: Vec::with_capacity(64),
            stat: Stat::default(),
        })
    }

    /// Which base this delta belongs to.
    ///
    /// **The obligation a writer inherits.** A written delta must carry this,
    /// and whoever reads one back must refuse a base whose
    /// [`Meta::fingerprint`](live_book::Meta::fingerprint) differs — three lines,
    /// and the reason they matter is that the failure without them is silent:
    /// `base_spent` is indexed by `Base::outref_entry`, so under the wrong base
    /// the same bit means a different output and a live output is reported
    /// **absent**. That reads as data rather than corruption, which is why it
    /// has to be checked rather than noticed.
    ///
    /// No such reader exists yet, so no check is written here — the identity is,
    /// so it is available the moment one does. `MITOS_AT_THE_EDGE.md` §7.4.
    pub fn base_fingerprint(&self) -> BaseFingerprint {
        self.base_fingerprint
    }

    /// The bytes the reader-side policy map costs. Reported separately from
    /// [`Delta::accounted`] because it is not the delta's own footprint.
    pub fn policy_map_bytes(&self) -> usize {
        self.policies.capacity() * (std::mem::size_of::<([u8; 28], u32)>() + 1)
    }

    /// The delta's own footprint, accounted the way the book accounts its table
    /// so the two are comparable rather than merely both in bytes.
    pub fn accounted(&self) -> Accounted {
        Accounted {
            arena: self.arena.len(),
            probes: self.by_outref.capacity() * (std::mem::size_of::<([u8; 36], u32)>() + 1),
            spent_bits: self.spent.len() * 8 + self.base_spent.len() * 8,
            policy_tail: self.policy_tail.len() * 28,
        }
    }

    pub fn stat(&self) -> Stat {
        self.stat
    }

    /// Creations still unspent — the half of the live set the book cannot hold.
    pub fn live(&self) -> u64 {
        (self.offsets.len() as u64) - self.stat.delta_rows_spent
    }

    /// The row for an output the delta holds, spent or not.
    ///
    /// Spending does not remove a row: a consumer analysing a transaction that
    /// just landed asks about *its inputs*, and one of them may have been spent by
    /// the very transaction it is looking at.
    pub fn utxo(&self, outref: OutRef) -> Option<Result<Row<'_>, RowRead>> {
        let ordinal = *self.by_outref.get(&outref_key(&outref))?;
        Some(self.row(ordinal))
    }

    /// The row at a creation ordinal.
    pub fn row(&self, ordinal: u32) -> Result<Row<'_>, RowRead> {
        let offset = *self
            .offsets
            .get(ordinal as usize)
            .ok_or(RowRead::OutOfRange)?;
        Row::parse(&self.arena[offset as usize..])
    }

    /// Is this creation still live?
    pub fn is_live(&self, ordinal: u32) -> bool {
        let word = (ordinal / 64) as usize;
        self.spent
            .get(word)
            .is_none_or(|w| w & (1 << (ordinal % 64)) == 0)
    }

    /// Mark one held output spent by a later chunk.
    fn mark_spent(&mut self, ordinal: u32) -> bool {
        let word = (ordinal / 64) as usize;
        let bit = 1u64 << (ordinal % 64);
        if self.spent[word] & bit != 0 {
            return false;
        }
        self.spent[word] |= bit;
        true
    }

    /// Mark one base row spent by a later chunk. `false` if it was already.
    fn mark_base_spent(&mut self, entry: u64) -> bool {
        let word = (entry / 64) as usize;
        let bit = 1u64 << (entry % 64);
        if self.base_spent[word] & bit != 0 {
            return false;
        }
        self.base_spent[word] |= bit;
        true
    }

    /// One spend: where is it, and is it still live?
    ///
    /// The order is the design. The delta is checked **first** because it is a
    /// hash probe over a working set of tens of thousands of entries, while the
    /// base is a directory page plus a binary search over a 10.9 M-row index. An
    /// output created after the boundary cannot be in the base, so asking the
    /// delta first is not just faster — it is the only order that cannot answer
    /// with a row the chain has already moved past.
    pub fn spend(&mut self, outref: OutRef) -> Tier {
        let key = outref_key(&outref);
        let t = std::time::Instant::now();
        let ordinal = self.by_outref.get(&key).copied();
        self.stat.lookup_nanos += t.elapsed().as_nanos() as u64;
        if let Some(ordinal) = ordinal {
            if self.mark_spent(ordinal) {
                self.stat.delta_rows_spent += 1;
            }
            self.stat.spend_delta += 1;
            return Tier::Delta;
        }

        match self
            .base
            .outref_entry(outref.0.as_ref().try_into().expect("32"), outref.1)
        {
            Some(entry) => {
                if self.mark_base_spent(entry) {
                    self.stat.base_rows_spent += 1;
                }
                self.stat.spend_base += 1;
                Tier::Base
            }
            None => {
                self.stat.spend_uncreated += 1;
                Tier::Uncreated
            }
        }
    }

    /// One created output, in the book's own row encoding.
    pub fn create_output<'e>(
        &mut self,
        outref: OutRef,
        era: u8,
        lovelace: u64,
        address: &[u8],
        datum: Datum<'_>,
        assets: impl Iterator<Item = (&'e [u8], &'e [u8], u64)>,
    ) -> Result<()> {
        let t = std::time::Instant::now();
        if address.len() > u16::MAX as usize {
            bail!(
                "address of {} bytes exceeds the row's u16 addr_len",
                address.len()
            );
        }
        if datum.payload().len() > u16::MAX as usize {
            bail!(
                "datum of {} bytes exceeds the row's u16",
                datum.payload().len()
            );
        }

        let base_len = self.base.policies().len();
        // Borrowed field by field rather than as one `&mut self`: the scratch is
        // written while the intern tables are read, which `&mut self` cannot
        // express and which a second pass over the assets would only hide.
        let Delta {
            arena,
            offsets,
            by_outref,
            spent,
            policies,
            policy_tail,
            body,
            stat,
            ..
        } = self;

        body.clear();
        let mut n_assets: u16 = 0;
        for (policy, name, qty) in assets {
            let id = intern(policies, policy_tail, base_len, policy, stat)?;
            row::write_asset_record(body, id, name, qty);
            n_assets = n_assets.saturating_add(1);
        }

        let offset = u32::try_from(arena.len())
            .map_err(|_| anyhow::anyhow!("delta arena passed 4 GiB at offset {}", arena.len()))?;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(outref.0.as_ref());
        let mut head = [0u8; ROW_HEADER];
        row::write_header(
            &mut head,
            (&hash, outref.1),
            era,
            lovelace,
            address.len() as u16,
            n_assets,
            &datum,
        );
        arena.extend_from_slice(&head);
        arena.extend_from_slice(address);
        arena.extend_from_slice(body);
        arena.extend_from_slice(datum.payload());

        let ordinal = offsets.len() as u32;
        offsets.push(offset);
        if spent.len() <= (ordinal / 64) as usize {
            spent.push(0);
        }
        by_outref.insert(outref_key(&outref), ordinal);
        stat.created += 1;
        stat.arena_bytes = arena.len() as u64;
        stat.create_nanos += t.elapsed().as_nanos() as u64;
        Ok(())
    }

    /// The policies a reader needs on top of the base's table, in id order from
    /// `base.policies().len()`.
    pub fn policy_tail(&self) -> &[[u8; 28]] {
        &self.policy_tail
    }
}

/// The id a policy already has, or a new one continuing from the base's own
/// count — so a delta row is readable with the base's `policy.tbl` plus the tail.
fn intern(
    policies: &mut HashMap<[u8; 28], u32, BuildHasherDefault<FxHasher>>,
    tail: &mut Vec<[u8; 28]>,
    base_len: usize,
    policy: &[u8],
    stat: &mut Stat,
) -> Result<u32> {
    if policy.len() != 28 {
        bail!("policy id of {} bytes is not 28", policy.len());
    }
    let mut key = [0u8; 28];
    key.copy_from_slice(policy);
    if let Some(id) = policies.get(&key) {
        return Ok(*id);
    }
    let id = (base_len + tail.len()) as u32;
    policies.insert(key, id);
    tail.push(key);
    stat.policies_new += 1;
    Ok(id)
}

/// The delta's own bytes, in the four places they go.
#[derive(Clone, Copy, Debug)]
pub struct Accounted {
    /// Rows, in the book's encoding.
    pub arena: usize,
    /// outref → ordinal, one bucket per entry plus a control byte.
    pub probes: usize,
    /// Spent bits: one per delta creation and one per base row.
    pub spent_bits: usize,
    /// Policies the base never saw.
    pub policy_tail: usize,
}

impl Accounted {
    pub fn total(&self) -> usize {
        self.arena + self.probes + self.spent_bits + self.policy_tail
    }
}
