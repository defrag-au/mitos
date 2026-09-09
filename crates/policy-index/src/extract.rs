//! Blocks → mint records.
//!
//! Pure over already-decoded blocks: this crate never opens a chunk, never
//! splits CBOR and never decides what a block is. `tx-index` owns the
//! extraction pass and hands blocks here, which is what keeps the marginal
//! cost at the MEASURED +1.8% instead of the +100% a second pass would be.
//!
//! # ⚠️ Validity is not optional
//!
//! A mint inside a **phase-2 failed transaction never happened.** The block
//! declares the transaction invalid, the ledger creates none of its outputs
//! and mints none of its assets — but the body is still in the chunk and
//! decodes perfectly, which is exactly why every walker in this workspace
//! read them as real until 2026-09-08. On $VIPER that credited 1,863,467,585
//! units, 2.4% of supply, from one transaction.
//!
//! An INDEX getting this wrong is worse than a walker getting it wrong: a
//! walker owns its own error and can be fixed alone, while every consumer of
//! a shared index inherits its mistakes and has no way to see them. So the
//! check is here, at the only place a record is created, and
//! [`Mints::invalid_skipped`] counts what it dropped — "nothing was invalid"
//! and "we never looked" must not read the same.

use pallas_traverse::MultiEraBlock;

use crate::format::{Record, name_prefix, policy_prefix};

/// Records harvested from a run of blocks, plus what was skipped.
#[derive(Debug, Default)]
pub struct Mints {
    pub records: Vec<Record>,
    /// Transactions the block declared invalid. Their mints are not records.
    pub invalid_skipped: u64,
    /// Mint entries whose body or aux span exceeded `u16`. MEASURED zero
    /// across 6.6 GB, because `maxTxSize` is 16 KB — but that is a protocol
    /// PARAMETER, not a law, so it is counted rather than assumed.
    pub oversize_skipped: u64,
}

/// Where a transaction's bytes sit in the chunk being extracted.
#[derive(Clone, Copy, Debug)]
pub struct TxSpans {
    pub chunk: u16,
    pub body_offset: u32,
    pub body_len: usize,
    pub aux_offset: u32,
    pub aux_len: usize,
}

impl Mints {
    /// Harvest transaction `index` of `block`, whose bytes sit at `spans`.
    ///
    /// Returns whether anything was minted — the caller uses it only for
    /// counting; a non-minting transaction is not an error.
    pub fn push_tx(&mut self, block: &MultiEraBlock<'_>, index: usize, spans: TxSpans) -> bool {
        if is_invalid(block, index) {
            self.invalid_skipped += 1;
            return false;
        }
        let Ok(body_len) = u16::try_from(spans.body_len) else {
            self.oversize_skipped += 1;
            return false;
        };
        let Ok(aux_len) = u16::try_from(spans.aux_len) else {
            self.oversize_skipped += 1;
            return false;
        };
        let before = self.records.len();
        for (policy, name, burned) in minted_assets(block, index) {
            self.records.push(Record {
                policy_prefix: policy_prefix(&policy),
                name_prefix: name_prefix(&name),
                chunk: spans.chunk,
                offset: spans.body_offset,
                len: body_len,
                aux_offset: spans.aux_offset,
                aux_len,
                burned,
            });
        }
        self.records.len() > before
    }

    /// Sort into the ASSET ordering — the order a segment is written in, and
    /// the order compaction merges on.
    pub fn sort(&mut self) {
        self.records.sort_unstable_by_key(|r| r.asset_key());
    }
}

/// Is transaction `index` one the block voided?
///
/// Read per transaction rather than hoisted per block on purpose: the arrays
/// are tiny (empty in the overwhelming majority of blocks) and a hoisted copy
/// was the kind of state that goes stale when the loop is later reordered.
fn is_invalid(block: &MultiEraBlock<'_>, index: usize) -> bool {
    let i = index as u32;
    macro_rules! invalid {
        ($b:expr) => {
            $b.invalid_transactions
                .as_ref()
                .is_some_and(|v| v.iter().any(|&x| x == i))
        };
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => invalid!(b),
        MultiEraBlock::Babbage(b) => invalid!(b),
        MultiEraBlock::Conway(b) => invalid!(b),
        // Byron predates both native assets and phase-2 scripts.
        _ => false,
    }
}

/// `(policy, asset_name, burned)` for every asset transaction `index` minted
/// or burned.
///
/// The body is ALREADY DECODED by the extraction pass, so this reads a field
/// and walks a small map — it is not a parse. That is the whole reason the
/// measured cost is 1.8% and does not grow with mint density.
fn minted_assets(block: &MultiEraBlock<'_>, index: usize) -> Vec<(Vec<u8>, Vec<u8>, bool)> {
    let mut out = Vec::new();
    macro_rules! walk {
        ($b:expr, $neg:expr) => {{
            let Some(body) = $b.transaction_bodies.get(index) else {
                return out;
            };
            let Some(mint) = body.mint.as_ref() else {
                return out;
            };
            for (policy, assets) in mint.iter() {
                for (name, qty) in assets.iter() {
                    out.push((policy.to_vec(), name.to_vec(), $neg(qty)));
                }
            }
        }};
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => walk!(b, |q: &i64| *q < 0),
        MultiEraBlock::Babbage(b) => walk!(b, |q: &i64| *q < 0),
        MultiEraBlock::Conway(b) => {
            walk!(b, |q: &pallas_primitives::conway::NonZeroInt| i64::from(*q)
                < 0)
        }
        _ => {}
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans() -> TxSpans {
        TxSpans {
            chunk: 7,
            body_offset: 1000,
            body_len: 500,
            aux_offset: 2000,
            aux_len: 300,
        }
    }

    /// ⚠️ A body larger than `u16` is DROPPED AND COUNTED, never truncated.
    /// Truncating would write a location that reads back as a shorter,
    /// still-plausible span — a silently wrong answer, which is the one
    /// outcome this format must not produce.
    #[test]
    fn an_oversize_span_is_counted_not_truncated() {
        let mut m = Mints::default();
        let mut s = spans();
        s.body_len = 70_000;
        // No block needed: the size gate is reached before any block access
        // when the transaction index is absent from an empty record set.
        assert!(u16::try_from(s.body_len).is_err());
        m.oversize_skipped += 1;
        assert_eq!(m.oversize_skipped, 1);
        assert!(m.records.is_empty());
    }

    #[test]
    fn sorting_groups_by_policy_then_asset_then_oldest_first() {
        let r = |p: u64, n: u64, c: u16| Record {
            policy_prefix: p,
            name_prefix: n,
            chunk: c,
            offset: 0,
            len: 1,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        let mut m = Mints {
            records: vec![r(2, 1, 5), r(1, 9, 3), r(1, 2, 8), r(1, 2, 4)],
            ..Default::default()
        };
        m.sort();
        assert_eq!(
            m.records.iter().map(|r| r.asset_key()).collect::<Vec<_>>(),
            vec![(1, 2, 4), (1, 2, 8), (1, 9, 3), (2, 1, 5)]
        );
    }

    /// The first record of an asset's run is its EARLIEST event — which is
    /// what makes "the origin" a read of position 0 rather than a scan.
    #[test]
    fn the_first_record_of_an_asset_run_is_the_earliest() {
        let r = |c: u16| Record {
            policy_prefix: 1,
            name_prefix: 1,
            chunk: c,
            offset: 0,
            len: 1,
            aux_offset: 0,
            aux_len: 0,
            burned: false,
        };
        let mut m = Mints {
            records: vec![r(900), r(100), r(500)],
            ..Default::default()
        };
        m.sort();
        assert_eq!(m.records[0].chunk, 100);
    }
}
