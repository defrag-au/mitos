//! Chain order, the watermark that bounds the merge, and duplicate resolution.
//!
//! The whole tool is these decisions, kept pure so they can be tested without a file or a
//! clock. A [`Feed`] is anything that yields events in ascending chain order; [`merge`]
//! pulls one event per feed at a time and releases what no feed can still contradict.

use std::collections::{BTreeMap, HashMap, VecDeque};

use anyhow::Result;
use chain_events::{ChainEvent, EventId, Family};

/// Where an event sits in chain order.
///
/// `(slot, tx index, event index)` is the chain's own ordering, and it is the reason the
/// producers carry a position at all: two facts in one block cannot be put back in order
/// without it.
///
/// `family` and `id` are TIE-BREAKS, not chain order. A buyer and a seller of the same
/// asset in the same transaction are two facts at one position — they still need an
/// order, and it has to be the same one on every run and whatever order the sources were
/// named in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    /// A fact the source could not place in a block sorts LAST: it cannot be claimed to
    /// have happened before one. Marketplace messages are this case — OpenSea reports a
    /// hash and a wall clock, never a block.
    ///
    /// Named for the ABSENCE (`false` = placed) so the derived `Ord` puts them last;
    /// `placed: true` would sort them first.
    pub unplaced: bool,
    pub slot: u64,
    pub tx_index: u32,
    pub event_index: u32,
    pub family: u8,
    pub id: EventId,
}

impl Key {
    pub fn of(event: &ChainEvent) -> Self {
        let (unplaced, slot) = order_of(event);
        // Zero is "the source did not say" for both positions, so a source that states
        // one sorts ahead of a source that does not — which is what makes a merged
        // stream no worse than its best-informed producer.
        let (tx_index, event_index) = match &event.tx {
            Some(tx) => (tx.index, tx.event_index),
            None => (0, 0),
        };
        Self {
            unplaced,
            slot,
            tx_index,
            event_index,
            family: rank(event.body.family()),
            id: event.id,
        }
    }
}

/// The part of a position a feed must be monotonic in: the block, and nothing finer.
///
/// A producer emits a block's events in its own fold order, not in the merge's tiebreak
/// order, and the merge sorts inside a block anyway — every event in a block is held until
/// the watermark passes it. Only the block has to ascend, because only the block is what
/// lets the merge release a slot.
pub fn order_of(event: &ChainEvent) -> (bool, u64) {
    match &event.coord {
        Some(coord) => (false, coord.height),
        None => (true, 0),
    }
}

/// A fixed order between families at one chain position. Arbitrary but stable — that is
/// the whole requirement.
fn rank(family: Family) -> u8 {
    match family {
        Family::Holder => 0,
        Family::Asset => 1,
        Family::Market => 2,
        Family::Admin => 3,
    }
}

/// A source that yields its events in ascending chain order.
///
/// Ascending is the contract because it is what makes the merge bounded: a feed that has
/// been read to key K can never produce anything below K again. A descending source has
/// to be turned into one — the archive producer pages newest-first — and that costs its
/// whole length in memory, which is why the report names it.
pub trait Feed {
    fn next_event(&mut self) -> Result<Option<ChainEvent>>;

    fn label(&self) -> String {
        "in-memory".to_owned()
    }

    fn malformed(&self) -> u64 {
        0
    }

    /// Whether this feed had to be buffered to become ascending.
    fn buffered(&self) -> bool {
        false
    }

    /// How many events this feed has served, for the report.
    fn read(&self) -> u64 {
        0
    }
}

/// What the merge did, for the report on stderr.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub read: u64,
    pub kept: u64,
    pub filtered: u64,
    pub duplicates: u64,
    pub superseded: u64,
    pub conflicts: u64,
    pub unplaced: u64,
    /// The most events ever held at once, waiting for the slowest feed.
    ///
    /// This is the merge's real memory cost: one block's worth when the feeds keep pace,
    /// and the whole gap between them when one lags.
    pub peak_held: u64,
    /// The first and last slot written, for the report.
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
}

/// Whether a collection survives the filter. No filter keeps everything.
pub fn keep(event: &ChainEvent, policies: &[String]) -> bool {
    policies.is_empty()
        || policies
            .iter()
            .any(|policy| event.collection.reference() == policy)
}

/// Merges every feed into chain order, one row per fact, emitting as it goes.
///
/// # The watermark
///
/// Every feed is ascending, so each feed's current head is the smallest thing it can
/// still produce, and the minimum across feeds is a floor on every future event. Anything
/// strictly below that slot is final: no feed can contradict it, so it is released and
/// dropped from memory.
///
/// It has to be the SLOT that passes, not the key. Two feeds can disagree about where in
/// the block a fact sits, and a copy with a larger position would arrive after a copy with
/// a smaller one had already been written — the slot is the smallest unit every feed
/// necessarily agrees on.
///
/// A fact with no block cannot be bounded by a slot at all, so it waits for the end.
pub fn merge(
    feeds: &mut [Box<dyn Feed>],
    policies: &[String],
    emit: &mut dyn FnMut(ChainEvent) -> Result<()>,
) -> Result<Stats> {
    let mut stats = Stats::default();
    let mut heads: Vec<Option<ChainEvent>> = Vec::with_capacity(feeds.len());
    for feed in feeds.iter_mut() {
        heads.push(feed.next_event()?);
    }

    // Events waiting for the watermark, in chain order.
    let mut pending: BTreeMap<Key, ChainEvent> = BTreeMap::new();
    // Which key each held id is at, so a copy can be found without scanning.
    let mut held: HashMap<EventId, Key> = HashMap::new();
    // Facts already written, with the slot they were written at, so a copy arriving
    // after that slot closed is caught rather than written twice. Pruned as the
    // watermark moves, so it never grows with the stream.
    let mut written: VecDeque<(u64, EventId)> = VecDeque::new();

    loop {
        // Take one event from every feed, so the watermark reflects where each one is
        // NOW rather than where it was last time round.
        for (index, feed) in feeds.iter_mut().enumerate() {
            let Some(event) = heads[index].take() else {
                continue;
            };
            heads[index] = feed.next_event()?;
            stats.read += 1;

            if !keep(&event, policies) {
                stats.filtered += 1;
                continue;
            }

            let key = Key::of(&event);
            if let Some(existing) = held.get(&event.id).copied() {
                stats.duplicates += 1;
                resolve(&mut pending, &mut held, event, key, existing, &mut stats);
                continue;
            }
            if written.iter().any(|(_, id)| *id == event.id) {
                // The same fact again after its slot closed. One source disagrees about
                // where it happened: a producer fault, not a second event.
                stats.duplicates += 1;
                stats.conflicts += 1;
                continue;
            }
            held.insert(event.id, key);
            pending.insert(key, event);
        }

        stats.peak_held = stats.peak_held.max(pending.len() as u64);

        let exhausted = heads.iter().all(Option::is_none);
        let watermark = heads
            .iter()
            .flatten()
            .filter_map(|event| event.coord.as_ref().map(|coord| coord.height))
            .min();

        while let Some((&key, _)) = pending.first_key_value() {
            let release = if exhausted {
                // Nothing is left to contradict anything.
                true
            } else if key.unplaced {
                false
            } else {
                // No placed head left means no more placed facts, so everything placed
                // is safe even though the feeds are not exhausted.
                watermark.is_none_or(|slot| key.slot < slot)
            };
            if !release {
                break;
            }

            let event = pending.remove(&key).expect("peeked above");
            held.remove(&event.id);
            written.push_back((key.slot, event.id));
            stats.kept += 1;
            if key.unplaced {
                stats.unplaced += 1;
            } else {
                stats.first_slot.get_or_insert(key.slot);
                stats.last_slot = Some(key.slot);
            }
            emit(event)?;
        }

        if let Some(slot) = watermark {
            while written
                .front()
                .is_some_and(|(written_slot, _)| *written_slot < slot)
            {
                written.pop_front();
            }
        }

        if exhausted && pending.is_empty() {
            return Ok(stats);
        }
    }
}

/// Keeps the better copy of a fact already held.
fn resolve(
    pending: &mut BTreeMap<Key, ChainEvent>,
    held: &mut HashMap<EventId, Key>,
    event: ChainEvent,
    key: Key,
    existing: Key,
    stats: &mut Stats,
) {
    let Some(kept) = pending.get(&existing) else {
        // The id names something already written and since pruned; `held` and `pending`
        // cannot disagree, so this is unreachable rather than a case to handle.
        debug_assert!(false, "held id with no pending row");
        return;
    };

    if event.revision > kept.revision {
        stats.superseded += 1;
        pending.remove(&existing);
        held.insert(event.id, key);
        pending.insert(key, event);
        return;
    }

    if event.revision == kept.revision && key != existing {
        stats.conflicts += 1;
        if better_placed(&key, &existing) {
            pending.remove(&existing);
            held.insert(event.id, key);
            pending.insert(key, event);
        }
    }
}

/// Whether `new` should replace `old` when both describe the same fact.
///
/// MORE INFORMED wins: a stated block beats an unplaced row, and a stated transaction
/// position beats a zero. When they are equally informed the keys must differ in a
/// tiebreak only, so the lower key wins and the choice is a function of the facts rather
/// than of the order the sources were named in.
fn better_placed(new: &Key, old: &Key) -> bool {
    let informed = |key: &Key| (!key.unplaced, key.tx_index, key.event_index);
    match informed(new).cmp(&informed(old)) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => new < old,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_events::{
        AccountRef, AssetType, ChainCoord, ChainRef, Coverage, EventBody, EventSource, HolderEvent,
        HolderKind, PartyDelta, TxRef,
    };
    use std::str::FromStr;

    const PERPS: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";
    const PERP: &str = "d9d59e71eb99e653a493956a6826a406060706bd8c9a2427b3564f64";

    fn chain() -> ChainRef {
        ChainRef::from_str("cardano:mainnet").unwrap()
    }

    fn collection(policy: &str) -> AssetType {
        AssetType::new(chain(), "cnt", policy).unwrap()
    }

    /// One mint, placed where the caller says. A different `who` is a different fact and
    /// therefore a different id; the same `who` at a different position is the same fact.
    fn mint(policy: &str, who: &str, slot: u64, tx_index: u32) -> ChainEvent {
        let body = EventBody::Holder(HolderEvent {
            kind: HolderKind::Mint,
            parties: vec![PartyDelta {
                account: AccountRef::new(chain(), who),
                delta: 1,
            }],
        });
        let discriminator = match &body {
            EventBody::Holder(event) => event.discriminator(),
            _ => unreachable!(),
        };
        ChainEvent::draft(
            chain(),
            collection(policy),
            body,
            1_700_000_000,
            EventSource::PolicyArchive,
        )
        .at(ChainCoord::new(slot))
        .tx(TxRef::new("ab".repeat(32), tx_index))
        .coverage(Coverage::Complete)
        .build(&discriminator, 1_700_000_000)
    }

    struct VecFeed(VecDeque<ChainEvent>);

    impl Feed for VecFeed {
        fn next_event(&mut self) -> Result<Option<ChainEvent>> {
            Ok(self.0.pop_front())
        }
    }

    fn run(feeds: Vec<Vec<ChainEvent>>) -> (Vec<ChainEvent>, Stats) {
        run_filtered(feeds, &[])
    }

    fn run_filtered(feeds: Vec<Vec<ChainEvent>>, policies: &[String]) -> (Vec<ChainEvent>, Stats) {
        let mut boxed: Vec<Box<dyn Feed>> = feeds
            .into_iter()
            .map(|events| Box::new(VecFeed(events.into())) as Box<dyn Feed>)
            .collect();
        let mut out = Vec::new();
        let stats = merge(&mut boxed, policies, &mut |event| {
            out.push(event);
            Ok(())
        })
        .unwrap();
        (out, stats)
    }

    #[test]
    fn chain_order_is_slot_then_transaction() {
        let a = mint(PERPS, "addr1a", 100, 0);
        let b = mint(PERPS, "addr1b", 100, 7);
        let c = mint(PERPS, "addr1c", 101, 0);

        assert!(
            Key::of(&a) < Key::of(&b),
            "the later transaction sorts later"
        );
        assert!(Key::of(&b) < Key::of(&c), "slot outranks position");
    }

    #[test]
    fn a_fact_with_no_block_sorts_last() {
        let placed = mint(PERPS, "addr1a", 100, 0);
        let mut unplaced = mint(PERPS, "addr1b", 100, 0);
        unplaced.coord = None;

        assert!(Key::of(&placed) < Key::of(&unplaced));
    }

    #[test]
    fn families_tie_break_in_a_fixed_order() {
        assert!(rank(Family::Holder) < rank(Family::Asset));
        assert!(rank(Family::Asset) < rank(Family::Market));
        assert!(rank(Family::Market) < rank(Family::Admin));
    }

    #[test]
    fn the_same_fact_from_two_sources_is_one_row() {
        let a = mint(PERPS, "addr1a", 100, 0);
        let b = mint(PERPS, "addr1a", 100, 0);
        assert_eq!(a.id, b.id, "the fixture really is one fact");

        let (merged, stats) = run(vec![vec![a], vec![b]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(stats.duplicates, 1);
        assert_eq!(stats.conflicts, 0);
    }

    #[test]
    fn a_correction_supersedes_the_row_it_corrects() {
        let original = mint(PERPS, "addr1a", 100, 0);
        let mut correction = mint(PERPS, "addr1a", 100, 0);
        correction.revision = 1;

        let (merged, stats) = run(vec![vec![original, correction]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].revision, 1);
        assert_eq!(stats.superseded, 1);
        assert_eq!(stats.duplicates, 1);
    }

    #[test]
    fn a_position_disagreement_is_counted_and_does_not_depend_on_argument_order() {
        // One fact, two positions: a producer that knows the transaction's place in its
        // block and one that says zero. The id is the same either way.
        let stated = mint(PERPS, "addr1a", 100, 7);
        let unknown = mint(PERPS, "addr1a", 100, 0);
        assert_eq!(stated.id, unknown.id);

        let (first, stats) = run(vec![vec![stated.clone()], vec![unknown.clone()]]);
        let (second, reversed) = run(vec![vec![unknown], vec![stated]]);

        assert_eq!(stats.conflicts, 1);
        assert_eq!(reversed.conflicts, 1);
        // The informed copy wins in both orders, so the answer is a function of the
        // facts rather than of how the command was typed.
        assert_eq!(Key::of(&first[0]).tx_index, 7);
        assert_eq!(Key::of(&first[0]), Key::of(&second[0]));
    }

    #[test]
    fn the_filter_keeps_only_the_named_collections() {
        let nft = mint(PERPS, "addr1a", 100, 0);
        let token = mint(PERP, "addr1b", 100, 1);

        let (all, _) = run(vec![vec![nft.clone(), token.clone()]]);
        assert_eq!(all.len(), 2);

        let (only_nft, stats) = run_filtered(vec![vec![nft, token]], &[PERPS.to_owned()]);
        assert_eq!(only_nft.len(), 1);
        assert_eq!(only_nft[0].collection.reference(), PERPS);
        assert_eq!(stats.filtered, 1);
    }

    #[test]
    fn two_interleaved_sources_come_back_in_chain_order() {
        let one = vec![mint(PERPS, "addr1a", 100, 0), mint(PERPS, "addr1c", 102, 0)];
        let two = vec![mint(PERPS, "addr1b", 100, 9), mint(PERPS, "addr1d", 101, 4)];

        let (merged, stats) = run(vec![one, two]);
        let slots: Vec<_> = merged.iter().map(|e| Key::of(e).slot).collect();
        assert_eq!(slots, vec![100, 100, 101, 102]);
        assert_eq!(merged.len(), 4);
        assert_eq!(stats.duplicates, 0);
    }

    #[test]
    fn three_sources_merge_the_same_way_two_do() {
        let sources: Vec<Vec<ChainEvent>> = (0..3)
            .map(|source| {
                (0..3)
                    .map(|n| mint(PERPS, &format!("addr1s{source}n{n}"), 100 + n, source))
                    .collect()
            })
            .collect();

        let (merged, stats) = run(sources);
        assert_eq!(merged.len(), 9);
        assert_eq!(stats.duplicates, 0);

        let keys: Vec<_> = merged.iter().map(Key::of).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "output is in chain order");
    }

    #[test]
    fn a_slot_is_held_until_every_feed_has_passed_it() {
        // One feed runs on ahead. Until the slow one leaves slot 100, nothing from 100
        // may be released — the slow feed could still be holding a copy of a fact the
        // fast one already wrote.
        let fast = vec![mint(PERPS, "addr1a", 100, 0), mint(PERPS, "addr1b", 200, 0)];
        let slow = vec![mint(PERPS, "addr1c", 100, 5)];

        let (merged, stats) = run(vec![fast, slow]);
        let slots: Vec<_> = merged.iter().map(|e| Key::of(e).slot).collect();
        assert_eq!(slots, vec![100, 100, 200]);
        // At most the two events in slot 100 were ever held at once.
        assert_eq!(stats.peak_held, 2);
    }

    #[test]
    fn a_fact_with_no_block_waits_for_the_end() {
        let mut unplaced = mint(PERPS, "addr1b", 100, 0);
        unplaced.coord = None;
        let placed = mint(PERPS, "addr1a", 900, 0);

        let (merged, stats) = run(vec![vec![placed, unplaced]]);
        let last = merged.last().expect("two events");
        assert!(last.coord.is_none(), "the unplaced fact is written last");
        assert_eq!(stats.unplaced, 1);
    }

    #[test]
    fn a_copy_arriving_after_its_slot_closed_is_not_written_twice() {
        // The second feed reports the same fact at a LATER slot. By the time it arrives,
        // the first copy's slot has closed, so it is caught by the written window rather
        // than emitted again.
        let first = mint(PERPS, "addr1a", 100, 0);
        let mut late = mint(PERPS, "addr1b", 100, 0);
        late.id = first.id;
        late.coord = Some(ChainCoord::new(300));

        let (merged, stats) = run(vec![vec![first], vec![late]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(stats.conflicts, 1);
    }
}
