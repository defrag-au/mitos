//! The firehose's model: what the stream has carried, bounded so a long walk cannot
//! grow the screen's memory.
//!
//! Every method that depends on time takes the clock as an argument, so the model is
//! tested without a terminal and a capture is a pure function of the same events.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use chain_events::{ChainEvent, Coverage, Direction, EventBody, HolderKind, TokenRef, direction};

/// Distinct blocks the sparkline shows. A Cardano block is about a second, so this is
/// roughly the last two minutes of chain activity.
const BLOCKS: usize = 120;

/// How often the wall-clock rate is recomputed.
const RATE_WINDOW: Duration = Duration::from_millis(500);

/// One line of the feed, flattened for the renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Where in the chain, when the source says.
    pub slot: Option<u64>,
    /// Chain time, unix seconds.
    pub time: u64,
    /// The body's kind, as the vocabulary spells it.
    pub kind: String,
    /// Units moved, for a holder family.
    pub quantity: Option<u64>,
    /// The asset's decoded name, or a short hex id.
    pub asset: String,
    /// Who moved it, already flattened to one line.
    pub movement: String,
    /// Why this row is worth a second look, if it is.
    pub notable: Option<&'static str>,
}

/// Events by family, with the holder family split by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Supply created.
    pub mint: u64,
    /// Ownership moved.
    pub transfer: u64,
    /// Supply destroyed.
    pub burn: u64,
    /// Marketplace transitions.
    pub market: u64,
    /// Metadata changes.
    pub asset: u64,
    /// Contract changes.
    pub admin: u64,
}

/// Slots per sparkline bucket. A Cardano slot is about a second, so 1,000 slots is
/// roughly a quarter of an hour: fine enough to show a mint burst, coarse enough that a
/// sparse stream still has a shape.
const SLOT_BUCKET: u64 = 1_000;

/// Which way the stream runs through the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Oldest block first — the live walk.
    Ascending,
    /// Newest block first — the archive pages newest-first.
    Descending,
}

/// Events per slot bucket, with the quiet gaps kept as zeroes so a burst after a lull
/// reads as a burst rather than as continuous activity.
#[derive(Debug, Clone)]
pub struct Blocks {
    counts: VecDeque<u64>,
    current: Option<u64>,
    order: Option<Order>,
    capacity: usize,
    /// Rows that stepped against the stream's own direction. A producer should not do
    /// this; if it does, the count is reported rather than the row hidden.
    pub out_of_order: u64,
}

impl Blocks {
    fn new(capacity: usize) -> Self {
        Self {
            counts: VecDeque::with_capacity(capacity),
            current: None,
            order: None,
            capacity,
            out_of_order: 0,
        }
    }

    fn observe(&mut self, slot: Option<u64>) {
        let Some(slot) = slot.map(|slot| slot / SLOT_BUCKET) else {
            return;
        };

        let Some(current) = self.current else {
            self.current = Some(slot);
            self.counts.push_back(1);
            return;
        };

        if slot == current {
            if let Some(back) = self.counts.back_mut() {
                *back += 1;
            }
            return;
        }

        // The first two distinct blocks decide the direction. After that, a step the other
        // way is a row out of order, not a change of direction — otherwise one late row
        // would turn every later row into a reversal.
        let ascending = slot > current;
        match self.order {
            None => {
                self.order = Some(if ascending {
                    Order::Ascending
                } else {
                    Order::Descending
                });
            }
            Some(Order::Ascending) if !ascending => {
                self.out_of_order += 1;
                if let Some(back) = self.counts.back_mut() {
                    *back += 1;
                }
                return;
            }
            Some(Order::Descending) if ascending => {
                self.out_of_order += 1;
                if let Some(back) = self.counts.back_mut() {
                    *back += 1;
                }
                return;
            }
            Some(_) => {}
        }

        // A descending stream's later arrivals are older. Once the window holds the newest
        // chain buckets, the ones that follow are off its far edge — the same rule the
        // feed uses — so they are dropped rather than sliding the window backwards.
        if self.order == Some(Order::Descending) && self.counts.len() >= self.capacity {
            return;
        }

        // One bucket per block, capped at the window. A gap wider than the window is the
        // same as a window of silence: the older history is already off screen.
        let gap = if ascending {
            slot - current
        } else {
            current - slot
        };
        let gap = gap.min(self.capacity as u64) as usize;
        for _ in 1..gap {
            self.counts.push_back(0);
        }
        self.counts.push_back(1);
        self.current = Some(slot);
        while self.counts.len() > self.capacity {
            self.counts.pop_front();
        }
    }

    /// In the stream's own chain direction, oldest first when ascending.
    pub fn counts(&self) -> Vec<u64> {
        self.counts.iter().copied().collect()
    }

    /// Which way the stream has been running, once two distinct buckets have arrived.
    pub fn order(&self) -> Option<Order> {
        self.order
    }
}

/// The stream as the screen sees it.
pub struct App {
    /// Where the events came from: a path, or `stdin`.
    pub source: String,
    /// Newest chain first, bounded to `capacity`.
    ///
    /// A stream that runs newest-first arrives with its newest rows first, so once the
    /// feed is full the later arrivals are older than everything kept and are dropped —
    /// the renderer reverses that deque so the newest chain row is at the top either way.
    pub feed: VecDeque<Row>,
    /// How many rows the feed keeps.
    pub capacity: usize,
    /// Events by family.
    pub counts: Counts,
    /// Net delta per account, so the biggest movers are a sort rather than a second pass.
    pub accounts: HashMap<String, i64>,
    /// Chain activity, per block.
    pub blocks: Blocks,
    /// Events consumed.
    pub lines: u64,
    /// Lines that were not a [`ChainEvent`].
    pub malformed: u64,
    /// Rows with a [`Row::notable`] reason.
    pub notable: u64,
    /// The last notable row, for the footer.
    pub notable_last: Option<String>,
    /// The last malformed line, for the footer.
    pub malformed_last: Option<String>,
    /// The last coverage any event claimed.
    pub coverage: Option<Coverage>,
    /// The collection the stream is about.
    pub collection: Option<String>,
    /// The oldest slot seen.
    pub min_slot: Option<u64>,
    /// The newest slot seen.
    pub max_slot: Option<u64>,
    /// Units moved, summed over every holder event.
    pub moved: u64,
    /// Events per second over the last [`RATE_WINDOW`], or the whole run once closed.
    pub rate: f64,
    /// While paused, the feed holds still but the counters keep counting.
    pub paused: bool,
    /// The producer closed the stream.
    pub closed: bool,
    /// How the walk ended, when this viewer started the walk itself.
    pub exit: Option<String>,
    /// The user asked to leave.
    pub quit: bool,
    since_rate: u64,
    rate_at: Instant,
    started: Instant,
}

impl App {
    /// An empty screen for a stream from `source`.
    pub fn new(source: impl Into<String>, capacity: usize) -> Self {
        Self {
            source: source.into(),
            feed: VecDeque::with_capacity(capacity.min(1_024)),
            capacity,
            counts: Counts::default(),
            accounts: HashMap::new(),
            blocks: Blocks::new(BLOCKS),
            lines: 0,
            malformed: 0,
            notable: 0,
            notable_last: None,
            malformed_last: None,
            coverage: None,
            collection: None,
            min_slot: None,
            max_slot: None,
            moved: 0,
            rate: 0.0,
            paused: false,
            closed: false,
            exit: None,
            quit: false,
            since_rate: 0,
            rate_at: Instant::now(),
            started: Instant::now(),
        }
    }

    /// Folds one event into the screen.
    pub fn push(&mut self, event: ChainEvent) {
        self.lines += 1;
        self.since_rate += 1;
        self.coverage = Some(event.coverage);
        self.collection
            .get_or_insert_with(|| event.collection.reference().to_owned());
        if let Some(slot) = event.coord.map(|coord| coord.height) {
            self.min_slot = Some(self.min_slot.map_or(slot, |seen| seen.min(slot)));
            self.max_slot = Some(self.max_slot.map_or(slot, |seen| seen.max(slot)));
        }

        match &event.body {
            EventBody::Holder(holder) => {
                match holder.kind {
                    HolderKind::Mint => self.counts.mint += 1,
                    HolderKind::Transfer => self.counts.transfer += 1,
                    HolderKind::Burn => self.counts.burn += 1,
                }
                self.moved += holder.quantity();
                for party in &holder.parties {
                    *self
                        .accounts
                        .entry(party.account.address.clone())
                        .or_default() += party.delta;
                }
            }
            EventBody::Market(_) => self.counts.market += 1,
            EventBody::Asset(_) => self.counts.asset += 1,
            EventBody::Admin(_) => self.counts.admin += 1,
        }

        let row = Row::of(&event);
        self.blocks.observe(row.slot);
        if let Some(reason) = row.notable {
            self.notable += 1;
            self.notable_last = Some(format!("{} · {reason}", row.kind));
        }

        if !self.paused {
            let descending = self.blocks.order() == Some(Order::Descending);
            if descending && self.feed.len() >= self.capacity {
                // The feed already holds the newest chain rows; this one is older.
            } else {
                self.feed.push_front(row);
                while self.feed.len() > self.capacity {
                    self.feed.pop_back();
                }
            }
        }
    }

    /// Records a line that was not an event. A bad line is a fact about the producer, not
    /// a reason to stop reading the stream.
    pub fn malformed(&mut self, line: String) {
        self.malformed += 1;
        self.malformed_last = Some(truncate(&line, 120));
    }

    /// Recomputes the ingest rate. Once the producer has closed, the rate is the whole
    /// run's average and stays there.
    pub fn tick(&mut self, now: Instant) {
        if self.closed {
            return;
        }
        let elapsed = now.saturating_duration_since(self.rate_at);
        if elapsed >= RATE_WINDOW {
            self.rate = self.since_rate as f64 / elapsed.as_secs_f64();
            self.since_rate = 0;
            self.rate_at = now;
        }
    }

    /// Marks the producer gone and freezes the rate at the run's average.
    pub fn close(&mut self) {
        self.closed = true;
        let elapsed = self.started.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.rate = self.lines as f64 / elapsed;
        }
    }

    /// Accounts by absolute net movement, largest first.
    pub fn top_movers(&self, n: usize) -> Vec<(&str, i64)> {
        let mut movers: Vec<(&str, i64)> = self
            .accounts
            .iter()
            .map(|(account, delta)| (account.as_str(), *delta))
            .collect();
        movers.sort_by(|a, b| b.1.abs().cmp(&a.1.abs()).then_with(|| a.0.cmp(b.0)));
        movers.truncate(n);
        movers
    }

    /// Events per slot bucket, in the stream's own chain direction.
    pub fn block_counts(&self) -> Vec<u64> {
        self.blocks.counts()
    }

    /// `complete`, `partial`, or `—` before the first event.
    pub fn coverage_label(&self) -> &'static str {
        match self.coverage {
            Some(Coverage::Complete) => "complete",
            Some(Coverage::Partial) => "partial",
            None => "—",
        }
    }

    /// The slot range the stream covers, low to high however it arrived.
    pub fn slot_range(&self) -> String {
        match (self.min_slot, self.max_slot) {
            (Some(first), Some(last)) => format!("{first}–{last}"),
            _ => "—".to_owned(),
        }
    }
}

impl Row {
    fn of(event: &ChainEvent) -> Self {
        let (quantity, movement, notable) = match &event.body {
            EventBody::Holder(holder) => {
                let movement = match direction(holder) {
                    Direction::Transfer { from, to } => {
                        format!("{} -> {}", short(&from.address), short(&to.address))
                    }
                    Direction::Mint { to } => format!("-> {}", short(&to.address)),
                    Direction::Burn { from } => format!("{} -> burn", short(&from.address)),
                    Direction::SourceBelowFloor { to } => {
                        format!("below floor -> {}", short(&to.address))
                    }
                    Direction::Ambiguous => format!("{} parties", holder.parties.len()),
                };
                let notable = if holder.parties.is_empty() {
                    Some("unattributed")
                } else if holder.parties.len() > 2 {
                    Some("split")
                } else if holder.quantity() > 1 {
                    Some("batch")
                } else {
                    None
                };
                (Some(holder.quantity()), movement, notable)
            }
            EventBody::Market(market) => {
                let movement = match market.order() {
                    Some(order) => format!("order {}", short(&order.id)),
                    None => "order unknown".to_owned(),
                };
                (None, movement, None)
            }
            _ => (None, "—".to_owned(), None),
        };

        Self {
            slot: event.coord.map(|coord| coord.height),
            time: event.occurred_at,
            kind: event.body.kind().to_owned(),
            quantity,
            asset: event
                .asset
                .as_ref()
                .map(asset_name)
                .unwrap_or_else(|| "collection".to_owned()),
            movement,
            notable,
        }
    }
}

/// An address or id with its middle removed, so two parties fit on one line.
pub fn short(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= 14 {
        return text.to_owned();
    }
    let head: String = chars[..6].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}..{tail}")
}

/// A line shortened for a footer, with an explicit ellipsis rather than a silent cut.
fn truncate(text: &str, width: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= width {
        return text.to_owned();
    }
    let mut cut: String = chars[..width].iter().collect();
    cut.push('…');
    cut
}

/// An asset's name, decoded from the CAIP-19 token id when it is text.
fn asset_name(asset: &TokenRef) -> String {
    let Some(id) = asset.token_id() else {
        return "—".to_owned();
    };
    match hex::decode(id) {
        Ok(bytes)
            if !bytes.is_empty()
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_graphic() || *byte == b' ') =>
        {
            String::from_utf8_lossy(&bytes).into_owned()
        }
        _ => short(id),
    }
}

/// Events built with the real types, for the model and screen tests.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;
    use chain_events::{
        AccountRef, AssetType, ChainCoord, ChainRef, EventSource, HolderEvent, PartyDelta, TxRef,
    };
    use std::str::FromStr;

    const POLICY: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";

    fn chain() -> ChainRef {
        ChainRef::from_str("cardano:mainnet").unwrap()
    }

    fn collection() -> AssetType {
        AssetType::new(chain(), "cnt", POLICY).unwrap()
    }

    pub(crate) fn holder(kind: HolderKind, parties: &[(&str, i64)], slot: u64) -> ChainEvent {
        let body = EventBody::Holder(HolderEvent {
            kind,
            parties: parties
                .iter()
                .map(|(address, delta)| PartyDelta {
                    account: AccountRef::new(chain(), *address),
                    delta: *delta,
                })
                .collect(),
        });
        let discriminator = match &body {
            EventBody::Holder(event) => event.discriminator(),
            _ => unreachable!(),
        };
        ChainEvent::draft(
            chain(),
            collection(),
            body,
            1_700_000_000,
            EventSource::PolicyArchive,
        )
        .asset(TokenRef::new(collection(), Some("5065727031393639")).unwrap())
        .at(ChainCoord::new(slot))
        .tx(TxRef::new("ab".repeat(32), 0))
        .coverage(Coverage::Complete)
        .build(&discriminator, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::holder;
    use super::*;

    #[test]
    fn a_mint_counts_and_names_the_recipient() {
        let mut app = App::new("test", 10);
        app.push(holder(HolderKind::Mint, &[("addr1vminter", 1)], 100));

        assert_eq!(app.counts.mint, 1);
        assert_eq!(app.accounts.len(), 1);
        assert_eq!(app.top_movers(1), vec![("addr1vminter", 1)]);
        assert_eq!(app.feed[0].asset, "Perp1969");
        assert_eq!(app.feed[0].quantity, Some(1));
        assert!(app.feed[0].movement.starts_with("-> "));
    }

    #[test]
    fn a_fee_split_is_notable_rather_than_guessed() {
        let mut app = App::new("test", 10);
        app.push(holder(
            HolderKind::Transfer,
            &[
                ("addr1vseller", -10),
                ("addr1vroyalty", 7),
                ("addr1vmarket", 3),
            ],
            100,
        ));

        assert_eq!(app.notable, 1);
        assert_eq!(app.feed[0].notable, Some("split"));
        assert_eq!(app.feed[0].movement, "3 parties");
    }

    #[test]
    fn a_batch_of_more_than_one_unit_is_notable() {
        let mut app = App::new("test", 10);
        app.push(holder(
            HolderKind::Transfer,
            &[("addr1va", -3), ("addr1vb", 3)],
            100,
        ));

        assert_eq!(app.feed[0].notable, Some("batch"));
        assert_eq!(app.moved, 3);
    }

    #[test]
    fn the_feed_is_bounded_and_newest_first() {
        let mut app = App::new("test", 2);
        for slot in 100..105 {
            app.push(holder(HolderKind::Mint, &[("addr1vminter", 1)], slot));
        }

        assert_eq!(app.feed.len(), 2);
        assert_eq!(app.feed[0].slot, Some(104));
        assert_eq!(app.feed[1].slot, Some(103));
        assert_eq!(app.lines, 5);
    }

    #[test]
    fn a_bucket_gap_is_kept_as_empty_buckets() {
        let mut app = App::new("test", 10);
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 2_100));

        assert_eq!(app.block_counts(), vec![2, 0, 1]);
        assert_eq!(app.blocks.order(), Some(Order::Ascending));
        assert_eq!(app.blocks.out_of_order, 0);
    }

    #[test]
    fn a_newest_first_stream_is_the_other_direction_not_an_error() {
        // The archive pages newest-first; the walk runs oldest-first. Both are orderly.
        let mut app = App::new("test", 10);
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 2_100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 2_100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));

        assert_eq!(app.block_counts(), vec![2, 0, 1]);
        assert_eq!(app.blocks.order(), Some(Order::Descending));
        assert_eq!(app.blocks.out_of_order, 0);
        // The range is low-to-high however the rows arrived.
        assert_eq!(app.slot_range(), "100–2100");
    }

    #[test]
    fn a_newest_first_stream_keeps_its_newest_rows() {
        let mut app = App::new("test", 2);
        // A descending stream arrives with the newest chain events first, so the rows
        // that arrive after the feed is full are the ones that can be dropped.
        for slot in [3_000_u64, 2_000, 1_000] {
            app.push(holder(HolderKind::Mint, &[("addr1va", 1)], slot));
        }

        let slots: Vec<Option<u64>> = app.feed.iter().map(|row| row.slot).collect();
        assert_eq!(slots, vec![Some(2_000), Some(3_000)]);
    }

    #[test]
    fn a_newest_first_window_keeps_its_newest_buckets() {
        let mut app = App::new("test", 10);
        // The newest bucket twice, then 129 older ones, with room for 120.
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 130_000));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 130_000));
        for bucket in (1_u64..130).rev() {
            app.push(holder(HolderKind::Mint, &[("addr1va", 1)], bucket * 1_000));
        }

        let counts = app.block_counts();
        assert_eq!(counts.len(), 120);
        // Kept from the newest end: the doubled bucket is still the first one.
        assert_eq!(counts.first(), Some(&2));
    }

    #[test]
    fn a_step_against_the_stream_is_counted_once() {
        let mut app = App::new("test", 10);
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 1_100));
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));

        assert_eq!(app.blocks.order(), Some(Order::Ascending));
        assert_eq!(app.blocks.out_of_order, 1);
    }

    #[test]
    fn a_paused_feed_still_counts() {
        let mut app = App::new("test", 10);
        app.paused = true;
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));

        assert!(app.feed.is_empty());
        assert_eq!(app.counts.mint, 1);
        assert_eq!(app.lines, 1);
    }

    #[test]
    fn a_malformed_line_is_counted_rather_than_fatal() {
        let mut app = App::new("test", 10);
        app.malformed("{not json".to_owned());

        assert_eq!(app.malformed, 1);
        assert!(app.malformed_last.as_deref().unwrap().contains("not json"));
        assert_eq!(app.lines, 0);
    }

    #[test]
    fn the_rate_is_events_over_the_elapsed_window() {
        let start = Instant::now();
        let mut app = App::new("test", 10);
        app.rate_at = start;
        for _ in 0..10 {
            app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));
        }
        app.tick(start + Duration::from_secs(1));

        assert!((app.rate - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn the_shortest_addresses_survive_whole() {
        assert_eq!(short("addr1va"), "addr1va");
        assert_eq!(short("addr1qykwu5cs0dzqz42vpqyx70q0e4"), "addr1q..q0e4");
    }
}
