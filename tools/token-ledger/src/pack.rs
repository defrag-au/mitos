//! `token-ledger pack` — a token's history as `token-tape` files.
//!
//! Reads the WHOLE archive once, folds it into the story, and writes:
//!
//! - the party table, fixed-width, and its overflow (`parties.bin`,
//!   `parties-overflow.bin`);
//! - one chunk per UTC day per stream (`market/`, `transfers/`, `orders/`);
//! - `meta.bin` (venues and pools) and `index.bin` (what exists).
//!
//! # Numbering is the chain's order
//!
//! A party's number is its place in order of first appearance on chain, from
//! the token's first mint: every address of every movement row, by slot,
//! then the transaction's place in its block, then its hash (rows written
//! before the archive kept the place all read 0), then the address. That is
//! data, not interpretation: the story's rules (what is a fill, what is
//! ambiguous) can change without moving a number. So the same history always
//! gives the same numbers, and a later build only adds numbers after the
//! last. A table already in `--out` is CHECKED against this one, and a
//! difference is an error rather than a quiet renumbering. An archive that
//! does not reach back to the mint is refused: its first appearances would
//! move when it is extended downward.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use policy_archive::owners::Owners;
use policy_archive::story::{Kind, Trader};
use policy_archive::{Completeness, Movement};
use token_tape::{
    Chunk, ChunkInfo, Event, HolderTable, Holding, Identities, Index, Meta, NAME_BYTES, PartyKey,
    PartyTable, Pool, Stream, What,
};

use crate::archive::{
    History, fold_rows, load_manifest, policy_dir, read_observations, venue_roles_for,
};
use crate::names::Rule;

#[derive(clap::Args, Debug)]
pub struct PackArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
    /// Where the files go: `<out>/<policy_hex>/…`. A party table already
    /// there must be the start of this one, or nothing is written.
    #[arg(long, default_value = ".tmp/tape")]
    pub out: PathBuf,
    /// Archive root holding the ADA Handle policy's archive, for naming the
    /// parties. Without one, every party is written unnamed.
    #[arg(long)]
    pub handles_dir: Option<PathBuf>,
    #[arg(long, default_value = crate::names::HANDLE_POLICY)]
    pub handle_policy: String,
    /// A tx-index over the chain, for naming the trader behind a fill on a
    /// SHARED order address from the placement it spent. Without one those
    /// fills stay anonymous.
    #[arg(long, requires = "data_dir")]
    pub tx_index_dir: Option<PathBuf>,
    /// The node database the tx-index reads bodies from (its `immutable/` is
    /// what is opened), as for `serve`.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// The token registry (`tokens.toml`), for the ticker and decimals the
    /// meta carries. Without it, decimals come from the shared table and the
    /// ticker is left to the reader.
    #[arg(long)]
    pub tokens: Option<PathBuf>,
}

/// The largest holders listed in `active.bin` and `opening.bin` whether or
/// not they ever traded: what a holders list shows first.
const TOP_HOLDERS: usize = 250;

/// Parties by identity, numbered in order of first appearance.
struct Parties {
    table: PartyTable,
    ids: HashMap<PartyKey, u32>,
    /// Stake credentials known as scripts, for a bare stake credential the
    /// story gives without saying which kind it is.
    scripts: std::collections::HashSet<[u8; 28]>,
    /// Addresses joined to an owner their stake credential does not show
    /// (`policy_archive::owners`): a Byron wallet's change chain is one party.
    owners: Owners,
    /// The venues, for telling a pool's address from a holder's.
    venues: policy_archive::trade::Roles,
}

impl Parties {
    /// Every party of `movements`, numbered in the chain's order (see the
    /// module's docs), each address taken as the owner `owners` says.
    fn number(
        movements: &[Movement],
        owners: Owners,
        venues: policy_archive::trade::Roles,
    ) -> Self {
        let mut rows: Vec<&Movement> = movements.iter().filter(|m| !m.address.is_empty()).collect();
        rows.sort_by(|a, b| {
            (a.slot, a.tx_index, &a.tx_hash, &a.address)
                .cmp(&(b.slot, b.tx_index, &b.tx_hash, &b.address))
        });
        let mut parties = Self {
            table: PartyTable::default(),
            ids: HashMap::new(),
            scripts: std::collections::HashSet::new(),
            owners,
            venues,
        };
        for m in rows {
            parties.of_address(&m.address);
        }
        parties
    }

    /// Check the table already published at `dir`, if any, is the start of
    /// this one: numbers only ever extend.
    fn check_against(&self, dir: &Path) -> Result<usize> {
        let read = |rel: &str| std::fs::read(dir.join(rel));
        let (table, overflow) = match (
            read(token_tape::HOLDERS_PATH),
            read(token_tape::HOLDERS_OVERFLOW_PATH),
        ) {
            (Ok(t), Ok(o)) => (t, o),
            (Err(e), _) | (_, Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }
            (Err(e), _) | (_, Err(e)) => return Err(e).context("reading the holder table"),
        };
        let before = HolderTable::decode(&table, &overflow).map_err(|e| anyhow::anyhow!("{e}"))?;
        let before: Vec<&PartyKey> = before.identities.iter().map(|i| &i.key).collect();
        let n = before.len();
        let same =
            self.table.keys.len() >= n && before.iter().zip(&self.table.keys).all(|(a, b)| *a == b);
        match same {
            true => Ok(n),
            false => {
                let at = before
                    .iter()
                    .zip(&self.table.keys)
                    .position(|(a, b)| *a != b)
                    .unwrap_or(n.min(self.table.keys.len()));
                bail!(
                    "the published party table ({n} parties) is not the start of this \
                     one ({}): they first differ at party {at}",
                    self.table.keys.len()
                )
            }
        }
    }

    fn id(&mut self, key: PartyKey) -> u32 {
        if let Some(&id) = self.ids.get(&key) {
            return id;
        }
        if let PartyKey::StakeScript(c) = &key {
            self.scripts.insert(*c);
        }
        let id = self.table.keys.len() as u32;
        self.table.keys.push(key.clone());
        self.ids.insert(key, id);
        id
    }

    /// The party behind an address, grouped by its stake credential.
    fn of_address(&mut self, address: &str) -> u32 {
        let address = self.owners.owner(address);
        let key = match self.venue_script(address) {
            Some(_) => address_identity(address),
            None => identity(address),
        };
        self.id(key)
    }

    /// Whether `address` is a pool's or a curve's.
    fn is_pool(&self, address: &str) -> bool {
        use policy_archive::trade::Role;
        matches!(self.venue_script(address), Some(Role::Pool | Role::Curve))
    }

    /// The role of `address` if it is a pool, a curve or an order contract:
    /// its own party, never grouped by the stake it carries.
    ///
    /// A pool's stake credential says nothing about whose tokens it holds.
    /// MEASURED on $NikePig: VyFi's pools carry the stake KEY of VyFi's own
    /// distribution wallet, so the two were one party, marked a pool, and the
    /// wallet's payouts to 608 holders vanished from the replay with the
    /// pool's legs.
    ///
    /// An order contract's stake is often the customer's, and grouped by it
    /// the order's tokens were still the customer's until it filled, while
    /// the replay takes them out at the placement: the two disagreed about
    /// every order open across a page's edge (MEASURED on $NikePig: 4,110 of
    /// 5,536 openings off). Apart, both say the tokens left at the placement.
    /// An order BOOK (Axo, SaturnSwap) stays its owner's: the replay credits
    /// its trades to the owner.
    fn venue_script(&self, address: &str) -> Option<policy_archive::trade::Role> {
        use policy_archive::trade::Role;
        let (cred, stake) = policy_archive::trade::address_parts(address)?;
        self.venues
            .role_at(&cred, stake.as_deref())
            .filter(|r| matches!(r, Role::Pool | Role::Curve | Role::Order))
    }

    fn of_trader(&mut self, t: &Trader) -> Option<u32> {
        match t {
            Trader::Wallet(address) => Some(self.of_address(address)),
            // A bare stake credential, in hex: a script one only if the table
            // has already met it as one.
            Trader::Stake(hex) => {
                let c: [u8; 28] = hex::decode(hex).ok()?.try_into().ok()?;
                Some(match self.scripts.contains(&c) {
                    true => self.id(PartyKey::StakeScript(c)),
                    false => self.id(PartyKey::StakeKey(c)),
                })
            }
            Trader::NotEncodedByVenue | Trader::Unknown => None,
        }
    }
}

/// An address as its own identity, whatever stake it carries: a pool's.
fn address_identity(address: &str) -> PartyKey {
    match address.parse::<pallas_addresses::Address>() {
        Ok(pallas_addresses::Address::Shelley(s)) => PartyKey::Address(s.to_vec()),
        _ => identity(address),
    }
}

/// An address's identity: its stake credential where it has one, else the
/// address itself.
pub(crate) fn identity(address: &str) -> PartyKey {
    use pallas_addresses::{Address, ShelleyDelegationPart, StakePayload};
    let cred = |h: &[u8]| -> [u8; 28] { h.try_into().unwrap_or([0; 28]) };
    match address.parse::<Address>() {
        Ok(Address::Shelley(s)) => match s.delegation() {
            ShelleyDelegationPart::Key(h) => PartyKey::StakeKey(cred(h.as_ref())),
            ShelleyDelegationPart::Script(h) => PartyKey::StakeScript(cred(h.as_ref())),
            _ => PartyKey::Address(s.to_vec()),
        },
        Ok(Address::Stake(s)) => match s.payload() {
            StakePayload::Stake(h) => PartyKey::StakeKey(cred(h.as_ref())),
            StakePayload::Script(h) => PartyKey::StakeScript(cred(h.as_ref())),
        },
        _ => PartyKey::Byron(address.to_string()),
    }
}

pub fn pack(args: PackArgs) -> Result<()> {
    let policy = args.policy.to_lowercase();
    let dir = policy_dir(&args.archive_dir, &policy);
    let Some(manifest) = load_manifest(&dir)? else {
        bail!("no archive at {}", dir.display());
    };
    if manifest.completeness() != Completeness::Complete {
        bail!(
            "{policy}: the archive does not reach back to the first mint, so a party's \
             first appearance is not final yet and its number could move"
        );
    }
    let started = std::time::Instant::now();
    let observations = read_observations(&dir, &manifest)?;
    let roles = venue_roles_for(&observations);
    let Some(mut history) = History::open(&dir)? else {
        bail!("no movements at {}", dir.display());
    };
    let movements = history.all_movements()?.to_vec();
    let rows = movements.len();
    // Numbered from the rows, before any interpretation of them beyond who
    // owns an address (`policy_archive::owners`).
    let feed = fold_rows(movements.clone());
    let owners = Owners::infer(&feed);
    let (joined, late_joins) = (owners.joined(), owners.late_joins());
    let mut parties = Parties::number(&movements, owners, roles.clone());
    let mut story = policy_archive::story::build(&feed, &observations, &roles);
    let read = started.elapsed();
    // A fill on a shared order address names nobody in its own movement; the
    // chain says who placed the order it spent. Every fill, not the story
    // route's capped page: a pack is built once, not while someone waits.
    let mut book_prices: HashMap<usize, u64> = HashMap::new();
    if let (Some(index_dir), Some(data_dir)) = (&args.tx_index_dir, &args.data_dir) {
        let index = tx_index::IndexHandle::open(index_dir, &data_dir.join("immutable"))
            .with_context(|| format!("opening the tx-index at {}", index_dir.display()))?;
        let index = index.get();
        let at = std::time::Instant::now();
        let n = crate::shared_fills::name_shared_fills(
            &mut story.events,
            &policy,
            &roles,
            &mut crate::shared_fills::IndexChain::new(&index),
            usize::MAX,
        );
        println!(
            "shared-order fills: {} named from the chain, {} not, in {:.1}s",
            n.named,
            n.unnamed,
            at.elapsed().as_secs_f64()
        );
        // An order book's fills: what each side paid or got, from the
        // transaction, where no pool reading can say.
        let at = std::time::Instant::now();
        let (prices, n) = crate::book_prices::price_book_fills(
            &story.events,
            &policy,
            &mut crate::shared_fills::IndexChain::new(&index),
        );
        book_prices = prices;
        println!(
            "book fills: {} priced from the chain, {} not, in {:.1}s",
            n.priced,
            n.unpriced,
            at.elapsed().as_secs_f64()
        );
        // `BOOK_TRACE=<n>`: the first n of each book venue's fills as the
        // chain has them, for reading a venue's shape.
        if let Ok(n) = std::env::var("BOOK_TRACE") {
            crate::book_prices::trace_book_fills(
                &story.events,
                &policy,
                &mut crate::shared_fills::IndexChain::new(&index),
                n.parse().unwrap_or(3),
            );
        }
    }

    // The unit the history is of: the one most events are about. A policy
    // of many units is a collection, and not this format's job yet.
    let mut by_unit: HashMap<&[u8], usize> = HashMap::new();
    for e in &story.events {
        *by_unit.entry(e.unit.as_slice()).or_default() += 1;
    }
    let unit: Vec<u8> = by_unit
        .iter()
        .max_by_key(|(u, n)| (**n, !u.is_empty()))
        .map(|(u, _)| u.to_vec())
        .unwrap_or_default();

    let out = args.out.join(&policy);
    let known = parties.check_against(&out)?;
    let numbered = parties.table.keys.len();

    let mut venues: Vec<String> = Vec::new();
    let mut venue = |name: &str| -> u16 {
        match venues.iter().position(|v| v == name) {
            Some(i) => i as u16,
            None => {
                venues.push(name.to_string());
                (venues.len() - 1) as u16
            }
        }
    };
    let mut pools: Vec<Pool> = story
        .pools
        .iter()
        .map(|p| Pool {
            venue: venue(&p.venue),
            address: p.address.clone(),
            key_policy: p.key_policy.clone(),
            key_name: p.key_name.clone(),
            quote_policy: Vec::new(),
            quote_name: Vec::new(),
            curve: false,
        })
        .collect();

    let units = |n: i64| n.unsigned_abs();
    // Transactions that spent the token out of an order contract: a sell in
    // one moved the trader's units at the PLACEMENT, and moves none of
    // theirs here. An order contract is its own party (`venue_script`), so
    // its balance, not the trader's, falls at the fill.
    let order_spent: std::collections::HashSet<&[u8]> = feed
        .iter()
        .filter(|r| {
            r.units.iter().any(|u| {
                u.name == unit
                    && u.parties.iter().any(|p| {
                        p.amount < 0
                            && parties.venue_script(&p.address)
                                == Some(policy_archive::trade::Role::Order)
                    })
            })
        })
        .map(|r| r.tx_hash.as_slice())
        .collect();
    let mut chunks: BTreeMap<(Stream, u32), Vec<Event>> = BTreeMap::new();
    let mut skipped = Skipped::default();
    let mut last_tx: HashMap<Stream, Option<&[u8]>> = HashMap::new();
    let mut seen_tx: std::collections::HashSet<(Stream, &[u8])> = std::collections::HashSet::new();
    let mut split_txs = 0usize;
    // Each transaction's events together, in the order the transactions
    // first appear. The story lists a block's events by kind, so a batch's
    // fills and the pool reading it left came apart with other transactions
    // between them, and a reader grouping by `new_tx` paired neither.
    // MEASURED on $NikePig: 51,842 transactions split, most of the trades
    // without an ADA figure.
    let mut first_seen: HashMap<&[u8], usize> = HashMap::new();
    for (i, e) in story.events.iter().enumerate() {
        first_seen.entry(e.tx.as_slice()).or_insert(i);
    }
    let mut ordered: Vec<(usize, &policy_archive::story::StoryEvent)> =
        story.events.iter().enumerate().collect();
    ordered.sort_by_key(|(_, e)| (e.slot, first_seen[e.tx.as_slice()]));
    for (at, e) in ordered {
        // Pool and curve readings are about the pool, whatever unit row
        // carried them; everything else is about one unit.
        let pool_reading = matches!(e.kind, Kind::PoolState(_) | Kind::CurveState { .. });
        if !pool_reading && e.unit != unit {
            skipped.other_units += 1;
            continue;
        }
        let what = match &e.kind {
            Kind::Fill {
                venue: v,
                party,
                amount,
                into_pool,
            } => What::Fill {
                party: parties.of_trader(party),
                venue: venue(v),
                sell: *into_pool,
                units: units(*amount),
                escrowed: *into_pool && order_spent.contains(e.tx.as_slice()),
                lovelace: book_prices.get(&at).copied(),
            },
            Kind::BatchedFill {
                venue: v,
                orders,
                amount,
            } => What::Batch {
                venue: venue(v),
                orders: *orders as u32,
                units: units(*amount),
            },
            Kind::PoolState(p) => {
                if let Some(pool) = pools.get_mut(p.pool as usize) {
                    pool.quote_policy = p.quote_policy.clone().unwrap_or_default();
                    pool.quote_name = p.quote_name.clone().unwrap_or_default();
                    pool.curve |= p.pricing == "bonding-curve";
                }
                What::Pool {
                    pool: p.pool as u16,
                    base: units(p.base),
                    quote: p.quote.map(units),
                }
            }
            Kind::CurveState {
                pool,
                lovelace,
                tokens_left,
                progress,
            } => {
                if let Some(p) = pools.get_mut(*pool as usize) {
                    p.curve = true;
                }
                What::Curve {
                    pool: *pool as u16,
                    lovelace: units(*lovelace),
                    units_left: units(*tokens_left),
                    progress_ppm: progress.map(|p| (p.clamp(0.0, 1.0) * 1e6) as u32),
                }
            }
            Kind::Transfer { from, to, amount } => {
                let (from, to) = (parties.of_address(from), parties.of_address(to));
                // One owner's addresses (a Byron change chain): nothing
                // changed hands, and a replay has nothing to draw.
                if from == to {
                    skipped.same_owner += 1;
                    continue;
                }
                What::Transfer {
                    from,
                    to,
                    units: units(*amount),
                }
            }
            Kind::Mint { to, amount } => What::Mint {
                to: to.as_deref().map(|a| parties.of_address(a)),
                units: units(*amount),
            },
            Kind::Burn { from, amount } => What::Burn {
                from: from.as_deref().map(|a| parties.of_address(a)),
                units: units(*amount),
            },
            Kind::ArrivedFromBelowFloor { to, amount } => What::Arrived {
                to: parties.of_address(to),
                units: units(*amount),
            },
            Kind::Placement {
                venue: v,
                party,
                amount,
            } => What::Placement {
                party: parties.of_trader(party),
                venue: venue(v),
                units: units(*amount),
            },
            Kind::Cancellation {
                venue: v,
                party,
                amount,
            } => What::Cancellation {
                party: parties.of_trader(party),
                venue: venue(v),
                units: units(*amount),
            },
            // Stated as gaps in the story, not events a replay draws. Counted
            // apart: one is a movement nobody can direct, the other a script
            // no decoder claims, and they are fixed in different places.
            Kind::Ambiguous { .. } => {
                skipped.ambiguous += 1;
                continue;
            }
            // A registered airdropper's vault is known, not a gap: its
            // payouts are already read as distributions.
            Kind::UnclaimedScript { address, .. } => {
                let airdrop = mitos_cohort::payment_cred(address)
                    .is_some_and(|c| crate::policy_api::airdropper(&c).is_some());
                if airdrop {
                    skipped.airdrop += 1;
                } else {
                    skipped.unclaimed += 1;
                }
                continue;
            }
        };
        // A new transaction WITHIN ITS STREAM: a reader of one stream groups
        // its events by this flag, so it must not count another stream's
        // events. Counted across streams, a fill whose transaction began with
        // a placement read as the market's previous transaction, and paired
        // with that transaction's pool reading. MEASURED on $NikePig: 68% of
        // trades left without an ADA figure.
        let stream = what.stream();
        let last = last_tx.entry(stream).or_default();
        let new_tx = *last != Some(e.tx.as_slice());
        *last = Some(e.tx.as_slice());
        if new_tx && !seen_tx.insert((stream, e.tx.as_slice())) {
            split_txs += 1;
        }
        let day = (e.unix / 86_400) as u32;
        chunks.entry((what.stream(), day)).or_default().push(Event {
            slot: e.slot,
            unix: e.unix,
            new_tx,
            what,
        });
    }

    // Written: parties, meta, chunks, then the index last, so a reader
    // never finds an index naming a file that is not there yet.
    let write = |rel: &str, bytes: &[u8]| -> Result<()> {
        let path = out.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))
    };
    // The story should name no one the rows did not; if it ever does, the
    // number it took is after every row's, and still stable.
    if parties.table.keys.len() != numbered {
        eprintln!(
            "warning: the story named {} parties no movement row did",
            parties.table.keys.len() - numbered
        );
    }
    let ids = identity_files(&args, &mut parties, &movements, &unit, &chunks)?;
    // A day that no longer exists must not survive from an earlier build:
    // the index would not name it, but an upload of the directory would.
    for dir in ["market", "transfers", "orders", "checkpoints", "pages"] {
        match std::fs::remove_dir_all(out.join(dir)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(e).with_context(|| format!("clearing {dir}"));
            }
            _ => {}
        }
    }
    let roles = roles_file(&mut parties, &movements, &pools);
    write(token_tape::ROLES_PATH, &roles.encode())?;
    // The pages: what a replay fetches, one request each.
    let grid = token_tape::grid(
        chunks.iter().map(|((_, d), e)| (*d, e.len() as u32)),
        token_tape::PAGE_BUDGET,
    );
    let ends: BTreeSet<u32> = grid.iter().map(|(_, last)| *last).collect();
    let closings: HashMap<u32, Holding> = checkpoints(&mut parties, &movements, &unit, &ends)
        .into_iter()
        .collect();
    let mut page_infos = Vec::with_capacity(grid.len());
    let mut page_bytes = 0usize;
    let mut before: Option<&Holding> = None;
    for &(first, last) in &grid {
        let closing = &closings[&last];
        let page = page_file(first, last, &chunks, before, closing, &roles, &ids.table);
        before = Some(closing);
        let bytes = page.encode();
        page_bytes += bytes.len();
        let digest = token_tape::page_digest(&bytes);
        write(&token_tape::page_path(first, digest), &bytes)?;
        page_infos.push(token_tape::PageInfo {
            first_day: first,
            last_day: last,
            events: page.chunks.iter().map(|c| c.events.len() as u32).sum(),
            bytes: bytes.len() as u32,
            digest,
        });
    }
    write(token_tape::HOLDERS_PATH, &ids.holders)?;
    write(token_tape::HOLDERS_OVERFLOW_PATH, &ids.overflow)?;
    write(token_tape::ACTIVE_PATH, &ids.active)?;
    write(token_tape::OPENING_PATH, &ids.opening)?;
    write(token_tape::HOLDING_PATH, &ids.holding)?;
    // What the token is called and how it scales: the registry's word where
    // it has one, the shared decimals table where not. Neither is on chain.
    let unit_id = format!("{policy}.{}", hex::encode(&unit));
    let entry = match &args.tokens {
        Some(path) => crate::registry::find_by_unit(path, &unit_id)?,
        None => None,
    };
    let decimals = match &entry {
        Some(e) => e.resolved_decimals(),
        None => chain_ledger::tokens::decimals(&unit_id).and_then(|d| u8::try_from(d).ok()),
    };
    let minted: i128 = feed
        .iter()
        .flat_map(|r| &r.units)
        .filter(|u| u.name == unit)
        .map(|u| i128::from(u.net_mint))
        .sum();
    let meta = Meta {
        policy: policy.clone(),
        unit,
        venues,
        pools,
        ticker: entry.map(|e| e.name.to_uppercase()).unwrap_or_default(),
        decimals,
        supply: u64::try_from(minted).ok(),
        // The launch, as the story route marks it: the first curve sighting.
        launch: policy_archive::story::markers(&observations, None, (0, u64::MAX))
            .into_iter()
            .find(|m| m.kind == policy_archive::story::MarkerKind::Launch)
            .map(|m| (m.slot, m.unix)),
    };
    write("meta.bin", &meta.encode())?;
    let mut index = Index {
        parties: parties.table.keys.len() as u32,
        chunks: Vec::new(),
        pages: page_infos,
    };
    for ((stream, day), events) in chunks {
        let info = ChunkInfo {
            stream,
            day,
            first_slot: events.first().map_or(0, |e| e.slot),
            last_slot: events.last().map_or(0, |e| e.slot),
            events: events.len() as u32,
            bytes: 0,
        };
        let bytes = Chunk {
            stream,
            day,
            events,
        }
        .encode();
        write(&token_tape::chunk_path(stream, day), &bytes)?;
        index.chunks.push(ChunkInfo {
            bytes: bytes.len() as u32,
            ..info
        });
    }
    write("index.bin", &index.encode())?;
    // What a replay reads first: the pages alone.
    write(
        token_tape::PAGES_INDEX_PATH,
        &Index {
            parties: index.parties,
            chunks: Vec::new(),
            pages: index.pages.clone(),
        }
        .encode(),
    )?;

    report(&out, &index, rows, known, &skipped, &ids, read, started);
    println!("transactions split across a stream: {split_txs}");
    println!(
        "owners: {joined} addresses joined to another's owner ({late_joins} joins of two \
         existing groups)"
    );
    let largest = index.pages.iter().map(|p| p.bytes).max().unwrap_or(0);
    let newest = index.pages.last().map_or(0, |p| p.bytes);
    println!(
        "roles: {} parties not people; pages: {}, {page_bytes} bytes, largest {largest}, newest \
         {newest}",
        roles.entries.len(),
        index.pages.len()
    );
    Ok(())
}

/// The page of days `first..=last`: every chunk of them, and the people it
/// needs, each with what they held when it closed (`closing`): everyone its
/// events name, everyone whose holding changed since the page `before` it
/// closed, and the largest holders at its close.
///
/// A holding can change with no event to tell it (tokens into an airdrop
/// vault or an unclaimed script output are skipped), and a reader joining
/// pages carries an unlisted holder's older closing forward unchanged: so
/// every holder a page does not list must hold what it held a page earlier.
fn page_file(
    first: u32,
    last: u32,
    chunks: &BTreeMap<(Stream, u32), Vec<Event>>,
    before: Option<&Holding>,
    closing: &Holding,
    roles: &token_tape::Roles,
    table: &HolderTable,
) -> token_tape::Page {
    let page_chunks: Vec<Chunk> = chunks
        .iter()
        .filter(|((_, d), _)| (first..=last).contains(d))
        .map(|((stream, day), events)| Chunk {
            stream: *stream,
            day: *day,
            events: events.clone(),
        })
        .collect();
    let held: HashMap<u32, u64> = closing.balances.iter().copied().collect();
    let mut named: BTreeSet<u32> = page_chunks
        .iter()
        .flat_map(|c| c.events.iter().flat_map(|e| e.what.parties()))
        .collect();
    if let Some(before) = before {
        let was: HashMap<u32, u64> = before.balances.iter().copied().collect();
        named.extend(
            closing
                .balances
                .iter()
                .filter(|(p, b)| was.get(p) != Some(b))
                .map(|(p, _)| *p),
        );
        named.extend(was.keys().filter(|p| !held.contains_key(p)).copied());
    }
    // Largest first already: the holders view's first screen.
    named.extend(
        closing
            .balances
            .iter()
            .filter(|(p, _)| roles.of(*p).is_none())
            .take(token_tape::PAGE_TOP_HOLDERS)
            .map(|(p, _)| *p),
    );
    // Every holder that is not a person: the burn sink, the locks, the
    // staking contracts and the venues are what the supply tiles read, and
    // none of them is a holder the people count includes.
    named.extend(
        closing
            .balances
            .iter()
            .filter(|(p, b)| *b > 0 && roles.of(*p).is_some())
            .take(token_tape::PAGE_TOP_HOLDERS)
            .map(|(p, _)| *p),
    );
    let people = named
        .into_iter()
        .filter_map(|party| {
            let id = table.identities.get(party as usize)?;
            Some(token_tape::Person {
                party,
                key: id.key.clone(),
                name: id.name.clone(),
                role: roles.of(party),
                closing: held.get(&party).copied().unwrap_or(0),
            })
        })
        .collect();
    token_tape::Page {
        first_day: first,
        last_day: last,
        holders_total: closing
            .balances
            .iter()
            .filter(|(p, b)| *b > 0 && roles.of(*p).is_none())
            .count() as u32,
        people,
        chunks: page_chunks,
    }
}

/// Every party that is not a person, by role: pools from the meta's pool
/// addresses, the rest from the address registry, judged on an address the
/// party was seen at.
///
/// The same rules as the arena's `story::role_of`, run once here so a reader
/// needs no registry. "What", never "who": names stay in the identity files.
fn roles_file(
    parties: &mut Parties,
    movements: &[Movement],
    pools: &[token_tape::Pool],
) -> token_tape::Roles {
    use token_tape::Role;
    let mut sample: BTreeMap<u32, &str> = BTreeMap::new();
    // Every pool's address is its own party: the decoded ones the meta
    // lists, and any whose venue the roles name a pool or a curve but whose
    // reserves nothing decoded (a contract registered after the walk).
    let mut pool_parties: BTreeSet<u32> = pools
        .iter()
        .map(|p| parties.of_address(&p.address))
        .collect();
    // An order contract's address is its own party too, and a venue's.
    let mut order_parties: BTreeSet<u32> = BTreeSet::new();
    for m in movements.iter().filter(|m| !m.address.is_empty()) {
        let p = parties.of_address(&m.address);
        sample.entry(p).or_insert(m.address.as_str());
        if parties.is_pool(&m.address) {
            pool_parties.insert(p);
        } else if parties.venue_script(&m.address).is_some() {
            order_parties.insert(p);
        }
    }
    let mut entries: Vec<(u32, Role)> = Vec::new();
    for (party, key) in parties.table.keys.iter().enumerate() {
        let party = party as u32;
        let role = match (
            pool_parties.contains(&party),
            order_parties.contains(&party),
        ) {
            (true, _) => Some(Role::Pool),
            (_, true) => Some(Role::Venue),
            _ => sample.get(&party).and_then(|a| role_of(a, key)),
        };
        if let Some(role) = role {
            entries.push((party, role));
        }
    }
    token_tape::Roles { entries }
}

/// What kind of holder `address` (one of `key`'s) is, or `None` for a
/// person. Mirrors the arena's `story::role_of`; see it for the cases.
fn role_of(address: &str, key: &PartyKey) -> Option<token_tape::Role> {
    use address_registry::{
        AddressCategory as AC, MatchKind, RegistryNetwork, ScriptCategory as SC,
        StakeServiceKind as SK,
    };
    use token_tape::Role;
    let by_class = |cat: &AC| match cat {
        AC::Script(SC::Burn { .. }) => Some(Role::Burn),
        AC::Script(SC::Vesting { .. }) => Some(Role::Lock),
        AC::Script(SC::Staking { .. }) => Some(Role::Staking),
        _ => None,
    };
    // The WHOLE address, stake included: the registered entity's.
    let hit = address_registry::lookup_address_match(address, RegistryNetwork::Mainnet);
    if let Some((cat, MatchKind::Exact)) = hit {
        return match cat {
            AC::Unknown => None,
            AC::Marketplace(_) | AC::Script(_) => by_class(cat).or(Some(Role::Venue)),
        };
    }
    // The SCRIPT only: a sink or a lock whatever stake it carries; a venue's
    // script only when the stake is not a wallet's (a wallet's stake on a
    // venue script is that wallet's listing or order, and still the wallet's).
    let script = match hit {
        Some((cat, MatchKind::VariableStakePrefix)) => Some(cat),
        _ => policy_archive::trade::address_parts(address)
            .and_then(|(cred, _)| address_registry::lookup_payment_credential(&cred))
            .map(|e| &e.category),
    };
    let wallet_stake = matches!(key, PartyKey::StakeKey(_));
    if let Some(cat) = script {
        if let Some(role) = by_class(cat) {
            return Some(role);
        }
        if matches!(cat, AC::Marketplace(_) | AC::Script(_)) && !wallet_stake {
            return Some(Role::Venue);
        }
    }
    if let Some(stake) = stake_address(key)
        && let Some(service) = address_registry::lookup_stake(&stake)
    {
        return Some(match service.kind {
            SK::Exchange | SK::Marketplace | SK::MintingProvider => Role::Venue,
            SK::Staking => Role::Staking,
        });
    }
    let script_payment = mitos_cohort::is_script_address(address);
    (script_payment && matches!(key, PartyKey::Address(_))).then_some(Role::Contract)
}

/// A stake-keyed party's stake address (`stake1…`), as the registry keys its
/// services.
fn stake_address(key: &PartyKey) -> Option<String> {
    use pallas_addresses::{Network, StakeAddress, StakePayload};
    let payload = match key {
        PartyKey::StakeKey(h) => StakePayload::Stake((*h).into()),
        PartyKey::StakeScript(h) => StakePayload::Script((*h).into()),
        PartyKey::Address(_) | PartyKey::Byron(_) => return None,
    };
    StakeAddress::new(Network::Mainnet, payload)
        .to_bech32()
        .ok()
}

/// Every party's balance at the end of each of `days` (UTC days since the
/// epoch), largest first: the checkpoints a page of whole days settles on.
fn checkpoints(
    parties: &mut Parties,
    movements: &[Movement],
    unit: &[u8],
    days: &BTreeSet<u32>,
) -> Vec<(u32, Holding)> {
    let mut rows: Vec<&Movement> = movements
        .iter()
        .filter(|m| m.unit_name == unit && !m.address.is_empty())
        .collect();
    rows.sort_by_key(|m| (m.block_time, m.slot));
    let mut balance: HashMap<u32, i128> = HashMap::new();
    let mut rows = rows.into_iter().peekable();
    let mut out = Vec::with_capacity(days.len());
    for &day in days {
        let end = (u64::from(day) + 1) * 86_400;
        while let Some(m) = rows.next_if(|m| m.block_time < end) {
            *balance.entry(parties.of_address(&m.address)).or_default() += i128::from(m.amount);
        }
        let mut balances: Vec<(u32, u64)> = balance
            .iter()
            .filter(|(_, b)| **b > 0)
            .map(|(p, b)| (*p, u64::try_from(*b).unwrap_or(u64::MAX)))
            .collect();
        balances.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        out.push((day, Holding { balances }));
    }
    out
}

/// The identity files' bytes, and what went into them.
struct IdentityFiles {
    holders: Vec<u8>,
    overflow: Vec<u8>,
    active: Vec<u8>,
    opening: Vec<u8>,
    holding: Vec<u8>,
    named: usize,
    handle_wallets: usize,
    too_long: usize,
    active_parties: usize,
    opening_parties: usize,
    holder_count: usize,
    /// Every party's key and handle, for the pages to name their people.
    table: HolderTable,
}

/// Who the parties are: every party's key and handle (`holders.bin`), the
/// active ones (everyone in the market and order streams, and the largest
/// holders), the first screen's (the largest holders and the latest day's
/// traders), and every current holder by balance.
fn identity_files(
    args: &PackArgs,
    parties: &mut Parties,
    movements: &[Movement],
    unit: &[u8],
    chunks: &BTreeMap<(Stream, u32), Vec<Event>>,
) -> Result<IdentityFiles> {
    // Every party's balance of the unit now.
    let mut balance: HashMap<u32, i128> = HashMap::new();
    for m in movements
        .iter()
        .filter(|m| m.unit_name == unit && !m.address.is_empty())
    {
        *balance.entry(parties.of_address(&m.address)).or_default() += i128::from(m.amount);
    }
    let mut holding: Vec<(u32, u64)> = balance
        .into_iter()
        .filter(|(_, b)| *b > 0)
        .map(|(p, b)| (p, u64::try_from(b).unwrap_or(u64::MAX)))
        .collect();
    holding.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let top: Vec<u32> = holding.iter().take(TOP_HOLDERS).map(|(p, _)| *p).collect();

    // Handles, from the Handle policy's archive where one is given.
    let handles = match &args.handles_dir {
        Some(dir) => Some(crate::names::fold_handles(dir, &args.handle_policy)?),
        None => None,
    };
    let mut too_long = 0;
    let names: Vec<Option<String>> = parties
        .table
        .keys
        .iter()
        .map(|k| {
            let name = handles.as_ref()?.name(k, Rule::ShortestOldestMint)?;
            match name.len() <= NAME_BYTES {
                true => Some(name.to_string()),
                false => {
                    too_long += 1;
                    None
                }
            }
        })
        .collect();
    let named = names.iter().flatten().count();
    let table = HolderTable::new(&parties.table, &names);
    let (holders, overflow) = table.encode().map_err(|e| anyhow::anyhow!("{e}"))?;

    let subset = |ids: &BTreeSet<u32>| -> Result<Vec<u8>> {
        Identities {
            entries: ids
                .iter()
                .map(|p| (*p, table.identities[*p as usize].clone()))
                .collect(),
        }
        .encode()
        .map_err(|e| anyhow::anyhow!("{e}"))
    };
    let mut active: BTreeSet<u32> = chunks
        .iter()
        .filter(|((s, _), _)| matches!(s, Stream::Market | Stream::Orders))
        .flat_map(|(_, events)| events.iter().flat_map(|e| e.what.parties()))
        .collect();
    active.extend(top.iter().copied());
    let mut opening: BTreeSet<u32> = top.iter().copied().collect();
    if let Some(day) = chunks
        .keys()
        .filter(|(s, _)| *s == Stream::Market)
        .map(|(_, d)| *d)
        .max()
    {
        opening.extend(
            chunks[&(Stream::Market, day)]
                .iter()
                .flat_map(|e| e.what.parties()),
        );
    }
    Ok(IdentityFiles {
        active: subset(&active)?,
        opening: subset(&opening)?,
        holding: Holding {
            balances: holding.clone(),
        }
        .encode(),
        holders,
        overflow,
        named,
        handle_wallets: handles.as_ref().map_or(0, |h| h.wallets()),
        too_long,
        active_parties: active.len(),
        opening_parties: opening.len(),
        holder_count: holding.len(),
        table,
    })
}

/// What the story held that the tape does not draw.
#[derive(Default)]
struct Skipped {
    /// Movements the feed could not direct and the trade fold could not read
    /// (`token-ledger ambiguous` says what they are).
    ambiguous: usize,
    /// Script outputs no decoder and no registered site claims.
    unclaimed: usize,
    /// Outputs at a registered airdropper's vault: known, not a gap.
    airdrop: usize,
    /// Transfers between addresses of one owner (a Byron change chain).
    same_owner: usize,
    /// Events about another unit under the same policy.
    other_units: usize,
}

#[allow(clippy::too_many_arguments)]
fn report(
    out: &Path,
    index: &Index,
    rows: usize,
    known: usize,
    skipped: &Skipped,
    ids: &IdentityFiles,
    read: std::time::Duration,
    started: std::time::Instant,
) {
    println!(
        "{}: {rows} movements read and folded in {:.1}s, packed in {:.1}s",
        out.display(),
        read.as_secs_f64(),
        started.elapsed().as_secs_f64()
    );
    println!(
        "parties: {} ({} beyond the published table); {} named from {} handle wallets ({} \
         handles too long for the field); {} hold the token now",
        index.parties,
        index.parties as usize - known,
        ids.named,
        ids.handle_wallets,
        ids.too_long,
        ids.holder_count
    );
    println!(
        "  holders.bin {} + overflow {} bytes; active.bin {} parties, {} bytes; opening.bin {} \
         parties, {} bytes; holding.bin {} bytes",
        ids.holders.len(),
        ids.overflow.len(),
        ids.active_parties,
        ids.active.len(),
        ids.opening_parties,
        ids.opening.len(),
        ids.holding.len()
    );
    for stream in Stream::ALL {
        let of: Vec<&ChunkInfo> = index.chunks.iter().filter(|c| c.stream == stream).collect();
        let events: u64 = of.iter().map(|c| u64::from(c.events)).sum();
        let bytes: u64 = of.iter().map(|c| u64::from(c.bytes)).sum();
        let largest = of.iter().max_by_key(|c| c.bytes);
        println!(
            "{:>9}: {} days, {events} events, {bytes} bytes ({:.1} per event); largest day {}",
            stream.name(),
            of.len(),
            bytes as f64 / events.max(1) as f64,
            largest.map_or("-".to_string(), |c| format!(
                "{} ({} events, {} bytes)",
                c.day, c.events, c.bytes
            )),
        );
    }
    let Skipped {
        ambiguous,
        unclaimed,
        airdrop,
        same_owner,
        other_units,
    } = skipped;
    println!(
        "skipped: {ambiguous} ambiguous movements, {unclaimed} unclaimed script outputs, \
         {airdrop} at airdrop vaults, {same_owner} within one owner, {other_units} of other \
         units"
    );
}
