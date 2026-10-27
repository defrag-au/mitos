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
}

impl Parties {
    /// Every party of `movements`, numbered in the chain's order (see the
    /// module's docs).
    fn number(movements: &[Movement]) -> Self {
        let mut rows: Vec<&Movement> = movements.iter().filter(|m| !m.address.is_empty()).collect();
        rows.sort_by(|a, b| {
            (a.slot, a.tx_index, &a.tx_hash, &a.address)
                .cmp(&(b.slot, b.tx_index, &b.tx_hash, &b.address))
        });
        let mut parties = Self {
            table: PartyTable::default(),
            ids: HashMap::new(),
            scripts: std::collections::HashSet::new(),
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
        self.id(identity(address))
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
    // Numbered from the rows, before any interpretation of them.
    let mut parties = Parties::number(&movements);
    let story = policy_archive::story::build(&fold_rows(movements.clone()), &observations, &roles);
    let read = started.elapsed();

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
    let mut chunks: BTreeMap<(Stream, u32), Vec<Event>> = BTreeMap::new();
    let mut skipped = Skipped::default();
    let mut last_tx: Option<&[u8]> = None;
    for e in &story.events {
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
            Kind::Transfer { from, to, amount } => What::Transfer {
                from: parties.of_address(from),
                to: parties.of_address(to),
                units: units(*amount),
            },
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
            Kind::UnclaimedScript { .. } => {
                skipped.unclaimed += 1;
                continue;
            }
        };
        let new_tx = last_tx != Some(e.tx.as_slice());
        last_tx = Some(e.tx.as_slice());
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
    write(token_tape::HOLDERS_PATH, &ids.holders)?;
    write(token_tape::HOLDERS_OVERFLOW_PATH, &ids.overflow)?;
    write(token_tape::ACTIVE_PATH, &ids.active)?;
    write(token_tape::OPENING_PATH, &ids.opening)?;
    write(token_tape::HOLDING_PATH, &ids.holding)?;
    let meta = Meta {
        policy: policy.clone(),
        unit,
        venues,
        pools,
    };
    write("meta.bin", &meta.encode())?;
    let mut index = Index {
        parties: parties.table.keys.len() as u32,
        chunks: Vec::new(),
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

    report(&out, &index, rows, known, &skipped, &ids, read, started);
    Ok(())
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
        other_units,
    } = skipped;
    println!(
        "skipped: {ambiguous} ambiguous movements, {unclaimed} unclaimed script outputs, \
         {other_units} of other units"
    );
}
