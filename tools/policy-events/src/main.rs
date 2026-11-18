//! Reads a policy archive and reports what its movement rows become as chain events.
//!
//! This is the Cardano ingest proof, not the store. It opens a sealed archive, folds its
//! rows exactly as the feed does, translates every unit move into a [`ChainEvent`], and
//! folds the events back into balances. The acceptance question is one line of output:
//! the events' balances must equal the archive's own holders, because a translation that
//! loses or invents a movement cannot reconcile.
//!
//! ```text
//! policy-events --archive-dir .tmp/archive-local --policy e6ba9c0f… --coverage complete
//! policy-events --archive-dir … --events > .tmp/archive.ndjson
//! ```
//!
//! `--events` writes the events as NDJSON on stdout and the report on stderr, so the
//! stream can be diffed against the walk's own `--events` output. That comparison is the
//! real acceptance test: the archive and the tap are two derivations of the same rows, so
//! a fact must carry the same [`chain_events::EventId`] out of both.
//!
//! `--tx-index-dir <dir> --immutable <dir>` repairs chain order. Archive files written
//! before the `tx_index` column existed read every row as position 0, so two transactions
//! in one block came out in file order — an asset could leave a wallet before it arrived,
//! which a replay reports as a disposal with no lot. With the index, every row at
//! position 0 is looked up and given its real position in its block. Ids do not change;
//! only placement does.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use chain_events::{ChainRef, Coverage, EventBody, HolderKind};
use chain_events_cardano::Collection;
use policy_archive::feed::fold_rows;
use policy_archive::holders::{self, Fold};
use policy_archive::reader::{Archive, SparseBytes};

const USAGE: &str = "usage: policy-events --archive-dir <dir> --policy <policy-id> \
                     [--coverage complete|partial] [--events] \
                     [--tx-index-dir <dir> --immutable <dir>]";

/// The report goes to stderr when stdout is the event stream: a pipe must carry NDJSON
/// and nothing else, or `jaq`, `grep` and the id comparison all read report lines.
macro_rules! report {
    ($events:expr, $($arg:tt)*) => {
        if $events {
            eprintln!($($arg)*);
        } else {
            println!($($arg)*);
        }
    };
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;
    let chain =
        ChainRef::from_str("cardano:mainnet").expect("mainnet is a chain this workspace names");
    let collection = Collection::new(chain, &args.policy, args.coverage)?;

    let root = args.archive_dir.join(&args.policy);
    let mut files = Vec::new();
    collect_parquet(&root, &mut files)?;
    files.sort();

    let mut rows = Vec::new();
    for path in &files {
        // The archive is pure over byte ranges; a local file is the whole range. See
        // `policy-archive`'s reader docs on why this is the caller's job.
        let bytes = std::fs::read(path)?;
        let sparse = SparseBytes::whole(bytes.into());
        let archive = Archive::open(&sparse)?;
        for group in 0..archive.num_groups() {
            rows.extend(archive.read_group(&sparse, group)?);
        }
    }

    let movement_rows = rows.len();

    let placed = match &args.tx_index {
        Some((index_dir, immutable)) => Some(place(&mut rows, index_dir, immutable)?),
        None => None,
    };

    // The archive's own definition of a holder, over exactly the units the events can
    // represent. Reconstructing it independently — rows straight into the archive's fold,
    // rather than through the feed and the translation — is what makes the comparison a
    // proof: a party or a delta lost on the way cannot appear on both sides.
    let expected = holders::fold(
        rows.iter()
            .filter(|movement| !movement.unit_name.is_empty()),
    );

    let feed = fold_rows(rows);
    let transactions = feed.len();
    let unit_moves: usize = feed.iter().map(|row| row.units.len()).sum();

    let recorded_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();

    // The stream is opened only in `--events` mode. `closed` is set when the consumer
    // goes away; the fold still finishes, because the report is about the archive rather
    // than about how much of it a pipe took.
    let mut stream = args.events.then(|| BufWriter::new(std::io::stdout()));
    let mut written = 0u64;
    let mut closed = false;

    let mut events = 0u64;
    // Why a unit was left out, by name: `(moves, net mint)`. Reported rather than
    // dropped, so a supply that does not reconcile has a stated reason.
    let mut unrepresentable: BTreeMap<Vec<u8>, (u64, i64)> = BTreeMap::new();
    let mut by_address: BTreeMap<String, i64> = BTreeMap::new();
    // Wallets rotate payment addresses; the archive folds a party under its stake
    // credential, so the like-for-like comparison has to as well.
    let mut by_party: BTreeMap<String, i64> = BTreeMap::new();
    let mut minted = 0i64;
    let mut burned = 0i64;

    for row in &feed {
        let translated = collection.from_row(row, recorded_at);
        for name in &translated.unrepresentable {
            let entry = unrepresentable.entry(name.clone()).or_default();
            entry.0 += 1;
        }

        for event in translated.events {
            events += 1;
            if let Some(stream) = stream.as_mut()
                && !closed
            {
                let mut line = serde_json::to_vec(&event)?;
                line.push(b'\n');
                match stream.write_all(&line) {
                    Ok(()) => written += 1,
                    // A closed pipe is the consumer's answer, not an error: stop writing
                    // and finish the fold so the report is still whole.
                    Err(error) if error.kind() == ErrorKind::BrokenPipe => closed = true,
                    Err(error) => return Err(error.into()),
                }
            }
            let EventBody::Holder(movement) = &event.body else {
                continue;
            };
            let net: i64 = movement.parties.iter().map(|party| party.delta).sum();
            match movement.kind {
                HolderKind::Mint => minted += net,
                HolderKind::Burn => burned += net,
                HolderKind::Transfer => {}
            }
            for party in &movement.parties {
                *by_address.entry(party.account.address.clone()).or_default() += party.delta;
                *by_party
                    .entry(holders::key_of(&party.account.address))
                    .or_default() += party.delta;
            }
        }
    }

    // The net mint of every left-out unit, so the reader can see the supply the
    // events do not account for. `from_row` reports names; the rows carry the rest.
    for row in &feed {
        for unit in &row.units {
            if let Some(entry) = unrepresentable.get_mut(&unit.name) {
                entry.1 += unit.net_mint;
            }
        }
    }

    let held: i64 = by_address
        .values()
        .copied()
        .filter(|balance| *balance > 0)
        .sum();
    let addresses = by_address.values().filter(|balance| **balance > 0).count();
    let holders = by_party.values().filter(|balance| **balance > 0).count();
    let negative = by_party.values().filter(|balance| **balance < 0).count();

    if let Some(mut stream) = stream
        && !closed
    {
        // A buffered writer surfaces the closed pipe here even when every `write_all`
        // succeeded, so the last flush is part of the same answer.
        match stream.flush() {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::BrokenPipe => closed = true,
            Err(error) => return Err(error.into()),
        }
    }

    report!(args.events, "policy            {}", collection.policy());
    report!(args.events, "files             {}", files.len());
    report!(args.events, "movement rows     {movement_rows}");
    if let Some(placed) = &placed {
        report!(
            args.events,
            "positions         {} transactions at 0 looked up · {} moved · {} not in the index",
            placed.looked_up,
            placed.moved,
            placed.missing
        );
    }
    report!(args.events, "transactions      {transactions}");
    report!(args.events, "unit moves        {unit_moves}");
    report!(args.events, "events            {events}");
    if args.events {
        report!(args.events, "written           {written}");
        if closed {
            report!(args.events, "stream            closed by the consumer");
        }
    }
    let omitted: u64 = unrepresentable.values().map(|(moves, _)| moves).sum();
    if unrepresentable.is_empty() {
        report!(args.events, "unrepresentable   0");
    } else {
        let names: Vec<String> = unrepresentable
            .iter()
            .map(|(name, (moves, net))| format!("{moves}× {} (net mint {net:+})", describe(name)))
            .collect();
        report!(
            args.events,
            "unrepresentable   {omitted}   {}",
            names.join(", ")
        );
    }
    report!(
        args.events,
        "accounts          {addresses} addresses in {holders} parties"
    );
    report!(args.events, "minted            {minted}");
    report!(args.events, "burned            {burned}");
    report!(args.events, "held              {held}");

    if negative > 0 {
        // An impossible-on-chain balance means the archive is missing inbound movements,
        // which is the archive's own reconciliation question rather than this tool's.
        eprintln!(
            "warning: {negative} party(s) ended negative — the archive is missing inbound rows"
        );
    }

    reconcile(&expected, &by_party, args.events)?;
    Ok(())
}

/// Compares the events' stake-folded balances with the archive's own holder fold.
fn reconcile(
    expected: &Fold,
    by_party: &BTreeMap<String, i64>,
    events: bool,
) -> Result<(), Box<dyn Error>> {
    let held: i64 = by_party
        .values()
        .copied()
        .filter(|balance| *balance > 0)
        .sum();
    let holders = by_party.values().filter(|balance| **balance > 0).count();

    let mut mismatched = Vec::new();
    let expected_balances: BTreeMap<&str, i64> = expected
        .holders
        .iter()
        .map(|holder| (holder.key.as_str(), holder.amount))
        .collect();
    for (key, amount) in &expected_balances {
        if *amount != by_party.get(*key).copied().unwrap_or_default() {
            mismatched.push((
                *key,
                *amount,
                by_party.get(*key).copied().unwrap_or_default(),
            ));
        }
    }
    for (key, amount) in by_party {
        if *amount > 0 && !expected_balances.contains_key(key.as_str()) {
            mismatched.push((key.as_str(), 0, *amount));
        }
    }

    report!(
        events,
        "archive fold      {} holders, held {}",
        expected.holders.len(),
        expected.held
    );

    if holders != expected.holders.len() || held != expected.held || !mismatched.is_empty() {
        eprintln!(
            "*** RECONCILIATION FAILED — events say {holders} holders and {held} held, \
             the rows say {} and {} ***",
            expected.holders.len(),
            expected.held
        );
        for (key, expected, got) in mismatched.iter().take(10) {
            eprintln!("  {key}: rows {expected}, events {got}");
        }
        return Err("the events do not fold back to the archive's holders".into());
    }

    report!(events, "reconciled        the events fold back to the rows");
    Ok(())
}

/// What [`place`] did, for the report.
struct Placed {
    /// Distinct transactions whose rows said position 0.
    looked_up: usize,
    /// Of those, the ones whose real position is not 0.
    moved: usize,
    /// Of those, the ones no indexed chunk holds — the tip's chunk is never indexed, and a
    /// row there was written with the column, so its 0 is already the truth.
    missing: usize,
}

/// Gives every row at position 0 its transaction's real position in its block.
///
/// A real 0 looks the same as an unrecorded one, so every 0 is asked; the first
/// transaction of a block resolves to 0 again and costs one lookup.
fn place(
    rows: &mut [policy_archive::Movement],
    index_dir: &Path,
    immutable: &Path,
) -> Result<Placed, Box<dyn Error>> {
    let index = tx_index::Index::open(index_dir, immutable)
        .map_err(|error| format!("opening the tx-index at {}: {error:#}", index_dir.display()))?;
    let mut positions = tx_index::Positions::new(&index);
    let mut resolved: HashMap<Vec<u8>, Option<u32>> = HashMap::new();
    for row in rows.iter_mut().filter(|row| row.tx_index == 0) {
        if !resolved.contains_key(&row.tx_hash) {
            let hash: [u8; 32] = row
                .tx_hash
                .as_slice()
                .try_into()
                .map_err(|_| format!("a {}-byte tx hash", row.tx_hash.len()))?;
            let position = positions
                .position(&hash)
                .map_err(|error| format!("placing tx {}: {error:#}", hex::encode(hash)))?;
            resolved.insert(row.tx_hash.clone(), position);
        }
        if let Some(Some(position)) = resolved.get(&row.tx_hash) {
            row.tx_index = *position;
        }
    }
    Ok(Placed {
        looked_up: resolved.len(),
        moved: resolved
            .values()
            .filter(|position| matches!(position, Some(p) if *p != 0))
            .count(),
        missing: resolved
            .values()
            .filter(|position| position.is_none())
            .count(),
    })
}

/// An asset name as text where it is legible, hex where it is not.
fn describe(name: &[u8]) -> String {
    if name.is_empty() {
        return "the policy's root token".to_owned();
    }
    match std::str::from_utf8(name) {
        Ok(text) if text.chars().all(|c| c.is_ascii_graphic() || c == ' ') => {
            format!("{text:?}")
        }
        _ => format!("0x{}", hex::encode(name)),
    }
}

struct Args {
    archive_dir: PathBuf,
    policy: String,
    coverage: Coverage,
    events: bool,
    /// `(index dir, immutable dir)` — both or neither.
    tx_index: Option<(PathBuf, PathBuf)>,
}

impl Args {
    fn parse() -> Result<Self, Box<dyn Error>> {
        let mut archive_dir = None;
        let mut policy = None;
        let mut coverage = Coverage::Partial;
        let mut events = false;
        let mut tx_index_dir = None;
        let mut immutable = None;

        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--archive-dir" => archive_dir = Some(PathBuf::from(args.next().ok_or(USAGE)?)),
                "--policy" => policy = Some(args.next().ok_or(USAGE)?),
                "--coverage" => {
                    let value = args.next().ok_or(USAGE)?;
                    coverage = match value.as_str() {
                        "complete" => Coverage::Complete,
                        "partial" => Coverage::Partial,
                        other => return Err(format!("unknown coverage {other:?}").into()),
                    };
                }
                "--events" => events = true,
                "--tx-index-dir" => tx_index_dir = Some(PathBuf::from(args.next().ok_or(USAGE)?)),
                "--immutable" => immutable = Some(PathBuf::from(args.next().ok_or(USAGE)?)),
                other => return Err(format!("unknown argument {other:?}. {USAGE}").into()),
            }
        }

        let tx_index = match (tx_index_dir, immutable) {
            (Some(dir), Some(immutable)) => Some((dir, immutable)),
            (None, None) => None,
            _ => return Err(format!("--tx-index-dir and --immutable go together. {USAGE}").into()),
        };

        Ok(Self {
            archive_dir: archive_dir.ok_or(USAGE)?,
            policy: policy.ok_or(USAGE)?,
            coverage,
            events,
            tx_index,
        })
    }
}

/// Every `.parquet` under `root`, recursively.
///
/// A sealed archive is `pass-0000/movements.parquet` today, but a pass directory is the
/// writer's choice and a tool that assumed one level would silently read none of a
/// differently-laid-out archive.
fn collect_parquet(root: &Path, out: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_parquet(&path, out)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            out.push(path);
        }
    }
    Ok(())
}
