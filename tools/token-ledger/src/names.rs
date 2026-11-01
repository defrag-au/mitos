//! `token-ledger names` — holders' handles for a packed token, from the chain.
//!
//! ADA Handles are tokens under one policy, so who holds which handle is
//! history like any other: the Handle policy's archive, folded to current
//! holders, joined to a packed token's party table by stake credential, with
//! no per-wallet lookups. `pack` writes the result into the token's identity
//! files (`token_tape::HolderTable`); this command measures the rule.
//!
//! A wallet may hold several handles; which it shows as is a RULE. Handle.me
//! answers with the holder's chosen default, which lives in the handle's
//! personalisation data and is not read here yet. `--compare` measures each
//! candidate rule against Handle.me's own answers, so the rule is chosen by
//! agreement, not by guess.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use token_tape::{HolderTable, PartyKey};

use crate::archive::{History, policy_dir};
use crate::pack::identity;

/// The ADA Handle policy.
pub const HANDLE_POLICY: &str = "f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a";

/// CIP-67 label (222): a CIP-68 handle's user token, the one a wallet holds.
const USER_TOKEN: [u8; 4] = [0x00, 0x0d, 0xe1, 0x40];

#[derive(clap::Args, Debug)]
pub struct NamesArgs {
    /// Archive root holding the Handle policy's archive.
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    #[arg(long, default_value = HANDLE_POLICY)]
    pub handle_policy: String,
    /// Where the token was packed: `<tape>/<policy>/parties.bin`.
    #[arg(long, default_value = ".tmp/tape")]
    pub tape: PathBuf,
    /// The packed token's policy.
    #[arg(long)]
    pub policy: String,
    /// Which handle a wallet of several shows as.
    #[arg(long, value_enum, default_value_t = Rule::Shortest)]
    pub rule: Rule,
    /// Handle.me's answers to measure against: `[[party, "$name" | null], …]`.
    #[arg(long)]
    pub compare: Option<PathBuf>,
}

/// Which of a wallet's handles it shows as.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// The shortest, then the first alphabetically.
    Shortest,
    /// The shortest, then the earliest minted: Handle.me's automatic default
    /// as far as the chain's movements show it (it also puts OG handles
    /// first, which needs their mint metadata).
    ShortestOldestMint,
    /// The first alphabetically.
    Alphabetical,
    /// The one it received most recently.
    Newest,
    /// The one it has held longest.
    Oldest,
}

/// One handle a wallet holds.
#[derive(Debug, Clone)]
pub(crate) struct Held {
    name: String,
    /// When this wallet last received it.
    since: u64,
    /// When the handle was minted.
    minted: u64,
}

impl Rule {
    const ALL: [Rule; 5] = [
        Rule::Shortest,
        Rule::ShortestOldestMint,
        Rule::Alphabetical,
        Rule::Newest,
        Rule::Oldest,
    ];

    fn pick(self, held: &[Held]) -> Option<&str> {
        let len = |h: &Held| h.name.chars().count();
        let best = match self {
            Rule::Shortest => held.iter().min_by_key(|h| (len(h), &h.name)),
            Rule::ShortestOldestMint => held.iter().min_by_key(|h| (len(h), h.minted, &h.name)),
            Rule::Alphabetical => held.iter().min_by_key(|h| &h.name),
            Rule::Newest => held
                .iter()
                .max_by(|a, b| (a.since, &b.name).cmp(&(b.since, &a.name))),
            Rule::Oldest => held.iter().min_by_key(|h| (h.since, &h.name)),
        };
        best.map(|h| h.name.as_str())
    }
}

/// A Handle-policy asset's handle, or `None` for one that is not a handle a
/// wallet holds (a CIP-68 reference token, another label).
fn handle_of(asset: &[u8]) -> Option<String> {
    let name = match asset.get(..4) {
        Some(label) if label == USER_TOKEN => &asset[4..],
        // Another CIP-67 label: `0` nibble, 16-bit label, checksum, `0` nibble.
        Some(label) if label[0] >> 4 == 0 && label[3] & 0x0f == 0 && asset.len() > 4 => {
            return None;
        }
        _ => asset,
    };
    String::from_utf8(name.to_vec())
        .ok()
        .filter(|n| !n.is_empty())
}

/// Who holds which handle now, by stake credential: the Handle policy's
/// archive, folded.
pub(crate) struct HandleHolders {
    by_stake: HashMap<PartyKey, Vec<Held>>,
    rows: usize,
    handles: usize,
    keyless: usize,
    skipped: usize,
    folded: std::time::Duration,
}

impl HandleHolders {
    /// The handle a stake credential shows as, by `rule`; `None` for a key
    /// that is not a stake credential, or holds no handle.
    pub(crate) fn name(&self, key: &PartyKey, rule: Rule) -> Option<&str> {
        self.by_stake.get(key).and_then(|held| rule.pick(held))
    }

    pub(crate) fn wallets(&self) -> usize {
        self.by_stake.len()
    }
}

pub fn names(args: NamesArgs) -> Result<()> {
    let holders = fold_handles(&args.archive_dir, &args.handle_policy)?;
    let HandleHolders {
        ref by_stake,
        rows: row_count,
        handles,
        keyless,
        skipped,
        folded,
    } = holders;
    report_lengths(by_stake);
    compare(
        &args, by_stake, row_count, handles, keyless, skipped, folded,
    )
}

/// Fold the Handle policy's archive under `archive_dir` into who holds what.
pub(crate) fn fold_handles(archive_dir: &std::path::Path, policy: &str) -> Result<HandleHolders> {
    let started = std::time::Instant::now();
    let dir = policy_dir(archive_dir, &policy.to_lowercase());
    let Some(mut history) = History::open(&dir)? else {
        bail!("no Handle archive at {}", dir.display());
    };
    let mut rows: Vec<&policy_archive::Movement> = history.all_movements()?.iter().collect();
    let row_count = rows.len();
    rows.sort_by_key(|m| (m.slot, m.tx_index));

    // Every (asset, address) balance, and when the address last received it;
    // and when each handle NAME was first minted, under any asset that
    // carries it. A personalised handle is minted again as a CIP-68 user
    // token, and Handle.me dates it from the original: MEASURED, dating it
    // by the asset held lost `cnftfart`, `919` and `kandahar` to younger
    // handles.
    let mut held: HashMap<(&[u8], &str), (i64, u64)> = HashMap::new();
    let mut minted: HashMap<String, u64> = HashMap::new();
    for m in &rows {
        if m.net_mint > 0
            && let Some(name) = handle_of(&m.unit_name)
        {
            minted.entry(name).or_insert(m.slot);
        }
        if m.address.is_empty() {
            continue;
        }
        let e = held
            .entry((m.unit_name.as_slice(), m.address.as_str()))
            .or_default();
        e.0 += m.amount;
        if m.amount > 0 {
            e.1 = m.slot;
        }
    }
    // Who holds what, by stake credential.
    let (mut handles, mut skipped, mut keyless) = (0usize, 0usize, 0usize);
    let mut by_stake: HashMap<PartyKey, Vec<Held>> = HashMap::new();
    for ((asset, address), (balance, since)) in held {
        if balance <= 0 {
            continue;
        }
        let Some(name) = handle_of(asset) else {
            skipped += 1;
            continue;
        };
        handles += 1;
        match identity(address) {
            k @ (PartyKey::StakeKey(_) | PartyKey::StakeScript(_)) => {
                let first = minted.get(&name).copied().unwrap_or(since);
                by_stake.entry(k).or_default().push(Held {
                    name,
                    since,
                    minted: first,
                });
            }
            _ => keyless += 1,
        }
    }
    Ok(HandleHolders {
        by_stake,
        rows: row_count,
        handles,
        keyless,
        skipped,
        folded: started.elapsed(),
    })
}

/// How long the handles held are: what a fixed-width field must fit.
fn report_lengths(by_stake: &HashMap<PartyKey, Vec<Held>>) {
    let lengths: Vec<(usize, bool)> = by_stake
        .values()
        .flatten()
        .map(|h| (h.name.len(), h.name.contains('@')))
        .collect();
    let longest = |sub: bool| {
        lengths
            .iter()
            .filter(|(_, s)| *s == sub)
            .map(|(l, _)| *l)
            .max()
            .unwrap_or(0)
    };
    let over15 = lengths.iter().filter(|(l, s)| !s && *l > 15).count();
    println!(
        "longest handle held: {} bytes ({over15} over 15); longest sub-handle: {} bytes",
        longest(false),
        longest(true)
    );
}

/// The rules measured against Handle.me's answers for a packed token.
#[allow(clippy::too_many_arguments)]
fn compare(
    args: &NamesArgs,
    by_stake: &HashMap<PartyKey, Vec<Held>>,
    row_count: usize,
    handles: usize,
    keyless: usize,
    skipped: usize,
    folded: std::time::Duration,
) -> Result<()> {
    // The token's parties.
    let tape = args.tape.join(args.policy.to_lowercase());
    let read = |rel: &str| {
        std::fs::read(tape.join(rel))
            .with_context(|| format!("reading {}", tape.join(rel).display()))
    };
    let table = HolderTable::decode(
        &read(token_tape::HOLDERS_PATH)?,
        &read(token_tape::HOLDERS_OVERFLOW_PATH)?,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let keys: Vec<&PartyKey> = table.identities.iter().map(|i| &i.key).collect();
    let stake_parties = keys
        .iter()
        .filter(|k| matches!(k, PartyKey::StakeKey(_) | PartyKey::StakeScript(_)))
        .count();
    let candidates: BTreeMap<u32, &Vec<Held>> = keys
        .iter()
        .enumerate()
        .filter_map(|(i, k)| by_stake.get(*k).map(|h| (i as u32, h)))
        .collect();
    let several = candidates.values().filter(|h| h.len() > 1).count();

    println!(
        "Handle archive: {row_count} movements, {handles} handles held ({keyless} at addresses \
         with no stake key, {skipped} other assets), {} wallets; folded in {:.1}s",
        by_stake.len(),
        folded.as_secs_f64()
    );
    println!(
        "{}: {} of {} stake-key parties hold a handle ({several} hold several)",
        args.policy,
        candidates.len(),
        stake_parties,
    );

    if let Some(path) = &args.compare {
        let theirs: Vec<(u32, Option<String>)> = serde_json::from_slice(&std::fs::read(path)?)
            .with_context(|| format!("reading {}", path.display()))?;
        let theirs: Vec<(u32, Option<String>)> = theirs
            .into_iter()
            .map(|(id, n)| (id, n.map(|n| n.trim_start_matches('$').to_string())))
            .collect();
        let named = theirs.iter().filter(|(_, n)| n.is_some()).count();
        let in_set = theirs
            .iter()
            .filter(|(id, n)| {
                n.as_ref().is_some_and(|n| {
                    candidates
                        .get(id)
                        .is_some_and(|h| h.iter().any(|c| &c.name == n))
                })
            })
            .count();
        let punycode = theirs
            .iter()
            .filter(|(_, n)| n.as_ref().is_some_and(|n| n.starts_with("xn--")))
            .count();
        println!(
            "compared with Handle.me on {} parties ({named} named, {punycode} in punycode): \
             its answer is among the chain's candidates for {in_set}",
            theirs.len()
        );
        // A few the default rule gets wrong, with the stake key and every
        // candidate: where to look for how Handle.me chooses.
        let mut shown = 0;
        for (id, their) in &theirs {
            let (Some(their), Some(held)) = (their, candidates.get(id)) else {
                continue;
            };
            let ours = args.rule.pick(held);
            if ours.is_some_and(|o| o != their) && shown < 100 {
                shown += 1;
                let stake = match keys[*id as usize] {
                    PartyKey::StakeKey(c) => format!("e1{}", hex::encode(c)),
                    PartyKey::StakeScript(c) => format!("f1{}", hex::encode(c)),
                    _ => String::new(),
                };
                let all: Vec<&str> = held.iter().map(|h| h.name.as_str()).collect();
                println!(
                    "  party {id} stake {stake}: chain {ours:?}, Handle.me {their:?}, held {all:?}"
                );
            }
        }
        for rule in Rule::ALL {
            let (mut same, mut differ, mut ours_only, mut theirs_only) = (0, 0, 0, 0);
            for (id, their) in &theirs {
                let ours = candidates.get(id).and_then(|h| rule.pick(h));
                match (ours, their.as_deref()) {
                    (Some(a), Some(b)) if a == b => same += 1,
                    (Some(_), Some(_)) => differ += 1,
                    (Some(_), None) => ours_only += 1,
                    (None, Some(_)) => theirs_only += 1,
                    (None, None) => {}
                }
            }
            println!(
                "  {rule:?}: agree {same}, differ {differ}, chain only {ours_only}, Handle.me \
                 only {theirs_only}"
            );
        }
    }
    Ok(())
}
