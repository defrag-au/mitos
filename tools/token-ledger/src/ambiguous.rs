//! `token-ledger ambiguous` — what a policy's "ambiguous" movements are.
//!
//! The feed calls a unit's movement in a transaction ambiguous when it is not
//! one loser and one gainer (`policy_archive::feed::direction`), and the story
//! drops it as `Kind::Ambiguous`. That is right as a refusal to guess a
//! sender, but on $NikePig it is 158,309 events, so it is worth knowing what
//! they are before deciding they can stay unshown. Read-only.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Result, bail};
use policy_archive::feed::{Direction, direction};
use policy_archive::trade::{Role, address_parts};
use token_tape::PartyKey;

use crate::archive::{
    History, fold_rows, load_manifest, policy_dir, read_observations, venue_roles_for,
};
use crate::pack::identity;

#[derive(clap::Args, Debug)]
pub struct AmbiguousArgs {
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    #[arg(long)]
    pub policy: String,
    /// Examples to print per kind.
    #[arg(long, default_value_t = 3)]
    pub examples: usize,
    /// Show these transactions' parties (hex hashes) and stop.
    #[arg(long)]
    pub tx: Vec<String>,
    /// Show `--examples` transactions touching this payment credential (hex),
    /// spread across the token's life, and stop.
    #[arg(long)]
    pub script: Option<String>,
    /// List the script credentials no venue claims, by transactions touching
    /// them, with the stake parts they appear under, and stop.
    #[arg(long)]
    pub scripts: bool,
}

/// What an ambiguous movement turns out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Shape {
    /// One wallet's addresses on a side: one sender and one receiver once
    /// addresses are grouped by stake key.
    OneWalletEachSide,
    /// Venues (pools, order contracts, curves) pay exactly one wallet: a buy
    /// whose shape the trade fold does not recognise.
    VenueToOneWallet,
    /// One wallet pays venues: a sell, likewise.
    OneWalletToVenue,
    /// Venues pay several wallets at once: batched buys.
    VenueToManyWallets,
    /// Several wallets pay venues at once.
    ManyWalletsToVenue,
    /// Wallets on both sides of a venue.
    VenueBothWays,
    /// Only venues: routing between pools and orders.
    VenueOnly,
    /// One sender, several receivers: a distribution.
    OneToMany,
    /// Several senders, one receiver: a collection.
    ManyToOne,
    /// Several of each.
    ManyToMany,
    /// Nobody lost or nobody gained once grouped (a wallet moving its own).
    SelfOnly,
    /// A mint or burn alongside other movement.
    MintOrBurn,
}

#[derive(Default)]
struct Tally {
    events: usize,
    units: u128,
    receivers: Vec<usize>,
    examples: Vec<String>,
}

pub fn ambiguous(args: AmbiguousArgs) -> Result<()> {
    let policy = args.policy.to_lowercase();
    let dir = policy_dir(&args.archive_dir, &policy);
    let Some(manifest) = load_manifest(&dir)? else {
        bail!("no archive at {}", dir.display());
    };
    let observations = read_observations(&dir, &manifest)?;
    let roles = venue_roles_for(&observations);
    let Some(mut history) = History::open(&dir)? else {
        bail!("no movements at {}", dir.display());
    };
    let rows = fold_rows(history.all_movements()?.to_vec());
    let is_venue = |address: &str| {
        address_parts(address)
            .and_then(|(cred, stake)| roles.role_at(&cred, stake.as_deref()))
            .is_some()
    };
    if args.scripts {
        // A Shelley address's payment part is a script when the character
        // after `addr1` is `z`, `x` or `w` (header types 1, 3 and 7).
        let is_script = |a: &str| {
            a.strip_prefix("addr1")
                .and_then(|r| r.chars().next())
                .is_some_and(|c| matches!(c, 'z' | 'x' | 'w'))
        };
        // cred → (transactions, units in, units out, stake parts → count, an
        // address, a transaction)
        type Seen = (usize, u128, u128, BTreeMap<String, usize>, String, String);
        let mut by: HashMap<String, Seen> = HashMap::new();
        for row in &rows {
            let mut touched: HashSet<String> = HashSet::new();
            for u in &row.units {
                for p in &u.parties {
                    if !is_script(&p.address) {
                        continue;
                    }
                    let Some((cred, stake)) = address_parts(&p.address) else {
                        continue;
                    };
                    if roles.role_at(&cred, stake.as_deref()).is_some() {
                        continue;
                    }
                    let e = by.entry(cred.clone()).or_insert_with(|| {
                        (
                            0,
                            0,
                            0,
                            BTreeMap::new(),
                            p.address.clone(),
                            hex::encode(&row.tx_hash),
                        )
                    });
                    if p.amount > 0 {
                        e.1 += u128::from(p.amount.unsigned_abs());
                    } else {
                        e.2 += u128::from(p.amount.unsigned_abs());
                    }
                    *e.3.entry(stake.map_or("-".to_string(), |s| s[..8].to_string()))
                        .or_default() += 1;
                    if touched.insert(cred.clone()) {
                        e.0 += 1;
                    }
                }
            }
        }
        let mut by: Vec<_> = by.into_iter().collect();
        by.sort_by_key(|(_, s)| std::cmp::Reverse(s.0));
        for (cred, (txs, units_in, units_out, stakes, address, tx)) in
            by.iter().take(args.examples)
        {
            let mut top: Vec<_> = stakes.iter().collect();
            top.sort_by_key(|(_, n)| std::cmp::Reverse(**n));
            let top: Vec<String> = top.iter().take(4).map(|(s, n)| format!("{s}×{n}")).collect();
            println!(
                "{cred} {txs} txs, in {units_in} out {units_out}, {} stake parts [{}]\n    \
                 {address}\n    tx {tx}",
                stakes.len(),
                top.join(" ")
            );
        }
        return Ok(());
    }
    let shown: Vec<&policy_archive::feed::FeedRow> = match &args.script {
        Some(script) => {
            let touching: Vec<_> = rows
                .iter()
                .filter(|r| {
                    r.units.iter().any(|u| {
                        u.parties
                            .iter()
                            .any(|p| address_parts(&p.address).is_some_and(|(c, _)| &c == script))
                    })
                })
                .collect();
            println!("{} transactions touch {script}", touching.len());
            let step = (touching.len() / args.examples.max(1)).max(1);
            touching
                .into_iter()
                .step_by(step)
                .take(args.examples)
                .collect()
        }
        None => rows
            .iter()
            .filter(|r| args.tx.contains(&hex::encode(&r.tx_hash)))
            .collect(),
    };
    if !shown.is_empty() || args.script.is_some() {
        for row in shown {
            println!("{} slot {}:", hex::encode(&row.tx_hash), row.slot);
            for u in &row.units {
                for p in &u.parties {
                    let (cred, stake) = address_parts(&p.address).unwrap_or_default();
                    let role = roles.role_at(&cred, stake.as_deref()).map(|r| {
                        let name = roles
                            .name_of(&cred)
                            .or_else(|| stake.as_deref().and_then(|s| roles.name_of(s)));
                        format!("{r:?} {}", name.unwrap_or("?"))
                    });
                    println!(
                        "    {:>14} {} stake {} {}",
                        p.amount,
                        p.address,
                        stake.as_deref().map_or("-", |s| &s[..8]),
                        role.unwrap_or_default()
                    );
                }
            }
        }
        return Ok(());
    }

    let mut tally: BTreeMap<Shape, Tally> = BTreeMap::new();
    let mut senders: HashMap<PartyKey, (usize, u128, HashSet<PartyKey>)> = HashMap::new();
    let mut total = 0usize;
    let (mut resolved, mut resolved_fills) = (0usize, 0usize);
    let mut orders: BTreeMap<i8, usize> = BTreeMap::new();
    for row in &rows {
        for u in &row.units {
            if !matches!(direction(u), Direction::Ambiguous) {
                continue;
            }
            total += 1;
            // What the trade fold now reads as trades is no longer a gap.
            let read = policy_archive::trade::classify_moves(u, &roles);
            if !matches!(read.as_slice(), [policy_archive::trade::Event::Transfer]) {
                resolved += 1;
                resolved_fills += read.len();
                continue;
            }
            // Where order contracts sit, by the sign of their net.
            let mut order_net: i128 = 0;
            let mut has_order = false;
            for p in &u.parties {
                if address_parts(&p.address)
                    .and_then(|(cred, stake)| roles.role_at(&cred, stake.as_deref()))
                    .is_some_and(|r| r == Role::Order)
                {
                    has_order = true;
                    order_net += i128::from(p.amount);
                }
            }
            if has_order {
                *orders.entry(order_net.signum() as i8).or_default() += 1;
            }
            // Net per wallet, not per address.
            let mut net: HashMap<PartyKey, i128> = HashMap::new();
            let mut venues: HashSet<PartyKey> = HashSet::new();
            for p in &u.parties {
                let k = identity(&p.address);
                if is_venue(&p.address) {
                    venues.insert(k.clone());
                }
                *net.entry(k).or_default() += i128::from(p.amount);
            }
            let venue = !venues.is_empty();
            let wallets = |sign: i128| {
                net.iter()
                    .filter(|(k, a)| a.signum() == sign && !venues.contains(*k))
                    .count()
            };
            let (wallet_losers, wallet_gainers) = (wallets(-1), wallets(1));
            let losers: Vec<(&PartyKey, i128)> = net
                .iter()
                .filter(|(_, a)| **a < 0)
                .map(|(k, a)| (k, *a))
                .collect();
            let gainers: Vec<(&PartyKey, i128)> = net
                .iter()
                .filter(|(_, a)| **a > 0)
                .map(|(k, a)| (k, *a))
                .collect();
            let moved: u128 = gainers.iter().map(|(_, a)| a.unsigned_abs()).sum();
            let shape = match (losers.len(), gainers.len()) {
                _ if u.net_mint != 0 => Shape::MintOrBurn,
                _ if venue => match (wallet_losers, wallet_gainers) {
                    (0, 0) => Shape::VenueOnly,
                    (0, 1) => Shape::VenueToOneWallet,
                    (1, 0) => Shape::OneWalletToVenue,
                    (0, _) => Shape::VenueToManyWallets,
                    (_, 0) => Shape::ManyWalletsToVenue,
                    _ => Shape::VenueBothWays,
                },
                (1, 1) => Shape::OneWalletEachSide,
                (0, _) | (_, 0) => Shape::SelfOnly,
                (1, _) => Shape::OneToMany,
                (_, 1) => Shape::ManyToOne,
                _ => Shape::ManyToMany,
            };
            if shape == Shape::OneToMany {
                let e = senders.entry(losers[0].0.clone()).or_default();
                e.0 += 1;
                e.1 += moved;
                e.2.extend(gainers.iter().map(|(k, _)| (*k).clone()));
            }
            let t = tally.entry(shape).or_default();
            t.events += 1;
            t.units += moved;
            t.receivers.push(gainers.len());
            if t.examples.len() < args.examples {
                t.examples.push(format!(
                    "{} slot {}: {} addresses, {} senders and {} receivers by wallet",
                    hex::encode(&row.tx_hash),
                    row.slot,
                    u.parties.len(),
                    losers.len(),
                    gainers.len()
                ));
            }
        }
    }

    println!(
        "{policy}: {total} ambiguous movements; the trade fold reads {resolved} of them as \
         {resolved_fills} fills; of the rest, {} have an order contract among the parties \
         (by its net: {:?})",
        orders.values().sum::<usize>(),
        orders
    );
    for (shape, t) in &tally {
        let mut r = t.receivers.clone();
        r.sort_unstable();
        println!(
            "  {shape:?}: {} ({:.1}%), {} units moved; receivers by wallet: median {}, max {}",
            t.events,
            100.0 * t.events as f64 / total.max(1) as f64,
            t.units,
            r.get(r.len() / 2).copied().unwrap_or(0),
            r.last().copied().unwrap_or(0)
        );
        for e in &t.examples {
            println!("      {e}");
        }
    }
    let mut top: Vec<_> = senders.into_iter().collect();
    top.sort_by_key(|s| std::cmp::Reverse(s.1.0));
    println!("  distributors (one sender, many receivers), top 5 by transactions:");
    for (k, (txs, units, receivers)) in top.iter().take(5) {
        println!(
            "      {:?}: {txs} transactions, {units} units, {} distinct receivers",
            short(k),
            receivers.len()
        );
    }

    // The other kind the story leaves unshown: a script output holding the
    // token that no decoder claimed. Counted apart, by address.
    let story = policy_archive::story::build(&rows, &observations, &roles);
    let mut unclaimed: HashMap<&str, (usize, u128, bool)> = HashMap::new();
    for e in &story.events {
        if let policy_archive::story::Kind::UnclaimedScript {
            address,
            amount,
            has_datum,
        } = &e.kind
        {
            let u = unclaimed.entry(address.as_str()).or_default();
            u.0 += 1;
            u.1 += u128::from(amount.unsigned_abs());
            u.2 |= *has_datum;
        }
    }
    let count: usize = unclaimed.values().map(|u| u.0).sum();
    println!(
        "{count} unclaimed script outputs, at {} addresses; the largest:",
        unclaimed.len()
    );
    let mut by: Vec<_> = unclaimed.into_iter().collect();
    by.sort_by_key(|(_, u)| std::cmp::Reverse(u.0));
    for (address, (n, units, datum)) in by.iter().take(15) {
        // What the address registry says it is, by address or by script.
        use address_registry::registry::{
            RegistryNetwork, lookup_address_match, lookup_payment_credential,
        };
        let named = lookup_address_match(address, RegistryNetwork::Mainnet)
            .map(|(c, _)| c.to_string())
            .or_else(|| {
                address_parts(address)
                    .and_then(|(cred, _)| lookup_payment_credential(&cred))
                    .map(|e| e.category.to_string())
            })
            .unwrap_or_else(|| "not in the registry".to_string());
        let (cred, stake) = address_parts(address).unwrap_or_default();
        println!(
            "      {n} outputs, {units} units, datum {datum}: {named}\n        {address}\n        \
             script {cred} stake {}",
            stake.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

fn short(k: &PartyKey) -> String {
    match k {
        PartyKey::StakeKey(c) => format!("stake key {}", hex::encode(&c[..6])),
        PartyKey::StakeScript(c) => format!("stake script {}", hex::encode(&c[..6])),
        PartyKey::Address(a) => format!("address {}", hex::encode(&a[..a.len().min(6)])),
        PartyKey::Byron(s) => format!("byron {}", &s[..s.len().min(10)]),
    }
}
