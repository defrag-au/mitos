//! `trace` — the query. Seed a credential, return the wallets that share a hand.
//!
//! Breadth-first over co-signing groups, bounded by `--max-hops`, skipping
//! suppressed operator keys. Pure sqlite + in-memory expansion: no chain access,
//! so this runs on a laptop against a copied index.
//!
//! ## Every merge cites a transaction
//!
//! A key enters the cluster because a specific transaction was signed by both it
//! and something already in the cluster, and that transaction hash is reported.
//! A claim nobody can re-derive is not a claim — the same discipline
//! `project-ledger`'s `score` follows by decomposing into signal rows.
//!
//! ## What a hop means here
//!
//! Hop 0 is the seed. Hop 1 is everything that co-signed a transaction with it —
//! which, for an ordinary HD wallet, is mostly its own other addresses, since a
//! wallet routinely spends UTxOs sitting at several of its own keys. Hop 2 is
//! where genuinely separate parties start appearing, and where false merges do
//! too. Default 2, and worth arguing about (see WALLET_TRACE.md §Open questions).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use pallas_addresses::Address;

use crate::creds::{cred_pair, stake_bech32};
use crate::store::Index;
use crate::witness::KeyHash;

#[derive(clap::Args, Debug)]
pub struct TraceArgs {
    #[arg(long, default_value = "wallet-trace.db")]
    pub db: PathBuf,

    /// Seed: a payment key hash, 28 bytes of hex.
    #[arg(long)]
    pub payment_key: Option<String>,

    /// Seed: a `stake1…` address. Expands to every payment credential the index
    /// has seen beside it.
    #[arg(long)]
    pub stake: Option<String>,

    /// Seed: any `addr1…`. Its payment credential is the seed.
    #[arg(long)]
    pub address: Option<String>,

    /// How far to expand. 0 = the seed's own group members only.
    #[arg(long, default_value_t = 2)]
    pub max_hops: usize,

    /// Ignore `suppressed_key`. Useful ONLY to see what the guard is removing —
    /// on a real index this will usually merge half the chain.
    #[arg(long)]
    pub no_suppression: bool,

    /// Stop expanding once the cluster reaches this many keys, and say so. A
    /// runaway cluster is a false-merge symptom, not a finding.
    #[arg(long, default_value_t = 5_000)]
    pub max_keys: usize,
}

/// How a key entered the cluster.
struct Provenance {
    hop: usize,
    /// The transaction that joined it, and the key it was joined to.
    via_tx: [u8; 32],
    via_key: KeyHash,
}

pub fn trace(args: &TraceArgs) -> Result<()> {
    let ix = Index::open(&args.db)?;
    let (groups, cosign_rows, pairs, suppressed_n) = ix.counts()?;
    if groups == 0 {
        bail!(
            "index at {} has no co-signing groups — run `wallet-trace index` first",
            args.db.display()
        );
    }
    if suppressed_n == 0 && !args.no_suppression {
        tracing::warn!(
            "no suppressed keys recorded — run `wallet-trace suppress` first, or this \
             trace may merge unrelated parties through a batcher"
        );
    }

    let seeds = resolve_seeds(&ix, args)?;
    if seeds.is_empty() {
        bail!(
            "seed resolved to no payment credentials. If you seeded with --stake, the \
             index has never seen that stake key beside a payment key — check the \
             index's slot range covers the wallet's activity."
        );
    }
    let suppressed = if args.no_suppression {
        HashSet::new()
    } else {
        ix.suppressed()?
    };

    tracing::info!(
        seeds = seeds.len(),
        index_groups = groups,
        index_rows = cosign_rows,
        cred_pairs = pairs,
        suppressed = suppressed.len(),
        "trace: expanding"
    );

    // BFS over groups. `seen` is keyed on the credential, so a key joined at
    // hop 1 is never re-joined at hop 2 — otherwise the evidence reported would
    // be whichever path happened to be walked last.
    let mut seen: HashMap<KeyHash, Provenance> = HashMap::new();
    let mut queue: VecDeque<(KeyHash, usize)> = VecDeque::new();
    for s in &seeds {
        seen.insert(
            *s,
            Provenance {
                hop: 0,
                via_tx: [0; 32],
                via_key: *s,
            },
        );
        queue.push_back((*s, 0));
    }

    let mut truncated = false;
    let mut groups_touched: HashSet<i64> = HashSet::new();
    while let Some((key, hop)) = queue.pop_front() {
        if hop >= args.max_hops {
            continue;
        }
        // Suppression stops a hub acting as a BRIDGE between unrelated parties.
        // It must never silence the SEED: the user asked about that key
        // explicitly, and refusing to expand it returns "nothing found" for a
        // wallet that plainly has co-signers.
        //
        // This is not hypothetical. `$uss.enterprise`'s key has degree 51 across
        // 1,126 groups — an ordinary wallet that repeatedly signs with its own
        // handful of keys, nothing like the degree-10^5 batchers — and a 0.5%
        // percentile cut suppressed it. The first run reported "NO CO-SIGNING
        // GROUPS AT ALL" for a key with 1,126 of them.
        if hop > 0 && suppressed.contains(&key) {
            continue;
        }
        for (gid, tx_hash, _slot) in ix.groups_for_key(&key)? {
            groups_touched.insert(gid);
            for member in ix.members_of_group(gid)? {
                if member == key || seen.contains_key(&member) {
                    continue;
                }
                if seen.len() >= args.max_keys {
                    truncated = true;
                    break;
                }
                seen.insert(
                    member,
                    Provenance {
                        hop: hop + 1,
                        via_tx: tx_hash,
                        via_key: key,
                    },
                );
                queue.push_back((member, hop + 1));
            }
            if truncated {
                break;
            }
        }
        if truncated {
            break;
        }
    }

    // A suppressed seed still expands (above), but the caller must be told:
    // its hop-1 neighbours may include parties it merely transacted with.
    let seeds_suppressed: Vec<KeyHash> = seeds
        .iter()
        .filter(|s| suppressed.contains(*s))
        .copied()
        .collect();

    report(
        &ix,
        &seeds,
        &seen,
        &groups_touched,
        truncated,
        &seeds_suppressed,
        args,
    )
}

fn resolve_seeds(ix: &Index, args: &TraceArgs) -> Result<Vec<KeyHash>> {
    let mut out = Vec::new();
    if let Some(h) = &args.payment_key {
        let raw = hex::decode(h.trim()).context("--payment-key must be hex")?;
        if raw.len() != 28 {
            bail!("--payment-key must be 28 bytes, got {}", raw.len());
        }
        let mut k = [0u8; 28];
        k.copy_from_slice(&raw);
        out.push(k);
    }
    if let Some(a) = &args.address {
        let addr = Address::from_bech32(a.trim()).context("--address must be bech32")?;
        match cred_pair(&addr) {
            Some(p) => out.push(p.payment),
            None => bail!(
                "--address has no key payment credential (script or stakeless). \
                 A script address belongs to a contract, not a person."
            ),
        }
    }
    if let Some(s) = &args.stake {
        let addr = Address::from_bech32(s.trim()).context("--stake must be bech32")?;
        let Address::Stake(st) = addr else {
            bail!("--stake must be a stake1… address");
        };
        let mut cred = [0u8; 28];
        cred.copy_from_slice(&st.payload().as_hash()[..28]);
        let found = ix.payments_for_stake(&cred)?;
        tracing::info!(
            payment_keys = found.len(),
            "trace: stake seed expanded via cred_pair"
        );
        out.extend(found);
    }
    if args.payment_key.is_none() && args.address.is_none() && args.stake.is_none() {
        bail!("pass one of --payment-key, --address or --stake");
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn report(
    ix: &Index,
    seeds: &[KeyHash],
    seen: &HashMap<KeyHash, Provenance>,
    groups_touched: &HashSet<i64>,
    truncated: bool,
    seeds_suppressed: &[KeyHash],
    args: &TraceArgs,
) -> Result<()> {
    // Group the cluster's payment keys by the stake credential they were seen
    // beside — a stake key IS the wallet, so this is the shape a human reads.
    let mut wallets: BTreeMap<String, Vec<KeyHash>> = BTreeMap::new();
    let mut unnamed: Vec<KeyHash> = Vec::new();
    for key in seen.keys() {
        let stakes = ix.stakes_for_payment(key)?;
        if stakes.is_empty() {
            unnamed.push(*key);
        }
        for (stake, is_script) in stakes {
            wallets
                .entry(stake_bech32(&stake, is_script))
                .or_default()
                .push(*key);
        }
    }

    println!();
    println!("wallet-trace");
    println!(
        "  seeds {}   cluster {} keys   {} groups touched   max-hops {}",
        seeds.len(),
        seen.len(),
        groups_touched.len(),
        args.max_hops
    );
    if truncated {
        println!(
            "\n  *** TRUNCATED at --max-keys {} ***\n  \
             A cluster this size is a false-merge symptom, not a finding. Run\n  \
             `suppress` with a lower --max-degree and trace again.",
            args.max_keys
        );
    }

    // Two very different outcomes both leave the cluster equal to the seed set,
    // and conflating them would be a wrong conclusion, not just vague wording:
    // "nothing co-signed with this wallet" vs "everything that did was already
    // part of it". The group count separates them.
    if !seeds_suppressed.is_empty() {
        println!(
            "\n  *** {} SEED KEY(S) ARE ON THE SUPPRESSION LIST ***",
            seeds_suppressed.len()
        );
        for k in seeds_suppressed {
            let d = ix.degree_of(k)?.unwrap_or((0, 0));
            println!(
                "  {}  degree {} in {} groups",
                &hex::encode(k)[..16],
                d.0,
                d.1
            );
        }
        println!(
            "  Expanded anyway — you asked about it. But the threshold judged this\n  \
             key hub-like, so hop-1 neighbours may be counterparties rather than the\n  \
             same hand. Compare against a higher `suppress --max-degree` before\n  \
             treating any of it as one owner."
        );
    }

    if seen.len() == seeds.len() {
        if groups_touched.is_empty() {
            println!(
                "\n  NO CO-SIGNING GROUPS AT ALL for this seed. Either the wallet never\n  \
                 spent from two credentials in one transaction, or the index's slot\n  \
                 range does not cover its activity — check `meta.first_slot`/`last_slot`."
            );
        } else {
            println!(
                "\n  NOTHING NEW: {} group(s) were found, but every co-signer was already\n  \
                 in the seed set. For a --stake seed that is the expected result when the\n  \
                 wallet only ever co-signs with its own addresses — it means no evidence\n  \
                 of a SEPARATE wallet, not an absence of activity.",
                groups_touched.len()
            );
        }
    }

    println!("\n  wallets in the cluster ({})", wallets.len());
    for (stake, keys) in &wallets {
        let hops: Vec<usize> = keys
            .iter()
            .filter_map(|k| seen.get(k).map(|p| p.hop))
            .collect();
        let min_hop = hops.iter().copied().min().unwrap_or(0);
        let is_seed = keys.iter().any(|k| seeds.contains(k));
        println!(
            "  {}  {:>2} key(s)  hop {}{}",
            stake,
            keys.len(),
            min_hop,
            if is_seed { "   <- SEED" } else { "" }
        );
    }
    if !unnamed.is_empty() {
        println!(
            "\n  {} key(s) with no address ever seen beside them — either the\n  \
             index skipped cred pairs, or they only ever signed without receiving.",
            unnamed.len()
        );
    }

    println!("\n  evidence — why each non-seed key joined");
    let mut rows: Vec<(&KeyHash, &Provenance)> = seen.iter().filter(|(_, p)| p.hop > 0).collect();
    rows.sort_by_key(|(_, p)| p.hop);
    for (k, p) in rows.iter().take(60) {
        println!(
            "  hop {}  {}  joined {}  via tx {}",
            p.hop,
            &hex::encode(k)[..16],
            &hex::encode(p.via_key)[..16],
            hex::encode(p.via_tx)
        );
    }
    if rows.len() > 60 {
        println!("  … and {} more", rows.len() - 60);
    }
    println!();
    Ok(())
}
