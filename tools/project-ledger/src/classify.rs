//! `classify` — name the counterparties the chain can identify.
//!
//! ## Why this exists
//!
//! A swap sends value out and gets value back. Both legs are real movements, so
//! the walk books both — and the return leg, arriving from an address the
//! ledger cannot name, reads as **income**. On Mekka that was **74,626 ₳ of a
//! supposed 132,590 ₳ of "unexplained" inbound to the treasury**: money it had
//! sent to Minswap moments earlier, coming back after a conversion.
//!
//! That is the change-vs-receipt error one level up. `book_unit_flows` already
//! refuses to book an output back to a funder *within* a transaction; this is
//! the same mistake across two transactions, and no rule inside a single tx can
//! catch it. Naming the counterparty is what makes the round trip visible.
//!
//! ## Assertion, not observation
//!
//! "This address is Minswap" is inherited from `address-registry`, not seen on
//! chain, so every row records its `source`. A reader must be able to tell a
//! derived fact from a borrowed claim — the same discipline the royalty address
//! gets, which IS derived and says so.
//!
//! ## It fails SILENTLY, so it must report
//!
//! A registry only knows the contracts someone has already added. A new DEX, a
//! new pool version, an aggregator nobody has catalogued — each one reverts to
//! the original bug, quietly, and the ledger looks fine. So this prints what it
//! could NOT name, ranked by the value flowing through it: an unclassified
//! address carrying millions is the next registry entry, and the operator only
//! knows to add it if the number is on screen.

use std::path::PathBuf;

use address_registry::{
    AddressCategory, MatchKind, RegistryNetwork, ScriptCategory, lookup_address_match,
    payment_credential_is_script,
};
use anyhow::Result;
use chain_ledger::{Basis, ProviderCapability};

use crate::store::{CounterpartyRow, Ledger};

#[derive(clap::Args, Debug)]
pub struct ClassifyArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// How many unnamed counterparties to list in the coverage report.
    #[arg(long, default_value_t = 10)]
    pub report: usize,

    /// Also INFER a service from activity, for counterparties no registry names.
    ///
    /// Off by default, because it is the one thing here that can destroy a real
    /// figure. Naming a wallet a service means value arriving from it is read
    /// as a round trip rather than income — so a false positive does not merely
    /// mislabel, it ERASES. A registry entry is evidence; this is a guess with
    /// a threshold, and it is opt-in for that reason.
    #[arg(long)]
    pub infer_service: bool,

    /// Flows above which an unnamed counterparty is inferred to be a service.
    ///
    /// Defaults to the frontier's own custodial proxy (`Thresholds::receipts`,
    /// 1000) rather than a fresh constant — the project already agreed that
    /// number means "too busy to be a person", and two different thresholds for
    /// one idea is how they drift apart. On Mekka it selects 115 of 35,718
    /// counterparties, against 25,692 with fewer than ten flows.
    #[arg(long, default_value_t = 1000)]
    pub service_min_flows: u64,

    /// Distinct wallets PAID, above which a provider looks like a CEX hot
    /// wallet — measured at 2,390 and 3,081 on two real ones.
    #[arg(long, default_value_t = 500)]
    pub service_min_fanout: u64,

    /// Distinct wallets it is paid BY, below which the fan-in is negligible.
    /// The two real hot wallets had 3 and 2; a busy project wallet has traffic
    /// both ways, which is why this bound is what stops the rule catching one.
    #[arg(long, default_value_t = 10)]
    pub service_max_fanin: u64,

    /// Payment legs at or above which a stakeless one-way address is inferred
    /// to be an OFF-RAMP (per-customer exchange deposit shape). Measured on a
    /// real one: dwess's exit took 31 payments from exactly one wallet with
    /// nothing ever back.
    #[arg(long, default_value_t = 5)]
    pub offramp_min_legs: u32,

    /// Distinct payers above which the address is NOT a per-customer exit —
    /// deposit addresses have one customer; a fee wallet has thousands.
    #[arg(long, default_value_t = 2)]
    pub offramp_max_payers: u32,
}

/// Map a registry category onto the kinds this ledger cares about.
///
/// Deliberately coarse. The question here is "did this value come back from a
/// service rather than from a person", and a pool, a batcher and an order
/// contract all answer it the same way.
fn kind_of(cat: &AddressCategory) -> Option<(ProviderCapability, Option<String>)> {
    match cat {
        // The registry's `Exchange` means an ON-CHAIN venue — every entry is a
        // DEX, an AMM or an aggregator (Minswap, Splash, CSWAP, DexHunter). It
        // never means a centralised exchange, which has no script address to
        // register. That distinction is not pedantry: a DEX is a round trip and
        // a CEX is a boundary, and confusing them either erases real income or
        // invents it.
        AddressCategory::Script(ScriptCategory::Exchange { label }) => {
            Some((ProviderCapability::Dex, Some((*label).to_string())))
        }
        AddressCategory::Script(ScriptCategory::Marketplace { marketplace, .. }) => Some((
            ProviderCapability::Marketplace,
            Some(format!("{marketplace:?}")),
        )),
        AddressCategory::Script(ScriptCategory::DeFi { label, .. }) => {
            Some((ProviderCapability::Lending, Some((*label).to_string())))
        }
        _ => None,
    }
}

/// Whether a registry hit may NAME the party it was found on.
///
/// A prefix hit identified only the payment SCRIPT — order and listing
/// contracts keep the CUSTOMER's staking credential on the script address, so
/// for a stake-keyed party the hit may mean "this person once placed a Splash
/// order", not "this is Splash". The discriminator is the party's other
/// addresses: a venue's stake credential lives exclusively behind scripts,
/// while a person also owns ordinary key-payment addresses. Getting this wrong
/// named 168 Mekka customers "Splash" and fed their real marketplace purchases
/// to the round-trip suppressor.
///
/// Known residue: a party seen ONLY via its order-script address still passes,
/// but such a party has no key-payment footprint in this ledger, so every leg
/// it touches is genuinely DEX-shaped anyway.
fn hit_names_the_party(match_kind: MatchKind, addrs: &[String]) -> bool {
    match match_kind {
        MatchKind::Exact => true,
        MatchKind::VariableStakePrefix => addrs.iter().all(|a| payment_credential_is_script(a)),
    }
}

pub fn run(args: &ClassifyArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;

    // Everything this pass writes is re-derived from the ledger and the
    // registry, so start clean. Without this, a FIXED rule cannot retract what
    // a broken one wrote — the 168 customers named "Splash" by the prefix bug
    // survived every re-run because the writes are additive.
    ledger.reset_counterparties()?;

    // `fan_shape` probes unit_flow BY COUNTERPARTY per party; without this
    // index that is a full scan per party — measured at ~1 HOUR for 448
    // parties over a 2.6M-row table. `score` creates the same index, but
    // classify runs first on a fresh ledger and must not depend on it.
    ledger.connection().execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_uf_counterparty ON unit_flow(counterparty)",
    )?;

    let candidates = ledger.counterparties_with_addresses()?;
    let total = candidates.len();

    // Every counterparty the mint's own fund split paid — OBSERVED, and the
    // thing that separates a service the project USED from a venue it merely
    // passed value through.
    let mint_providers: std::collections::HashSet<String> =
        ledger.mint_payment_destinations()?.into_iter().collect();

    let mut rows: Vec<CounterpartyRow> = Vec::new();
    let mut customers_spared = 0usize;
    for (key, addrs) in candidates {
        let mut caps: Vec<(ProviderCapability, Basis)> = Vec::new();
        let mut name = None;
        let mut sources: Vec<String> = Vec::new();

        // First address the registry recognises wins; a party's addresses all
        // belong to one wallet, so two answers would be a registry conflict
        // rather than something to average.
        if let Some((addr, match_kind, (cap, label))) = addrs.iter().find_map(|a| {
            lookup_address_match(a, RegistryNetwork::Mainnet)
                .and_then(|(c, mk)| kind_of(c).map(|k| (a.clone(), mk, k)))
        }) {
            if hit_names_the_party(match_kind, &addrs) {
                caps.push((cap, Basis::Asserted));
                name = label;
                sources.push(format!("address-registry: {}", elide(&addr)));
            } else {
                customers_spared += 1;
            }
        }
        // Minting is ADDITIVE, not an alternative. `bank.pillar` is both a
        // mint provider and (elsewhere) a payout service, and the old
        // single-label table could only keep one of those.
        if mint_providers.contains(&key) {
            caps.push((ProviderCapability::Minting, Basis::Observed));
            sources.push("mint_payment fund split".to_string());
        }
        if caps.is_empty() {
            continue;
        }
        rows.push(CounterpartyRow {
            key,
            name,
            capabilities: caps,
            source: sources.join("; "),
        });
    }

    let named = ledger.put_counterparty(&rows)?;
    tracing::info!(
        counterparties = total,
        named,
        customers_spared,
        "classify: counterparties named from the address registry \
         (customers_spared = stake keys seen on an order/listing script but \
         owning ordinary addresses too — people, not the venue)"
    );

    // THE DOMINANT MINT-PROCEEDS DESTINATION — a derived fact with a stated
    // rule, recorded in meta for the app, `emit-registry` and `provenance`.
    //
    // On S2 the mint-proceeds treasury took 99 of 100 fund-split legs (99.9%
    // of split value); nothing in the ledger MARKED that dominance, so a cold
    // reader had to rediscover it by query. The rule: majority of fund-split
    // value across a non-trivial number of mints. The label "treasury" stays
    // a human assertion — this records only the arithmetic.
    match dominant_mint_destination(&ledger.mint_payment_totals()?) {
        Some((key, share, mints)) => {
            tracing::info!(
                dest = %elide(&key),
                share = format!("{:.1}%", share * 100.0),
                mints,
                "classify: dominant mint-proceeds destination (derived)"
            );
            ledger.meta_set(
                "mint_proceeds_dominant",
                &format!("{key} {share:.4} {mints}"),
            )?;
        }
        None => ledger.meta_set("mint_proceeds_dominant", "")?,
    }

    // INFERRED services, only when asked. Runs AFTER the registry write so the
    // exclusion in `busy_unnamed_counterparties` sees those rows — a registry
    // claim about WHO must never be replaced by a guess about WHAT.
    if args.infer_service {
        let busy = ledger.busy_unnamed_counterparties(args.service_min_flows)?;
        let mut inferred: Vec<CounterpartyRow> = Vec::new();
        for (key, flows, _) in &busy {
            // A service by volume, function unknown. Recorded as a provider
            // with NO capability rather than guessed at: an empty set is
            // honest, and `is_round_trip()` then correctly returns false, so
            // nothing is erased on a hunch.
            inferred.push(CounterpartyRow {
                key: key.clone(),
                name: None,
                capabilities: Vec::new(),
                source: format!(
                    "inferred provider: {flows} flows >= threshold {}",
                    args.service_min_flows
                ),
            });
        }

        // CEX FROM SHAPE — over PARTIES, because that is the only set whose
        // fan-out is measurable. An exchange hot wallet pays thousands and
        // receives from a handful: customer deposits land on per-user addresses
        // and the hot wallet is replenished from cold storage. Measured on two
        // real ones: 2,390 and 3,081 distinct recipients against 3 and 2 senders.
        //
        // For a wallet that is merely a counterparty we see only its dealings
        // with the watch set, so its apparent fan-out is bounded by the size of
        // that set and means nothing. `fan_shape` returns `None` there rather
        // than a small number that would quietly never match.
        //
        // DERIVED, never asserted, and the name is left NULL: this says
        // "behaves like a CEX", not which exchange, because chain data cannot
        // answer that and inventing it would be the worst kind of confidence.
        let mut cex = 0usize;
        for key in ledger.party_keys()? {
            let Some((paid, paid_by)) = ledger.fan_shape(&key)? else {
                continue;
            };
            if paid >= args.service_min_fanout && paid_by <= args.service_max_fanin {
                cex += 1;
                inferred.push(CounterpartyRow {
                    key,
                    name: None,
                    capabilities: vec![(ProviderCapability::Cex, Basis::Derived)],
                    source: format!("inferred cex: paid {paid} wallets, received from {paid_by}"),
                });
            }
        }
        if cex > 0 {
            tracing::warn!(
                cex,
                "classify: wallets inferred to be CEX hot wallets by fan-out shape. Value \
                 arriving from these came from OUTSIDE the chain — it is NOT a round trip, and \
                 it is where an off-chain funding leg becomes visible."
            );
        }

        // OFF-RAMPS from shape: a stakeless address that few wallets ever pay
        // and that NEVER pays anything back is the per-customer exchange
        // deposit pattern — a private door out of the chain. Derived, unnamed
        // (which exchange is unknowable), and a BOUNDARY: money into it is
        // gone, and its sole payer is the identification that matters.
        let mut offramps = 0usize;
        for (key, payers, legs, back, lovelace) in
            ledger.stakeless_exit_shapes(args.offramp_min_legs)?
        {
            if payers == 0 || payers > args.offramp_max_payers || back > 0 {
                continue;
            }
            // KEY-payment addresses only: a script taking one-way deposits is
            // a CONTRACT (a lock, a vesting schedule), not somebody's
            // exchange deposit address, and calling it an off-ramp would
            // launder "we don't decode this contract" into "money left".
            if payment_credential_is_script(&key) {
                continue;
            }
            offramps += 1;
            inferred.push(CounterpartyRow {
                key,
                name: None,
                capabilities: vec![(ProviderCapability::Offramp, Basis::Derived)],
                source: format!(
                    "inferred offramp: {legs} payments from {payers} wallet(s), \
                     {:.0} ada in, nothing ever back",
                    lovelace as f64 / 1e6
                ),
            });
        }
        if offramps > 0 {
            tracing::info!(
                offramps,
                min_legs = args.offramp_min_legs,
                "classify: per-customer OFF-RAMPS inferred from shape — one-way stakeless \
                 exits. Money into these has left the chain; the payer is the finding."
            );
        }

        let n = ledger.put_counterparty(&inferred)?;
        tracing::warn!(
            inferred = n,
            threshold = args.service_min_flows,
            "classify: services INFERRED from activity — these are guesses, not registry \
             entries. Value arriving from them will now read as a round trip rather than \
             income, so review the list below before trusting any figure that moved."
        );
        for (key, flows, vol) in busy.iter().take(args.report) {
            tracing::warn!(
                counterparty = %elide(key),
                flows,
                ada = format!("{:.0}", *vol as f64 / 1e6),
                "classify: inferred service"
            );
        }
    }

    // WHAT IT COULD NOT NAME. Ranked by value, because that is the order in
    // which adding a registry entry pays.
    let unknown = ledger.unclassified_by_value(args.report)?;
    if unknown.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        "classify: the following counterparties are UNNAMED — value arriving from any of \
         them still reads as income. A high-volume entry here is probably a DEX or service \
         missing from `address-registry`."
    );
    for (key, n, vol) in unknown {
        tracing::warn!(
            counterparty = %elide(&key),
            flows = n,
            ada = format!("{:.0}", vol as f64 / 1e6),
            "classify: unnamed"
        );
    }
    Ok(())
}

/// The majority destination of the mint's own fund splits, if one exists:
/// `(key, share_of_value, distinct_mints)`.
///
/// Majority of VALUE (not legs) across at least five mints — one big leg to a
/// one-off wallet is a payment, not a treasury pattern. Returns `None` when
/// nothing clears both bars, and the caller records that as absence.
fn dominant_mint_destination(totals: &[(String, i128, u32)]) -> Option<(String, f64, u32)> {
    let all: i128 = totals.iter().map(|(_, v, _)| *v).sum();
    if all <= 0 {
        return None;
    }
    let (key, v, mints) = totals.iter().max_by_key(|(_, v, _)| *v)?;
    let share = *v as f64 / all as f64;
    (share >= 0.5 && *mints >= 5).then(|| (key.clone(), share, *mints))
}

fn elide(s: &str) -> String {
    if s.chars().count() <= 24 {
        return s.to_string();
    }
    let head: String = s.chars().take(16).collect();
    let tail: String = s
        .chars()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use address_registry::lookup_address;

    use super::*;

    /// The treasury pattern is MAJORITY OF VALUE across a real number of
    /// mints — one big leg to a one-off wallet is a payment, and a platform's
    /// per-mint fee legs never clear the value bar. Measured on S2: the
    /// treasury took 99 of 100 legs (99.9% of value), Pillar's residual was
    /// one 42 ₳ leg.
    #[test]
    fn the_dominant_mint_destination_needs_majority_value_and_recurrence() {
        let rows = vec![
            ("treasury".to_string(), 57_209_000_000_i128, 99_u32),
            ("pillar".to_string(), 42_000_000, 1),
        ];
        let (key, share, mints) = dominant_mint_destination(&rows).expect("dominant");
        assert_eq!(key, "treasury");
        assert!(share > 0.99);
        assert_eq!(mints, 99);

        // Majority value in ONE mint: a payment, not a treasury.
        let one_off = vec![
            ("big-payee".to_string(), 900_000_000_000_i128, 1_u32),
            ("rest".to_string(), 100_000_000_000, 40),
        ];
        assert_eq!(dominant_mint_destination(&one_off), None);

        // No majority: nobody is dominant, and absence is recorded.
        let split = vec![
            ("a".to_string(), 400_i128, 20_u32),
            ("b".to_string(), 350, 20),
            ("c".to_string(), 250, 20),
        ];
        assert_eq!(dominant_mint_destination(&split), None);
        assert_eq!(dominant_mint_destination(&[]), None);
    }

    /// A wallet that placed a Splash order carries the order SCRIPT among its
    /// addresses — the registry hit identifies the script, not the person.
    /// Their ordinary key-payment address is what proves personhood.
    #[test]
    fn a_splash_customer_keeps_their_own_name() {
        let addrs = vec![
            // their actual wallet
            "addr1qy9mg28evkzcfghlrg8vjqvmmga4dmg2vwm4x2s9r2vmqtd7c9k425ezp5cw8a3ssg".to_string(),
            // their order UTxO — Splash script, THEIR stake credential
            "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207qd7c9k425ezp5cw8a3ssg".to_string(),
        ];
        assert!(!hit_names_the_party(MatchKind::VariableStakePrefix, &addrs));
    }

    /// A pool's stake credential exists only behind script addresses, so the
    /// all-script party still takes the venue's name — losing THIS is losing
    /// round-trip suppression, and the 2.9M → 68k collapse depended on it.
    #[test]
    fn an_all_script_party_is_still_the_venue() {
        let addrs = vec!["addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207qpoolcred".to_string()];
        assert!(hit_names_the_party(MatchKind::VariableStakePrefix, &addrs));
        // and a full-address registry entry vouches for its stake itself
        assert!(hit_names_the_party(MatchKind::Exact, &addrs));
    }

    /// The off-ramp verdict, on the measured shapes: dwess's real exit (one
    /// payer, 31 legs, nothing back) qualifies; a fee wallet (thousands of
    /// payers) and a rewards distributor (pays OUT) never do.
    #[test]
    fn an_offramp_needs_few_payers_and_strict_one_way() {
        let qualifies = |addr: &str, payers: u32, legs: u32, back: u32| -> bool {
            payers > 0
                && payers <= 2
                && back == 0
                && legs >= 5
                && !payment_credential_is_script(addr)
        };
        let person = "addr1v9hz2kw8csaqglxsu87m85c03pzzupefv0hxhjy6sfjt03gffa6dm";
        assert!(qualifies(person, 1, 31, 0), "dwess's exit shape");
        assert!(
            !qualifies(person, 1, 31, 1),
            "one payment back breaks one-way"
        );
        assert!(
            !qualifies(person, 3000, 40, 0),
            "a fee wallet has thousands of payers"
        );
        assert!(!qualifies(person, 1, 2, 0), "two payments is not a pattern");
        assert!(
            !qualifies(person, 0, 8, 0),
            "no identified payer, no identification"
        );
        // A one-way SCRIPT is a contract (a lock, a vesting schedule), not
        // somebody's exchange deposit address.
        assert!(
            !qualifies("addr1w8qmxkacjdffxah0l3qg8vesting", 1, 9, 0),
            "scripts are contracts, not personal exits"
        );
    }

    /// The Minswap batcher — the contract that settles swaps, and the single
    /// address most responsible for swap returns reading as income.
    #[test]
    fn the_registry_names_the_minswap_batcher_as_a_dex() {
        let addr = "addr1w8p79rpkcdz8x9d6tft0x0dx5mwuzac2sa4gm8cvkw5hcnqst2ctf";
        let cat = lookup_address(addr).expect("batcher is in the registry");
        let (cap, label) = kind_of(cat).expect("a venue");
        // DEX, emphatically not CEX: a swap is a round trip, an exchange
        // withdrawal is money from outside. Conflating them either erases real
        // income or invents it.
        assert_eq!(cap, ProviderCapability::Dex);
        assert!(cap.is_round_trip());
        assert!(!cap.is_boundary());
        assert_eq!(label.as_deref(), Some("Minswap"));
    }

    /// An ordinary wallet must NOT be named — classifying a person as a service
    /// would erase real income rather than a round trip.
    #[test]
    fn a_plain_wallet_is_left_unnamed() {
        let addr = "stake1u98f5mr0mn8tv2kqndk5cwen4uasc7cewlzdklz6y664zacl9lvjz";
        assert!(lookup_address(addr).and_then(kind_of).is_none());
    }

    /// A mint provider and an exchange are both high-fan-out services that must
    /// never expand, but they mean opposite things on a chart: an exchange is a
    /// BOUNDARY where money leaves this project's story, while a mint provider
    /// is an intermediary the project used, whose onward payments — airdrops,
    /// artist splits — ARE the project's distribution.
    ///
    /// `bank.pillar` is the live case: a mint payee AND an airdrop payer, with
    /// a fan-out rate (11.2 new payees/day) indistinguishable from the two
    /// exchanges beside it (9.4 and 12.3). Rate alone cannot separate them;
    /// the mint payment can, and it is observed rather than asserted.
    #[test]
    fn a_mint_provider_is_not_labelled_a_generic_service() {
        let mut l = crate::store::Ledger::open_in_memory().unwrap();
        l.insert_mint_payments(&[crate::store::MintPaymentRow {
            tx_hash: "mint1".into(),
            destination: "stake1pillar".into(),
            lovelace: 44_000_000,
            slot: 1,
            block_time: 1,
        }])
        .unwrap();

        let providers = l.mint_payment_destinations().unwrap();
        assert!(providers.contains(&"stake1pillar".to_string()));
        assert!(
            !providers.contains(&"stake1someexchange".to_string()),
            "an exchange never appears in a mint's fund split"
        );
    }

    /// Fan-out is only measurable for a WATCHED PARTY. For a mere counterparty
    /// we see just its dealings with the watch set, so the count is bounded by
    /// that set's size — and a rule comparing it to a threshold in the hundreds
    /// silently matches nothing, which reads as "no exchanges found" rather
    /// than "not measurable". `None` is the honest answer.
    #[test]
    fn fan_shape_refuses_to_measure_a_non_party() {
        let mut l = crate::store::Ledger::open_in_memory().unwrap();
        l.insert_unit_flows(&[crate::store::UnitFlowRow {
            tx_hash: "t1".into(),
            output_index: 0,
            party: "stake1watched".into(),
            counterparty: "stake1stranger".into(),
            unit: "lovelace".into(),
            quantity: -100,
            payers: 1,
            min_utxo: 0,
            slot: 1,
            block_time: 1,
        }])
        .unwrap();

        assert_eq!(
            l.fan_shape("stake1stranger").unwrap(),
            None,
            "a stranger's true fan-out is invisible to this walk"
        );
    }

    /// THE REASON THIS TABLE WAS RESHAPED. `bank.pillar` is a minting provider
    /// AND an airdrop payer, and those facts arrive from different evidence in
    /// different passes. Under the old single-label column the second write
    /// destroyed the first; capabilities must accumulate.
    #[test]
    fn capabilities_accumulate_across_passes_instead_of_replacing() {
        let mut l = crate::store::Ledger::open_in_memory().unwrap();
        let key = "stake1pillar".to_string();

        // Pass one: observed in a mint's fund split.
        l.put_counterparty(&[CounterpartyRow {
            key: key.clone(),
            name: None,
            capabilities: vec![(ProviderCapability::Minting, Basis::Observed)],
            source: "mint_payment fund split".into(),
        }])
        .unwrap();

        // Pass two, later, from entirely different evidence — and it names it.
        l.put_counterparty(&[CounterpartyRow {
            key: key.clone(),
            name: Some("bank.pillar".into()),
            capabilities: vec![(ProviderCapability::Airdrop, Basis::Derived)],
            source: "CIP-20 airdrop tags".into(),
        }])
        .unwrap();

        let caps = l.capabilities_of(&key).unwrap();
        assert_eq!(caps.len(), 2, "both facts survive: {caps:?}");
        assert!(
            caps.iter()
                .any(|(c, b)| *c == ProviderCapability::Minting && *b == Basis::Observed)
        );
        assert!(
            caps.iter()
                .any(|(c, b)| *c == ProviderCapability::Airdrop && *b == Basis::Derived)
        );
        assert_eq!(
            l.counterparty_name(&key).unwrap().as_deref(),
            Some("bank.pillar")
        );
    }

    /// A registry entry says WHO a counterparty is; the activity threshold only
    /// guesses WHAT it is. If the guess could overwrite the claim, a wallet
    /// known to be Minswap would degrade to an anonymous "service" on the next
    /// run — losing information, and silently.
    #[test]
    fn an_inferred_service_never_overwrites_a_registry_claim() {
        let mut l = crate::store::Ledger::open_in_memory().unwrap();
        let key = "stake1busy".to_string();
        l.put_counterparty(&[CounterpartyRow {
            key: key.clone(),
            name: Some("Minswap".into()),
            capabilities: vec![(ProviderCapability::Dex, Basis::Asserted)],
            source: "address-registry: addr1w8p…".into(),
        }])
        .unwrap();

        // The busy list is what the inference draws from, and it must already
        // exclude anything named.
        let busy = l.busy_unnamed_counterparties(0).unwrap();
        assert!(
            !busy.iter().any(|(k, _, _)| k == &key),
            "a named counterparty must not be offered up for inference"
        );
    }

    #[test]
    fn elide_keeps_both_ends_recognisable() {
        let s = "addr1w8p79rpkcdz8x9d6tft0x0dx5mwuzac2sa4gm8cvkw5hcnqst2ctf";
        let e = elide(s);
        assert!(e.starts_with("addr1w8p79rpkcd"));
        assert!(e.ends_with("st2ctf"));
        assert_eq!(elide("short"), "short");
    }
}
