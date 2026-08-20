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

use address_registry::{AddressCategory, ScriptCategory, lookup_address};
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

pub fn run(args: &ClassifyArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;
    let candidates = ledger.counterparties_with_addresses()?;
    let total = candidates.len();

    // Every counterparty the mint's own fund split paid — OBSERVED, and the
    // thing that separates a service the project USED from a venue it merely
    // passed value through.
    let mint_providers: std::collections::HashSet<String> =
        ledger.mint_payment_destinations()?.into_iter().collect();

    let mut rows: Vec<CounterpartyRow> = Vec::new();
    for (key, addrs) in candidates {
        let mut caps: Vec<(ProviderCapability, Basis)> = Vec::new();
        let mut name = None;
        let mut sources: Vec<String> = Vec::new();

        // First address the registry recognises wins; a party's addresses all
        // belong to one wallet, so two answers would be a registry conflict
        // rather than something to average.
        if let Some((addr, (cap, label))) = addrs
            .iter()
            .find_map(|a| lookup_address(a).and_then(|c| kind_of(c).map(|k| (a.clone(), k))))
        {
            caps.push((cap, Basis::Asserted));
            name = label;
            sources.push(format!("address-registry: {}", elide(&addr)));
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
        "classify: counterparties named from the address registry"
    );

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
    use super::*;

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
