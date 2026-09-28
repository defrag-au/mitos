//! Which offer outputs at an offer contract have a datum the chain can
//! actually yield — and which are permanently opaque.
//!
//! # The question
//!
//! The live-book view can see an offer output sitting at a Wayup or jpg offer
//! contract and still not know what it offers: the output carries only a datum
//! HASH, and a hash is not a datum. The frontend showed 4,255 live rows at the
//! Wayup contract against 21 readable offers, and the gap is not a decode bug —
//! it is a question about where a preimage can live.
//!
//! A preimage reaches the chain three ways:
//!
//! 1. **inline** — the output carries the datum itself;
//! 2. **witness set** — some transaction supplied the datum in order to spend an
//!    output that referenced it by hash. The ledger requires this: a Plutus
//!    spend of a hash-attached output without its datum is a `MissingDatum`
//!    failure, so spend ⇒ reveal, always;
//! 3. **metadata convention** — an off-chain agreement (jpg's labels 50+) that
//!    the creation tx also wrote the datum into auxiliary data. Deliberately
//!    NOT read here: it needs `mitos-marketplace-decode`, and mixing a
//!    convention into a census of what the chain itself guarantees is how a
//!    "recoverable" verdict turns into a claim about one vendor's serializer.
//!
//! So this walks a slot window oldest-first and classifies every output created
//! at a given offer contract:
//!
//! - `inline` — readable from the output itself;
//! - `revealed in its own tx` — hash-attached, preimage in this same tx (its
//!   witness set, or an inline datum on one of its outputs);
//! - `revealed earlier` — hash-attached, preimage already seen earlier in the
//!   window (a sibling output with the identical datum was spent or inlined);
//! - `UNREADABLE` — hash-attached, no preimage anywhere in the window: the
//!   state a live offer actually sits in;
//! - `no datum` — an output at the contract with no datum at all.
//!
//! Liveness is tracked too: an output is live unless a later tx in the window
//! spends it. The number that matters is **live UNREADABLE** — offers the
//! frontend can see and cannot name.
//!
//! # What it shows about recovery
//!
//! `unreadable at creation → later revealed` is counted, split by whether the
//! revealing tx spent one of the offer outputs carrying that hash. Because a
//! spend must reveal, that split should be nearly all of them — the empirical
//! form of the rule that a live hash-attached offer **has no preimage anywhere
//! on chain until it is spent**. The corollary is the design conclusion: the
//! only capture that can hold such a datum is one taken at creation (a mirror,
//! the vendor's own database, or a walker that persists witness datums as it
//! passes).
//!
//! # Cost
//!
//! Every transaction in the window is decoded. Reveals are keyed by datum hash,
//! so there is nothing to byte-search for and no cheap prefilter without losing
//! the reveal leg. Budget with a smoke run (`--to-slot`) before pointing it far
//! below the tip.
//!
//! # A window is not the chain
//!
//! `revealed earlier` and `live` are both relative to where the walk starts: a
//! preimage that arrived before `--from-slot`, or a spend that happened before
//! it, is invisible. A window that starts too late overstates `UNREADABLE`; a
//! window that starts early is the only way to understate it.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use mitos_chain_walk::{OutRef, decode_tx, open_blocks, slot_to_unix};
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_primitives::Hash;
use pallas_traverse::MultiEraBlock;

/// Live is index 0 so `usize::from(!live)` picks the slot without a branch.
const LIVE: usize = 0;
const SPENT: usize = 1;

#[derive(Parser)]
#[command(
    about = "Census offer-contract outputs by whether their datum is recoverable from chain artifacts"
)]
struct Args {
    /// The immutable DB root — `<data-dir>/immutable`, not the data dir.
    #[arg(long)]
    db: PathBuf,

    /// Slot to start the walk at. An EMPTY-hash fuzzy seek: the first block at
    /// slot >= this.
    #[arg(long)]
    from_slot: u64,

    /// Stop after the first block past this slot. Omitted = walk to the end.
    #[arg(long)]
    to_slot: Option<u64>,

    /// Progress line every N blocks (stderr).
    #[arg(long, default_value_t = 25)]
    progress_every: u64,

    /// Restrict a second tally to one stake credential: a `stake1…` bech32
    /// address, any Shelley address (its delegation part is taken), or 28-byte
    /// hex. This is the "what has THIS holder offered" cut — Wayup locks each
    /// offer at the franken address formed from the offer script's payment
    /// credential and the bidder's own stake part.
    #[arg(long)]
    holder: Option<String>,

    /// Print every witness datum and every output datum of transactions whose
    /// hash hex starts with this string, and keep walking. The audit path: a
    /// classification is checked by reading the transaction, not by trusting
    /// the count. Hex prefix, not a full hash, so a sample line can be pasted
    /// in as-is.
    #[arg(long)]
    trace: Option<String>,

    /// `label=<value>` or bare `<value>`; a 28-byte payment credential as hex
    /// (`cred:` / `0x` prefixes tolerated) or a Shelley address, from which the
    /// PAYMENT credential is taken. That is what an offer output is keyed by:
    /// Wayup locks each offer under the bidder's own stake part, so the
    /// contract credential is the whole address class.
    #[arg(required = true, value_name = "CONTRACT")]
    contracts: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Class {
    Inline,
    RevealedHere,
    RevealedEarlier,
    Unreadable,
    NoDatum,
}

impl Class {
    const ALL: [Class; 5] = [
        Class::Inline,
        Class::RevealedHere,
        Class::RevealedEarlier,
        Class::Unreadable,
        Class::NoDatum,
    ];

    fn idx(self) -> usize {
        match self {
            Class::Inline => 0,
            Class::RevealedHere => 1,
            Class::RevealedEarlier => 2,
            Class::Unreadable => 3,
            Class::NoDatum => 4,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Class::Inline => "inline",
            Class::RevealedHere => "revealed in its own tx",
            Class::RevealedEarlier => "revealed earlier",
            Class::Unreadable => "UNREADABLE",
            Class::NoDatum => "no datum",
        }
    }

    /// A datum can be read for an output in this class with no further capture.
    fn readable(self) -> bool {
        matches!(
            self,
            Class::Inline | Class::RevealedHere | Class::RevealedEarlier
        )
    }
}

/// One output created at a contract during the window.
struct Offer {
    contract: usize,
    /// The class decided at creation — kept so a later spend can move the
    /// count out of the LIVE column without re-deriving it.
    class: Class,
    live: bool,
    /// Whether this offer sits on a franken address carrying the `--holder`
    /// stake part — so a later reveal credits the holder tally too.
    holder_match: bool,
}

/// Where a datum preimage first appeared, in the walk.
struct Reveal {
    seq: u64,
    tx: Hash<32>,
    slot: u64,
}

#[derive(Default)]
struct Tally {
    /// `by_class[class][LIVE|SPENT]`.
    by_class: [[u64; 2]; 5],
    addresses: HashSet<String>,
    /// Up to three example lines per class — the audit trail for a
    /// classification, since a count can be wrong in ways a line cannot.
    samples: [Vec<String>; 5],
    /// Distinct datum hashes seen on this contract's outputs. `creations` far
    /// above this means the venue re-locks the same datum, which is how an
    /// unrevealed hash can still have its preimage on chain.
    distinct_hashes: HashSet<Hash<32>>,
    /// First and last slot a contract output was created at, in the window —
    /// a creation at the window's own edge means the window does not contain
    /// the contract's activity, and liveness/readability for anything older is
    /// simply not in view.
    first_seen: Option<u64>,
    last_seen: Option<u64>,
    /// Outputs that arrived with no preimage in the window.
    unreadable_created: u64,
    /// …of those, how many had one arrive later in the window.
    later_revealed: u64,
    /// …and how many of those reveals happened in a tx that spent one of the
    /// offer outputs carrying that hash — the spend-reveal mechanism itself.
    later_revealed_by_spend: u64,
}

impl Tally {
    fn add(&mut self, class: Class, live: bool, address: &str, slot: u64) {
        self.by_class[class.idx()][if live { LIVE } else { SPENT }] += 1;
        self.addresses.insert(address.to_string());
        self.first_seen.get_or_insert(slot);
        self.last_seen = Some(slot);
    }

    /// Move a count from LIVE to SPENT — the output was spent inside the
    /// window. The creation class does not change; only its liveness does.
    fn mark_spent(&mut self, class: Class) {
        self.by_class[class.idx()][LIVE] -= 1;
        self.by_class[class.idx()][SPENT] += 1;
    }

    fn push_sample(&mut self, class: Class, line: String) {
        let samples = &mut self.samples[class.idx()];
        if samples.len() < 3 {
            samples.push(line);
        }
    }

    fn count(&self, class: Class, live: bool) -> u64 {
        self.by_class[class.idx()][if live { LIVE } else { SPENT }]
    }

    fn total(&self, live: bool) -> u64 {
        Class::ALL.iter().map(|c| self.count(*c, live)).sum()
    }

    fn readable(&self, live: bool) -> u64 {
        Class::ALL
            .iter()
            .filter(|c| c.readable())
            .map(|c| self.count(*c, live))
            .sum()
    }
}

struct Walk {
    tallies: Vec<Tally>,
    /// Same shape, restricted to outputs whose address carries `holder` as its
    /// stake part. Empty when `--holder` was not given.
    holder_tallies: Vec<Tally>,
    holder: Option<[u8; 28]>,
    contracts: Vec<(String, [u8; 28])>,
    /// Every contract output created in the window, by outref — the liveness
    /// and reveal-linkage index.
    offers: HashMap<OutRef, Offer>,
    /// Datum hash → where its preimage first appeared. Only the first sighting
    /// is kept: "was it on chain before this tx" is the whole question.
    first_reveal: HashMap<Hash<32>, Reveal>,
    /// Hash-attached contract outputs whose preimage had not arrived yet, by
    /// hash — so a later reveal can be traced back to the offers it unblocks.
    unreadable_by_hash: HashMap<Hash<32>, Vec<OutRef>>,
    seq: u64,
    txs: u64,
    blocks: u64,
    bytes: u64,
}

impl Walk {
    fn new(contracts: Vec<(String, [u8; 28])>, holder: Option<[u8; 28]>) -> Self {
        let tallies = contracts.iter().map(|_| Tally::default()).collect();
        let holder_tallies = contracts.iter().map(|_| Tally::default()).collect();
        Walk {
            tallies,
            holder_tallies,
            holder,
            contracts,
            offers: HashMap::new(),
            first_reveal: HashMap::new(),
            unreadable_by_hash: HashMap::new(),
            seq: 0,
            txs: 0,
            blocks: 0,
            bytes: 0,
        }
    }

    /// Record a preimage that this tx has just put on chain, and credit any
    /// contract output it unblocks.
    fn note_reveal(&mut self, hash: Hash<32>, inputs: &[OutRef], slot: u64, tx: Hash<32>) {
        if self.first_reveal.contains_key(&hash) {
            return;
        }
        self.first_reveal.insert(
            hash,
            Reveal {
                seq: self.seq,
                tx,
                slot,
            },
        );
        let Some(orefs) = self.unreadable_by_hash.remove(&hash) else {
            return;
        };
        for oref in orefs {
            let by_spend = inputs.contains(&oref);
            let Some(offer) = self.offers.get_mut(&oref) else {
                continue;
            };
            let (contract, holder_match) = (offer.contract, offer.holder_match);
            let tally = &mut self.tallies[contract];
            tally.later_revealed += 1;
            if by_spend {
                tally.later_revealed_by_spend += 1;
            }
            if holder_match {
                let tally = &mut self.holder_tallies[contract];
                tally.later_revealed += 1;
                if by_spend {
                    tally.later_revealed_by_spend += 1;
                }
            }
        }
    }

    fn classify(
        &mut self,
        contract: usize,
        oref: OutRef,
        address: &str,
        slot: u64,
        datum: &Datum,
        holder_match: bool,
    ) {
        let reveal = match datum {
            Datum::Hash(h) => self.first_reveal.get(h),
            _ => None,
        };
        let class = match datum {
            Datum::Inline => Class::Inline,
            Datum::Absent => Class::NoDatum,
            Datum::Hash(_) => match reveal {
                None => Class::Unreadable,
                Some(r) if r.seq == self.seq => Class::RevealedHere,
                Some(_) => Class::RevealedEarlier,
            },
        };
        // Built for every class, not just the worrying one: the audit line is
        // how a classification gets checked rather than believed.
        let sample = match datum {
            Datum::Inline => Some(format!(
                "{}#{} slot {slot} {address} INLINE datum on the output",
                hex::encode(oref.0),
                oref.1,
            )),
            Datum::Hash(h) => Some(match reveal {
                Some(r) if r.seq == self.seq => format!(
                    "{}#{} slot {slot} {address} hash {} — preimage in THIS tx's witness set",
                    hex::encode(oref.0),
                    oref.1,
                    hex::encode(h.as_slice()),
                ),
                Some(r) => format!(
                    "{}#{} slot {slot} {address} hash {} — preimage first on chain in {}@{} (earlier)",
                    hex::encode(oref.0),
                    oref.1,
                    hex::encode(h.as_slice()),
                    hex::encode(r.tx.as_slice()),
                    r.slot,
                ),
                None => format!(
                    "{}#{} slot {slot} {address} hash {} — NO preimage anywhere in the window",
                    hex::encode(oref.0),
                    oref.1,
                    hex::encode(h.as_slice()),
                ),
            }),
            Datum::Absent => None,
        };
        let hash = match datum {
            Datum::Hash(h) => Some(*h),
            _ => None,
        };

        let tally = &mut self.tallies[contract];
        tally.add(class, true, address, slot);
        if let Some(h) = hash {
            tally.distinct_hashes.insert(h);
        }
        if let Some(s) = &sample {
            tally.push_sample(class, s.clone());
        }
        if class == Class::Unreadable {
            tally.unreadable_created += 1;
            // Only an unreadable-at-creation output can be UNBLOCKED by a
            // later reveal, so only these are indexed for one.
            if let Some(h) = hash {
                self.unreadable_by_hash.entry(h).or_default().push(oref);
            }
        }
        if holder_match {
            let tally = &mut self.holder_tallies[contract];
            tally.add(class, true, address, slot);
            if let Some(h) = hash {
                tally.distinct_hashes.insert(h);
            }
            if let Some(s) = &sample {
                tally.push_sample(class, s.clone());
            }
            if class == Class::Unreadable {
                tally.unreadable_created += 1;
            }
        }
        self.offers.insert(
            oref,
            Offer {
                contract,
                class,
                live: true,
                holder_match,
            },
        );
    }
}

/// The datum state of one output, as decoded.
enum Datum {
    Inline,
    Hash(Hash<32>),
    Absent,
}

/// Every transaction's body and witness-set bytes in the block, in block order.
/// Both borrow the block buffer, so their offsets within it are pointer
/// arithmetic — the same fact `extract_chunk` relies on to record a body span.
///
/// This exists because the obvious route to a witness datum — read past the end of
/// the body — is not a route: Shelley-onward blocks keep the bodies and the witness
/// sets in TWO SEPARATE ARRAYS (`[header, bodies, witness_sets, aux, invalid]`), so
/// the bytes after a body are the next body. A witness set has to be addressed
/// through a span the index recorded, or recovered from a capture taken on a walk.
fn tx_slices<'a>(block: &'a MultiEraBlock<'_>) -> Vec<(&'a [u8], &'a [u8])> {
    macro_rules! pairs {
        ($b:expr) => {
            $b.transaction_bodies
                .iter()
                .zip($b.transaction_witness_sets.iter())
                .map(|(b, w)| (b.raw_cbor(), w.raw_cbor()))
                .collect()
        };
    }
    match block {
        MultiEraBlock::AlonzoCompatible(b, _) => pairs!(b),
        MultiEraBlock::Babbage(b) => pairs!(b),
        MultiEraBlock::Conway(b) => pairs!(b),
        _ => Vec::new(),
    }
}

fn payment_cred(address: &str) -> Option<[u8; 28]> {
    address_creds(address).map(|(pay, _)| pay)
}

/// Payment credential, and the delegation credential when the address has one.
/// `None` stake for enterprise/pointer/Byron shapes — a franken offer address
/// always has one, because that is where the bidder's stake part lives.
fn address_creds(address: &str) -> Option<([u8; 28], Option<[u8; 28]>)> {
    let Address::Shelley(sh) = Address::from_bech32(address).ok()? else {
        return None;
    };
    let pay = match sh.payment() {
        ShelleyPaymentPart::Key(h) | ShelleyPaymentPart::Script(h) => **h,
    };
    let stake = match sh.delegation() {
        ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => Some(**h),
        ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => None,
    };
    Some((pay, stake))
}

fn cred28(bytes: &[u8]) -> Option<[u8; 28]> {
    if bytes.len() != 28 {
        return None;
    }
    let mut out = [0u8; 28];
    out.copy_from_slice(bytes);
    Some(out)
}

fn strip_prefixes(value: &str) -> &str {
    let v = value.strip_prefix("cred:").unwrap_or(value);
    v.strip_prefix("0x").unwrap_or(v)
}

fn parse_credential(value: &str, what: &str) -> Result<[u8; 28]> {
    let value = strip_prefixes(value);
    if value.len() == 56 && value.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = hex::decode(value).with_context(|| format!("{what} credential hex"))?;
        let mut cred = [0u8; 28];
        cred.copy_from_slice(&bytes);
        return Ok(cred);
    }
    if let Ok(Address::Stake(st)) = Address::from_bech32(value) {
        return cred28(st.payload().as_ref())
            .with_context(|| format!("{what} stake address payload is not 28 bytes"));
    }
    let (pay, stake) = address_creds(value).with_context(|| {
        format!("{what} `{value}` is neither 28-byte hex, a stake address, nor a Shelley address")
    })?;
    // A full address names both credentials; which one is wanted depends on the
    // question, so the two callers take the part they mean rather than sharing
    // a guess.
    Ok(stake.unwrap_or(pay))
}

fn parse_contract(spec: &str) -> Result<(String, [u8; 28])> {
    let (label, value) = match spec.split_once('=') {
        Some((l, v)) => (l.to_string(), v),
        None => (String::new(), spec),
    };
    let value = strip_prefixes(value);
    let cred = if value.len() == 56 && value.chars().all(|c| c.is_ascii_hexdigit()) {
        parse_credential(value, "contract")?
    } else {
        payment_cred(value).with_context(|| {
            format!(
                "contract `{value}` is neither 28-byte hex nor a Shelley address \
                 with a payment part"
            )
        })?
    };
    let label = if label.is_empty() {
        hex::encode(&cred[..4])
    } else {
        label
    };
    Ok((label, cred))
}

fn thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn main() -> Result<()> {
    let args = Args::parse();
    let contracts = args
        .contracts
        .iter()
        .map(|s| parse_contract(s))
        .collect::<Result<Vec<_>>>()?;
    let holder = args
        .holder
        .as_deref()
        .map(|h| parse_credential(h, "holder"))
        .transpose()?;

    eprintln!(
        "offer_datums: opening {} as an immutable DB",
        args.db.display()
    );
    eprintln!(
        "  from_slot {} ({})",
        args.from_slot,
        slot_to_unix(args.from_slot)
    );
    for (label, cred) in &contracts {
        eprintln!("  contract {label}  {}", hex::encode(cred));
    }
    if let Some(h) = &holder {
        eprintln!("  holder stake {}", hex::encode(h));
    }

    let started = Instant::now();
    let mut walk = Walk::new(contracts, holder);
    let blocks = open_blocks(&args.db, Some((args.from_slot, Vec::new())))
        .with_context(|| format!("opening immutable DB at {}", args.db.display()))?;

    let mut last_slot = args.from_slot;
    for block in blocks {
        let bytes = block.map_err(|e| anyhow::anyhow!("reading block: {e:?}"))?;
        walk.bytes += bytes.len() as u64;
        let blk = MultiEraBlock::decode(&bytes).map_err(|e| {
            anyhow::anyhow!(
                "decoding block at ~{} (slot {last_slot}): {e:?}",
                walk.blocks
            )
        })?;
        walk.blocks += 1;
        last_slot = blk.slot();
        if let Some(t) = args.to_slot
            && last_slot > t
        {
            eprintln!(
                "  reached --to-slot {t} at block {}, slot {last_slot}",
                walk.blocks
            );
            break;
        }

        // `tx_at` is the tx's position in the block's BODY ARRAY, which is what
        // the witness-set array below is index-aligned against.
        for (tx_at, tx) in blk.txs().into_iter().enumerate() {
            // PHASE-2: a tx that failed validation moved nothing — it creates
            // no output to classify and put no witness datum on chain.
            if !tx.is_valid() {
                continue;
            }
            let d = decode_tx(&tx);
            walk.seq += 1;
            walk.txs += 1;

            if let Some(prefix) = &args.trace
                && hex::encode(d.tx_hash.as_slice()).starts_with(prefix)
            {
                println!(
                    "TRACE tx {} slot {last_slot}",
                    hex::encode(d.tx_hash.as_slice())
                );
                for (h, bytes) in &d.witness_datums {
                    println!(
                        "   witness datum {}  ({} bytes CBOR)",
                        hex::encode(h.as_slice()),
                        bytes.len()
                    );
                }
                for i in &d.inputs {
                    println!("   input {}#{}", hex::encode(i.oref.0.as_slice()), i.oref.1);
                }
                for o in &d.outputs {
                    let datum = match (&o.inline_datum, o.datum_hash) {
                        (Some(bytes), _) => {
                            let h = o.datum_hash.map(|h| hex::encode(h.as_slice()));
                            format!(
                                "inline ({} bytes, hash {})",
                                bytes.len(),
                                h.unwrap_or_default()
                            )
                        }
                        (None, Some(h)) => format!("hash {}", hex::encode(h.as_slice())),
                        (None, None) => "none".to_string(),
                    };
                    println!(
                        "   output #{} {} {} lovelace datum {datum}",
                        o.index, o.address, o.lovelace
                    );
                }
                // Where the block keeps this tx's witness set, against where it keeps
                // the body: the answer decides whether a reader holding only a body
                // span can reach the datum.
                let base = bytes.as_ptr() as usize;
                let slices = tx_slices(&blk);
                // This tx, and one either side of it — enough to show bodies running
                // back to back while the witness sets sit elsewhere. The three are
                // already ascending, so a repeat is skipped rather than sorted away.
                let mut previous: Option<usize> = None;
                for at in [
                    tx_at.saturating_sub(1),
                    tx_at,
                    (tx_at + 1).min(slices.len().saturating_sub(1)),
                ] {
                    if previous == Some(at) {
                        continue;
                    }
                    previous = Some(at);
                    let Some((body, witness)) = slices.get(at) else {
                        continue;
                    };
                    let (body_at, wit_at) = (
                        body.as_ptr() as usize - base,
                        witness.as_ptr() as usize - base,
                    );
                    println!(
                        "   block tx #{at} ({} of {}): body @{body_at}+{} witness @{wit_at}+{} {}",
                        at + 1,
                        slices.len(),
                        body.len(),
                        witness.len(),
                        if wit_at == body_at + body.len() {
                            "ADJACENT"
                        } else {
                            "NOT adjacent"
                        },
                    );
                }
            }

            // Preimages first, so an output created in this tx sees its own
            // tx's reveals.
            let input_orefs: Vec<OutRef> = d.inputs.iter().map(|i| i.oref).collect();
            for h in d.witness_datums.keys() {
                walk.note_reveal(*h, &input_orefs, last_slot, d.tx_hash);
            }
            for o in &d.outputs {
                if o.inline_datum.is_some()
                    && let Some(h) = o.datum_hash
                {
                    walk.note_reveal(h, &input_orefs, last_slot, d.tx_hash);
                }
            }

            // Liveness: anything of ours this tx spends is not live any more,
            // and the count moves out of the LIVE column. This is what makes
            // "live UNREADABLE" a statement about the UTxO set rather than
            // about the creations.
            for i in &d.inputs {
                if let Some(offer) = walk.offers.get_mut(&i.oref)
                    && offer.live
                {
                    offer.live = false;
                    let (contract, class, holder_match) =
                        (offer.contract, offer.class, offer.holder_match);
                    walk.tallies[contract].mark_spent(class);
                    if holder_match {
                        walk.holder_tallies[contract].mark_spent(class);
                    }
                }
            }

            for o in &d.outputs {
                let Some((pay, stake)) = address_creds(&o.address) else {
                    continue;
                };
                let Some(contract) = walk.contracts.iter().position(|(_, c)| *c == pay) else {
                    continue;
                };
                let holder_match = matches!((&walk.holder, stake), (Some(h), Some(s)) if *h == s);
                let datum = if o.inline_datum.is_some() {
                    Datum::Inline
                } else {
                    match o.datum_hash {
                        Some(h) => Datum::Hash(h),
                        None => Datum::Absent,
                    }
                };
                walk.classify(
                    contract,
                    (d.tx_hash, o.index),
                    &o.address,
                    last_slot,
                    &datum,
                    holder_match,
                );
            }
        }

        if walk.blocks.is_multiple_of(args.progress_every) {
            let per_contract: String = walk
                .tallies
                .iter()
                .zip(&walk.contracts)
                .map(|(t, (label, _))| {
                    format!(
                        " [{} live {} readable {} unreadable {}]",
                        label,
                        thousands(t.total(true)),
                        thousands(t.readable(true)),
                        thousands(t.count(Class::Unreadable, true)),
                    )
                })
                .collect();
            eprintln!(
                "  block {} slot {last_slot} txs {} offers {} elapsed {:.0}s{per_contract}",
                thousands(walk.blocks),
                thousands(walk.txs),
                thousands(walk.offers.len() as u64),
                started.elapsed().as_secs_f64(),
            );
        }
    }

    report(&walk, args.from_slot, last_slot, started.elapsed());
    Ok(())
}

fn report(walk: &Walk, from_slot: u64, last_slot: u64, elapsed: std::time::Duration) {
    let secs = elapsed.as_secs_f64().max(0.001);
    println!();
    println!(
        "walked {} blocks / {} txs / {:.1} GB   slots {}..{}   ({}..{})   in {:.0}s ({:.0} MB/s)",
        thousands(walk.blocks),
        thousands(walk.txs),
        walk.bytes as f64 / 1e9,
        from_slot,
        last_slot,
        slot_to_unix(from_slot),
        slot_to_unix(last_slot),
        secs,
        walk.bytes as f64 / 1e6 / secs,
    );
    println!(
        "  datum hashes seen (any preimage on chain): {}",
        thousands(walk.first_reveal.len() as u64)
    );

    for (i, (label, cred)) in walk.contracts.iter().enumerate() {
        println!();
        println!("── {label}  {} ─────────────────────", hex::encode(cred));
        print_tally(&walk.tallies[i]);
        if let Some(h) = walk.holder {
            println!();
            println!(
                "   ┈┈ restricted to stake {} (franken addresses)\n",
                hex::encode(h)
            );
            print_tally(&walk.holder_tallies[i]);
        }
        println!(
            "   (window is authoritative only for creations INSIDE it; a preimage or spend before --from-slot is invisible)"
        );
    }
}

fn print_tally(t: &Tally) {
    match (t.first_seen, t.last_seen) {
        (Some(a), Some(b)) => println!(
            "   creations span slots {a}..{b}   ({}..{})  — a creation AT the window edge means activity continues outside it",
            slot_to_unix(a),
            slot_to_unix(b),
        ),
        _ => {
            println!("   no outputs created in the window");
            return;
        }
    }
    println!(
        "   outputs created in window : {}   (live {}, spent {})",
        thousands(t.total(true) + t.total(false)),
        thousands(t.total(true)),
        thousands(t.total(false)),
    );
    println!(
        "   distinct addresses seen   : {}   (created, live or spent)",
        thousands(t.addresses.len() as u64)
    );
    println!(
        "   distinct datum hashes    : {}   (creations far above this = a re-locked datum, whose preimage a sibling already published)",
        thousands(t.distinct_hashes.len() as u64)
    );
    for class in Class::ALL {
        let live = t.count(class, true);
        let spent = t.count(class, false);
        if live + spent == 0 {
            continue;
        }
        println!(
            "     {:<22} created {:>9}   live {:>9}  spent {:>9}",
            class.label(),
            thousands(live + spent),
            thousands(live),
            thousands(spent)
        );
    }
    let live = t.total(true);
    let readable = t.readable(true);
    let unreadable = t.count(Class::Unreadable, true);
    let pct = |n: u64| {
        if live == 0 {
            0.0
        } else {
            n as f64 * 100.0 / live as f64
        }
    };
    println!(
        "   live readable             : {} of {}  ({:.1}%)",
        thousands(readable),
        thousands(live),
        pct(readable),
    );
    println!(
        "   live UNREADABLE           : {}  ({:.1}%)",
        thousands(unreadable),
        pct(unreadable),
    );
    println!(
        "   unreadable at creation    : {}  → later revealed {}, of them {} by a spend of one",
        thousands(t.unreadable_created),
        thousands(t.later_revealed),
        thousands(t.later_revealed_by_spend),
    );
    for class in Class::ALL {
        if t.samples[class.idx()].is_empty() {
            continue;
        }
        println!("   samples [{}]:", class.label());
        for s in &t.samples[class.idx()] {
            println!("     {s}");
        }
    }
}
