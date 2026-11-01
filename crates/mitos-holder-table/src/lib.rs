//! The holder table — the first production aggregate on the fold plane.
//!
//! For one policy, the table is every current holder and the policy's assets they hold,
//! grouped by stake credential, classified with [`mitos_cohort`], and sorted so two runs
//! over the same blocks are byte-comparable.
//!
//! # A position, not a history
//!
//! "Current" is the load-bearing word. The aggregate keeps the policy's **live** outputs
//! and removes one when it is spent, so an asset that was created and later moved never
//! reaches the table. Accumulating creates without their spends gives a holder everything
//! it has ever held — which is what the first version of this crate did, and it reported
//! 362 holders and 1,421,293,459 units for a policy whose live set is 294 holders and
//! 68,994,134. The set is policy-filtered, so it is 396 outputs for that policy rather
//! than the chain's forty million.
//!
//! # What this is not yet
//!
//! It does not do `holder-distribution`'s LP-pool or CrowdLock-vesting decomposition — that
//! is the module's snapshot-time transform, and it needs the pool's datum and the lock
//! set, which are separate inputs. The pilot uses this table to prove the fold shape and
//! the holder identity; the decomposed view follows.
//!
//! Classification is only as good as the registries the caller supplies. Empty `pools` and
//! `lock_creds` mean a DEX pool or a lock script is classified by its payment shape alone
//! (`Script`), which is honest but not the module's final answer. Supplying them is the
//! caller's job and does not change the aggregate.
//!
//! The aggregate is pure: it observes [`DecodedBlock`]s and produces the table. It does
//! not read a chunk, write a file, or know how it was walked.
//!
//! # Two shapes, one artifact
//!
//! [`HolderTableAggregate`] reads the decoded stream and keeps the policy's live outputs as
//! the walk goes. [`HolderTableView`] reads the published book after the walk and enumerates
//! the same outputs through `by_policy`. Both are shipped deliberately: they cannot share a
//! mistake, because one never touches the book and the other never observes a block, and the
//! pilot asserts they agree. They share the `table_from` fold, so they can only disagree
//! about *which* outputs are live, never about what to do with them.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chain_decode::{DecodedBlock, DecodedOutput};
use mitos_cohort::Cohort;
use mitos_fold::{Aggregate, AggregateResult, SchemaId, Stamp, View};
use pallas_addresses::{Address, ShelleyDelegationPart};

/// An output reference: origin tx hash and output index, as a hashable key.
///
/// `chain_decode::OutRef` carries a `Hash<32>`, which is not `Ord` — and a `BTreeMap`
/// is what keeps the finished table's row order independent of the walk.
type OutRefKey = ([u8; 32], u32);

/// How a holder is identified.
///
/// Mirrors the wire vocabulary: a delegated address is grouped by its 28-byte stake
/// credential; an enterprise address has no such grouping and is keyed by its full bech32
/// form. Pointer-stake and Byron addresses are dropped — they have no stable holder
/// identity and the module drops them for the same reason.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HolderKey {
    /// A delegated address, keyed by its stake credential.
    Stake([u8; 28]),
    /// An enterprise address, keyed by its full bech32 form.
    Enterprise(String),
}

/// One holder's stake of the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderRow {
    /// The holder's identity.
    pub holder: HolderKey,
    /// The canonical (lexicographically smallest) address the holder was seen at.
    pub address: String,
    /// The cohort the canonical address was classified as.
    pub cohort: Cohort,
    /// The policy's assets held, sorted by asset name.
    pub assets: Vec<AssetBalance>,
}

/// One asset name and how much of it the holder holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetBalance {
    /// The on-chain asset name bytes. Empty is legal for a single-fungible policy.
    pub name: Vec<u8>,
    /// The summed quantity across every output the holder had.
    pub quantity: u64,
}

/// One policy's holders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderTable {
    /// The 28-byte policy this table covers.
    pub policy: [u8; 28],
    /// The holders, sorted by [`HolderKey`].
    pub holders: Vec<HolderRow>,
}

impl HolderTable {
    /// The rows file's name inside an artifact directory.
    pub const ROWS_FILE: &'static str = "holders.tsv";

    /// The envelope extension tag that names the policy this table covers.
    ///
    /// The envelope generalises over artifacts, so it has no field for "which policy" — the
    /// table's subject is exactly the kind of additive field `Extension` exists for. The body
    /// is the 28 policy bytes. Tag 1 is assigned here and never reused, per the contract.
    pub const POLICY_EXTENSION: u32 = 1;

    /// The policy an artifact's extensions name, if they name one.
    ///
    /// `None` for an artifact that carries no policy field or one of the wrong length — a
    /// reader cannot key a table on a policy that is not 28 bytes, and inventing a default
    /// would attach the rows to somebody else's token.
    pub fn policy_of(extensions: &[mitos_fold::Extension]) -> Option<[u8; 28]> {
        extensions
            .iter()
            .find(|extension| extension.tag == Self::POLICY_EXTENSION)?
            .body
            .as_slice()
            .try_into()
            .ok()
    }

    /// How many `(holder, asset)` balances the file holds.
    ///
    /// One line each, so this is the file's data-row count rather than its holder count.
    pub fn balances(&self) -> usize {
        self.holders.iter().map(|row| row.assets.len()).sum()
    }

    /// Render the table as its rows file.
    ///
    /// **TSV, deliberately and temporarily.** The row encoding is the one part of this
    /// artifact that will change — a binary sorted array is smaller and pages without
    /// parsing — and everything around it is encoding-agnostic: the envelope names the file
    /// and its length, `counts` says what is in it, and a reader checks the length before
    /// trusting it. Replacing this function is the whole of that change.
    pub fn to_rows(&self) -> String {
        let mut text = String::new();
        writeln!(
            &mut text,
            "holder\taddress\tcohort\tasset_name_hex\tquantity"
        )
        .expect("writing to a String cannot fail");
        for row in &self.holders {
            let holder = match &row.holder {
                HolderKey::Stake(credential) => hex(credential),
                HolderKey::Enterprise(address) => address.clone(),
            };
            for asset in &row.assets {
                writeln!(
                    &mut text,
                    "{holder}\t{}\t{}\t{}\t{}",
                    row.address,
                    row.cohort.as_str(),
                    hex(&asset.name),
                    asset.quantity,
                )
                .expect("writing to a String cannot fail");
            }
        }
        text
    }

    /// How many distinct holders the table holds.
    pub fn distinct_holders(&self) -> usize {
        self.holders.len()
    }

    /// The summed quantity of every asset under the policy, across every holder.
    ///
    /// `u128` for the same reason the book sums lovelace in one: a token near `u64::MAX`
    /// supply, held by many addresses, overflows a `u64` accumulator and reports a
    /// fabricated number.
    pub fn total_quantity(&self) -> u128 {
        self.holders
            .iter()
            .flat_map(|row| &row.assets)
            .map(|asset| u128::from(asset.quantity))
            .sum()
    }

    /// The row for one holder, if present.
    pub fn holder(&self, key: &HolderKey) -> Option<&HolderRow> {
        self.holders.iter().find(|row| &row.holder == key)
    }
}

/// The aggregate state. One instance covers one policy for one walk.
pub struct HolderTableAggregate {
    policy: [u8; 28],
    pools: Vec<String>,
    lock_creds: Vec<[u8; 28]>,
    /// The policy's live outputs, by outref. A spend removes one.
    live: BTreeMap<OutRefKey, LiveOutput>,
}

/// One live output that holds the policy.
struct LiveOutput {
    holder: HolderKey,
    address: String,
    /// The policy's assets in this output, by name. Quantities are summed per name at
    /// `finish` rather than kept per output, so a row that holds the same name twice —
    /// illegal on chain, but not something this aggregate needs to assume — still totals.
    assets: Vec<(Vec<u8>, u64)>,
}

struct PendingRow {
    address: String,
    assets: BTreeMap<Vec<u8>, u64>,
}

impl HolderTableAggregate {
    /// Build an aggregate for `policy`.
    ///
    /// `pools` are exact addresses and `lock_creds` are 28-byte payment credentials, the
    /// same two registries [`mitos_cohort::classify`] takes. Empty vectors are a valid
    /// configuration; see the module docs for what that leaves unclassified.
    pub fn new(policy: [u8; 28], pools: Vec<String>, lock_creds: Vec<[u8; 28]>) -> Self {
        Self {
            policy,
            pools,
            lock_creds,
            live: BTreeMap::new(),
        }
    }

    /// The policy this aggregate is collecting.
    pub fn policy(&self) -> [u8; 28] {
        self.policy
    }
}

impl Aggregate<DecodedBlock> for HolderTableAggregate {
    type Artifact = HolderTable;

    fn schema(&self) -> SchemaId {
        SchemaId::new("holder-table", 1)
    }

    fn observe(&mut self, block: &DecodedBlock) -> AggregateResult<()> {
        for tx in &block.txs {
            // Spends before creates. A transaction cannot spend its own output, so the
            // order does not change the answer; it states the intent.
            for input in &tx.inputs {
                // A miss is the ordinary case, not an error: the output was created
                // before the window opened, or it is another policy's.
                self.live.remove(&outref_key(input.oref.0, input.oref.1));
            }
            for output in &tx.outputs {
                let assets = self.policy_assets(output)?;
                if assets.is_empty() {
                    continue;
                }
                let Some(holder) = holder_key(&output.address) else {
                    continue;
                };
                self.live.insert(
                    outref_key(tx.tx_hash, output.index),
                    LiveOutput {
                        holder,
                        address: output.address.clone(),
                        assets,
                    },
                );
            }
        }
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<HolderTable> {
        let Self {
            policy,
            pools,
            lock_creds,
            live,
        } = *self;
        table_from(policy, &pools, &lock_creds, live.into_values())
    }
}

impl HolderTableAggregate {
    /// The policy's assets in one output — empty when it holds none.
    ///
    /// An unmeasured quantity is an error rather than a zero: a decode that could have
    /// supplied the amount and did not is a bug, and a zero would hide it inside a total
    /// that looks like an answer.
    fn policy_assets(&self, output: &DecodedOutput) -> AggregateResult<Vec<(Vec<u8>, u64)>> {
        let mut held = Vec::new();
        for asset in &output.assets {
            if asset.policy != self.policy {
                continue;
            }
            let Some(quantity) = asset.quantity else {
                let index = output.index;
                let name = String::from_utf8_lossy(&asset.name);
                return Err(std::io::Error::other(format!(
                    "output {index} carries an unmeasured quantity of {name}"
                ))
                .into());
            };
            held.push((asset.name.clone(), quantity));
        }
        Ok(held)
    }
}

/// Lowercase hex, for the rows file's credential and asset-name columns.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

/// A hash and an index as a hashable key.
///
/// Generic over the hash's `AsRef` so the crate does not need `pallas-primitives` in its
/// manifest for one conversion.
fn outref_key(hash: impl AsRef<[u8]>, index: u32) -> OutRefKey {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(hash.as_ref());
    (bytes, index)
}

/// Fold live outputs into a table.
///
/// The one place a table is built, shared by both shapes. They differ only in where they get
/// their live set: the aggregate tracks it through the walk, the view enumerates
/// `by_policy`. Keeping the fold in one function is what makes "the two shapes agree" a
/// statement about the live set rather than about two copies of the same arithmetic.
fn table_from(
    policy: [u8; 28],
    pools: &[String],
    lock_creds: &[[u8; 28]],
    live: impl Iterator<Item = LiveOutput>,
) -> AggregateResult<HolderTable> {
    let mut holders: BTreeMap<HolderKey, PendingRow> = BTreeMap::new();
    for output in live {
        let row = holders.entry(output.holder).or_insert_with(|| PendingRow {
            address: output.address.clone(),
            assets: BTreeMap::new(),
        });
        // The module classifies the smallest address it saw, not the first: the same stake
        // credential can appear at a key address and at a script address, and the cohort
        // must not depend on which output was met first.
        if output.address < row.address {
            row.address = output.address.clone();
        }
        for (name, quantity) in output.assets {
            let total = row.assets.entry(name).or_insert(0);
            *total = total
                .checked_add(quantity)
                .ok_or_else(|| std::io::Error::other("holder-table quantity overflow"))?;
        }
    }

    let holders = holders
        .into_iter()
        .map(|(holder, row)| {
            let classification = mitos_cohort::classify(&row.address, pools, lock_creds);
            let assets = row
                .assets
                .into_iter()
                .map(|(name, quantity)| AssetBalance { name, quantity })
                .collect();
            HolderRow {
                holder,
                address: row.address,
                cohort: classification.cohort,
                assets,
            }
        })
        .collect();

    Ok(HolderTable { policy, holders })
}

/// The holder table as a projection over the published book.
///
/// The same artifact as [`HolderTableAggregate`], from the other side of the walk. The book
/// already holds exactly the set the aggregate tracks — every live output — so a view of it
/// is a page through `by_policy` rather than a second fold over the chain. It is also much
/// cheaper for a large policy: the walk has already paid to write the rows.
///
/// It cannot run until the book is finished, which is the whole reason it is a [`View`] and
/// not an [`Aggregate`].
pub struct HolderTableView {
    policy: [u8; 28],
    pools: Vec<String>,
    lock_creds: Vec<[u8; 28]>,
    live: Vec<LiveOutput>,
    refs: u64,
    dropped: u64,
}

impl HolderTableView {
    /// Build a view for `policy`. The two registries are as [`HolderTableAggregate::new`].
    pub fn new(policy: [u8; 28], pools: Vec<String>, lock_creds: Vec<[u8; 28]>) -> Self {
        Self {
            policy,
            pools,
            lock_creds,
            live: Vec::new(),
            refs: 0,
            dropped: 0,
        }
    }

    /// The policy this view is projecting.
    pub fn policy(&self) -> [u8; 28] {
        self.policy
    }

    /// How many live outputs the book named for the policy — after [`View::view`].
    ///
    /// One per `(row, policy)`, so a row holding two assets of the policy counts once. It is
    /// the number `LARGE_POLICY_VIEWS.md` calls the policy's refs, and comparing it with the
    /// aggregate's own count is how a projection that silently lost rows is caught.
    pub fn refs(&self) -> u64 {
        self.refs
    }

    /// How many of those rows had no holder identity — pointer-stake or Byron addresses.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Build the table without a stamp.
    ///
    /// The artifact does not read the stamp — the envelope carries it — so a caller
    /// inspecting a published book has nothing to stamp with and nothing to gain from
    /// inventing one. A build goes through [`ViewRegistry`](mitos_fold::ViewRegistry) and
    /// stamps the artifact it produced.
    pub fn table(self) -> AggregateResult<HolderTable> {
        table_from(
            self.policy,
            &self.pools,
            &self.lock_creds,
            self.live.into_iter(),
        )
    }
}

impl View<live_book::Base> for HolderTableView {
    type Artifact = HolderTable;

    fn schema(&self) -> SchemaId {
        SchemaId::new("holder-table", 1)
    }

    fn view(&mut self, book: &live_book::Base) -> AggregateResult<()> {
        let mut after = None;
        loop {
            let page = book.by_policy(&self.policy, after, live_book::MAX_PAGE)?;
            for (hash, index) in &page.refs {
                self.refs += 1;
                // `utxo` is the zero-copy read, so a row that does not parse is `None`.
                // `by_policy` named it, so it is a row: refusing is the only honest answer.
                let Some(row) = book.utxo(hash, *index)? else {
                    return Err(std::io::Error::other(format!(
                        "by_policy named an outref at index {index} whose row did not read back"
                    ))
                    .into());
                };
                let Some((holder, address)) = holder_key_bytes(row.address()) else {
                    self.dropped += 1;
                    continue;
                };
                let mut assets = Vec::new();
                for asset in row.assets() {
                    // The row's own interned id is exact only for the id being enumerated;
                    // any other id costs one table read, and that read is what makes the
                    // filter exact rather than prefix-approximate.
                    if book.policies().get(asset.policy)? != Some(self.policy) {
                        continue;
                    }
                    assets.push((asset.name.to_vec(), asset.quantity));
                }
                self.live.push(LiveOutput {
                    holder,
                    address,
                    assets,
                });
            }
            match page.next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<HolderTable> {
        self.table()
    }
}

/// The holder identity of a Shelley address, from its raw bytes, and its bech32 form.
///
/// Deliberately the same rule `holder-distribution` applies: Key or Script delegation is a
/// stake credential; Null delegation is an enterprise address; Pointer and Byron are
/// dropped.
///
/// Public because a *reader* of this table has to key a holder the same way, and the
/// identity rule is the one thing two implementations would eventually disagree about.
/// The book stores rows as raw address bytes, so that is the input shape here.
pub fn holder_key_bytes(address: &[u8]) -> Option<(HolderKey, String)> {
    let decoded = Address::from_bytes(address).ok()?;
    let Address::Shelley(shelley) = &decoded else {
        return None;
    };
    let text = shelley.to_bech32().ok()?;
    let holder = match shelley.delegation() {
        ShelleyDelegationPart::Key(hash) | ShelleyDelegationPart::Script(hash) => {
            HolderKey::Stake(**hash)
        }
        ShelleyDelegationPart::Null => HolderKey::Enterprise(text.clone()),
        _ => return None,
    };
    Some((holder, text))
}

/// The holder identity of a bech32 Shelley address.
fn holder_key(address: &str) -> Option<HolderKey> {
    let decoded = Address::from_bech32(address).ok()?;
    holder_key_bytes(&decoded.to_vec()).map(|(holder, _)| holder)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use mitos_fold::Fingerprint;

    use super::*;

    fn fixture() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../mitos-platform/tests/fixtures/186000000.block.cbor")
    }

    fn fixture_block() -> Option<DecodedBlock> {
        let bytes = std::fs::read(fixture()).ok()?;
        let block = pallas_traverse::MultiEraBlock::decode(&bytes).ok()?;
        Some(chain_decode::decode_block(&block))
    }

    fn first_policy(block: &DecodedBlock) -> Option<[u8; 28]> {
        for tx in &block.txs {
            for output in &tx.outputs {
                for asset in &output.assets {
                    if asset.policy.len() == 28 {
                        return asset.policy.as_slice().try_into().ok();
                    }
                }
            }
        }
        None
    }

    fn table_for(block: &DecodedBlock, policy: [u8; 28]) -> HolderTable {
        let mut aggregate = Box::new(HolderTableAggregate::new(policy, Vec::new(), Vec::new()));
        aggregate.observe(block).expect("observe");
        aggregate
            .finish(Stamp::new(0, None, Fingerprint::from_u128(1)))
            .expect("finish")
    }

    #[test]
    fn the_real_fixture_produces_a_table() {
        let Some(block) = fixture_block() else {
            eprintln!("skipping: fixture not present at {}", fixture().display());
            return;
        };
        let Some(policy) = first_policy(&block) else {
            eprintln!("skipping: fixture has no asset policy");
            return;
        };

        let table = table_for(&block, policy);
        assert_eq!(table.policy, policy);
        assert!(!table.holders.is_empty(), "the policy has holders");
        assert_eq!(table.distinct_holders(), table.holders.len());
        assert!(table.total_quantity() > 0);

        for row in &table.holders {
            assert!(!row.address.is_empty());
            assert!(!row.assets.is_empty());
            assert!(row.assets.iter().all(|asset| asset.quantity > 0));
        }
    }

    #[test]
    fn the_same_block_produces_the_same_table() {
        let Some(block) = fixture_block() else {
            return;
        };
        let Some(policy) = first_policy(&block) else {
            return;
        };

        assert_eq!(table_for(&block, policy), table_for(&block, policy));
    }

    #[test]
    fn a_policy_the_block_does_not_hold_produces_an_empty_table() {
        let Some(block) = fixture_block() else {
            return;
        };

        let table = table_for(&block, [0u8; 28]);
        assert!(table.holders.is_empty());
        assert_eq!(table.total_quantity(), 0);
    }

    #[test]
    fn a_holder_key_keeps_the_stake_credential_and_drops_pointer_addresses() {
        let Some(block) = fixture_block() else {
            return;
        };

        let address = block
            .txs
            .iter()
            .flat_map(|tx| &tx.outputs)
            .map(|output| output.address.as_str())
            .find(|address| !address.is_empty())
            .expect("the fixture has an output address");

        match holder_key(address) {
            Some(HolderKey::Stake(cred)) => assert_eq!(cred.len(), 28),
            Some(HolderKey::Enterprise(full)) => assert_eq!(full, address),
            None => panic!("the fixture's first address should be a holder"),
        }
    }

    // ---- netting ----
    //
    // The aggregate's first version added every output and never removed one, so a
    // holder's row was everything it had *ever* held. These three tests are the
    // difference between that and a position. They build blocks by hand rather than
    // reading a fixture because the property is about the sequence of spends, and a
    // single fixture block cannot express one.

    /// A block carrying `txs`, at the fixture's slot and hash — only the txs matter to
    /// an aggregate.
    fn block_of(base: &DecodedBlock, txs: Vec<chain_decode::DecodedTx>) -> DecodedBlock {
        DecodedBlock {
            slot: base.slot,
            hash: base.hash,
            txs,
        }
    }

    /// A transaction with the template's hash: a distinct hash per template, and the
    /// template is the only way to a `Hash<32>` without a manifest of its own.
    fn tx_like(
        template: &chain_decode::DecodedTx,
        inputs: Vec<chain_decode::DecodedInput>,
        outputs: Vec<chain_decode::DecodedOutput>,
    ) -> chain_decode::DecodedTx {
        chain_decode::DecodedTx {
            tx_hash: template.tx_hash,
            inputs,
            outputs,
            required_signers: Vec::new(),
            witness_datums: Default::default(),
            aux_data: None,
        }
    }

    /// An output at `address` holding `quantity` of the policy's single asset.
    fn holding(address: &str, index: u32, policy: [u8; 28], quantity: u64) -> DecodedOutput {
        DecodedOutput {
            address: address.to_owned(),
            lovelace: 2_000_000,
            assets: vec![chain_decode::Asset::new(
                policy.to_vec(),
                b"TKN".to_vec(),
                quantity,
            )],
            index,
            datum_hash: None,
            inline_datum: None,
            min_utxo: 0,
        }
    }

    /// A spend of one output reference.
    fn spending(hash_of: &chain_decode::DecodedTx, index: u32) -> chain_decode::DecodedInput {
        chain_decode::DecodedInput {
            oref: (hash_of.tx_hash, index),
            redeemer: None,
        }
    }

    /// The fixture's first holder address and policy, for a test that needs real values.
    fn a_real_holder(block: &DecodedBlock) -> Option<(String, [u8; 28])> {
        for tx in &block.txs {
            for output in &tx.outputs {
                let policy: [u8; 28] = output
                    .assets
                    .iter()
                    .find(|asset| asset.policy.len() == 28)
                    .and_then(|asset| asset.policy.as_slice().try_into().ok())?;
                if holder_key(&output.address).is_some() {
                    return Some((output.address.clone(), policy));
                }
            }
        }
        None
    }

    #[test]
    fn a_spend_removes_the_output_and_the_holder_with_it() {
        let Some(base) = fixture_block() else {
            return;
        };
        let Some((address, policy)) = a_real_holder(&base) else {
            return;
        };
        // Built twice because `finish` consumes the aggregate and `DecodedTx` is not
        // `Clone`: one aggregate to see the create alone, one to see it spent.
        let create = || {
            tx_like(
                &base.txs[0],
                Vec::new(),
                vec![holding(&address, 0, policy, 100)],
            )
        };

        let mut aggregate = Box::new(HolderTableAggregate::new(policy, Vec::new(), Vec::new()));
        aggregate
            .observe(&block_of(&base, vec![create()]))
            .expect("observe the create");
        let held = aggregate
            .finish(Stamp::new(0, None, Fingerprint::from_u128(1)))
            .expect("finish");
        assert_eq!(held.distinct_holders(), 1);
        assert_eq!(held.total_quantity(), 100);

        let mut aggregate = Box::new(HolderTableAggregate::new(policy, Vec::new(), Vec::new()));
        let spend = tx_like(&base.txs[1], vec![spending(&create(), 0)], Vec::new());
        aggregate
            .observe(&block_of(&base, vec![create(), spend]))
            .expect("observe the spend");
        let spent = aggregate
            .finish(Stamp::new(0, None, Fingerprint::from_u128(1)))
            .expect("finish");
        assert_eq!(
            spent.distinct_holders(),
            0,
            "a fully spent holder is not a holder"
        );
        assert_eq!(spent.total_quantity(), 0);
    }

    #[test]
    fn spending_one_of_two_outputs_leaves_the_other() {
        let Some(base) = fixture_block() else {
            return;
        };
        let Some((address, policy)) = a_real_holder(&base) else {
            return;
        };
        let first = tx_like(
            &base.txs[0],
            Vec::new(),
            vec![holding(&address, 0, policy, 100)],
        );
        let second = tx_like(
            &base.txs[1],
            Vec::new(),
            vec![holding(&address, 0, policy, 30)],
        );
        let spend = tx_like(&base.txs[2], vec![spending(&first, 0)], Vec::new());

        let mut aggregate = Box::new(HolderTableAggregate::new(policy, Vec::new(), Vec::new()));
        aggregate
            .observe(&block_of(&base, vec![first, second, spend]))
            .expect("observe");
        let table = aggregate
            .finish(Stamp::new(0, None, Fingerprint::from_u128(1)))
            .expect("finish");

        assert_eq!(table.distinct_holders(), 1);
        assert_eq!(
            table.total_quantity(),
            30,
            "the surviving output's quantity"
        );
    }

    #[test]
    fn a_spend_the_window_never_created_is_not_an_error() {
        let Some(base) = fixture_block() else {
            return;
        };
        let Some(policy) = first_policy(&base) else {
            return;
        };
        // The spend names the fixture's own first tx, which the aggregate never saw
        // created — the ordinary case at the floor of any window that is not genesis.
        let spend = tx_like(&base.txs[1], vec![spending(&base.txs[0], 7)], Vec::new());

        let mut aggregate = Box::new(HolderTableAggregate::new(policy, Vec::new(), Vec::new()));
        aggregate
            .observe(&block_of(&base, vec![spend]))
            .expect("a spend the window cannot resolve is not a failure");
        let table = aggregate
            .finish(Stamp::new(0, None, Fingerprint::from_u128(1)))
            .expect("finish");
        assert!(table.holders.is_empty());
    }
}
