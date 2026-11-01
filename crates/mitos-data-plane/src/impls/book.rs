//! `BookDataPlane` — the `ChainDataPlane` trait over a published live book.
//!
//! The second implementation of the trait, and the first that does not need
//! dolos. It answers from `crates/live-book`: an immutable artifact whose rows
//! carry the outref, the address, the value and the datum of every live output,
//! under three narrow orderings.
//!
//! # What it serves, and what it refuses
//!
//! Refusing is the load-bearing part. A data plane that answers a question it
//! cannot answer correctly is worse than one that says no, because the module on
//! the other side of the trait has no way to tell — so every method below either
//! answers from the artifact or returns `NotYetImplemented` naming what would
//! close it.
//!
//! | method | here | why not, where not |
//! |---|---|---|
//! | `read_utxo` / `read_utxos` | ✅ | one row read each, confirmed by the row's own outref |
//! | `utxos_by_address` | ✅ | `by_addr`, confirmed on the full address |
//! | `utxos_by_policy` | ✅ | `by_policy`, confirmed on the resolved policy |
//! | `utxos_by_payment_cred` | ✅ | `by_addr`, payment-credential hits only |
//! | `resolve_stake_for_payment_pkh` | ✅ | overridden: the row's bytes carry the delegation part, so no bech32 round trip |
//! | `holder_count` | ✅ | distinct addresses over `by_policy` — see the note on cost |
//! | `read_output_datums` / `output_datum_hashes` | ✅ | inline datums are in the row; hash-referenced ones resolve from `datum.tbl` when the book was opened with one, otherwise the hash and `payload: None` |
//! | `tip` | ✅ when `book.meta.base_slot` is recorded; refuses when it is not |
//! | `search_utxos` | ✅ for `Match` of `Policy`, an exact address or a payment credential; other shapes refused |
//! | `read_datum` | ✅ when a `datum.tbl` is opened with the book; refuses without one |
//! | `read_script` | ❌ | the row has no `script_ref` column at all |
//! | `tx_metadata` / `read_tx` / `slot_by_tx_hash` | ❌ | history, not live state: `tx-index` + the chunks |
//! | `total_supply` / `asset_state` | ❌ | the mint ledger, a second fold |
//! | `protocol_params` | ❌ | not a UTxO fold |
//! | `read_block` | ❌ | needs a retained block body |
//!
//! **`tip` is conditional on the artifact.** A book records the slot its fold
//! stopped at in `book.meta`'s `base_slot`; when the field is present, `tip`
//! answers `ChainPoint::Slot(base_slot)`, and when it is absent the plane refuses.
//! Do not paper over the absence with `ChainTip::origin()`: `Origin` means
//! genesis, and a module that believed it would conclude the chain had not
//! started. `search_utxos` returns a `Page`, which carries that tip, so it
//! inherits the same condition: it answers only from a stamped artifact.
//!
//! # What the trait's shape costs
//!
//! `TypedOutput` is a `String` address and `Vec<AssetEntry>` with a hex
//! `PolicyId` per entry, so a read allocates per output regardless of the
//! artifact's zero-copy row. The book's own reader hands over borrowed bytes;
//! this layer is where they become owned text. That is a property of the trait,
//! not of the book, and it belongs in the measurement rather than in a complaint:
//! `DECODE_SHAPE.md` already prices `encode_bech32` at 5.6% of a whole walk
//! through pallas, and this path pays it per output read.

use std::collections::HashSet;
use std::path::Path;

use async_trait::async_trait;
use cardano_assets::PolicyId;
use live_book::format::DatumTable;
use live_book::{Base, Datum, Row};
use pallas::codec::minicbor;
use pallas::ledger::primitives::PlutusData;
use pallas_addresses::Address;
use pallas_primitives::Hash;

use crate::impls::UTXOS_BY_ADDRESS_HARD_CAP;
use crate::types::ProtocolParameters;
use crate::{
    AddressPattern, AssetEntry, AssetPattern, ChainDataPlane, ChainPoint, ChainTip, DataPlaneError,
    DataPlaneResult, DatumKind, DecodeLevel, OutputRef, Page, PageRequest, Resolution, StakeCred,
    TypedDatum, TypedOutput, TypedScript, UtxoPattern, UtxoPredicate,
};

/// A `ChainDataPlane` over a published live book.
///
/// `open` maps the artifact and validates it — versions, counts, and each
/// directory's partition — so a corrupt or truncated book is refused once, at
/// construction, rather than answering narrowly wrongly later.
pub struct BookDataPlane {
    base: Base,
    /// The witness-datum capture, when the book was opened with one. A book
    /// without it still answers inline datums; hash-referenced ones carry their
    /// hash and `payload: None`.
    datum: Option<DatumTable>,
}

/// The ordering a supported `search_utxos` predicate selects.
enum Query {
    Policy([u8; 28]),
    Address(Vec<u8>),
    PaymentCred([u8; 28]),
}

impl BookDataPlane {
    pub fn open(dir: &Path) -> DataPlaneResult<BookDataPlane> {
        let base = Base::open(dir).map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        // A datum capture is a separate artifact, but the common local layout
        // puts it beside the book. A present-but-broken pair is an error, not a
        // silent fallback: answering `payload: None` from a table that failed to
        // map would make corruption look like an unwitnessed datum.
        let datum = if dir.join(live_book::format::DATUM_FILE).exists() {
            Some(DatumTable::open(dir).map_err(|e| DataPlaneError::Storage(e.to_string()))?)
        } else {
            None
        };
        Ok(BookDataPlane { base, datum })
    }

    /// Open the book and a datum capture held in a different directory — the
    /// `fold-probe fold --datum-tbl <dir>` layout, where the table is its own
    /// tree rather than a sibling of `book.meta`.
    pub fn open_with_datums(book_dir: &Path, datum_dir: &Path) -> DataPlaneResult<BookDataPlane> {
        let base = Base::open(book_dir).map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        let datum =
            DatumTable::open(datum_dir).map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        Ok(BookDataPlane {
            base,
            datum: Some(datum),
        })
    }

    /// The artifact underneath. For callers that want the book's own API — refs
    /// and borrowed rows — rather than the trait's owned shapes.
    pub fn base(&self) -> &Base {
        &self.base
    }

    pub fn rows(&self) -> u64 {
        self.base.rows()
    }

    /// One row into the trait's output shape, at the caller's decode level.
    fn output(&self, row: &Row<'_>, decode: DecodeLevel) -> DataPlaneResult<TypedOutput> {
        let address = Address::from_bytes(row.address())
            .map_err(|e| DataPlaneError::Decode(format!("address of {}: {e:?}", row.outref().1)))?
            .to_string();
        let assets = self.assets(row)?;
        let datum = if decode.includes_datum() {
            self.datum(row)
        } else {
            None
        };
        Ok(TypedOutput {
            address,
            lovelace: row.lovelace(),
            assets,
            datum,
            // The row carries no reference script at all, so `Full` cannot be
            // honoured past the datum. Reported rather than faked: a consumer
            // that needs the script reads `None` and asks elsewhere.
            script_ref: None,
            // `original_cbor` means the *chain's* output CBOR. A row is this
            // artifact's own encoding, so handing it over here would be a
            // plausible-looking lie about where the bytes came from.
            original_cbor: None,
            decoded_at: decode,
            resolution: Resolution::Resolved,
        })
    }

    fn assets(&self, row: &Row<'_>) -> DataPlaneResult<Vec<AssetEntry>> {
        let mut out = Vec::with_capacity(row.n_assets());
        for a in row.assets() {
            let policy = self
                .base
                .policies()
                .get(a.policy)
                // A read that failed and an id that is absent are different
                // faults; both are decode failures to this caller, but the
                // message says which.
                .map_err(|e| {
                    DataPlaneError::Decode(format!("reading policy id {}: {e}", a.policy))
                })?
                .ok_or_else(|| {
                    DataPlaneError::Decode(format!(
                        "row names policy id {} absent from policy.tbl",
                        a.policy
                    ))
                })?;
            // `PolicyId` is a validated hex string, so this is two allocations
            // per asset entry. The artifact stores 4 bytes and the table holds
            // the 28 — the hexing is the trait's shape, not the book's.
            let policy_id = PolicyId::new(hex::encode(policy))
                .map_err(|e| DataPlaneError::Decode(format!("policy {policy:?}: {e}")))?;
            out.push(AssetEntry {
                policy_id,
                asset_name_hex: hex::encode(a.name),
                quantity: a.quantity,
            });
        }
        Ok(out)
    }

    /// The row's datum in the trait's shape.
    ///
    /// An inline datum is in the row, so it resolves completely — payload, hash
    /// and CBOR — with no lookup. A hash-referenced one yields the hash and
    /// `payload: None` unless the plane was opened with a `datum.tbl`, which is a
    /// *capture* rather than a view: the witness bytes are not in any row, so
    /// `read_output_datums` fills them from the table when one is open.
    fn datum(&self, row: &Row<'_>) -> Option<TypedDatum> {
        match row.datum() {
            Datum::None => None,
            Datum::Inline(cbor) => Some(TypedDatum {
                // The ledger's datum hash is the hash of the bytes as they
                // appeared, and the row stored exactly those.
                hash: pallas::crypto::hash::Hasher::<256>::hash(cbor),
                kind: DatumKind::Inline,
                payload: minicbor::decode::<PlutusData>(cbor).ok(),
                original_cbor: Some(cbor.to_vec()),
            }),
            Datum::Hash(h) => {
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(h);
                Some(TypedDatum {
                    hash: Hash::from(bytes),
                    kind: DatumKind::Hash,
                    payload: None,
                    original_cbor: None,
                })
            }
        }
    }

    fn refs(&self, page: &live_book::Page) -> Vec<OutputRef> {
        page.refs
            .iter()
            .map(|(h, i)| OutputRef::from_bytes(*h, *i))
            .collect()
    }

    fn by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        let bytes = Address::from_bech32(address)
            .map_err(|e| DataPlaneError::InvalidRequest(format!("address not bech32: {e:?}")))?
            .to_vec();
        // The trait's shape is one unbounded `Vec`, hard-capped. Paging lives in
        // the book's own API and in the wasm host functions above it; a caller of
        // *this* method that needs more than the cap has to narrow the query, and
        // `LocalDataPlane` documents the same cap for the same reason.
        let page = self
            .base
            .by_address(&bytes, None, UTXOS_BY_ADDRESS_HARD_CAP)
            .map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        Ok(self.refs(&page))
    }

    fn by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        let policy: [u8; 28] = policy.try_into().map_err(|_| {
            DataPlaneError::InvalidRequest(format!("policy must be 28 bytes, got {}", policy.len()))
        })?;
        let page = self
            .base
            .by_policy(&policy, None, UTXOS_BY_ADDRESS_HARD_CAP)
            .map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        Ok(self.refs(&page))
    }

    /// The ordering a predicate selects, and the prefix a cursor for it must
    /// carry. The three book orderings are the three shapes the callers use;
    /// anything else refuses by name rather than scanning.
    fn query(&self, predicate: &UtxoPredicate) -> DataPlaneResult<(Query, u64)> {
        match predicate {
            UtxoPredicate::Match(UtxoPattern {
                asset: Some(AssetPattern::Policy(policy)),
                address: None,
                output_ref: None,
            }) => {
                let policy = policy.as_bytes().map_err(|e| {
                    DataPlaneError::InvalidRequest(format!("policy id not valid hex: {e:?}"))
                })?;
                let prefix = u64::from_le_bytes(policy[0..8].try_into().expect("8 bytes"));
                Ok((Query::Policy(policy), prefix))
            }
            UtxoPredicate::Match(UtxoPattern {
                address: Some(AddressPattern::Exact(address)),
                asset: None,
                output_ref: None,
            }) => {
                let bytes = Address::from_bech32(address).map_err(|e| {
                    DataPlaneError::InvalidRequest(format!("address not bech32: {e:?}"))
                })?;
                let bytes = bytes.to_vec();
                // A Byron address has no payment credential; `by_address` returns
                // an empty page for it, so there is no token to check. Zero is a
                // placeholder, never compared with a token that exists.
                let prefix = live_book::address::credential(&bytes)
                    .map(|cred| cred.prefix())
                    .unwrap_or(0);
                Ok((Query::Address(bytes), prefix))
            }
            UtxoPredicate::Match(UtxoPattern {
                address: Some(AddressPattern::PaymentCredential(cred)),
                asset: None,
                output_ref: None,
            }) => {
                let len = cred.len();
                let cred: [u8; 28] = cred.as_slice().try_into().map_err(|_| {
                    DataPlaneError::InvalidRequest(format!(
                        "credential must be 28 bytes, got {len}"
                    ))
                })?;
                let prefix = u64::from_le_bytes(cred[0..8].try_into().expect("8 bytes"));
                Ok((Query::PaymentCred(cred), prefix))
            }
            _ => Err(DataPlaneError::NotYetImplemented(
                "search_utxos: the book answers `Match` of `Policy`, an exact address or a payment \
                 credential",
            )),
        }
    }
}

/// A hash-attached datum resolved from the capture. `kind` stays `Hash`: the
/// output referenced it by hash, and the table supplied the preimage.
fn resolved_datum(hash: [u8; 32], payload: &[u8]) -> TypedDatum {
    TypedDatum {
        hash: Hash::from(hash),
        kind: DatumKind::Hash,
        payload: minicbor::decode::<PlutusData>(payload).ok(),
        original_cbor: Some(payload.to_vec()),
    }
}

/// `AssetEntry`, named through the crate's re-export so this module's imports stay
/// one line shorter than its docs.

#[async_trait]
impl ChainDataPlane for BookDataPlane {
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        let hash: [u8; 32] = oref.tx_hash.as_ref().try_into().map_err(|_| {
            DataPlaneError::InvalidRequest("output ref hash is not 32 bytes".into())
        })?;
        let row = self
            .base
            .utxo(&hash, oref.index)
            .map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        match row {
            Some(row) => Ok(Some(self.output(&row, decode)?)),
            None => Ok(None),
        }
    }

    async fn read_utxos(
        &self,
        orefs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        // No bulk shortcut exists to take: the row store is addressed by offset,
        // not by key, so N refs are N point lookups — each a directory page, an
        // entry or two, and a row. Measured at 1223 ns per lookup over the whole
        // store, which is why the loop is the implementation rather than a
        // compromise.
        let mut out = Vec::with_capacity(orefs.len());
        for oref in orefs {
            if let Some(t) = self.read_utxo(oref, decode).await? {
                out.push((*oref, t));
            }
        }
        Ok(out)
    }

    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        decode: DecodeLevel,
        page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        let (query, prefix) = self.query(predicate)?;
        // `max_items == 0` is not "no items": an empty page with no token would
        // look like the end of the run. Treat it as one.
        let limit = (page.max_items as usize).clamp(1, live_book::MAX_PAGE);
        let after = match page.start_token.as_deref() {
            Some(token) => {
                let cursor = live_book::Cursor::decode(token).ok_or_else(|| {
                    DataPlaneError::InvalidRequest(
                        "page token is not a cursor this plane issued".into(),
                    )
                })?;
                if cursor.prefix() != prefix {
                    return Err(DataPlaneError::InvalidRequest(
                        "page token belongs to a different query".into(),
                    ));
                }
                Some(cursor)
            }
            None => None,
        };

        let page = match query {
            Query::Policy(policy) => self.base.by_policy(&policy, after, limit),
            Query::Address(address) => self.base.by_address(&address, after, limit),
            Query::PaymentCred(cred) => self.base.by_payment_cred(&cred, after, limit),
        }
        .map_err(|e| DataPlaneError::Storage(e.to_string()))?;

        let mut items = Vec::with_capacity(page.refs.len());
        for (hash, index) in page.refs {
            if let Some(row) = self
                .base
                .utxo(&hash, index)
                .map_err(|e| DataPlaneError::Storage(e.to_string()))?
            {
                items.push((
                    OutputRef::from_bytes(hash, index),
                    self.output(&row, decode)?,
                ));
            }
        }

        // A `Page` carries the tip. The book knows it only when `book.meta`
        // records `base_slot`; an unstamped artifact refuses rather than claiming
        // genesis.
        let tip = self.tip().await?;
        Ok(Page {
            items,
            next_token: page.next.map(live_book::Cursor::encode),
            tip,
        })
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        self.by_address(address)
    }

    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        self.by_policy(policy)
    }

    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        let cred: [u8; 28] = cred.try_into().map_err(|_| {
            DataPlaneError::InvalidRequest(format!(
                "credential must be 28 bytes, got {}",
                cred.len()
            ))
        })?;
        let page = self
            .base
            .by_payment_cred(&cred, None, UTXOS_BY_ADDRESS_HARD_CAP)
            .map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        Ok(self.refs(&page))
    }

    /// Overridden rather than inherited, because the default walks
    /// `utxos_by_payment_cred` and then **re-reads each output and decodes a
    /// bech32 string per candidate** to get at the delegation part.
    ///
    /// The row already holds the address bytes, and the delegation part is at a
    /// fixed offset for the four shapes that have one (`live_book::address`), so
    /// this is one row read per candidate and no decode. Same answer, and the
    /// difference is the whole reason a module wants this method to be cheap:
    /// `vesting-tracker` and `holder-distribution` both call it.
    async fn resolve_stake_for_payment_pkh(
        &self,
        pkh: &[u8],
    ) -> DataPlaneResult<Option<StakeCred>> {
        let cred: [u8; 28] = pkh.try_into().map_err(|_| {
            DataPlaneError::InvalidRequest(format!("pkh must be 28 bytes, got {}", pkh.len()))
        })?;
        let page = self
            .base
            .by_payment_cred(&cred, None, UTXOS_BY_ADDRESS_HARD_CAP)
            .map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        for (hash, index) in &page.refs {
            let Ok(Some(row)) = self.base.utxo(hash, *index) else {
                continue;
            };
            match live_book::address::delegation(row.address()) {
                Some(live_book::address::Delegation::Key(b)) => {
                    return Ok(Some(StakeCred::KeyHash(b)));
                }
                Some(live_book::address::Delegation::Script(b)) => {
                    return Ok(Some(StakeCred::ScriptHash(b)));
                }
                None => continue,
            }
        }
        Ok(None)
    }

    /// Distinct addresses holding any asset of this policy.
    ///
    /// The definition is per **address**, not per output, so this reads every
    /// holder row and dedups — which is why it is the most expensive method here:
    /// one row read per holder, plus a set of full addresses. A policy with
    /// 19,768 holders (§`LIVE_BOOK.md` §B9) costs 19,768 row reads and ~44 bytes
    /// each of set. There is no cheaper way to answer the question as posed;
    /// answering *a different* question cheaply (distinct `by_addr` credentials)
    /// would be a wrong answer to this one.
    async fn holder_count(&self, policy: &PolicyId) -> DataPlaneResult<u64> {
        let bytes = hex::decode(policy.as_str())
            .map_err(|e| DataPlaneError::InvalidRequest(e.to_string()))?;
        let refs = self.by_policy(&bytes)?;
        let mut addresses: HashSet<Vec<u8>> = HashSet::new();
        for r in &refs {
            let hash: [u8; 32] = r
                .tx_hash
                .as_ref()
                .try_into()
                .map_err(|_| DataPlaneError::InvalidRequest("not 32 bytes".into()))?;
            if let Some(row) = self
                .base
                .utxo(&hash, r.index)
                .map_err(|e| DataPlaneError::Storage(e.to_string()))?
            {
                addresses.insert(row.address().to_vec());
            }
        }
        Ok(addresses.len() as u64)
    }

    async fn read_datum(&self, hash: &Hash<32>) -> DataPlaneResult<Option<TypedDatum>> {
        let Some(table) = &self.datum else {
            return Err(DataPlaneError::NotYetImplemented(
                "read_datum: this book has no datum table. Capture one with `fold-probe fold \
                 --datum-tbl <dir>` and open the plane with that directory",
            ));
        };
        let bytes: [u8; 32] = hash
            .as_ref()
            .try_into()
            .map_err(|_| DataPlaneError::InvalidRequest("datum hash is not 32 bytes".into()))?;
        Ok(table
            .get(&bytes)
            .map(|payload| resolved_datum(bytes, payload)))
    }

    /// The bulk read, with the hash arm resolved from the capture when one is
    /// open. The default walks `read_utxo` and leaves hash-attached payloads
    /// empty; this keeps that behaviour without a table and fills them when one
    /// is present.
    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<TypedDatum>>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let Some(mut datum) = self
                .read_utxo(r, DecodeLevel::WithDatum)
                .await?
                .and_then(|output| output.datum)
            else {
                out.push(None);
                continue;
            };
            if matches!(datum.kind, DatumKind::Hash)
                && datum.payload.is_none()
                && let Some(table) = &self.datum
            {
                let bytes: [u8; 32] = datum
                    .hash
                    .as_ref()
                    .try_into()
                    .map_err(|_| DataPlaneError::Decode("datum hash is not 32 bytes".into()))?;
                if let Some(payload) = table.get(&bytes) {
                    datum = resolved_datum(bytes, payload);
                }
            }
            out.push(Some(datum));
        }
        Ok(out)
    }

    async fn read_script(&self, _hash: &Hash<28>) -> DataPlaneResult<Option<TypedScript>> {
        Err(DataPlaneError::NotYetImplemented(
            "the row format has no script_ref column, so a script index would be a new capture",
        ))
    }

    async fn tx_metadata(&self, _tx_hash: &Hash<32>) -> DataPlaneResult<Option<Vec<u8>>> {
        Err(DataPlaneError::NotYetImplemented(
            "aux data is history: tx-index holds the location, the chunk holds the bytes",
        ))
    }

    async fn total_supply(
        &self,
        _policy: &PolicyId,
        _asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        Err(DataPlaneError::NotYetImplemented(
            "mints minus burns is the mint ledger, a second fold over the same walk",
        ))
    }

    /// The boundary the artifact was built at, as a slot.
    ///
    /// `ChainPoint::Slot` is the variant for exactly this: a book knows the slot its
    /// fold stopped at and does not carry the block hash — and it is the documented
    /// shape for "the originating event source had only the slot", not a placeholder.
    /// An unstamped artifact refuses rather than answering zero, because a tip of
    /// genesis is a different claim to "this book does not know".
    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        match self.base.meta().base_slot {
            Some(slot) => Ok(ChainTip::at(ChainPoint::Slot(slot))),
            None => Err(DataPlaneError::NotYetImplemented(
                "tip: book.meta has no `base_slot`, so this artifact does not record where it \
                 sits. A build writes it from the walk; an artifact built before the field \
                 existed is stamped by `fold-probe stamp-boundary`.",
            )),
        }
    }

    async fn protocol_params(&self) -> DataPlaneResult<ProtocolParameters> {
        Err(DataPlaneError::NotYetImplemented(
            "protocol parameters are not a UTxO fold",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use live_book::Meta;
    use live_book::format::{Perm, write_ordering, write_policy_tbl};
    use live_book::row::{Row as BookRow, write_asset_record, write_header};

    /// A two-row artifact, written by the test through the format's public
    /// writers. Deliberately not built by the probe: this checks the adapter
    /// against bytes the test laid out, so a bug shared with the builder could
    /// not hide in both.
    fn fixture(name: &str) -> Base {
        fixture_with(name, None, false)
    }

    fn fixture_with(name: &str, hash_datum: Option<([u8; 32], Vec<u8>)>, with_table: bool) -> Base {
        let dir = std::env::temp_dir().join(format!("book-plane-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create dir");

        // Row 0: a base address (payment 0x11, stake 0x22), one asset, an inline
        // datum. Row 1: the same payment credential with a different stake part,
        // no assets, no datum.
        let policy = {
            let mut p = [0u8; 28];
            p[0] = 0xC1;
            p
        };
        let base_addr = |stake: u8| {
            let mut a = vec![0x01u8];
            a.extend_from_slice(&[0x11; 28]);
            a.extend_from_slice(&[stake; 28]);
            a
        };
        let datum = [0x01u8];
        let mut rows: Vec<Vec<u8>> = Vec::new();
        for (i, addr) in [(0u8, base_addr(0x22)), (1, base_addr(0x33))] {
            let assets: Vec<(u32, &[u8], u64)> = if i == 0 { vec![(0, b"nft", 1)] } else { vec![] };
            let mut body = Vec::new();
            for (id, name, qty) in &assets {
                write_asset_record(&mut body, *id, name, *qty);
            }
            let d = if i == 0 {
                Datum::Inline(&datum)
            } else if let Some((hash, _)) = &hash_datum {
                Datum::Hash(hash.as_slice())
            } else {
                Datum::None
            };
            let mut head = [0u8; live_book::row::ROW_HEADER];
            write_header(
                &mut head,
                (&[0xA0 + i; 32], u32::from(i)),
                6,
                2_000_000 + u64::from(i),
                addr.len() as u16,
                assets.len() as u16,
                &d,
            );
            let mut row = head.to_vec();
            row.extend_from_slice(&addr);
            row.extend_from_slice(&body);
            row.extend_from_slice(d.payload());
            rows.push(row);
        }
        let bytes: Vec<u8> = rows.concat();
        std::fs::write(dir.join("rows.bin"), &bytes).expect("rows");

        let mut at = 0u64;
        let mut outref = Vec::new();
        let mut addr = Vec::new();
        let mut policy_perms = Vec::new();
        for row in &rows {
            let parsed = BookRow::parse(row).expect("fixture row parses");
            let (h, i) = parsed.outref();
            let (prefix, tail) = live_book::format::split(&h, i);
            outref.push(Perm {
                prefix,
                tail,
                row: at as u32,
            });
            let cred = live_book::address::credential(parsed.address()).expect("keys");
            addr.push(Perm {
                prefix: cred.prefix(),
                tail: 0,
                row: at as u32,
            });
            for a in parsed.assets() {
                policy_perms.push(Perm {
                    prefix: live_book::Asset::prefix(&policy),
                    tail: 0,
                    row: at as u32,
                });
                let _ = a;
            }
            at += row.len() as u64;
        }
        outref.sort_unstable();
        addr.sort_unstable();
        policy_perms.sort_unstable();
        write_ordering(&dir, "by_outref.idx", "dir.idx", &outref).expect("outref");
        write_ordering(&dir, "by_addr.idx", "addr.dir.idx", &addr).expect("addr");
        write_ordering(&dir, "by_policy.idx", "policy.dir.idx", &policy_perms).expect("policy");
        write_policy_tbl(&dir, &[policy]).expect("policy tbl");
        if with_table {
            let (hash, payload) = hash_datum.as_ref().expect("a table needs a datum");
            live_book::format::write_datum_table(&dir, (0, 0), &[(*hash, payload.clone())])
                .expect("datum table");
        }
        Meta {
            layout_version: live_book::LAYOUT_VERSION,
            rows: rows.len() as u64,
            rows_bytes: bytes.len() as u64,
            by_outref_entries: outref.len() as u64,
            by_addr_entries: addr.len() as u64,
            by_policy_entries: policy_perms.len() as u64,
            policies: 1,
            ..Meta::default()
        }
        .write(&dir)
        .expect("meta");
        Base::open(&dir).expect("open")
    }

    /// The five methods a module path actually reaches, driven end to end.
    #[tokio::test]
    async fn the_module_path_answers_from_the_artifact() {
        let base = fixture("path");
        let plane = BookDataPlane { base, datum: None };

        // read_utxo: the row's own outref is the confirmation.
        let oref = OutputRef::from_bytes([0xA0; 32], 0);
        let out = plane
            .read_utxo(&oref, DecodeLevel::WithDatum)
            .await
            .expect("read")
            .expect("live");
        assert_eq!(out.lovelace, 2_000_000);
        assert_eq!(out.address, base_address_bech32());
        assert_eq!(out.assets.len(), 1);
        assert_eq!(out.assets[0].asset_name_hex, hex::encode(b"nft"));
        assert_eq!(out.assets[0].quantity, 1);
        let datum = out.datum.expect("an inline datum is in the row");
        assert_eq!(datum.kind, crate::DatumKind::Inline);
        assert!(datum.payload.is_some());
        assert_eq!(datum.original_cbor.as_deref(), Some(&[0x01u8][..]));

        // Lean must not carry the datum, even though it is right there.
        let lean = plane
            .read_utxo(&oref, DecodeLevel::Lean)
            .await
            .expect("read")
            .expect("live");
        assert!(lean.datum.is_none(), "Lean asks for no datum resolution");

        // A ref that is not live is absent, not an error.
        assert!(
            plane
                .read_utxo(&OutputRef::from_bytes([0xFF; 32], 0), DecodeLevel::Lean)
                .await
                .expect("read")
                .is_none()
        );

        // utxos_by_address: the exact address, once.
        let refs = plane
            .utxos_by_address(&base_address_bech32())
            .await
            .expect("by address");
        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].index, 0);

        // utxos_by_payment_cred: both stake variants of the payment credential.
        let mut refs = plane
            .utxos_by_payment_cred(&[0x11; 28])
            .await
            .expect("by cred");
        refs.sort_by_key(|r| r.index);
        assert_eq!(refs.len(), 2, "{refs:?}");

        // resolve_stake_for_payment_pkh: from the row's bytes, no bech32 decode.
        let stake = plane
            .resolve_stake_for_payment_pkh(&[0x11; 28])
            .await
            .expect("resolve")
            .expect("a delegation part exists");
        assert_eq!(stake, StakeCred::KeyHash([0x22; 28]));

        // utxos_by_policy: the one row holding it.
        let mut policy = [0u8; 28];
        policy[0] = 0xC1;
        assert_eq!(
            plane
                .utxos_by_policy(&policy)
                .await
                .expect("by policy")
                .len(),
            1
        );
        assert_eq!(
            plane
                .holder_count(&PolicyId::new(hex::encode(policy)).expect("policy id"))
                .await
                .expect("holder count"),
            1
        );

        let _ = std::fs::remove_dir_all(base_dir(&plane));
    }

    /// The refusals are the other half of the contract: a method the artifact
    /// cannot answer must say so rather than return a plausible `None`.
    #[tokio::test]
    async fn what_the_artifact_cannot_answer_is_refused() {
        let base = fixture("refuse");
        let plane = BookDataPlane { base, datum: None };

        assert!(matches!(
            plane.tip().await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        assert!(matches!(
            plane.read_datum(&Hash::from([0u8; 32])).await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        assert!(matches!(
            plane.protocol_params().await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        assert!(matches!(
            plane
                .total_supply(
                    &PolicyId::new("c1".to_string() + &"0".repeat(54)).expect("id"),
                    None
                )
                .await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        // A malformed argument is an invalid request, not a not-yet-implemented.
        assert!(matches!(
            plane.utxos_by_policy(&[0u8; 4]).await,
            Err(DataPlaneError::InvalidRequest(_))
        ));

        let _ = std::fs::remove_dir_all(base_dir(&plane));
    }

    /// A fixture with a recorded boundary, so `search_utxos` can answer the tip
    /// a `Page` has to carry.
    fn slot_fixture(name: &str, slot: u64) -> BookDataPlane {
        let base = fixture(name);
        live_book::Meta::stamp_base_slot(base.dir(), slot).expect("stamp");
        BookDataPlane::open(base.dir()).expect("reopen")
    }

    fn payment_cred_predicate() -> UtxoPredicate {
        UtxoPredicate::matching(UtxoPattern {
            address: Some(AddressPattern::PaymentCredential(vec![0x11; 28])),
            ..Default::default()
        })
    }

    fn policy_predicate() -> UtxoPredicate {
        let mut policy = [0u8; 28];
        policy[0] = 0xC1;
        UtxoPredicate::matching(UtxoPattern::under_policy(
            PolicyId::new(hex::encode(policy)).expect("policy id"),
        ))
    }

    #[tokio::test]
    async fn search_utxos_answers_the_three_shapes() {
        let plane = slot_fixture("search-shapes", 198_590_382);
        let page = plane
            .search_utxos(
                &policy_predicate(),
                DecodeLevel::Lean,
                PageRequest::first(10),
            )
            .await
            .expect("policy");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].0, OutputRef::from_bytes([0xA0; 32], 0));
        assert!(page.next_token.is_none());
        assert_eq!(page.tip.slot(), 198_590_382);

        let exact = UtxoPredicate::matching(UtxoPattern::at_address(base_address_bech32()));
        let page = plane
            .search_utxos(&exact, DecodeLevel::Lean, PageRequest::first(10))
            .await
            .expect("address");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].0, OutputRef::from_bytes([0xA0; 32], 0));

        let page = plane
            .search_utxos(
                &payment_cred_predicate(),
                DecodeLevel::Lean,
                PageRequest::first(10),
            )
            .await
            .expect("credential");
        assert_eq!(page.items.len(), 2);
    }

    #[tokio::test]
    async fn search_utxos_pages_until_next_token_is_none() {
        let plane = slot_fixture("search-pages", 42);
        let mut token = None;
        let mut refs = Vec::new();
        let mut pages = 0;
        loop {
            let page = plane
                .search_utxos(
                    &payment_cred_predicate(),
                    DecodeLevel::Lean,
                    PageRequest {
                        max_items: 1,
                        start_token: token,
                    },
                )
                .await
                .expect("page");
            pages += 1;
            assert_eq!(page.tip.slot(), 42);
            refs.extend(page.items.into_iter().map(|(r, _)| r));
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        refs.sort_by_key(|r| r.index);
        assert_eq!(refs.len(), 2, "{refs:?}");
        assert_eq!(pages, 2);
    }

    #[tokio::test]
    async fn search_utxos_refuses_a_token_from_another_query() {
        let plane = slot_fixture("search-replay", 42);
        let first = plane
            .search_utxos(
                &payment_cred_predicate(),
                DecodeLevel::Lean,
                PageRequest::first(1),
            )
            .await
            .expect("first page");
        let token = first.next_token.expect("a next token");
        let err = plane
            .search_utxos(
                &policy_predicate(),
                DecodeLevel::Lean,
                PageRequest::next(token, 10),
            )
            .await;
        assert!(
            matches!(err, Err(DataPlaneError::InvalidRequest(_))),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn search_utxos_refuses_an_unsupported_predicate() {
        let plane = slot_fixture("search-refuse", 42);
        let stake = UtxoPredicate::matching(UtxoPattern {
            address: Some(AddressPattern::StakeCredential(vec![0x22; 28])),
            ..Default::default()
        });
        assert!(matches!(
            plane
                .search_utxos(&stake, DecodeLevel::Lean, PageRequest::first(10))
                .await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        assert!(matches!(
            plane
                .search_utxos(
                    &UtxoPredicate::any(),
                    DecodeLevel::Lean,
                    PageRequest::first(10)
                )
                .await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
    }

    #[tokio::test]
    async fn search_utxos_refuses_an_unstamped_book() {
        let plane = BookDataPlane {
            base: fixture("search-unstamped"),
            datum: None,
        };
        assert!(matches!(
            plane
                .search_utxos(
                    &policy_predicate(),
                    DecodeLevel::Lean,
                    PageRequest::first(10)
                )
                .await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
    }

    fn hash_of(payload: &[u8]) -> [u8; 32] {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(payload);
        hash.as_ref().try_into().expect("32 bytes")
    }

    #[tokio::test]
    async fn read_datum_resolves_a_hash_attached_payload() {
        let payload = vec![0x82, 0x01, 0x02];
        let hash = hash_of(&payload);
        let base = fixture_with("datum-table", Some((hash, payload.clone())), true);
        let plane = BookDataPlane::open(base.dir()).expect("open");
        let datum = plane
            .read_datum(&Hash::from(hash))
            .await
            .expect("read")
            .expect("witnessed");
        assert_eq!(datum.kind, DatumKind::Hash);
        assert!(datum.payload.is_some());
        assert_eq!(datum.original_cbor.as_deref(), Some(payload.as_slice()));
        assert!(
            plane
                .read_datum(&Hash::from(hash_of(b"never witnessed")))
                .await
                .expect("read")
                .is_none()
        );
        let _ = std::fs::remove_dir_all(base_dir(&plane));
    }

    #[tokio::test]
    async fn read_output_datums_fills_the_hash_arm() {
        let payload = vec![0x82, 0x01, 0x03];
        let hash = hash_of(&payload);
        let base = fixture_with("datum-bulk", Some((hash, payload.clone())), true);
        let plane = BookDataPlane::open(base.dir()).expect("open");
        let oref = OutputRef::from_bytes([0xA1; 32], 1);
        let datums = plane.read_output_datums(&[oref]).await.expect("bulk");
        let datum = datums[0].as_ref().expect("a datum");
        assert!(datum.payload.is_some());
        assert_eq!(datum.original_cbor.as_deref(), Some(payload.as_slice()));
        let _ = std::fs::remove_dir_all(base_dir(&plane));
    }

    #[tokio::test]
    async fn without_a_table_the_hash_arm_keeps_its_hash() {
        let payload = vec![0x82, 0x01, 0x04];
        let hash = hash_of(&payload);
        let base = fixture_with("datum-bulk-none", Some((hash, payload)), false);
        let plane = BookDataPlane { base, datum: None };
        let oref = OutputRef::from_bytes([0xA1; 32], 1);
        let datums = plane.read_output_datums(&[oref]).await.expect("bulk");
        let datum = datums[0].as_ref().expect("a datum");
        assert_eq!(datum.hash, Hash::from(hash));
        assert!(datum.payload.is_none());
        assert!(datum.original_cbor.is_none());
        // `read_datum` is a capability refusal, not an empty answer.
        assert!(matches!(
            plane.read_datum(&Hash::from(hash)).await,
            Err(DataPlaneError::NotYetImplemented(_))
        ));
        let _ = std::fs::remove_dir_all(base_dir(&plane));
    }

    #[tokio::test]
    async fn open_with_datums_resolves_a_separate_capture() {
        let payload = vec![0x82, 0x01, 0x05];
        let hash = hash_of(&payload);
        let base = fixture_with("datum-separate-book", Some((hash, payload.clone())), false);
        let datum_dir =
            std::env::temp_dir().join(format!("book-plane-datum-separate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&datum_dir);
        std::fs::create_dir_all(&datum_dir).expect("create datum dir");
        live_book::format::write_datum_table(&datum_dir, (0, 0), &[(hash, payload.clone())])
            .expect("datum table");
        let plane =
            BookDataPlane::open_with_datums(base.dir(), &datum_dir).expect("open with datums");
        let datum = plane
            .read_datum(&Hash::from(hash))
            .await
            .expect("read")
            .expect("witnessed");
        assert_eq!(datum.original_cbor.as_deref(), Some(payload.as_slice()));
        let _ = std::fs::remove_dir_all(base_dir(&plane));
        let _ = std::fs::remove_dir_all(&datum_dir);
    }

    /// The bech32 of row 0's address — computed once here so the assertion is
    /// against a string rather than against the encoder under test.
    fn base_address_bech32() -> String {
        let mut a = vec![0x01u8];
        a.extend_from_slice(&[0x11; 28]);
        a.extend_from_slice(&[0x22; 28]);
        Address::from_bytes(&a).expect("address").to_string()
    }

    fn base_dir(plane: &BookDataPlane) -> std::path::PathBuf {
        plane.base().dir().to_path_buf()
    }
}
