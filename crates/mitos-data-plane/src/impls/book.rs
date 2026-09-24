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
//! | `read_output_datums` / `output_datum_hashes` | ✅ | inline datums are in the row; hash-referenced ones yield the hash and no payload |
//! | `tip` | ❌ | **the artifact records no chain position.** See below |
//! | `search_utxos` | ❌ | it returns a `Page`, and a `Page` carries a tip |
//! | `read_datum` | ❌ | needs `datum.tbl` — `LIVE_BOOK.md` phase 3, capture-side work |
//! | `read_script` | ❌ | the row has no `script_ref` column at all |
//! | `tx_metadata` / `read_tx` / `slot_by_tx_hash` | ❌ | history, not live state: `tx-index` + the chunks |
//! | `total_supply` / `asset_state` | ❌ | the mint ledger, a second fold |
//! | `protocol_params` | ❌ | not a UTxO fold |
//! | `read_block` | ❌ | needs a retained block body |
//!
//! **The `tip` refusal is the structural one.** A book is a snapshot of the live
//! set at *some* chunk boundary, and `book.meta` does not record which — so the
//! plane cannot say what slot it is at, and anything a consumer does with a chain
//! position (staleness, `TickEvery`, drift across a paginated query) is
//! unanswerable. That is not a gap in this adapter; it is a gap in the artifact,
//! and it is the first thing `LIVE_BOOK.md` phase 4 has to close. Do not paper
//! over it with `ChainTip::origin()`: `Origin` means genesis, and a module that
//! believed it would conclude the chain had not started. It answers
//! `ChainPoint::Slot(base_slot)` when the artifact records where it sits, and
//! refuses when it does not.
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
use live_book::{Base, Datum, Row};
use pallas::codec::minicbor;
use pallas::ledger::primitives::PlutusData;
use pallas_addresses::Address;
use pallas_primitives::Hash;

use crate::impls::UTXOS_BY_ADDRESS_HARD_CAP;
use crate::types::ProtocolParameters;
use crate::{
    AssetEntry, ChainDataPlane, ChainPoint, ChainTip, DataPlaneError, DataPlaneResult, DatumKind,
    DecodeLevel, OutputRef, Page, PageRequest, Resolution, StakeCred, TypedDatum, TypedOutput,
    TypedScript, UtxoPredicate,
};

/// A `ChainDataPlane` over a published live book.
///
/// `open` maps the artifact and validates it — versions, counts, and each
/// directory's partition — so a corrupt or truncated book is refused once, at
/// construction, rather than answering narrowly wrongly later.
pub struct BookDataPlane {
    base: Base,
}

impl BookDataPlane {
    pub fn open(dir: &Path) -> DataPlaneResult<BookDataPlane> {
        let base = Base::open(dir).map_err(|e| DataPlaneError::Storage(e.to_string()))?;
        Ok(BookDataPlane { base })
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
            let policy = self.base.policies().get(a.policy).ok_or_else(|| {
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
    /// `payload: None`, which is the documented meaning of `None` here: "the
    /// plane could not resolve the bytes", not "there is no datum". Closing that
    /// is `datum.tbl` (`LIVE_BOOK.md` phase 3), which is a *capture* rather than
    /// a view — the witness bytes are not in any row.
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
        _predicate: &UtxoPredicate,
        _decode: DecodeLevel,
        _page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        // The three shapes the book can answer — `at_address`, `under_policy` and a
        // payment credential — all go through the enumerations above, and a `Page`
        // can now carry a tip (`tip()` answers from `base_slot`). What is still
        // missing is the predicate algebra and the cursor discipline between
        // requests; `PHONEBOOK_COVERAGE.md` §3 and §4a name the shapes callers use,
        // which is where a first implementation should start.
        Err(DataPlaneError::NotYetImplemented(
            "search_utxos: the book can answer `tip` now, but not a predicate algebra over \
             utxos, nor the cursor a Page must carry between requests",
        ))
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

    async fn read_datum(&self, _hash: &Hash<32>) -> DataPlaneResult<Option<TypedDatum>> {
        Err(DataPlaneError::NotYetImplemented(
            "datum_by_hash needs the datum table (LIVE_BOOK.md phase 3): witness bytes are not in any row",
        ))
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
        let plane = BookDataPlane { base };

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
        let plane = BookDataPlane { base };

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
