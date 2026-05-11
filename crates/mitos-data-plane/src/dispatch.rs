//! v2 dispatch composition.
//!
//! Glues `block_events` (raw block decode) and `types::interest`
//! (InterestSet matching) into the platform's event dispatch
//! flow:
//!
//! 1. Decode block CBOR → `DecodedBlockV2` (drafts).
//! 2. Bulk-resolve prior outputs for all inputs + reference
//!    inputs across all TXs in one data-plane call (amortises
//!    the boundary cost).
//! 3. Build full events from drafts + resolved priors.
//! 4. For each TX: if any event matches the InterestSet, emit a
//!    `TxEventBatch` containing **every** event that TX
//!    produced, in dispatch order. Modules get full atomic TX
//!    context whenever the TX is relevant; matching-only events
//!    would lose that.
//!
//! The "any match → emit all" rule is the v2 contract per the
//! design doc — modules that want to ignore non-matching events
//! post-filter inside `handle-events` cheaply (one match arm).
//!
//! Returns `Vec<TxEventBatch>` ready to hand to the wasm dispatch
//! path one batch at a time.

use std::collections::HashMap;

use crate::ChainDataPlane;
use crate::block_events::{
    DecodedBlockV2, InputDraft, MintDraft, OutputDraft, TxDraft, datum_from_draft,
};
use crate::types::{
    ChainPoint, ConsumedEvent, DataPlaneResult, DecodeLevel, InterestSet, MintedEvent, OutputRef,
    ProducedEvent, ReferencedEvent, TxContextEvent, TxEventBatch, TypedDatum, TypedOutput,
    UtxoEvent,
};

/// Build per-TX event batches from a decoded block, filtering
/// against the module's interest set. Async because prior-output
/// resolution hits the data plane.
///
/// Returns only TXs that produced at least one event matching
/// the interest. For matching TXs, the batch contains every
/// event the TX produced (atomic context).
pub async fn build_event_batches<P: ChainDataPlane + Sync>(
    block: DecodedBlockV2,
    interest: &InterestSet,
    plane: &P,
) -> DataPlaneResult<Vec<TxEventBatch>> {
    // Step 1: collect every ref we'll need to resolve. Inputs
    // and reference inputs across every TX. Dedup via sort +
    // dedup after the gather so a TX referencing the same
    // output twice doesn't double the host call.
    let mut refs_to_resolve: Vec<OutputRef> = Vec::new();
    for tx in &block.txs {
        for input in &tx.inputs {
            refs_to_resolve.push(input.oref);
        }
        for ref_oref in &tx.reference_inputs {
            refs_to_resolve.push(*ref_oref);
        }
    }
    // Cheap dedup via HashSet of (tx_hash bytes, index).
    refs_to_resolve.sort_by_key(|a| (a.tx_hash, a.index));
    refs_to_resolve.dedup();

    // Step 2: bulk-resolve outputs + datums.
    let resolved_outputs = if refs_to_resolve.is_empty() {
        Vec::new()
    } else {
        plane
            .read_utxos(&refs_to_resolve, DecodeLevel::WithDatum)
            .await?
    };
    // Build a lookup: ref → (output, datum) for present-in-set
    // refs. Spent / unknown refs simply absent — the dispatcher
    // emits the corresponding events with empty-but-typed
    // placeholder shapes (modules treat the absence as
    // "data plane couldn't resolve" same as today).
    let mut resolved: HashMap<
        (pallas_primitives::Hash<32>, u32),
        (TypedOutput, Option<TypedDatum>),
    > = HashMap::new();
    for (oref, output) in resolved_outputs {
        let key = (oref.tx_hash, oref.index);
        let datum = output.datum.clone();
        resolved.insert(key, (output, datum));
    }

    // Step 3+4: build events per TX, decide relevance, emit
    // batches.
    let mut batches: Vec<TxEventBatch> = Vec::new();
    for tx in block.txs {
        if let Some(batch) = build_tx_batch(tx, &block.cursor, interest, &resolved) {
            batches.push(batch);
        }
    }
    Ok(batches)
}

fn build_tx_batch(
    tx: TxDraft,
    cursor: &ChainPoint,
    interest: &InterestSet,
    resolved: &HashMap<(pallas_primitives::Hash<32>, u32), (TypedOutput, Option<TypedDatum>)>,
) -> Option<TxEventBatch> {
    // Build per-category events first; we need them all to
    // decide relevance, then we order them into the batch.
    let referenced: Vec<UtxoEvent> = tx
        .reference_inputs
        .iter()
        .filter_map(|oref| {
            let (mut prior_output, mut prior_datum) =
                resolved.get(&(oref.tx_hash, oref.index)).cloned()?;
            backfill_prior_datum(&mut prior_output, &mut prior_datum, &tx.witness_datums);
            Some(UtxoEvent::Referenced(ReferencedEvent {
                cursor: cursor.clone(),
                referencing_tx_hash: tx.tx_hash,
                referencing_tx_idx: tx.tx_idx,
                oref: *oref,
                prior_output,
                prior_datum,
            }))
        })
        .collect();

    let consumed: Vec<UtxoEvent> = tx
        .inputs
        .iter()
        .filter_map(|input: &InputDraft| {
            let (mut prior_output, mut prior_datum) = resolved
                .get(&(input.oref.tx_hash, input.oref.index))
                .cloned()?;
            backfill_prior_datum(&mut prior_output, &mut prior_datum, &tx.witness_datums);
            Some(UtxoEvent::Consumed(ConsumedEvent {
                cursor: cursor.clone(),
                consuming_tx_hash: tx.tx_hash,
                consuming_tx_idx: tx.tx_idx,
                oref: input.oref,
                prior_output,
                prior_datum,
                redeemer: input.redeemer.clone(),
            }))
        })
        .collect();

    let produced: Vec<UtxoEvent> = tx
        .outputs
        .iter()
        .enumerate()
        .map(|(idx, draft): (usize, &OutputDraft)| {
            let oref = OutputRef::new(tx.tx_hash, idx as u32);
            let datum = datum_from_draft(draft.datum_hash, draft.inline_datum_bytes.clone());
            let mut output = draft.output.clone();
            output.datum = datum.clone();
            UtxoEvent::Produced(ProducedEvent {
                cursor: cursor.clone(),
                tx_hash: tx.tx_hash,
                tx_idx: tx.tx_idx,
                oref,
                output,
                datum,
            })
        })
        .collect();

    let minted: Vec<UtxoEvent> = tx
        .mints
        .iter()
        .map(|m: &MintDraft| {
            UtxoEvent::Minted(MintedEvent {
                cursor: cursor.clone(),
                tx_hash: tx.tx_hash,
                tx_idx: tx.tx_idx,
                policy: m.policy.clone(),
                asset_name: m.asset_name.clone(),
                quantity_delta: m.quantity_delta,
            })
        })
        .collect();

    // Relevance check: does any non-tx-context event match the
    // interest set? (tx-context alone shouldn't trigger
    // dispatch — there'd be nothing for the module to act on.)
    let any_match = referenced.iter().any(|e| event_matches(e, interest))
        || consumed.iter().any(|e| event_matches(e, interest))
        || produced.iter().any(|e| event_matches(e, interest))
        || minted.iter().any(|e| event_matches(e, interest));

    if !any_match {
        return None;
    }

    // Assemble the batch in dispatch order: tx-context first,
    // then referenced, consumed, produced, minted.
    let tx_context = UtxoEvent::TxContext(TxContextEvent {
        cursor: cursor.clone(),
        tx_hash: tx.tx_hash,
        tx_idx: tx.tx_idx,
        validity_interval: tx.validity_interval,
        required_signers: tx.required_signers,
    });

    let mut batch = TxEventBatch::new(tx.tx_idx);
    batch.push(tx_context);
    for e in referenced {
        batch.push(e);
    }
    for e in consumed {
        batch.push(e);
    }
    for e in produced {
        batch.push(e);
    }
    for e in minted {
        batch.push(e);
    }
    Some(batch)
}

/// Apply the witness-set fallback to both copies of the prior
/// datum. `prior_output.datum` and the standalone `prior_datum`
/// field carry the same `TypedDatum` clone — fill both so a
/// module observing either path sees consistent bytes.
fn backfill_prior_datum(
    prior_output: &mut TypedOutput,
    prior_datum: &mut Option<TypedDatum>,
    witness_datums: &[(pallas_primitives::Hash<32>, Vec<u8>)],
) {
    if witness_datums.is_empty() {
        return;
    }
    if let Some(d) = prior_output.datum.as_mut() {
        d.fill_from_witness(witness_datums);
    }
    if let Some(d) = prior_datum.as_mut() {
        d.fill_from_witness(witness_datums);
    }
}

fn event_matches(event: &UtxoEvent, interest: &InterestSet) -> bool {
    match event {
        UtxoEvent::TxContext(_) => false, // tx-context never matches on its own
        UtxoEvent::Referenced(e) => interest.matches_output(&e.prior_output),
        UtxoEvent::Consumed(e) => interest.matches_output(&e.prior_output),
        UtxoEvent::Produced(e) => interest.matches_output(&e.output),
        UtxoEvent::Minted(e) => interest.matches_mint(&e.policy, &e.asset_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InterestPredicate, OutputRef};

    /// Stub data plane that just returns canned outputs for the
    /// refs it knows about. Sufficient for unit-testing the
    /// dispatch composition.
    struct StubPlane {
        utxos: HashMap<(pallas_primitives::Hash<32>, u32), TypedOutput>,
    }

    #[async_trait::async_trait]
    impl ChainDataPlane for StubPlane {
        async fn read_utxo(
            &self,
            oref: &OutputRef,
            _decode: DecodeLevel,
        ) -> DataPlaneResult<Option<TypedOutput>> {
            Ok(self.utxos.get(&(oref.tx_hash, oref.index)).cloned())
        }

        async fn read_utxos(
            &self,
            orefs: &[OutputRef],
            _decode: DecodeLevel,
        ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
            Ok(orefs
                .iter()
                .filter_map(|r| {
                    self.utxos
                        .get(&(r.tx_hash, r.index))
                        .map(|o| (*r, o.clone()))
                })
                .collect())
        }

        async fn tip(&self) -> DataPlaneResult<crate::types::ChainTip> {
            Ok(crate::types::ChainTip::origin())
        }

        async fn protocol_params(&self) -> DataPlaneResult<crate::types::ProtocolParameters> {
            Err(crate::types::DataPlaneError::NotYetImplemented(
                "stub plane has no protocol params",
            ))
        }

        async fn search_utxos(
            &self,
            _predicate: &crate::types::UtxoPredicate,
            _decode: DecodeLevel,
            _page: crate::types::PageRequest,
        ) -> DataPlaneResult<crate::types::Page<(OutputRef, TypedOutput)>> {
            Ok(crate::types::Page {
                items: Vec::new(),
                next_token: None,
                tip: crate::types::ChainTip::origin(),
            })
        }

        async fn utxos_by_address(&self, _address: &str) -> DataPlaneResult<Vec<OutputRef>> {
            Ok(Vec::new())
        }

        async fn tx_metadata(
            &self,
            _tx_hash: &pallas_primitives::Hash<32>,
        ) -> DataPlaneResult<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn read_datum(
            &self,
            _hash: &pallas_primitives::Hash<32>,
        ) -> DataPlaneResult<Option<TypedDatum>> {
            Ok(None)
        }

        async fn read_script(
            &self,
            _hash: &pallas_primitives::Hash<28>,
        ) -> DataPlaneResult<Option<crate::types::TypedScript>> {
            Ok(None)
        }

        async fn total_supply(
            &self,
            _policy: &cardano_assets::PolicyId,
            _asset_name_hex: Option<&str>,
        ) -> DataPlaneResult<u64> {
            Ok(0)
        }

        async fn holder_count(&self, _policy: &cardano_assets::PolicyId) -> DataPlaneResult<u64> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn empty_block_yields_no_batches() {
        let plane = StubPlane {
            utxos: HashMap::new(),
        };
        let block = DecodedBlockV2 {
            cursor: ChainPoint::Slot(100),
            txs: Vec::new(),
        };
        let interest =
            InterestSet::default().with_predicate(InterestPredicate::AtAddress("addr1".to_owned()));
        let batches = build_event_batches(block, &interest, &plane).await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn produced_at_watched_address_emits_batch_with_tx_context_first() {
        let plane = StubPlane {
            utxos: HashMap::new(),
        };
        let tx_hash = pallas_primitives::Hash::new([0xab; 32]);
        let watched_addr = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";
        let output = TypedOutput {
            address: watched_addr.to_owned(),
            lovelace: 5_000_000,
            assets: Vec::new(),
            datum: None,
            script_ref: None,
            original_cbor: None,
            decoded_at: DecodeLevel::Lean,
        };
        let block = DecodedBlockV2 {
            cursor: ChainPoint::Slot(100),
            txs: vec![TxDraft {
                tx_hash,
                tx_idx: 0,
                validity_interval: Default::default(),
                required_signers: Vec::new(),
                inputs: Vec::new(),
                reference_inputs: Vec::new(),
                outputs: vec![OutputDraft {
                    output: output.clone(),
                    datum_hash: None,
                    inline_datum_bytes: None,
                }],
                mints: Vec::new(),
                aux_data_cbor: None,
                witness_datums: Vec::new(),
            }],
        };
        let interest = InterestSet::default()
            .with_predicate(InterestPredicate::AtAddress(watched_addr.to_owned()));
        let batches = build_event_batches(block, &interest, &plane).await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.tx_idx, 0);
        assert_eq!(batch.events.len(), 2, "tx-context + 1 produced");
        // First event must be tx-context.
        assert!(matches!(batch.events[0], UtxoEvent::TxContext(_)));
        // Second event must be the produced output we created.
        match &batch.events[1] {
            UtxoEvent::Produced(p) => {
                assert_eq!(p.output.address, watched_addr);
                assert_eq!(p.tx_idx, 0);
            }
            other => panic!("expected Produced second, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tx_with_no_matching_event_is_skipped() {
        let plane = StubPlane {
            utxos: HashMap::new(),
        };
        let tx_hash = pallas_primitives::Hash::new([0xab; 32]);
        let unwatched = TypedOutput {
            address: "addr1notwatched".to_owned(),
            lovelace: 5_000_000,
            assets: Vec::new(),
            datum: None,
            script_ref: None,
            original_cbor: None,
            decoded_at: DecodeLevel::Lean,
        };
        let block = DecodedBlockV2 {
            cursor: ChainPoint::Slot(100),
            txs: vec![TxDraft {
                tx_hash,
                tx_idx: 0,
                validity_interval: Default::default(),
                required_signers: Vec::new(),
                inputs: Vec::new(),
                reference_inputs: Vec::new(),
                outputs: vec![OutputDraft {
                    output: unwatched,
                    datum_hash: None,
                    inline_datum_bytes: None,
                }],
                mints: Vec::new(),
                aux_data_cbor: None,
                witness_datums: Vec::new(),
            }],
        };
        let interest = InterestSet::default()
            .with_predicate(InterestPredicate::AtAddress("addr1watched".to_owned()));
        let batches = build_event_batches(block, &interest, &plane).await.unwrap();
        assert!(batches.is_empty());
    }

    /// Hash-datum prior output whose bytes the data plane couldn't
    /// resolve directly (no inline datum, no `DATUM_NS` entry yet)
    /// — the consuming TX's witness set carries the matching
    /// datum, so the dispatcher must backfill `original_cbor` on
    /// both `prior_output.datum` and the standalone `prior_datum`
    /// before dispatch reaches the module. This is the path
    /// jpg.store CO cancellations rely on.
    #[tokio::test]
    async fn consumed_event_backfills_prior_datum_from_witness_set() {
        // Prior CO output: hash-datum, bytes unresolved.
        // Datum hash and bytes are arbitrary — the dispatch logic
        // only checks key equality between the prior datum's hash
        // and a `witness_datums` entry, so we don't need a real
        // hash-of-bytes relationship for the lookup test.
        let prior_tx_hash = pallas_primitives::Hash::new([0x11; 32]);
        let datum_hash = pallas_primitives::Hash::<32>::new([0x77; 32]);
        let datum_bytes: Vec<u8> = vec![0xd8, 0x79, 0x9f, 0xff];
        let watched_addr = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";
        let prior_output = TypedOutput {
            address: watched_addr.to_owned(),
            lovelace: 100_000_000,
            assets: Vec::new(),
            datum: Some(TypedDatum {
                hash: datum_hash,
                payload: None,
                original_cbor: None,
            }),
            script_ref: None,
            original_cbor: None,
            decoded_at: DecodeLevel::WithDatum,
        };
        let mut utxos = HashMap::new();
        utxos.insert((prior_tx_hash, 0), prior_output);
        let plane = StubPlane { utxos };

        // Consuming TX: spends the prior CO; witness set carries
        // the matching datum bytes.
        let consuming_tx_hash = pallas_primitives::Hash::new([0x22; 32]);
        let block = DecodedBlockV2 {
            cursor: ChainPoint::Slot(200),
            txs: vec![TxDraft {
                tx_hash: consuming_tx_hash,
                tx_idx: 0,
                validity_interval: Default::default(),
                required_signers: Vec::new(),
                inputs: vec![InputDraft {
                    oref: OutputRef::new(prior_tx_hash, 0),
                    redeemer: Some(vec![0xd8, 0x7a, 0x80]), // cancel redeemer
                }],
                reference_inputs: Vec::new(),
                outputs: Vec::new(),
                mints: Vec::new(),
                aux_data_cbor: None,
                witness_datums: vec![(datum_hash, datum_bytes.clone())],
            }],
        };
        let interest = InterestSet::default()
            .with_predicate(InterestPredicate::AtAddress(watched_addr.to_owned()));
        let batches = build_event_batches(block, &interest, &plane).await.unwrap();

        assert_eq!(batches.len(), 1, "consumed at watched addr matches");
        let consumed = batches[0]
            .events
            .iter()
            .find_map(|e| match e {
                UtxoEvent::Consumed(c) => Some(c),
                _ => None,
            })
            .expect("Consumed event present");

        let prior_datum = consumed
            .prior_datum
            .as_ref()
            .expect("prior_datum populated");
        assert_eq!(
            prior_datum.original_cbor.as_deref(),
            Some(datum_bytes.as_slice()),
            "standalone prior_datum.original_cbor backfilled from witness set",
        );
        let inline_datum = consumed
            .prior_output
            .datum
            .as_ref()
            .expect("prior_output carries the datum");
        assert_eq!(
            inline_datum.original_cbor.as_deref(),
            Some(datum_bytes.as_slice()),
            "prior_output.datum.original_cbor backfilled symmetrically",
        );
    }
}
