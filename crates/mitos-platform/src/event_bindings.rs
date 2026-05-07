//! Conversions between mitos's owned event types
//! (`mitos_data_plane::types::event::*`) and the bindgen-
//! generated WIT types (`crate::bindings_v2::*`).
//!
//! Owned types use Pallas's strongly-typed hash newtypes
//! (`Hash<32>`, `Hash<28>`) and `cardano_assets::PolicyId`;
//! WIT types use `list<u8>` everywhere. This module is the
//! single seam where we bridge the two.
//!
//! All conversions are infallible — owned types are constructed
//! by the host's own code (block decode + data plane), so they
//! always have valid lengths. Going the other direction
//! (`from-WIT-to-internal`) is only used when the guest invokes
//! `update-interest`; that path validates lengths via
//! `try_from`.

use mitos_data_plane::block_events::MintDraft;
use mitos_data_plane::{
    ConsumedEvent, DispatchEvent, MintedEvent, ProducedEvent, ReferencedEvent, RollbackEvent,
    TickEvent, TxContextEvent, UtxoEvent, ValidityInterval,
};

use crate::bindings_v2::{
    self, ConsumedEvent as WitConsumedEvent, MintedEvent as WitMintedEvent,
    ProducedEvent as WitProducedEvent, ReferencedEvent as WitReferencedEvent,
    RollbackEvent as WitRollbackEvent, TickEvent as WitTickEvent,
    TxContextEvent as WitTxContextEvent, UtxoEvent as WitUtxoEvent,
};

/// Public entry point: convert an internal `DispatchEvent` to
/// the WIT-bindgen shape.
pub fn dispatch_to_wit(event: DispatchEvent) -> bindings_v2::DispatchEvent {
    match event {
        DispatchEvent::Utxo(u) => bindings_v2::DispatchEvent::Utxo(utxo_to_wit(*u)),
        DispatchEvent::Tick(t) => bindings_v2::DispatchEvent::Tick(tick_to_wit(t)),
        DispatchEvent::Rollback(r) => bindings_v2::DispatchEvent::Rollback(rollback_to_wit(r)),
    }
}

fn utxo_to_wit(event: UtxoEvent) -> WitUtxoEvent {
    match event {
        UtxoEvent::TxContext(e) => WitUtxoEvent::TxContext(tx_context_to_wit(e)),
        UtxoEvent::Referenced(e) => WitUtxoEvent::Referenced(referenced_to_wit(e)),
        UtxoEvent::Consumed(e) => WitUtxoEvent::Consumed(consumed_to_wit(e)),
        UtxoEvent::Produced(e) => WitUtxoEvent::Produced(produced_to_wit(e)),
        UtxoEvent::Minted(e) => WitUtxoEvent::Minted(minted_to_wit(e)),
    }
}

fn produced_to_wit(e: ProducedEvent) -> WitProducedEvent {
    WitProducedEvent {
        cursor: chain_point_to_wit(e.cursor),
        tx_hash: e.tx_hash.as_ref().to_vec(),
        tx_idx: e.tx_idx,
        oref: output_ref_to_wit(e.oref),
        output: typed_output_to_wit(e.output),
        datum: e.datum.map(typed_datum_to_wit),
    }
}

fn consumed_to_wit(e: ConsumedEvent) -> WitConsumedEvent {
    WitConsumedEvent {
        cursor: chain_point_to_wit(e.cursor),
        consuming_tx_hash: e.consuming_tx_hash.as_ref().to_vec(),
        consuming_tx_idx: e.consuming_tx_idx,
        oref: output_ref_to_wit(e.oref),
        prior_output: typed_output_to_wit(e.prior_output),
        prior_datum: e.prior_datum.map(typed_datum_to_wit),
        redeemer: e.redeemer,
    }
}

fn referenced_to_wit(e: ReferencedEvent) -> WitReferencedEvent {
    WitReferencedEvent {
        cursor: chain_point_to_wit(e.cursor),
        referencing_tx_hash: e.referencing_tx_hash.as_ref().to_vec(),
        referencing_tx_idx: e.referencing_tx_idx,
        oref: output_ref_to_wit(e.oref),
        prior_output: typed_output_to_wit(e.prior_output),
        prior_datum: e.prior_datum.map(typed_datum_to_wit),
    }
}

fn minted_to_wit(e: MintedEvent) -> WitMintedEvent {
    WitMintedEvent {
        cursor: chain_point_to_wit(e.cursor),
        tx_hash: e.tx_hash.as_ref().to_vec(),
        tx_idx: e.tx_idx,
        // PolicyId is hex-typed; `as_bytes` gives us the 28
        // raw bytes when the inner string is valid hex. Fall
        // back to a zero-policy on internal corruption (which
        // shouldn't happen since the host built this PolicyId
        // from chain bytes).
        policy: e.policy.as_bytes().unwrap_or([0u8; 28]).to_vec(),
        asset_name: e.asset_name,
        quantity_delta: e.quantity_delta,
    }
}

fn tx_context_to_wit(e: TxContextEvent) -> WitTxContextEvent {
    WitTxContextEvent {
        cursor: chain_point_to_wit(e.cursor),
        tx_hash: e.tx_hash.as_ref().to_vec(),
        tx_idx: e.tx_idx,
        validity_interval: validity_interval_to_wit(e.validity_interval),
        required_signers: e
            .required_signers
            .into_iter()
            .map(|h| h.as_ref().to_vec())
            .collect(),
    }
}

fn tick_to_wit(e: TickEvent) -> WitTickEvent {
    WitTickEvent {
        cursor: chain_point_to_wit(e.cursor),
        timestamp: e.timestamp,
        interval_seconds: e.interval_seconds,
    }
}

fn rollback_to_wit(e: RollbackEvent) -> WitRollbackEvent {
    WitRollbackEvent {
        to_cursor: chain_point_to_wit(e.to_cursor),
    }
}

fn chain_point_to_wit(cp: mitos_data_plane::ChainPoint) -> bindings_v2::ChainPoint {
    match cp {
        mitos_data_plane::ChainPoint::Origin => bindings_v2::ChainPoint::Origin,
        mitos_data_plane::ChainPoint::Slot(s) => bindings_v2::ChainPoint::SlotOnly(s),
        mitos_data_plane::ChainPoint::Specific(s, h) => {
            bindings_v2::ChainPoint::Specific(bindings_v2::SpecificPoint {
                slot: s,
                block_hash: h.as_ref().to_vec(),
            })
        }
    }
}

fn output_ref_to_wit(r: mitos_data_plane::OutputRef) -> bindings_v2::OutputRef {
    bindings_v2::OutputRef {
        tx_hash: r.tx_hash.as_ref().to_vec(),
        index: r.index,
    }
}

fn typed_output_to_wit(o: mitos_data_plane::TypedOutput) -> bindings_v2::TypedOutput {
    bindings_v2::TypedOutput {
        address: o.address,
        lovelace: o.lovelace,
        assets: o
            .assets
            .into_iter()
            .map(|a| bindings_v2::AssetEntry {
                asset: bindings_v2::AssetId {
                    policy: a.policy_id.as_bytes().unwrap_or([0u8; 28]).to_vec(),
                    name: hex::decode(&a.asset_name_hex).unwrap_or_default(),
                },
                quantity: a.quantity,
            })
            .collect(),
    }
}

fn typed_datum_to_wit(d: mitos_data_plane::TypedDatum) -> bindings_v2::TypedDatum {
    bindings_v2::TypedDatum {
        hash: d.hash.as_ref().to_vec(),
        // `payload` carries the on-chain CBOR bytes when the
        // host could resolve. `original_cbor` is the source we
        // shipped through `read_output_datums` /
        // `block_events::extract_datum_info`. Empty `Vec` means
        // "host couldn't resolve, only the hash is meaningful"
        // — modules detect that and fall back via
        // `chain-data::tx-metadata`.
        payload: d.original_cbor.unwrap_or_default(),
    }
}

fn validity_interval_to_wit(v: ValidityInterval) -> bindings_v2::ValidityInterval {
    bindings_v2::ValidityInterval {
        valid_from: v.valid_from,
        valid_to: v.valid_to,
    }
}

// Suppress dead-code warning on the imported MintDraft —
// downstream code (mint conversions when we wire `read-tx`)
// will reference it, but for now the conversion entry points
// only consume MintedEvent.
#[allow(dead_code)]
fn _mint_draft_marker(_: MintDraft) {}
