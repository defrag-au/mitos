//! Pure block/tx decode → the pieces a walker's outref buffer + decode assembly
//! need. Mirrors mitos-data-plane's `block_events::project_tx` on bare pallas
//! (no dolos): canonical spend-redeemer ordering, witness-datum extraction, and
//! per-output datum (hash + inline bytes). Keeping the exact pallas calls the
//! live host uses is what makes a walker's `DecodeTx` byte-compatible with the
//! modules' decode.
//!
//! Derives on the types are the minimum a downstream walker needs to persist a
//! buffered output or key a map: `Asset` is `Clone + PartialEq + Eq + Hash`
//! (market-ledger checkpoints it; project-ledger keys the policy filter on it).

use std::collections::HashMap;

use pallas_codec::utils::Nullable;
use pallas_primitives::Hash;
use pallas_primitives::conway::DatumOption;
use pallas_traverse::{MultiEraOutput, MultiEraTx, OriginalHash};

/// An output reference (origin tx hash + output index).
pub type OutRef = (Hash<32>, u32);

/// A native asset (policy id + on-chain asset-name bytes).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Asset {
    pub policy: Vec<u8>,
    pub name: Vec<u8>,
}

/// A decoded produced output.
pub struct DecodedOutput {
    pub address: String,
    pub lovelace: u64,
    pub assets: Vec<Asset>,
    pub index: u32,
    pub datum_hash: Option<Hash<32>>,
    pub inline_datum: Option<Vec<u8>>,
    /// The protocol minimum this output must hold, in lovelace — **0 when the
    /// output carries no assets**.
    ///
    /// A token cannot sit on the chain alone: every output carrying one must
    /// also carry enough ADA to pay for its own bytes. That ADA is a CARRIER,
    /// not a payment, and booking it as value movement is how a mint's asset
    /// distribution turns into tens of thousands of meaningless ADA flows. The
    /// meaningful half of such a transfer is the asset — which matters a great
    /// deal, since assets moving between insiders can BE the payment for
    /// services.
    ///
    /// Recorded rather than subtracted: an output holding a token AND real
    /// value is common (a wallet sending 500 ₳ and an NFT together), so the
    /// interesting quantity is `lovelace - min_utxo`, and only a consumer can
    /// decide what to do with it. Measured on Mekka, 45.5% of lovelace flow
    /// rows ride with a token but only ~6% of those are at the floor — a
    /// blanket "ignore ADA on token outputs" rule would have deleted 43M ₳ of
    /// genuine movement.
    ///
    /// Babbage/Conway rule: `(160 + |serialised output|) * coins_per_utxo_byte`.
    /// Skipped entirely for asset-free outputs because `encode()` allocates,
    /// and the walk decodes tens of millions of outputs where the answer could
    /// not matter.
    pub min_utxo: u64,
}

/// Protocol parameter `coinsPerUTxOByte`, Babbage onward.
///
/// A parameter, not a constant of nature — it is protocol-updatable, and a walk
/// spanning an update would want it per-era. It has held at this value for
/// every era this tool walks; when that stops being true, this is the thing to
/// make era-aware.
pub const COINS_PER_UTXO_BYTE: u64 = 4310;

/// Overhead the ledger adds to the serialised size before pricing it.
const MIN_UTXO_OVERHEAD_BYTES: u64 = 160;

/// A spent input: its outref + the spend redeemer's PlutusData bytes (resolved
/// against canonical input order).
pub struct DecodedInput {
    pub oref: OutRef,
    pub redeemer: Option<Vec<u8>>,
}

/// One transaction's decoded parts.
pub struct DecodedTx {
    pub tx_hash: Hash<32>,
    pub inputs: Vec<DecodedInput>,
    pub outputs: Vec<DecodedOutput>,
    /// 28-byte required-signer key hashes.
    pub required_signers: Vec<Vec<u8>>,
    /// Datums revealed in this tx's witness set: hash → CBOR bytes. The local
    /// equivalent of the firehose's Koios `/datum_info` — a hash-only script
    /// UTxO's datum must be supplied here to spend it.
    pub witness_datums: HashMap<Hash<32>, Vec<u8>>,
    /// Raw auxiliary-data (tx metadata) CBOR, for jpg's labels-50 offer-datum
    /// recovery at create time.
    pub aux_data: Option<Vec<u8>>,
}

pub fn decode_tx(tx: &MultiEraTx<'_>) -> DecodedTx {
    let tx_hash = tx.hash();

    let outputs: Vec<DecodedOutput> = tx
        .outputs()
        .iter()
        .enumerate()
        .map(|(i, o)| decode_output(i as u32, o))
        .collect();

    // Spend-redeemers reference inputs by their canonical index — the position
    // in the ledger-sorted set (by (tx_hash, index) ascending), NOT tx-body
    // order. Compute the canonical index per body position, then look up the
    // redeemer by it. (Mirrors project_tx; skipping this mis-routes redeemers on
    // txs whose wallet + script inputs sort opposite their body order.)
    let consumed = tx.consumes();
    let canonical_for_body: Vec<u32> = {
        let mut sortable: Vec<(usize, (Hash<32>, u32))> = consumed
            .iter()
            .enumerate()
            .map(|(i, inp)| (i, (*inp.hash(), inp.index() as u32)))
            .collect();
        sortable.sort_by_key(|a| a.1);
        let mut canonical = vec![0u32; consumed.len()];
        for (canonical_idx, (body_idx, _)) in sortable.into_iter().enumerate() {
            canonical[body_idx] = canonical_idx as u32;
        }
        canonical
    };
    let inputs: Vec<DecodedInput> = consumed
        .iter()
        .enumerate()
        .map(|(body_idx, inp)| {
            let canonical_idx = canonical_for_body[body_idx];
            // The redeemer's `data` field (PlutusData) only — the crate's decode
            // matches on the constructor prefix (`d879`/`d87a`), so the bare
            // datum bytes are what it needs.
            let redeemer = tx.find_spend_redeemer(canonical_idx).map(|r| {
                let mut buf = Vec::with_capacity(8);
                let _ = pallas_codec::minicbor::encode(r.data(), &mut buf);
                buf
            });
            DecodedInput {
                oref: (*inp.hash(), inp.index() as u32),
                redeemer,
            }
        })
        .collect();

    let required_signers: Vec<Vec<u8>> = tx
        .required_signers()
        .collect::<Vec<&Hash<28>>>()
        .iter()
        .map(|h| h.as_slice().to_vec())
        .collect();

    let witness_datums: HashMap<Hash<32>, Vec<u8>> = tx
        .plutus_data()
        .iter()
        .map(|d| (d.original_hash(), d.raw_cbor().to_vec()))
        .collect();

    DecodedTx {
        tx_hash,
        inputs,
        outputs,
        required_signers,
        witness_datums,
        aux_data: extract_aux_data(tx),
    }
}

fn decode_output(index: u32, o: &MultiEraOutput<'_>) -> DecodedOutput {
    let address = o.address().map(|a| a.to_string()).unwrap_or_default();
    let value = o.value();
    let lovelace = value.coin();

    let assets: Vec<Asset> = value
        .assets()
        .iter()
        .flat_map(|pa| {
            let policy = pa.policy().as_slice().to_vec();
            pa.assets()
                .iter()
                .map(|a| Asset {
                    policy: policy.clone(),
                    name: a.name().to_vec(),
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let (datum_hash, inline_datum) = match o.datum() {
        Some(DatumOption::Hash(h)) => (Some(h), None),
        Some(DatumOption::Data(w)) => (Some(w.0.original_hash()), Some(w.0.raw_cbor().to_vec())),
        None => (None, None),
    };

    // Only asset-bearing outputs get priced — see `DecodedOutput::min_utxo`.
    let min_utxo = if assets.is_empty() {
        0
    } else {
        (MIN_UTXO_OVERHEAD_BYTES + o.encode().len() as u64) * COINS_PER_UTXO_BYTE
    };

    DecodedOutput {
        address,
        lovelace,
        assets,
        index,
        datum_hash,
        inline_datum,
        min_utxo,
    }
}

fn extract_aux_data(tx: &MultiEraTx<'_>) -> Option<Vec<u8>> {
    match tx {
        MultiEraTx::AlonzoCompatible(t, _) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        MultiEraTx::Babbage(t) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        MultiEraTx::Conway(t) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_traverse::MultiEraBlock;
    use std::path::PathBuf;

    /// Decode every tx of a captured mainnet block fixture — exercises the real
    /// pallas decode path (redeemer ordering, witness datums, output datums) on
    /// real data without needing a full immutable DB.
    #[test]
    fn decodes_mainnet_block_fixture() {
        let fixture =
            PathBuf::from("../../crates/mitos-platform/tests/fixtures/186000000.block.cbor");
        if !fixture.exists() {
            eprintln!("skipping: fixture not present at {}", fixture.display());
            return;
        }
        let cbor = std::fs::read(&fixture).expect("read fixture");
        let block = MultiEraBlock::decode(&cbor).expect("decode block");
        let mut txs = 0usize;
        let mut outputs = 0usize;
        for tx in block.txs() {
            let d = decode_tx(&tx);
            assert_eq!(d.tx_hash, tx.hash());
            outputs += d.outputs.len();
            // Every input carries its outref; redeemer is Some only for script
            // spends. No panic in canonical-order mapping is the real assertion.
            for inp in &d.inputs {
                let _ = inp.oref;
            }
            txs += 1;
        }
        assert!(txs > 0, "fixture has txs");
        assert!(outputs > 0, "fixture txs produce outputs");
    }
}
