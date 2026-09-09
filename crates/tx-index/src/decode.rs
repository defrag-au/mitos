//! Body → outputs, by era. The index points at a BODY, so this decodes the
//! era-specific body type directly rather than a whole `Tx` — the witness
//! set is not on disk next to it in Shelley+ blocks, and an output lookup
//! never needs it.

use std::ops::Deref;

use anyhow::{Result, anyhow};
use pallas_codec::minicbor;
use pallas_primitives::{alonzo, babbage, byron, conway};
use pallas_traverse::{Era, MultiEraOutput};

use crate::wire::{OutputAsset, ResolvedOutput};

pub fn outputs(era: Era, body: &[u8]) -> Result<Vec<ResolvedOutput>> {
    let decode_err = |e: minicbor::decode::Error| anyhow!("decoding {era} body: {e}");
    match era {
        Era::Byron => {
            let tx: byron::Tx = minicbor::decode(body).map_err(decode_err)?;
            Ok(tx
                .outputs
                .iter()
                .enumerate()
                .map(|(i, o)| convert(i, &MultiEraOutput::from_byron(o)))
                .collect())
        }
        Era::Shelley | Era::Allegra | Era::Mary | Era::Alonzo => {
            let b: alonzo::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.outputs
                .iter()
                .enumerate()
                .map(|(i, o)| convert(i, &MultiEraOutput::from_alonzo_compatible(o, era)))
                .collect())
        }
        Era::Babbage => {
            let b: babbage::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.outputs
                .iter()
                .enumerate()
                .map(|(i, o)| convert(i, &MultiEraOutput::from_babbage(o.deref())))
                .collect())
        }
        Era::Conway => {
            let b: conway::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.outputs
                .iter()
                .enumerate()
                .map(|(i, o)| convert(i, &MultiEraOutput::from_conway(o)))
                .collect())
        }
        // `Era` is `#[non_exhaustive]` upstream. A new era means a new body
        // type this build does not know; say so rather than mis-decode.
        other => Err(anyhow!("era {other} is newer than this build of tx-index")),
    }
}

fn convert(index: usize, o: &MultiEraOutput<'_>) -> ResolvedOutput {
    let address = o
        .address()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unparsable>".into());

    let mut assets = Vec::new();
    for bundle in o.value().assets() {
        for a in bundle.assets() {
            if let Some(quantity) = a.output_coin() {
                assets.push(OutputAsset {
                    policy: hex::encode(a.policy()),
                    name: hex::encode(a.name()),
                    quantity,
                });
            }
        }
    }

    let (datum_hash, inline_datum) = match o.datum() {
        None => (None, None),
        Some(conway::DatumOption::Hash(h)) => (Some(hex::encode(h)), None),
        Some(conway::DatumOption::Data(d)) => (None, Some(hex::encode(d.0.raw_cbor()))),
    };

    ResolvedOutput {
        index: index as u32,
        address,
        lovelace: o.value().coin(),
        assets,
        datum_hash,
        inline_datum,
        has_script_ref: o.script_ref().is_some(),
        cbor: hex::encode(o.encode()),
    }
}
