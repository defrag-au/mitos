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

/// Body → the outrefs it spends, `(tx hash, output index)`, in body order.
///
/// Shelley-family bodies only: a Byron input is a different shape, and
/// nothing that asks for inputs (following a DEX order back to whoever
/// placed it) reaches back before Shelley.
pub fn inputs(era: Era, body: &[u8]) -> Result<Vec<([u8; 32], u32)>> {
    let decode_err = |e: minicbor::decode::Error| anyhow!("decoding {era} body: {e}");
    let of = |i: &alonzo::TransactionInput| (*i.transaction_id, i.index as u32);
    match era {
        Era::Byron => Err(anyhow!("Byron inputs are not decoded")),
        Era::Shelley | Era::Allegra | Era::Mary | Era::Alonzo => {
            let b: alonzo::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.inputs.iter().map(of).collect())
        }
        Era::Babbage => {
            let b: babbage::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.inputs.iter().map(of).collect())
        }
        Era::Conway => {
            let b: conway::TransactionBody = minicbor::decode(body).map_err(decode_err)?;
            Ok(b.inputs.iter().map(of).collect())
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The smallest body each era accepts: `{0: [[hash, 1], [hash', 7]], 1: [], 2: 0}`
    /// (inputs, no outputs, no fee).
    fn body(a: [u8; 32], b: [u8; 32]) -> Vec<u8> {
        let mut out = vec![0xa3, 0x00, 0x82];
        for (h, ix) in [(a, 1u8), (b, 7u8)] {
            out.extend([0x82, 0x58, 0x20]);
            out.extend(h);
            out.push(ix);
        }
        out.extend([0x01, 0x80, 0x02, 0x00]);
        out
    }

    #[test]
    fn a_body_lists_the_outrefs_it_spends() {
        let (a, b) = ([0x11; 32], [0x22; 32]);
        for era in [Era::Alonzo, Era::Babbage, Era::Conway] {
            let got = inputs(era, &body(a, b)).unwrap_or_else(|e| panic!("{era}: {e}"));
            let mut want = vec![(a, 1), (b, 7)];
            // A Conway input SET is canonical-ordered; compare as a set.
            let mut got = got;
            got.sort();
            want.sort();
            assert_eq!(got, want, "{era}");
        }
        assert!(inputs(Era::Byron, &body(a, b)).is_err());
    }
}
