//! jpg.store labels-50+ offer-datum recovery.
//!
//! jpg CO offer outputs commit a hash-only datum whose bytes are published in
//! the create tx's metadata across labels 50..=63 (hex chunks). We reassemble
//! and blake2b-verify against the output's datum hash. Ported verbatim from the
//! `jpg-store-offer` module so the walker recovers offer terms at create time
//! identically. (Consumed offers also reveal their datum in the spend tx's
//! witness set, so this is only load-bearing for the *create* event.)

use pallas_codec::minicbor::data::Type;
use pallas_crypto::hash::Hasher;

/// Recover the datum CBOR matching `datum_hash` from a tx's auxiliary-data CBOR,
/// or `None` if no labels-50+ reconstruction hash-verifies.
pub fn recover_datum(aux_cbor: &[u8], datum_hash: &[u8]) -> Option<Vec<u8>> {
    for candidate in parse_metadata_datums(aux_cbor) {
        if candidate.len() % 2 != 0 || !candidate.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let bytes = hex::decode(&candidate).ok()?;
        if Hasher::<256>::hash(&bytes).as_ref() == datum_hash {
            return Some(bytes);
        }
    }
    None
}

fn parse_metadata_datums(aux_cbor: &[u8]) -> Vec<String> {
    let mut entries: Vec<(u64, String)> = Vec::new();
    if extract_metadata_entries(aux_cbor, &mut entries).is_err() {
        return Vec::new();
    }
    entries.sort_by_key(|(k, _)| *k);

    let mut datums = Vec::new();
    let mut current = String::new();
    for (label, val) in entries {
        if label < 50 {
            continue;
        }
        if val.contains("::") {
            continue;
        }
        if let Some((prefix, _)) = val.split_once(',') {
            if !prefix.is_empty() {
                current.push_str(prefix);
            }
            if !current.is_empty() {
                datums.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(&val);
        }
    }
    if !current.is_empty() {
        datums.push(current);
    }
    datums
}

fn extract_metadata_entries(
    aux_cbor: &[u8],
    out: &mut Vec<(u64, String)>,
) -> Result<(), pallas_codec::minicbor::decode::Error> {
    let mut d = pallas_codec::minicbor::Decoder::new(aux_cbor);

    if d.datatype()? == Type::Tag {
        let _tag = d.tag()?;
        let outer_len = d.map()?;
        let mut found = false;
        let mut i = 0u64;
        loop {
            if let Some(n) = outer_len
                && i >= n
            {
                break;
            }
            if outer_len.is_none() && d.datatype()? == Type::Break {
                d.skip()?;
                break;
            }
            let key: u64 = d.u64()?;
            if key == 0 {
                found = true;
                break;
            }
            d.skip()?;
            i += 1;
        }
        if !found {
            return Ok(());
        }
    }

    let map_len = d.map()?;
    let mut i = 0u64;
    loop {
        if let Some(n) = map_len
            && i >= n
        {
            break;
        }
        if map_len.is_none() && d.datatype()? == Type::Break {
            d.skip()?;
            break;
        }
        let label: u64 = d.u64()?;
        match d.datatype()? {
            Type::String => {
                let s: &str = d.str()?;
                out.push((label, s.to_owned()));
            }
            _ => {
                d.skip()?;
            }
        }
        i += 1;
    }
    Ok(())
}
