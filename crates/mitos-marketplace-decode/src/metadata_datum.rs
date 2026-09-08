//! Recovering a hash-only datum from jpg.store's transaction metadata.
//!
//! jpg commits listing and offer datums by **hash**, and publishes the preimage
//! in the same transaction's auxiliary data under labels 50+, chunked as hex
//! strings. Without this recovery a jpg `Create` decodes to no payouts and no
//! price — the datum is simply not in the output.
//!
//! The offer modules have done this since the Phase 0 extraction; the listing
//! modules did not, which is why the jpg **ask** book decoded priceless while
//! the bid book worked. This is that recovery, shared so the two cannot drift.
//!
//! Everything here is pure: the caller fetches the aux-data bytes from its own
//! host (`chain_data::tx_metadata`) and passes them in. That keeps the crate
//! wasm-safe and import-free, and it is why the recovery is cheap — the
//! metadata rides in the block the host already has, so there is no per-listing
//! lookup and no boot-stall risk.

use pallas_codec::minicbor::data::Type;
use pallas_crypto::hash::Hasher;

/// Recover a datum preimage from a transaction's auxiliary data, verified
/// against the hash the output committed to.
///
/// Returns `None` when no candidate hashes to `datum_hash` — the hash check is
/// what makes this safe: metadata is attacker-controllable, so a candidate is
/// only accepted when it *is* the datum the output committed to.
pub fn recover_datum_from_metadata(aux_cbor: &[u8], datum_hash: &[u8]) -> Option<Vec<u8>> {
    for candidate in parse_metadata_datums(aux_cbor) {
        if candidate.len() % 2 != 0 || !candidate.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let Ok(bytes) = hex::decode(&candidate) else {
            continue;
        };
        if Hasher::<256>::hash(&bytes).as_ref() == datum_hash {
            return Some(bytes);
        }
    }
    None
}

/// Walk aux-data for jpg.store's labels-50+ chunked-hex convention.
///
/// A datum can span several labels; a value containing `,` terminates the
/// current chunk group, and values containing `::` are jpg's own annotations
/// rather than datum bytes.
pub fn parse_metadata_datums(aux_cbor: &[u8]) -> Vec<String> {
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

    // Post-Alonzo aux data is a tagged map whose key 0 holds the metadata;
    // Shelley-era aux data is the bare metadata map.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build minimal Alonzo-style aux data: `tag(259) { 0: { label: text } }`.
    fn aux(entries: &[(u64, &str)]) -> Vec<u8> {
        let mut e = pallas_codec::minicbor::Encoder::new(Vec::new());
        e.tag(pallas_codec::minicbor::data::Tag::new(259)).unwrap();
        e.map(1).unwrap();
        e.u64(0).unwrap();
        e.map(entries.len() as u64).unwrap();
        for (label, val) in entries {
            e.u64(*label).unwrap();
            e.str(val).unwrap();
        }
        e.into_writer()
    }

    #[test]
    fn recovers_a_single_label_datum() {
        let datum = b"\xd8\x79\x80".to_vec();
        let hex_datum = hex::encode(&datum);
        let hash = Hasher::<256>::hash(&datum);

        let aux = aux(&[(50, hex_datum.as_str())]);
        assert_eq!(
            recover_datum_from_metadata(&aux, hash.as_ref()),
            Some(datum)
        );
    }

    /// Real datums exceed one metadata string and are chunked across labels.
    #[test]
    fn recovers_a_datum_chunked_across_labels() {
        let datum = vec![0xab; 96];
        let hex_datum = hex::encode(&datum);
        let (a, b) = hex_datum.split_at(64);
        let hash = Hasher::<256>::hash(&datum);

        let aux = aux(&[(50, a), (51, b)]);
        assert_eq!(
            recover_datum_from_metadata(&aux, hash.as_ref()),
            Some(datum)
        );
    }

    /// The hash check is the security boundary — metadata is caller-supplied,
    /// so a candidate that is not the committed datum must be refused.
    #[test]
    fn refuses_a_candidate_that_does_not_hash_to_the_commitment() {
        let real = vec![0x01; 32];
        let impostor = vec![0x02; 32];
        let aux = aux(&[(50, hex::encode(&impostor).as_str())]);

        let hash = Hasher::<256>::hash(&real);
        assert_eq!(recover_datum_from_metadata(&aux, hash.as_ref()), None);
    }

    #[test]
    fn ignores_labels_below_50_and_annotation_values() {
        let datum = vec![0xcd; 32];
        let hash = Hasher::<256>::hash(&datum);
        let aux = aux(&[
            (49, "not a datum"),
            (50, "jpg::annotation"),
            (51, hex::encode(&datum).as_str()),
        ]);
        assert_eq!(
            recover_datum_from_metadata(&aux, hash.as_ref()),
            Some(datum)
        );
    }

    #[test]
    fn malformed_aux_is_empty_not_a_panic() {
        assert!(parse_metadata_datums(&[]).is_empty());
        assert!(parse_metadata_datums(&[0xff, 0x00]).is_empty());
        assert_eq!(recover_datum_from_metadata(&[0x00], &[0u8; 32]), None);
    }
}
