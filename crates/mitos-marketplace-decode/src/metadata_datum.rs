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
//! wasm-safe and import-free.
//!
//! **What it costs the caller.** On the LIVE path the metadata rides in the
//! block the host is already processing, so the fetch is local and free. On a
//! BOOTSTRAP re-walk it is not: those transactions are years old, and each one
//! is a lookup against whatever the host's fallback is. Measured on mainnet
//! that was ~4/second — hours, for the residual jpg book. The fix is on the
//! host side (a local aux-data index), not here; this module is only noting
//! that the call it asks for is not always cheap.

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

/// The raw auxiliary-data CBOR of a whole transaction, ready for
/// [`recover_datum_from_metadata`].
///
/// A caller that has a tx from an indexer (Koios `/tx_cbor`, a chunk store)
/// rather than a block cannot reach its metadata through `pallas_traverse`:
/// `MultiEraTx::aux_data` is `pub(crate)`, and `metadata()` hands back a
/// DECODED view. Recovery needs the original bytes — the datum hash is taken
/// over CBOR that does not survive a decode/re-encode round trip.
///
/// Auxiliary data is the LAST element of the transaction array in every era
/// that has it (`[body, wits, is_valid, aux]` from Alonzo, `[body, wits, aux]`
/// before), so it is found positionally rather than by era. `None` covers both
/// a `null` aux field and a transaction carrying none.
pub fn aux_data_from_tx_cbor(tx_cbor: &[u8]) -> Option<Vec<u8>> {
    let mut d = pallas_codec::minicbor::Decoder::new(tx_cbor);
    let len = d.array().ok()??;
    if len < 3 {
        return None;
    }

    // Walk to the last element, remembering where it starts.
    let mut start = 0usize;
    for _ in 0..len {
        start = d.position();
        d.skip().ok()?;
    }
    let end = d.position();

    let slice = tx_cbor.get(start..end)?;
    // A nullable aux field encodes as CBOR `null` (0xf6) when absent — that is
    // a present-but-empty answer, not metadata.
    if slice == [0xf6] {
        return None;
    }
    Some(slice.to_vec())
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

    /// Auxiliary data of a REAL jpg.store V2 listing create: mainnet tx
    /// `c3e6f5a6…`, immutable chunk 5429, Babbage. Output 0 sits at the jpg V2
    /// listing address and commits its datum by HASH ONLY — precisely the
    /// shape that decoded to no payouts and price 0 before this recovery
    /// existed.
    ///
    /// These are the exact bytes `tx-index` serves from the chunk store, so
    /// this test is the seam between the index and the decoder.
    ///
    /// Note the shape: a BARE metadata map (`a8`, a map of 8), NOT the tagged
    /// post-Alonzo form — in a Babbage-era transaction. Both are live on
    /// mainnet, which is why the parser must accept either.
    const JPG_V2_LISTING_AUX: &str = "a8181e61361832784064383739396639666438373939666438373939666438373939663538316336366235646461336666383334343236636163633864666337343238663635343562183378403261656166376266646539313131633361396234373066666438373939666438373939666438373939663538316363373363316638666661633334643734383018347840653835363866333330633965396561343463393330376533643865613832643266333466323366666666666666663161303163396333383066666438373939661835784064383739396664383739396635383163366230313530663761343262373730373635663130616438353365623765333337353138633566613064616363373861183678406432393334353063666664383739396664383739396664383739396635383163303135643637336539396236323235646466636630633033306635666165386618377840323565383735386134323135323364353161663965333466666666666666666631613162366230623030666666663538316336623031353066376134326237371838782d30373635663130616438353365623765333337353138633566613064616363373861643239333435306366662c";

    /// The hash output 0 of that transaction actually committed to.
    const JPG_V2_LISTING_DATUM_HASH: &str =
        "a7f897b965eb2919b9626f8524b0f3ead45a64c99b5dfa4cda6f97a58c9bb186";

    #[test]
    fn recovers_a_real_jpg_listing_datum_from_chain_aux_data() {
        let aux = hex::decode(JPG_V2_LISTING_AUX).expect("fixture is hex");
        let want = hex::decode(JPG_V2_LISTING_DATUM_HASH).expect("fixture is hex");

        let datum = recover_datum_from_metadata(&aux, &want)
            .expect("the listing datum is recoverable from its own tx metadata");

        // The hash check is what makes recovery safe, so assert it directly
        // rather than trusting the function that already checked it.
        assert_eq!(Hasher::<256>::hash(&datum).as_ref(), want.as_slice());
        assert_eq!(
            &datum[..2],
            &[0xd8, 0x79],
            "a jpg listing datum is a constructor-0 Plutus datum"
        );
    }

    /// The label-30 entry in that same fixture is jpg's own annotation and sits
    /// below the 50 floor; a parser that swept it in would offer a candidate
    /// that cannot hash, masking a real failure as a near miss.
    #[test]
    fn the_real_fixture_yields_exactly_one_candidate() {
        let aux = hex::decode(JPG_V2_LISTING_AUX).expect("fixture is hex");
        assert_eq!(parse_metadata_datums(&aux).len(), 1);
    }

    /// Build a minimal Alonzo-shaped tx: `[body, wits, is_valid, aux]`.
    fn tx_with(aux: Option<&[u8]>) -> Vec<u8> {
        let mut e = pallas_codec::minicbor::Encoder::new(Vec::new());
        e.array(4).unwrap();
        e.map(0).unwrap(); // body
        e.map(0).unwrap(); // witness set
        e.bool(true).unwrap(); // is_valid
        match aux {
            Some(bytes) => e.writer_mut().extend_from_slice(bytes),
            None => {
                e.null().unwrap();
            }
        }
        e.into_writer()
    }

    #[test]
    fn extracts_aux_from_a_whole_tx() {
        let aux_bytes = aux(&[(50, "abcd")]);
        let tx = tx_with(Some(&aux_bytes));
        assert_eq!(aux_data_from_tx_cbor(&tx), Some(aux_bytes));
    }

    /// A `null` aux field is "this tx has no metadata", not a byte string to
    /// hand the parser.
    #[test]
    fn a_null_aux_field_is_none() {
        assert_eq!(aux_data_from_tx_cbor(&tx_with(None)), None);
    }

    /// Pre-Alonzo transactions are `[body, wits, aux]` — three elements, aux
    /// still last. Finding it positionally is what makes one rule cover both.
    #[test]
    fn extracts_aux_from_a_three_element_tx() {
        let aux_bytes = aux(&[(50, "beef")]);
        let mut e = pallas_codec::minicbor::Encoder::new(Vec::new());
        e.array(3).unwrap();
        e.map(0).unwrap();
        e.map(0).unwrap();
        e.writer_mut().extend_from_slice(&aux_bytes);
        assert_eq!(aux_data_from_tx_cbor(&e.into_writer()), Some(aux_bytes));
    }

    /// The extracted bytes must be the ORIGINAL slice: the datum hash is taken
    /// over CBOR that a decode/re-encode would not reproduce, so a round trip
    /// here would silently break recovery.
    #[test]
    fn extracted_aux_still_recovers_a_datum() {
        let datum = vec![0x2a; 40];
        let hash = Hasher::<256>::hash(&datum);
        let tx = tx_with(Some(&aux(&[(50, hex::encode(&datum).as_str())])));

        let extracted = aux_data_from_tx_cbor(&tx).expect("tx carries aux data");
        assert_eq!(
            recover_datum_from_metadata(&extracted, hash.as_ref()),
            Some(datum)
        );
    }

    #[test]
    fn malformed_tx_cbor_is_none_not_a_panic() {
        assert_eq!(aux_data_from_tx_cbor(&[]), None);
        assert_eq!(aux_data_from_tx_cbor(&[0xff, 0x00]), None);
        // An array too short to be a transaction.
        assert_eq!(aux_data_from_tx_cbor(&[0x82, 0xa0, 0xa0]), None);
    }

    #[test]
    fn malformed_aux_is_empty_not_a_panic() {
        assert!(parse_metadata_datums(&[]).is_empty());
        assert!(parse_metadata_datums(&[0xff, 0x00]).is_empty());
        assert_eq!(recover_datum_from_metadata(&[0x00], &[0u8; 32]), None);
    }
}
