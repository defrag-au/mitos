//! The fold's offer pass — step 3 of `docs/design/OFFER_INDEX_FORMAT.md`.
//!
//! The offers view asks "what has this holder offered?" and answers it today by
//! reading every live row at each venue's credential and filtering locally:
//! **151,880 rows and 65 MB to keep 879**, because the bidder is a field *inside* the
//! datum and the book indexes attributes of the output. This pass builds the missing
//! association — *the credential a venue's decoder names → the offer's outref* —
//! while the fold already has the bytes in hand.
//!
//! # What it reads
//!
//! A candidate is a created output whose address sits at one of [`venues`]: exactly,
//! for a contract with a single address (jpg), or by its **payment** credential for a
//! contract that issues one address per bidder (Wayup, whose stake part is the
//! bidder's). Its datum comes from wherever the chain put it:
//!
//! - **inline** on the output — read directly;
//! - **revealed in a witness set**, matched by hash — a transaction that creates or
//!   spends a hash-attached script output supplies the preimage, and the walk passes
//!   every one. [`OfferPass::reveal`] keeps the datums shaped like an offer, so an
//!   output created *after* an identical datum was revealed elsewhere still resolves;
//! - ⚠️ **not read: jpg's labels-50+ metadata convention.** That is an off-chain
//!   agreement rather than a chain guarantee, and a candidate it would have answered
//!   is counted (`unreadable`) rather than guessed at. The census in
//!   `DATUM_RESOLVER_FORMAT.md` §3 is the scale of it.
//!
//! # What it does not decide
//!
//! Liveness. An offer created and spent inside the walk is still in the list; the
//! book's own index is the only thing that knows, and it is asked at the boundary
//! ([`OfferPass::live`]).
//!
//! # Nothing per-venue reaches the artifact
//!
//! The pass decodes more than it publishes. What it hands the writer is a pair —
//! 28 bytes of credential, an outref — and the venue is deliberately not in it: a
//! reader derives it from the row it fetches, which already carries the address.

use std::collections::HashMap;

use address_registry::registry::{
    AddressCategory, Marketplace, MarketplacePurpose, RegistryNetwork, ScriptCategory,
    all_known_addresses, all_known_credentials, lookup_address_match,
};
use live_book::address::{self, Kind};
use live_book::publish::OfferAssociation;
use live_book::row::Datum;
use mitos_marketplace_decode::OfferDecode;
use pallas_addresses::Address;
use pallas_codec::minicbor::Decoder;
use pallas_primitives::Hash;

/// The credential an address is keyed by, when that credential is its payment part.
fn payment_credential(address: &[u8]) -> Option<[u8; 28]> {
    address::credential(address)
        .filter(|c| c.kind == Kind::Payment)
        .map(|c| *c.bytes())
}

/// An offer contract, as the registry describes it.
#[derive(Clone, Debug)]
pub struct Venue {
    /// The marketplace's own name, for the run report.
    pub label: String,
    /// The payment credential a bid is looked up by. **Not the address**: Wayup
    /// issues one address per bidder (the staking part is theirs, so their delegation
    /// survives the offer), and only the credential finds them all.
    pub credential: [u8; 28],
    /// The contract's own address, when it has exactly one. Load-bearing for jpg: its
    /// collection-offer contract and its sale escrow are one script under two
    /// delegation forms, therefore one credential — so a credential match alone would
    /// index a collection's *listings* as its holders' bids.
    pub address: Option<Vec<u8>>,
    pub decode: OfferDecode,
}

impl Venue {
    /// Whether an output at `address` sits at this contract.
    ///
    /// ⚠️ **The header is tested before the credential is derived**, and that is not
    /// micro-optimism: this runs for every output the walk creates, and
    /// `address::credential` parses a Byron payload to reach a root — kilobytes, on
    /// 32% of the store's blocks — for an address that could not carry a venue's
    /// payment credential. Only the script-payment shapes (types 1/3/5/7) can.
    pub fn owns(&self, address: &[u8]) -> bool {
        match &self.address {
            Some(own) => own.as_slice() == address,
            None => {
                let Some(header) = address.first().map(|b| b >> 4) else {
                    return false;
                };
                matches!(header, 1 | 3 | 5 | 7)
                    && payment_credential(address) == Some(self.credential)
            }
        }
    }
}

/// Which decoder reads a marketplace's collection offers. `None` for a marketplace
/// with no offer decoder — the registry may know its contract, but there is nothing
/// here to read it with, and an outref nobody can interpret is not an answer.
fn decode_for(marketplace: Marketplace) -> Option<OfferDecode> {
    match marketplace {
        Marketplace::JpgStore => Some(OfferDecode::Jpg),
        Marketplace::Wayup => Some(OfferDecode::Wayup),
        Marketplace::Unknown | Marketplace::Abandonware => None,
    }
}

/// Every offer contract the registry knows and this build can decode.
pub fn venues() -> Vec<Venue> {
    let mut out: Vec<Venue> = Vec::new();

    for addr in all_known_addresses() {
        let Some((cat, _)) = lookup_address_match(addr, RegistryNetwork::Mainnet) else {
            continue;
        };
        let AddressCategory::Script(ScriptCategory::Marketplace {
            marketplace,
            purpose: MarketplacePurpose::Offer,
            ..
        }) = cat
        else {
            continue;
        };
        let (Some(decode), Ok(bytes)) = (decode_for(*marketplace), bech32_bytes(addr)) else {
            continue;
        };
        let Some(credential) = payment_credential(&bytes) else {
            continue;
        };
        out.push(Venue {
            label: marketplace.to_string(),
            credential,
            address: Some(bytes),
            decode,
        });
    }

    // A venue with no address anywhere in the registry — Wayup locks each offer under
    // the bidder's own staking part, so the credential is the whole address class.
    for (hex, entry) in all_known_credentials() {
        let AddressCategory::Script(ScriptCategory::Marketplace {
            marketplace,
            purpose: MarketplacePurpose::Offer,
            ..
        }) = &entry.category
        else {
            continue;
        };
        let (Some(decode), Some(credential)) = (decode_for(*marketplace), parse_credential(hex))
        else {
            continue;
        };
        if out.iter().any(|v| v.credential == credential) {
            continue;
        }
        out.push(Venue {
            label: marketplace.to_string(),
            credential,
            address: None,
            decode,
        });
    }

    out
}

fn bech32_bytes(addr: &str) -> Result<Vec<u8>, ()> {
    match Address::from_bech32(addr) {
        Ok(Address::Shelley(shelley)) => Ok(shelley.to_vec()),
        // A reward address is a stake credential with no output form, and a Byron
        // address is not addressable by the book at all.
        _ => Err(()),
    }
}

fn parse_credential(hex: &str) -> Option<[u8; 28]> {
    <[u8; 28]>::try_from(hex::decode(hex).ok()?.as_slice()).ok()
}

/// Whether a revealed datum is the shape both venues share:
/// `Constr 0 [ Bytes(28), … ]`.
///
/// ⚠️ A **shape test, not a decode**, and it is the pass's hot path: every witness
/// datum of every transaction comes through here, and the datum set's capture has
/// 44.5 M of them store-wide. What it has to do is answer "could a venue's decoder
/// read this" without allocating; the decoders do the reading, and only for datums
/// that get this far.
fn offer_shaped(cbor: &[u8]) -> bool {
    let mut d = Decoder::new(cbor);
    matches!(d.tag(), Ok(t) if u64::from(t) == 121)
        && matches!(d.array(), Ok(None) | Ok(Some(2)))
        && matches!(d.bytes(), Ok(b) if b.len() == 28)
}

/// The fold's offer pass: the venues it reads, what it has decoded, and the
/// associations the writer publishes.
pub struct OfferPass {
    venues: Vec<Venue>,
    /// Associations in the order the walk found them. `publish_offers` sorts and
    /// dedupes, so nothing here has to.
    associations: Vec<OfferAssociation>,
    /// Datums the walk has seen revealed, by the hash an output commits to.
    ///
    /// Whole-run rather than per-block: an offer created after an identical datum was
    /// revealed elsewhere resolves from here, and that is a quarter of Wayup's live
    /// offers (`DATUM_RESOLVER_FORMAT.md` §3).
    revealed: HashMap<Hash<32>, Vec<u8>>,
    /// Outputs at a venue, and what happened to their datum.
    pub candidates: u64,
    pub decoded: u64,
    pub unreadable: u64,
    pub not_offers: u64,
    /// Of [`Self::decoded`]: how many read the datum off the output itself, and how
    /// many needed the reveal index. The split is the whole argument for keeping that
    /// index in memory — see the run report.
    pub from_output: u64,
    pub from_revealed: u64,
    /// Witness datums the walk passed, and how many were kept as offer-shaped.
    pub revelations: u64,
    pub retained: u64,
}

impl OfferPass {
    /// The pass over the registry's venues.
    pub fn new() -> OfferPass {
        OfferPass::over(venues())
    }

    /// The pass over a given venue list — the tests' entry, and the one that lets a
    /// run report what it is about to read before it reads anything.
    pub fn over(venues: Vec<Venue>) -> OfferPass {
        OfferPass {
            venues,
            associations: Vec::new(),
            revealed: HashMap::new(),
            candidates: 0,
            decoded: 0,
            unreadable: 0,
            not_offers: 0,
            from_output: 0,
            from_revealed: 0,
            revelations: 0,
            retained: 0,
        }
    }

    pub fn venues(&self) -> &[Venue] {
        &self.venues
    }

    /// One datum a transaction revealed, as the walk passes it.
    pub fn reveal(&mut self, hash: Hash<32>, cbor: &[u8]) {
        self.revelations += 1;
        if !offer_shaped(cbor) {
            return;
        }
        self.retained += 1;
        self.revealed.entry(hash).or_insert_with(|| cbor.to_vec());
    }

    /// One created output: candidate, and an association when its datum reads.
    pub fn output(&mut self, outref: (Hash<32>, u32), address: &[u8], datum: Datum<'_>) {
        let Some(venue) = self.venues.iter().find(|v| v.owns(address)) else {
            return;
        };
        self.candidates += 1;
        let (bytes, read_from) = match datum {
            Datum::Inline(b) => (b, &mut self.from_output),
            Datum::Hash(h) => match hash_of(h).and_then(|h| self.revealed.get(&h)) {
                Some(b) => (b.as_slice(), &mut self.from_revealed),
                None => {
                    self.unreadable += 1;
                    return;
                }
            },
            Datum::None => {
                self.unreadable += 1;
                return;
            }
        };
        let Some(offer) = venue.decode.datum(bytes) else {
            self.not_offers += 1;
            return;
        };
        let Some(credential) = offer.bidder_credential() else {
            self.not_offers += 1;
            return;
        };
        *read_from += 1;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(outref.0.as_ref());
        self.decoded += 1;
        self.associations.push(OfferAssociation {
            credential,
            outref: (hash, outref.1),
        });
    }

    /// The associations whose offers are still live at the boundary, in walk order.
    ///
    /// `is_live` is the book's own index — the one thing that knows whether a row
    /// created inside the walk survived it.
    pub fn live(&self, is_live: impl Fn(&live_book::OutRef) -> bool) -> Vec<OfferAssociation> {
        let mut live: Vec<OfferAssociation> = self
            .associations
            .iter()
            .copied()
            .filter(|a| is_live(&a.outref))
            .collect();
        live.sort_unstable_by(|a, b| {
            a.credential
                .cmp(&b.credential)
                .then_with(|| a.outref.cmp(&b.outref))
        });
        live.dedup();
        live
    }
}

/// `Hash<32>` from the 32 bytes a row's datum carries, or `None` for a length that
/// is not a datum hash at all.
fn hash_of(bytes: &[u8]) -> Option<Hash<32>> {
    <[u8; 32]>::try_from(bytes).ok().map(Hash::from)
}

/// `Constr 0 [ Bytes(28), [] ]`, encoded — the smallest thing both decoders read —
/// and the hash the ledger gives it.
///
/// `compute_hash` rather than a hasher called here: it is the crate's own definition
/// of a `PlutusData`'s datum hash, so a fixture cannot hash to something the chain
/// would not produce. Shared by this module's tests and the sink's, so both drive
/// the same bytes.
#[cfg(test)]
pub(crate) fn offer_datum(bidder: [u8; 28]) -> (Vec<u8>, Hash<32>) {
    use pallas_primitives::{Constr, MaybeIndefArray, PlutusData};

    let outer = PlutusData::Constr(Constr {
        tag: 121,
        any_constructor: None,
        fields: MaybeIndefArray::Def(vec![
            PlutusData::BoundedBytes(bidder.to_vec().into()),
            PlutusData::Array(MaybeIndefArray::Def(Vec::new())),
        ]),
    });
    let bytes = pallas_codec::minicbor::to_vec(&outer).expect("a PlutusData encodes");
    let hash: Hash<32> = pallas_traverse::ComputeHash::compute_hash(&outer);
    (bytes, hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// jpg's collection-offer contract, as the registry records it.
    const JPG_CO: &str = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";

    fn registry_jpg() -> Venue {
        venues()
            .into_iter()
            .find(|v| v.address.is_some())
            .expect("jpg's offer contract is registered with an address")
    }

    fn registry_wayup() -> Venue {
        venues()
            .into_iter()
            .find(|v| v.address.is_none())
            .expect("Wayup's offer contract is registered as a credential")
    }

    #[test]
    fn the_registry_gives_both_venues_and_no_credential_twice() {
        let found = venues();
        let jpg = registry_jpg();
        assert_eq!(jpg.decode, OfferDecode::Jpg);
        assert_eq!(
            jpg.address.as_deref(),
            bech32_bytes(JPG_CO).ok().as_deref(),
            "the address is the registry's own, not one derived here"
        );
        let wayup = registry_wayup();
        assert_eq!(wayup.decode, OfferDecode::Wayup);

        let mut creds: Vec<[u8; 28]> = found.iter().map(|v| v.credential).collect();
        creds.sort_unstable();
        creds.dedup();
        assert_eq!(creds.len(), found.len(), "a credential is listed twice");
    }

    /// ⚠️ **The trap the address filter exists for.** jpg's offer contract and its
    /// sale escrow are one script under two delegation forms — one payment credential,
    /// two addresses — so a credential match alone would read every listing as a bid.
    #[test]
    fn a_same_credential_at_another_address_is_not_this_venue() {
        let jpg = registry_jpg();
        let own = jpg.address.clone().expect("jpg has one address");
        assert!(jpg.owns(&own));

        // The same payment part under a delegation — the escrow's shape.
        let mut other = own.clone();
        other[0] = (other[0] & 0xF0) | 0x01;
        other.extend_from_slice(&[0xCD; 28]);
        assert!(!jpg.owns(&other));

        // Wayup is the other way round: no address of its own, so its credential is
        // the whole test — and the stake part, which is the bidder's, must not matter.
        let wayup = registry_wayup();
        let mut franken = vec![0x11u8];
        franken.extend_from_slice(&wayup.credential);
        franken.extend_from_slice(&[0x0A; 28]);
        assert!(wayup.owns(&franken));
        assert!(!wayup.owns(&own), "one venue's contract is not another's");
    }

    #[test]
    fn the_shape_test_accepts_what_both_decoders_read_and_rejects_the_rest() {
        assert!(offer_shaped(&offer_datum([0x11; 28]).0));
        for cbor in [
            &[][..],
            &[0x00][..],
            &[0xd8, 0x79, 0x80][..],             // Constr 0 [], not an offer
            &[0xd8, 0x7a, 0x9f, 0x58, 0x1c][..], // Constr 1
        ] {
            assert!(!offer_shaped(cbor), "{cbor:02x?}");
        }
    }

    /// The whole pass, end to end: a revealed datum, then an output committing to it,
    /// becomes an association — and the same walk with the reveal missing does not.
    #[test]
    fn an_output_resolves_the_datum_its_hash_names() {
        let (cbor, hash) = offer_datum([0x11; 28]);
        let wayup = registry_wayup();
        let mut address = vec![0x11u8];
        address.extend_from_slice(&wayup.credential);
        address.extend_from_slice(&[0x0A; 28]);

        let mut pass = OfferPass::over(vec![wayup]);
        pass.reveal(hash, &cbor);
        pass.output(
            (Hash::from([0xAB; 32]), 7),
            &address,
            Datum::Hash(hash.as_ref()),
        );
        assert_eq!(pass.candidates, 1);
        assert_eq!(pass.decoded, 1);
        assert_eq!(pass.unreadable, 0);
        assert_eq!((pass.from_revealed, pass.from_output), (1, 0));
        assert_eq!(pass.associations.len(), 1);
        assert_eq!(pass.associations[0].credential, [0x11; 28]);
        assert_eq!(pass.associations[0].outref, ([0xAB; 32], 7));

        // The same output with nothing revealed: counted, not guessed at.
        let mut cold = OfferPass::over(venues());
        cold.output(
            (Hash::from([0xAB; 32]), 7),
            &address,
            Datum::Hash(hash.as_ref()),
        );
        assert_eq!((cold.decoded, cold.unreadable), (0, 1));
        assert!(cold.associations.is_empty());
    }

    #[test]
    fn an_inline_datum_needs_no_reveal_and_a_spent_offer_can_be_filtered_out() {
        let (cbor, _) = offer_datum([0x22; 28]);
        let mut pass = OfferPass::over(venues());
        pass.output(
            (Hash::from([0xCD; 32]), 1),
            &registry_jpg().address.unwrap(),
            Datum::Inline(&cbor),
        );
        assert_eq!(pass.decoded, 1);
        assert_eq!((pass.from_output, pass.from_revealed), (1, 0));
        assert_eq!(pass.live(|_| true).len(), 1);
        assert!(pass.live(|_| false).is_empty());
    }
}
