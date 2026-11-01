//! How a collection-offer venue's datums read, and which credential a bid at it
//! names.
//!
//! Two questions about collection offers have to be answered the same way by every
//! reader, and they are the two this module answers:
//!
//! - **How is a venue's datum read?** jpg.store takes the **last** payout and reads
//!   its policy; Wayup scans **all** payouts for the non-ADA one and decodes that
//!   payout's recipient. Which of the two applies is a property of the **contract**,
//!   not of the datum.
//! - **Which credential does a bid name?** jpg's owner field is the bidder's
//!   **payment** key hash, Wayup's is their **stake** credential. Comparing the
//!   wrong one against a holder answers "not yours" for every offer they have open.
//!
//! The fold's offer pass (`tools/fold-probe`) and the live-book frontend's offers
//! view both consume this, so "whose offer is this" cannot be answered two ways.
//!
//! ⚠️ **Which addresses and credentials the registry lists for those venues is
//! deliberately not here.** That walk reads `address-registry`'s tables — some
//! hundreds of KB of static data — and this crate compiles into every wasm module
//! that decodes a sale, where a registry lookup is weight nothing ever runs. The
//! caller builds the venue list; this module says how each entry reads.

use crate::offer_datum::{DecodedOffer, decode_jpg_offer_datum, decode_wayup_offer_datum};

/// How one venue's collection-offer datums are read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OfferDecode {
    /// jpg.store: the target is the **last** payout.
    Jpg,
    /// Wayup: every payout is scanned for the non-ADA one, and the recipient
    /// credential comes off that payout's address.
    Wayup,
}

/// Which of a holder's two credentials a venue's owner field holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OfferKey {
    /// A payment key hash — the credential `by_payment_cred` answers.
    Payment,
    /// A stake credential.
    Stake,
}

impl OfferDecode {
    /// This venue's read of one offer datum. `None` when the bytes are not an offer.
    pub fn datum(self, cbor: &[u8]) -> Option<DecodedOffer> {
        match self {
            OfferDecode::Jpg => decode_jpg_offer_datum(cbor),
            OfferDecode::Wayup => decode_wayup_offer_datum(cbor),
        }
    }

    /// Which credential the datum's owner field holds at this venue.
    ///
    /// A method rather than a field on [`Self::datum`]'s result, because the datum
    /// does not know: [`DecodedOffer::bidder_pkh`] is 28 bytes of hex either way, and
    /// only the venue says whose.
    pub fn keys_on(self) -> OfferKey {
        match self {
            OfferDecode::Jpg => OfferKey::Payment,
            OfferDecode::Wayup => OfferKey::Stake,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_primitives::{Constr, MaybeIndefArray, PlutusData};

    const BIDDER: [u8; 28] = [0x11; 28];
    const POLICY: [u8; 28] = [0x22; 28];
    const RECIPIENT: [u8; 28] = [0x33; 28];

    fn constr(tag: u64, fields: Vec<PlutusData>) -> PlutusData {
        PlutusData::Constr(Constr {
            tag,
            any_constructor: None,
            fields: MaybeIndefArray::Def(fields),
        })
    }

    fn bytes(b: &[u8]) -> PlutusData {
        PlutusData::BoundedBytes(b.to_vec().into())
    }

    /// `Constr 0 [ Bytes(28), [ Constr0[ Address, Map<policy, ...> ] ] ]` — the shape
    /// both venues share, with a payout whose address yields Wayup's recipient.
    ///
    /// 121 is the CBOR tag a Plutus `Constr 0` carries; pallas keeps the tag rather
    /// than the constructor number in [`Constr::tag`].
    fn offer() -> Vec<u8> {
        let credential = constr(121, vec![bytes(&RECIPIENT)]);
        let address = constr(121, vec![credential]);
        // The buyer's payout carries the policy; the fee payout carries the empty
        // ADA policy, as both venues' datums do in the wild.
        let value = vec![
            (
                bytes(&POLICY),
                constr(121, vec![PlutusData::BoundedBytes(Vec::new().into())]),
            ),
            (
                PlutusData::BoundedBytes(Vec::new().into()),
                constr(121, vec![PlutusData::BoundedBytes(Vec::new().into())]),
            ),
        ];
        let payout = constr(121, vec![address, PlutusData::Map(value.into())]);
        let outer = constr(
            121,
            vec![
                bytes(&BIDDER),
                PlutusData::Array(MaybeIndefArray::Def(vec![payout])),
            ],
        );
        pallas_codec::minicbor::to_vec(&outer).expect("a PlutusData encodes")
    }

    #[test]
    fn each_venue_reads_the_bidder_and_only_wayup_reads_the_recipient() {
        let cbor = offer();
        let jpg = OfferDecode::Jpg.datum(&cbor).expect("jpg reads this shape");
        let wayup = OfferDecode::Wayup.datum(&cbor).expect("so does Wayup");
        assert_eq!(jpg.bidder_pkh, hex::encode(BIDDER));
        assert_eq!(wayup.bidder_pkh, hex::encode(BIDDER));
        assert_eq!(jpg.target_policy, Some(hex::encode(POLICY)));
        assert_eq!(wayup.target_policy, Some(hex::encode(POLICY)));
        // ⚠️ The difference the venue decides: only Wayup's decode carries the wallet
        // an accept has to deliver to.
        assert!(jpg.target_recipient.is_none());
        assert_eq!(wayup.target_recipient, Some(RECIPIENT));
    }

    #[test]
    fn bytes_that_are_not_an_offer_are_not_an_offer_at_either_venue() {
        for cbor in [&[][..], &[0x00][..], &[0xd8, 0x79, 0x80][..]] {
            assert!(OfferDecode::Jpg.datum(cbor).is_none(), "{cbor:02x?}");
            assert!(OfferDecode::Wayup.datum(cbor).is_none(), "{cbor:02x?}");
        }
    }

    #[test]
    fn a_venue_names_the_credential_its_datum_carries() {
        // The mapping itself, which is what a holder is compared against: a payment
        // key at jpg, a stake credential at Wayup.
        assert_eq!(OfferDecode::Jpg.keys_on(), OfferKey::Payment);
        assert_eq!(OfferDecode::Wayup.keys_on(), OfferKey::Stake);
    }
}
