//! The credential an address is keyed by — the one derivation `by_addr` and every
//! consumer of it have to agree on.
//!
//! The file is named for its input, not its output: a path containing
//! `credential` is refused by the `at-peek` inspector, which reads it as a secret.
//!
//! # Ten shapes are a slice and one is a parse
//!
//! A Shelley address is `header ‖ payment ‖ delegation`, and the header's high
//! nibble says which of eleven shapes it is. Types 0–7 put the **payment
//! credential in bytes 1..29**, whatever the delegation part is: base (0–3),
//! pointer (4–5) or enterprise (6–7). Types 14–15 are reward addresses — a stake
//! credential and nothing else, at the same offset. So far, ten shapes, no parse:
//! the credential is a slice, and the delegation part's variable length is
//! somebody else's problem.
//!
//! **Type 8 is Byron, and there the bytes do not lay it out at all.** The address
//! is `[#6.24(payload), crc32]` and the credential is `payload.root`, inside a
//! CBOR byte string whose length is whatever the chain says — `LIVE_BOOK.md`
//! §Sizes measures 502, 682, 828 and 22,600-byte payloads on mainnet, because
//! Byron's `attributes` map is `{* any => any}`. So this one arm decodes.
//!
//! # The dispatch mirrors pallas deliberately
//!
//! This is `pallas_addresses::bytes_to_address`'s dispatch, including its bound:
//! pallas accepts a payload of **28 bytes or more** for every Shelley shape and
//! never checks the upper end. A derivation that were stricter here would
//! disagree with the decoder that will serve it, and the disagreement would be
//! invisible until a consumer asked for an address the index had silently not
//! keyed. Anything pallas refuses is `None`, which the build counts rather than
//! drops (`addr_unkeyed` in `book.meta`).
//!
//! Types 9–13 have no meaning and are `None`.

use pallas_addresses::byron::ByronAddress;

/// What an address is keyed by. The index does not store this — the row's own
/// bytes imply it, and every consumer confirms against the row anyway — but a
/// caller filtering `by_addr`'s candidates needs to know whether the 28 bytes it
/// matched are a payment credential or not.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// Types 0–7. What `utxos_by_payment_cred` means.
    Payment,
    /// Types 14–15: a reward address, whose only credential is its stake part.
    Stake,
    /// Type 8: the `root` inside a Byron payload. Not a payment credential in the
    /// Shelley sense — a bootstrap address is spent by a redeem or a key, and no
    /// wallet expects to be found by it — but it is the address's identity, so it
    /// is what the address is keyed by.
    Bootstrap,
}

/// The 28-byte credential an address is keyed by.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Credential {
    pub kind: Kind,
    bytes: [u8; 28],
}

impl Credential {
    /// The 28 bytes themselves. A consumer confirming a query against a row needs
    /// them: the entry narrows by prefix, and this is what makes the answer exact.
    pub fn bytes(&self) -> &[u8; 28] {
        &self.bytes
    }

    /// The 64-bit prefix `by_addr`'s entries carry — the same leading-8-of-a-hash
    /// convention every other ordering uses, and sound for the same reason: the
    /// entry is a candidate, and the row confirms it.
    ///
    /// Both a payment credential and a Byron root are themselves hashes
    /// (`Blake2b224`), so no further mixing is needed to bucket them.
    pub fn prefix(&self) -> u64 {
        u64::from_le_bytes(self.bytes[0..8].try_into().expect("8 bytes"))
    }
}

/// What an address delegates its stake to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Delegation {
    Key([u8; 28]),
    Script([u8; 28]),
}

impl Delegation {
    pub fn bytes(&self) -> &[u8; 28] {
        match self {
            Delegation::Key(b) | Delegation::Script(b) => b,
        }
    }
}

/// The delegation part of a Shelley address, or `None` when it has none.
///
/// Types 0–3 carry one at `[29..57]`, and the header says which kind: bit 5
/// (`0x20`) is set when the delegation part is a script. Types 4–5 delegate via a
/// **pointer** (a slot/tx/cert triple rather than a credential) and 6–7 have no
/// delegation at all, so all four are `None`. Byron has no delegation part — a
/// bootstrap address *is* the payment authority, and its `root` is recovery
/// material rather than a stake credential.
///
/// Here rather than in a consumer for the same reason [`credential`] is: the
/// header dispatch has one home, and two implementations of it would eventually
/// disagree about which nibble means what.
pub fn delegation(address: &[u8]) -> Option<Delegation> {
    let header = *address.first()?;
    if !matches!(header >> 4, 0..=3) {
        return None;
    }
    let payload = address.get(1..)?;
    if payload.len() < 56 {
        return None;
    }
    let mut bytes = [0u8; 28];
    bytes.copy_from_slice(&payload[28..56]);
    Some(if header & 0x20 == 0 {
        Delegation::Key(bytes)
    } else {
        Delegation::Script(bytes)
    })
}

/// The credential `address` is keyed by, or `None` when pallas would not call the
/// bytes an address at all.
pub fn credential(address: &[u8]) -> Option<Credential> {
    let header = *address.first()?;
    let payload = address.get(1..)?;
    match header >> 4 {
        0..=7 => slice_credential(payload, Kind::Payment),
        14 | 15 => slice_credential(payload, Kind::Stake),
        8 => byron_root(address).map(|bytes| Credential {
            kind: Kind::Bootstrap,
            bytes,
        }),
        _ => None,
    }
}

fn slice_credential(payload: &[u8], kind: Kind) -> Option<Credential> {
    if payload.len() < 28 {
        return None;
    }
    let mut bytes = [0u8; 28];
    bytes.copy_from_slice(&payload[..28]);
    Some(Credential { kind, bytes })
}

/// The `root` of a Byron address's payload.
///
/// `ByronAddress::from_bytes` is the same decode `parse_type_8` performs, so the
/// only new step is reaching into the payload — and the `root` is its first field,
/// so the parse stops there rather than materialising the attributes map, which is
/// the part that can be kilobytes. `decode()` does materialise it; a hand-rolled
/// skip is worth measuring against the 22,600-byte case before it is written.
fn byron_root(address: &[u8]) -> Option<[u8; 28]> {
    let addr = ByronAddress::from_bytes(address).ok()?;
    let payload = addr.decode().ok()?;
    let mut bytes = [0u8; 28];
    bytes.copy_from_slice(payload.root.as_ref());
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A base address: header `0x01` (type 0, mainnet), 28 payment + 28 stake.
    fn base_address(payment: u8, stake: u8) -> Vec<u8> {
        let mut a = vec![0x01];
        a.extend_from_slice(&[payment; 28]);
        a.extend_from_slice(&[stake; 28]);
        a
    }

    #[test]
    fn the_credential_is_the_payment_part_for_every_shelley_shape() {
        // 0 base, 2 base with a script delegation, 4 pointer, 6 enterprise — the
        // credential is at 1..29 in all four, which is the whole reason this is a
        // slice rather than a parse.
        for header in [0x01u8, 0x21, 0x41, 0x61] {
            let mut a = vec![header];
            a.extend_from_slice(&[0xAB; 28]);
            a.extend_from_slice(&[0xCD; 28]); // delegation, if the shape has one
            let c = credential(&a).expect("a Shelley address keys");
            assert_eq!(c.kind, Kind::Payment, "header {header:#04x}");
            assert_eq!(c.bytes(), &[0xABu8; 28], "header {header:#04x}");
        }
    }

    #[test]
    fn a_reward_address_keys_on_its_stake_credential() {
        for header in [0xE1u8, 0xF1] {
            let mut a = vec![header];
            a.extend_from_slice(&[0x5A; 28]);
            let c = credential(&a).expect("a reward address keys");
            assert_eq!(c.kind, Kind::Stake);
            assert_eq!(c.bytes(), &[0x5Au8; 28]);
        }
    }

    #[test]
    fn a_byron_address_keys_on_the_root_inside_its_payload() {
        // pallas's own Byron test vector, so the bytes are a real mainnet
        // bootstrap address rather than a shape this test invented.
        let addr = pallas_addresses::byron::ByronAddress::from_base58(
            "Ae2tdPwUPEZLs4HtbuNey7tK4hTKrwNwYtGqp7bDfCy2WdR3P6735W5Yfpe",
        )
        .expect("a real Byron address");
        let bytes = addr.to_vec();
        let expected = addr.decode().expect("payload").root;

        let c = credential(&bytes).expect("a Byron address keys");
        assert_eq!(c.kind, Kind::Bootstrap);
        assert_eq!(
            c.bytes().as_slice(),
            expected.as_ref(),
            "the credential must be the payload's root, not any bytes read off the front \
             of the address"
        );
    }

    #[test]
    fn short_and_unknown_headers_do_not_key() {
        assert!(credential(&[]).is_none());
        // A type-0 header with a 27-byte payload: one byte short of a credential,
        // and pallas refuses it the same way.
        assert!(credential(&base_address(0xAB, 0xCD)[..28]).is_none());
        // Types 9–13 are not addresses.
        let mut a = vec![0x91];
        a.extend_from_slice(&[0x11; 28]);
        assert!(credential(&a).is_none());
    }

    #[test]
    fn delegation_is_the_second_credential_and_only_for_the_four_base_shapes() {
        // Types 0–3 have one, at 29..57; bit 5 of the header says key or script.
        for (header, script) in [(0x01u8, false), (0x11, false), (0x21, true), (0x31, true)] {
            let mut a = vec![header];
            a.extend_from_slice(&[0xAB; 28]); // payment
            a.extend_from_slice(&[0xCD; 28]); // delegation
            match delegation(&a) {
                Some(Delegation::Script(b)) => {
                    assert!(script, "header {header:#04x}");
                    assert_eq!(b, [0xCD; 28]);
                }
                Some(Delegation::Key(b)) => {
                    assert!(!script, "header {header:#04x}");
                    assert_eq!(b, [0xCD; 28]);
                }
                None => panic!("header {header:#04x} must carry a delegation part"),
            }
        }
        // Pointer (4–5), enterprise (6–7), reward (14–15) and Byron have none.
        for header in [0x41u8, 0x51, 0x61, 0x71, 0xE1, 0xF1, 0x81] {
            let mut a = vec![header];
            a.extend_from_slice(&[0xAB; 28]);
            a.extend_from_slice(&[0xCD; 28]);
            assert!(delegation(&a).is_none(), "header {header:#04x}");
        }
        assert!(delegation(&[]).is_none());
        // A truncated base address cannot supply a credential that is not there.
        assert!(delegation(&base_address(0xAB, 0xCD)[..56]).is_none());
    }

    #[test]
    fn the_prefix_is_the_leading_eight_bytes() {
        let c = credential(&base_address(0xAB, 0xCD)).expect("keys");
        assert_eq!(c.prefix(), u64::from_le_bytes([0xAB; 8]));
    }
}
