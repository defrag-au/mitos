//! The $TOOL staking site — one payment script holding every staker's position.
//!
//! Its positions sit at a Shelley **enterprise** address paying to this 28-byte
//! script (`addr1w…`: script payment, no stake part), so a holder map keyed by
//! stake credential reads all of it as one wallet's — the same trap $PERP's site
//! sets, and the reason this is a registered staking contract rather than a
//! holder. At the 2026-11-19 walk it held 44.2% of supply, the largest single
//! row the holder map had.
//!
//! Its datum is the OWNER-ONLY shape, `Constr 0 [ Bytes(owner_pkh) ]` — no
//! timestamp at all. That is why it is not part of the lock family and is read by
//! [`super::decode_owner_only_datum`] rather than `decode_vesting_datum`: a
//! locked position's date is a deadline, and a staker's is nothing, so a decoder
//! that filled one in would be inventing a maturity date. What matters is the
//! owner, who is the holder the staked $TOOL belongs to.
//!
//! # One deployment per project, not one platform
//!
//! Like $PERP's site, this is a per-project deployment rather than a shared
//! script: the registry registers the credential alone, and when another project
//! stakes this way it belongs beside this module as its own, named for that
//! project — not as a widening of this constant.

/// The $TOOL staking site's payment credential (script hash, 28 bytes).
///
/// Established 2026-11-19 from the registry's attestation of the walk, and
/// confirmed against chain the same day: `/credential_utxos` for this credential
/// returns positions at one enterprise address, each carrying one owner and
/// nothing else.
pub const PAYMENT_CRED: [u8; 28] = [
    0xb0, 0xeb, 0xdd, 0x68, 0xbd, 0x00, 0xf3, 0xa6, 0x33, 0xc5, 0x09, 0x7f, 0xe0, 0x24, 0x3d, 0x69,
    0xfb, 0x27, 0xab, 0xac, 0xd7, 0xf6, 0x95, 0x9b, 0xec, 0x63, 0xc4, 0xc4,
];

/// True when `payment_cred` is the $TOOL staking site's script hash.
pub fn is_tsuru_staking(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &PAYMENT_CRED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_owner_only_datum;

    /// A real position, captured from chain 2026-11-19: 42,568 $TOOL
    /// (`ac015c38….5453555255`) in UTxO
    /// `8953fc1650f4b0622eb11056de007434a8de6b06631c2fa46d503445c0555ea5:0`
    /// at the site's enterprise address.
    const POSITION: &str = "d87981581c6ec2284b5cd330d2c140b8472cd8ff71ccb2c66d50e563f51d8c710f";

    #[test]
    fn decodes_a_real_position_to_its_owner() {
        let decoded = decode_owner_only_datum(&hex::decode(POSITION).unwrap()).expect("decodes");
        assert_eq!(
            decoded.owner_pkh_hex,
            "6ec2284b5cd330d2c140b8472cd8ff71ccb2c66d50e563f51d8c710f"
        );
    }

    #[test]
    fn recognises_only_its_own_script() {
        assert!(is_tsuru_staking(&PAYMENT_CRED));
        assert!(!is_tsuru_staking(&crate::perps_staking::PAYMENT_CRED));
        assert!(!is_tsuru_staking(&crate::snek_fun::PAYMENT_CRED));
    }
}
