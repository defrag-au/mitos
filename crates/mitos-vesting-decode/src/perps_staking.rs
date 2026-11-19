//! The $PERP staking site — one payment script holding every staker's position.
//!
//! Its positions sit at a Shelley address with this 28-byte payment credential and the
//! **site's own** stake credential (`9c70edc0…`), so a holder map keyed by stake credential read
//! all 211M staked $PERP as one wallet's — the same trap CrowdLock sets, and the reason this is a
//! lock platform rather than a holder.
//!
//! Its datum is snek.fun's spelling of the lock family, `Constr 0 [ Int(ms), Bytes(owner_pkh) ]`,
//! so [`super::decode_vesting_datum`] reads it unchanged. The `Int` here is when the position was
//! staked, not when it unlocks: a consumer that needs a maturity date must not read one off it.
//! What matters is the owner, who is the holder the staked $PERP belongs to.
//!
//! # One deployment per project, not one platform
//!
//! The site is built by cnft.tools, which runs staking for other projects too, so the obvious
//! guess is a shared script like CrowdLock's. Checked 2026-11-11 against $NIKEPIG
//! (`c881c20e….4e696b65`): this credential holds no $NIKEPIG, and none of $NIKEPIG's fifteen
//! script-held positions sits at it — they are Minswap, CSWAP and an unnamed contract whose datum
//! is a different shape. The script also carries its own CIP-68 reference token, named
//! `perpstaking`, which a per-project deployment would and a shared platform would not. So this
//! credential is Perps' alone. When cnft.tools stakes another token the same way it will be
//! another credential with this datum, and belongs beside this one as its own module, named for
//! its project — not a widening of this constant.

/// The $PERP staking site's payment credential (script hash, 28 bytes; a Plutus V3 script).
///
/// Established 2026-11-10 from `addr1zyzayvten00vtt9tz0n8pdqs0kq62ujcznpdu7gq6s30md5uwrkupakg244…`:
/// its live UTxOs hold one $PERP position each (211,639,387 in all, the holder map's largest
/// "holder"), every one with an inline datum naming an owner payment key hash, and the project
/// identified the address as its staking site.
pub const PAYMENT_CRED: [u8; 28] = [
    0x05, 0xd2, 0x31, 0x79, 0x9b, 0xde, 0xc5, 0xac, 0xab, 0x13, 0xe6, 0x70, 0xb4, 0x10, 0x7d, 0x81,
    0xa5, 0x72, 0x58, 0x14, 0xc2, 0xde, 0x79, 0x00, 0xd4, 0x22, 0xfd, 0xb6,
];

/// True when `payment_cred` is the $PERP staking site's script hash.
pub fn is_perps_staking(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &PAYMENT_CRED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_vesting_datum;

    /// Two real staking positions, captured from chain 2026-11-10 (1,035,179 and 328,938 $PERP).
    const POSITION_A: &str =
        "d879821b0000019c247836ec581ccce969f5c0a16d5735feb093b883e4656f0dd4eb377dbc5d787391bf";
    const POSITION_B: &str =
        "d879821b0000019c247b4650581c8974b7a444f372d07f15abcc0ecd9729e79f9e196b72a0bb3fc5410c";

    #[test]
    fn decodes_real_staking_positions_to_their_owners() {
        let a = decode_vesting_datum(&hex::decode(POSITION_A).unwrap()).expect("decodes");
        assert_eq!(
            a.owner_pkh_hex,
            "cce969f5c0a16d5735feb093b883e4656f0dd4eb377dbc5d787391bf"
        );
        assert_eq!(
            a.unlock_ts_ms, 1_770_138_384_108,
            "the staked-at time, not an unlock"
        );
        let b = decode_vesting_datum(&hex::decode(POSITION_B).unwrap()).expect("decodes");
        assert_eq!(
            b.owner_pkh_hex,
            "8974b7a444f372d07f15abcc0ecd9729e79f9e196b72a0bb3fc5410c"
        );
    }

    #[test]
    fn recognises_only_its_own_script() {
        assert!(is_perps_staking(&PAYMENT_CRED));
        assert!(!is_perps_staking(&crate::snek_fun::PAYMENT_CRED));
    }
}
