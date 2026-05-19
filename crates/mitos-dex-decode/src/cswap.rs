//! CSwap DEX decode surface — script addresses, pool datum,
//! staking (farm) datum.
//!
//! Lifted from `community-modules/cswap-dex/cswap_dex.rs` so
//! `holder-distribution`'s LP decomposition can reuse it without
//! a second copy of contract-variant-specific datum logic.
//! `cswap-dex` itself now depends on this module.

use pallas_codec::minicbor;
use pallas_primitives::{BigInt, Constr, PlutusData};

/// CSwap pool script address. All 82 pools live at this one
/// address (CSwap uses a single canonical stake credential
/// across its pools).
pub const POOL_SCRIPT_ADDR: &str =
    "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";

/// CSwap order / batcher script address prefix (51 chars =
/// `addr1z` + header byte + 28-byte payment hash worth of
/// bech32). Each user's order glues its own stake credential to
/// the same payment script, so consumers prefix-match.
pub const ORDER_SCRIPT_ADDR_PREFIX: &str =
    "addr1z8d9k3aw6w24eyfjacy809h68dv2rwnpw0arrfau98jk6nh";

/// CSwap farm script address. LP tokens are locked here when a
/// user stakes for yield. CSwap uses one canonical farm script;
/// staked-LP UTxOs all sit at this address and carry the
/// staker's identity in their datum (see [`decode_staking_datum`]).
pub const FARM_SCRIPT_ADDR: &str =
    "addr1z9xf82dwn6aaaftz6dnjslhkgu0pvtxrhfqsxqm8h42u7uggjrxwszhuqj73gufx56c8qwnuhvf2nw5dzdr5f50rqr5qt8sqcf";

/// Plutus constructor tag for alternative 0 (`Constr` tag 121).
/// Both the CSwap pool datum and the staking datum are
/// alternative-0 records.
const CONSTR_ALT_0: u64 = 121;

/// The stable identity of a pool — `(quote_policy, quote_name,
/// base_policy, base_name)` — independent of swap direction.
pub type PairKey = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

/// Decoded CSwap pool datum (Constr 121, 8 fields):
///
/// ```text
/// [0] totalLpTokens: BigInt   total LP supply in circulation
/// [1] poolFee:       BigInt   swap fee in basis points
/// [2] quotePolicy:   Bytes    pricing-denominator policy (empty = ADA)
/// [3] quoteName:     Bytes    pricing-denominator asset name
/// [4] basePolicy:    Bytes    priced-asset policy
/// [5] baseName:      Bytes    priced-asset name
/// [6] lpTokenPolicy: Bytes    LP-token policy id
/// [7] lpTokenName:   Bytes    LP-token name
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CswapPoolDatum {
    pub total_lp_tokens: u64,
    pub pool_fee_bps: u64,
    pub quote_policy: Vec<u8>,
    pub quote_name: Vec<u8>,
    pub base_policy: Vec<u8>,
    pub base_name: Vec<u8>,
    pub lp_policy: Vec<u8>,
    pub lp_name: Vec<u8>,
}

impl CswapPoolDatum {
    /// The pool's direction-independent pair identity.
    pub fn pair_key(&self) -> PairKey {
        (
            self.quote_policy.clone(),
            self.quote_name.clone(),
            self.base_policy.clone(),
            self.base_name.clone(),
        )
    }
}

/// Decode a CSwap pool datum from inline-datum CBOR. Returns
/// `None` for anything that isn't a well-formed 8-field
/// alternative-0 record — defensive against unrelated
/// `PlutusData` landing at the pool address.
pub fn decode_pool_datum(cbor: &[u8]) -> Option<CswapPoolDatum> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let constr = as_constr(&pd)?;
    if constr.tag != CONSTR_ALT_0 || constr.fields.len() != 8 {
        return None;
    }
    Some(CswapPoolDatum {
        total_lp_tokens: bigint_u64(constr.fields.first()?)?,
        pool_fee_bps: bigint_u64(constr.fields.get(1)?)?,
        quote_policy: bounded_bytes(constr.fields.get(2)?)?,
        quote_name: bounded_bytes(constr.fields.get(3)?)?,
        base_policy: bounded_bytes(constr.fields.get(4)?)?,
        base_name: bounded_bytes(constr.fields.get(5)?)?,
        lp_policy: bounded_bytes(constr.fields.get(6)?)?,
        lp_name: bounded_bytes(constr.fields.get(7)?)?,
    })
}

/// Extract the staker's 28-byte stake credential from a CSwap
/// farm (staking) datum.
///
/// The staked LP UTxOs all rest at one [`FARM_SCRIPT_ADDR`], so
/// the staker's wallet is recoverable only from the datum:
///
/// ```text
/// staking datum: Constr 121, >= 1 field
///   [0] Address = Constr { [0] payment cred, [1] Maybe StakingCredential }
///        [1] -> Just(StakingHash(Credential(<28-byte hash>)))
///             = Constr( Constr( Constr( Bytes ) ) )
/// ```
///
/// Returns the credential hash regardless of key-vs-script
/// credential kind — `holder-distribution`'s ledger keys on the
/// 28-byte credential either way. `None` for an enterprise
/// (no-stake) address or any malformed datum.
pub fn decode_staking_datum(cbor: &[u8]) -> Option<[u8; 28]> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let root = as_constr(&pd)?;
    if root.tag != CONSTR_ALT_0 {
        return None;
    }
    let address = root.fields.first()?; // [0] Address
    let stake_cred = constr_field(address, 1)?; // Address [1]: Maybe StakingCredential
    let staking_hash = constr_field(stake_cred, 0)?; // Just -> StakingCredential
    let credential = constr_field(staking_hash, 0)?; // StakingHash -> Credential
    let hash = bounded_bytes(constr_field(credential, 0)?)?; // Credential -> hash bytes
    <[u8; 28]>::try_from(hash.as_slice()).ok()
}

// ── PlutusData helpers ──────────────────────────────────────

fn as_constr(pd: &PlutusData) -> Option<&Constr<PlutusData>> {
    match pd {
        PlutusData::Constr(c) => Some(c),
        _ => None,
    }
}

/// Field `i` of `pd`, where `pd` is expected to be a `Constr`.
fn constr_field(pd: &PlutusData, i: usize) -> Option<&PlutusData> {
    as_constr(pd)?.fields.get(i)
}

fn bigint_u64(pd: &PlutusData) -> Option<u64> {
    match pd {
        PlutusData::BigInt(BigInt::Int(i)) => {
            let v: i128 = (*i).into();
            if v < 0 { None } else { u64::try_from(v).ok() }
        }
        PlutusData::BigInt(BigInt::BigUInt(b)) => {
            let bytes: &[u8] = b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        _ => None,
    }
}

fn bounded_bytes(pd: &PlutusData) -> Option<Vec<u8>> {
    match pd {
        PlutusData::BoundedBytes(b) => Some((**b).to_vec()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_codec::utils::{Int, MaybeIndefArray};

    fn constr(fields: Vec<PlutusData>) -> PlutusData {
        PlutusData::Constr(Constr {
            tag: CONSTR_ALT_0,
            any_constructor: None,
            fields: MaybeIndefArray::Indef(fields),
        })
    }

    fn bytes_pd(b: &[u8]) -> PlutusData {
        PlutusData::BoundedBytes(b.to_vec().into())
    }

    fn int_pd(n: u64) -> PlutusData {
        PlutusData::BigInt(BigInt::Int(Int::from(n as i64)))
    }

    fn encode(pd: &PlutusData) -> Vec<u8> {
        minicbor::to_vec(pd).expect("encode PlutusData")
    }

    #[test]
    fn pool_datum_round_trips() {
        let datum = constr(vec![
            int_pd(1_000_000),     // total LP
            int_pd(85),            // fee bps
            bytes_pd(&[]),         // quote policy (ADA)
            bytes_pd(&[]),         // quote name
            bytes_pd(&[0xaa; 28]), // base policy
            bytes_pd(b"ALIEN"),    // base name
            bytes_pd(&[0xbb; 28]), // lp policy
            bytes_pd(b"LP"),       // lp name
        ]);
        let decoded = decode_pool_datum(&encode(&datum)).expect("decode");
        assert_eq!(
            decoded,
            CswapPoolDatum {
                total_lp_tokens: 1_000_000,
                pool_fee_bps: 85,
                quote_policy: vec![],
                quote_name: vec![],
                base_policy: vec![0xaa; 28],
                base_name: b"ALIEN".to_vec(),
                lp_policy: vec![0xbb; 28],
                lp_name: b"LP".to_vec(),
            }
        );
    }

    #[test]
    fn pool_datum_rejects_wrong_field_count() {
        let short = constr(vec![int_pd(1), int_pd(2)]);
        assert!(decode_pool_datum(&encode(&short)).is_none());
    }

    #[test]
    fn staking_datum_extracts_stake_credential() {
        let stake_hash = [0x7c; 28];
        // Address = Constr { payment_cred, Just(StakingHash(Cred(hash))) }
        let address = constr(vec![
            constr(vec![bytes_pd(&[0x11; 28])]), // payment credential
            constr(vec![constr(vec![constr(vec![bytes_pd(&stake_hash)])])]),
        ]);
        // Staking datum: Constr 121, field[0] = Address (other
        // fields irrelevant to credential extraction).
        let datum = constr(vec![address, int_pd(0), int_pd(0)]);
        let decoded = decode_staking_datum(&encode(&datum)).expect("decode");
        assert_eq!(decoded, stake_hash);
    }

    #[test]
    fn staking_datum_none_for_enterprise_address() {
        // Address with `Nothing` for the staking credential —
        // Nothing is alternative 1, zero fields.
        let nothing = PlutusData::Constr(Constr {
            tag: CONSTR_ALT_0 + 1,
            any_constructor: None,
            fields: MaybeIndefArray::Indef(vec![]),
        });
        let address = constr(vec![constr(vec![bytes_pd(&[0x11; 28])]), nothing]);
        let datum = constr(vec![address]);
        assert!(decode_staking_datum(&encode(&datum)).is_none());
    }
}
