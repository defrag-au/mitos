//! `mitos-vesting-decode` — shared vesting-contract decode
//! library for mitos community modules.
//!
//! The vesting-tracker module and `holder-distribution`'s
//! vesting-decomposition step both need to read the same
//! on-chain shapes — Shield / CrowdLock lock datums, the
//! CrowdLock platform payment-credential constant. This crate
//! is pure decode logic, no WIT bindings, so a module depending
//! on it pulls in nothing transitive beyond pallas.
//!
//! Companion to `mitos-dex-decode`. See
//! `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md` —
//! "the decode library is the genuine reuse unit; composition
//! over coordination."

use pallas_primitives::{BigInt, PlutusData};

pub mod crowd_lock;

const HASH_BYTES: usize = 28;

/// Decoded vesting lock datum — the unified Shield + CrowdLock
/// shape: `Constructor 0 [ Int(unlock_ts_ms), List[ Bytes(owner_pkh) ] ]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VestingDatum {
    pub unlock_ts_ms: u64,
    /// 56-char hex of the owner's payment-key-hash (28 bytes).
    pub owner_pkh_hex: String,
}

/// Decode a Shield / CrowdLock lock datum from inline-datum CBOR.
/// Returns `None` for any shape that isn't a well-formed
/// alternative-0 record with the expected fields — defensive
/// against unrelated `PlutusData` landing at the contract.
pub fn decode_vesting_datum(cbor: &[u8]) -> Option<VestingDatum> {
    let pd: PlutusData = pallas_codec::minicbor::decode(cbor).ok()?;
    let outer = match pd {
        PlutusData::Constr(c) => c,
        _ => return None,
    };
    let fields: Vec<PlutusData> = outer.fields.into();
    if fields.len() < 2 {
        return None;
    }
    let unlock_ts_ms = match &fields[0] {
        PlutusData::BigInt(i) => bigint_to_u64(i)?,
        _ => return None,
    };
    // Field 1 is a list with exactly one entry — the owner PKH.
    let owner_pkh = match &fields[1] {
        PlutusData::Array(items) => match items.first()? {
            PlutusData::BoundedBytes(b) => {
                let raw: &[u8] = b;
                if raw.len() != HASH_BYTES {
                    return None;
                }
                hex::encode(raw)
            }
            _ => return None,
        },
        _ => return None,
    };
    Some(VestingDatum {
        unlock_ts_ms,
        owner_pkh_hex: owner_pkh,
    })
}

pub fn bigint_to_u64(i: &BigInt) -> Option<u64> {
    match i {
        BigInt::Int(n) => {
            let v: i128 = (*n).into();
            if v < 0 { None } else { u64::try_from(v).ok() }
        }
        BigInt::BigUInt(b) => {
            let bytes: &[u8] = b;
            if bytes.len() > 8 {
                return None;
            }
            let mut buf = [0u8; 8];
            buf[8 - bytes.len()..].copy_from_slice(bytes);
            Some(u64::from_be_bytes(buf))
        }
        BigInt::BigNInt(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_codec::utils::{Int, MaybeIndefArray};
    use pallas_primitives::Constr;

    fn constr(tag: u64, fields: Vec<PlutusData>) -> PlutusData {
        PlutusData::Constr(Constr {
            tag,
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
        pallas_codec::minicbor::to_vec(pd).expect("encode PlutusData")
    }

    #[test]
    fn vesting_datum_round_trips() {
        let owner_pkh = [0x7c; 28];
        let datum = constr(
            121,
            vec![
                int_pd(1_793_765_349_809),
                PlutusData::Array(MaybeIndefArray::Indef(vec![bytes_pd(&owner_pkh)])),
            ],
        );
        let decoded = decode_vesting_datum(&encode(&datum)).expect("decode");
        assert_eq!(decoded.unlock_ts_ms, 1_793_765_349_809);
        assert_eq!(decoded.owner_pkh_hex, hex::encode(owner_pkh));
    }

    #[test]
    fn vesting_datum_rejects_short_fields() {
        let short = constr(121, vec![int_pd(1)]);
        assert!(decode_vesting_datum(&encode(&short)).is_none());
    }
}
