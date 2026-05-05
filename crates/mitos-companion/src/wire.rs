//! Wire-format types — re-exported from `mitos-protocol`.
//!
//! Single source of truth: every wire type the companion runtime
//! handles lives in `mitos/crates/mitos-protocol/src/wire.rs`. Both
//! the host (mitos-core) and the CF companion (this crate) import
//! from there. No drift.
//!
//! This module is a thin re-export layer + a small `Result`-shaped
//! wrapper around the encode/decode helpers (the mitos-protocol
//! versions return `Result<T, String>` so they stay framework-free;
//! the companion runtime wraps them with `CompanionError::Wire`).

use crate::error::{CompanionError, Result};

pub use mitos_protocol::{
    ChainPoint, ClientMessage, Interest, InterestOp, ServerMessage, SubscribeReply,
};

/// CBOR-encode a `ClientMessage`. Wraps the mitos-protocol helper
/// with the runtime's error type.
pub fn encode_client(msg: &ClientMessage) -> Result<Vec<u8>> {
    mitos_protocol::wire::encode_client(msg).map_err(CompanionError::Wire)
}

/// CBOR-decode a `ServerMessage`.
pub fn decode_server(bytes: &[u8]) -> Result<ServerMessage> {
    mitos_protocol::wire::decode_server(bytes).map_err(CompanionError::Wire)
}

/// CBOR-decode a `ClientMessage`.
pub fn decode_client(bytes: &[u8]) -> Result<ClientMessage> {
    mitos_protocol::wire::decode_client(bytes).map_err(CompanionError::Wire)
}

/// CBOR-encode a `ServerMessage`.
pub fn encode_server(msg: &ServerMessage) -> Result<Vec<u8>> {
    mitos_protocol::wire::encode_server(msg).map_err(CompanionError::Wire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_apply() {
        let original = ServerMessage::Apply {
            emission_id: 42,
            cursor: ChainPoint::Specific(123, "abcdef".into()),
            change: vec![1, 2, 3, 4],
        };
        let bytes = encode_server(&original).unwrap();
        let decoded = decode_server(&bytes).unwrap();
        match decoded {
            ServerMessage::Apply {
                emission_id,
                cursor,
                change,
            } => {
                assert_eq!(emission_id, 42);
                assert_eq!(cursor, ChainPoint::Specific(123, "abcdef".into()));
                assert_eq!(change, vec![1, 2, 3, 4]);
            }
            other => panic!("expected Apply, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_connected() {
        let original = ServerMessage::Connected {
            last_emission_id: 999,
        };
        let bytes = encode_server(&original).unwrap();
        let decoded = decode_server(&bytes).unwrap();
        match decoded {
            ServerMessage::Connected { last_emission_id } => {
                assert_eq!(last_emission_id, 999);
            }
            other => panic!("expected Connected, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_ack_nack() {
        let ack = ClientMessage::Ack { emission_id: 7 };
        let bytes = encode_client(&ack).unwrap();
        let decoded = decode_client(&bytes).unwrap();
        match decoded {
            ClientMessage::Ack { emission_id } => assert_eq!(emission_id, 7),
            other => panic!("expected Ack, got {other:?}"),
        }

        let nack = ClientMessage::Nack {
            emission_id: 8,
            error: "boom".into(),
        };
        let bytes = encode_client(&nack).unwrap();
        let decoded = decode_client(&bytes).unwrap();
        match decoded {
            ClientMessage::Nack { emission_id, error } => {
                assert_eq!(emission_id, 8);
                assert_eq!(error, "boom");
            }
            other => panic!("expected Nack, got {other:?}"),
        }
    }

    #[test]
    fn chain_point_origin_round_trips() {
        let original = ChainPoint::Origin;
        let mut buf = Vec::new();
        ciborium::into_writer(&original, &mut buf).unwrap();
        let decoded: ChainPoint = ciborium::from_reader(&buf[..]).unwrap();
        assert_eq!(original, decoded);
        assert_eq!(decoded.slot(), None);
        assert_eq!(decoded.hash(), None);
    }
}
