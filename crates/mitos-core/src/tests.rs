//! Unit tests for the protocol layer.
//!
//! Wire types live in `mitos-protocol`; these tests round-trip them
//! using the same encode/decode helpers re-exported through
//! `crate::replicate::*`. The host-internal `crate::indexer::SubscribeReply`
//! (which uses `dolos_core::ChainPoint`) is converted at the wire
//! boundary via `subscribe_reply_to_wire`, also tested below.

use crate::indexer::SubscribeReply as InternalSubscribeReply;
use crate::replicate::{
    ClientMessage, ServerMessage, chain_point_from_wire, chain_point_to_wire, decode_client,
    decode_server, encode_client, encode_server, subscribe_reply_to_wire,
};
use dolos_core::{BlockHash, ChainPoint};
use mitos_protocol::{ChainPoint as WireChainPoint, SubscribeReply as WireSubscribeReply};

fn fixed_hash() -> BlockHash {
    BlockHash::from([0xa1u8; 32])
}

fn fixed_hash_hex() -> String {
    hex::encode([0xa1u8; 32])
}

#[test]
fn client_subscribe_roundtrip() {
    let original = ClientMessage::Subscribe {
        scope: vec![0xde, 0xad, 0xbe, 0xef],
        cursor: WireChainPoint::Specific(186_076_148, fixed_hash_hex()),
    };
    let bytes = encode_client(&original).unwrap();
    match decode_client(&bytes).unwrap() {
        ClientMessage::Subscribe { scope, cursor } => {
            assert_eq!(scope, vec![0xde, 0xad, 0xbe, 0xef]);
            match cursor {
                WireChainPoint::Specific(slot, hash) => {
                    assert_eq!(slot, 186_076_148);
                    assert_eq!(hash, fixed_hash_hex());
                }
                other => panic!("expected Specific, got {other:?}"),
            }
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn client_ack_roundtrip() {
    // Q5 emission-id Ack — replaces the legacy cursor-Ack.
    let original = ClientMessage::Ack { emission_id: 42 };
    let bytes = encode_client(&original).unwrap();
    match decode_client(&bytes).unwrap() {
        ClientMessage::Ack { emission_id } => assert_eq!(emission_id, 42),
        other => panic!("expected Ack, got {other:?}"),
    }
}

#[test]
fn client_nack_roundtrip() {
    let original = ClientMessage::Nack {
        emission_id: 99,
        error: "apply failed: example".into(),
    };
    let bytes = encode_client(&original).unwrap();
    match decode_client(&bytes).unwrap() {
        ClientMessage::Nack { emission_id, error } => {
            assert_eq!(emission_id, 99);
            assert!(error.contains("apply failed"));
        }
        other => panic!("expected Nack, got {other:?}"),
    }
}

#[test]
fn server_subscribe_reply_resume_roundtrip() {
    let original = ServerMessage::SubscribeReply(WireSubscribeReply::Resume {
        cursor: WireChainPoint::Origin,
    });
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::SubscribeReply(WireSubscribeReply::Resume { cursor }) => {
            assert!(matches!(cursor, WireChainPoint::Origin));
        }
        other => panic!("expected SubscribeReply::Resume, got {other:?}"),
    }
}

#[test]
fn server_subscribe_reply_snapshot_redirect_roundtrip() {
    let original = ServerMessage::SubscribeReply(WireSubscribeReply::SnapshotRedirect {
        snapshot_url: "r2://mitos-snapshots/collection-ownership/snapshot-186076148-a1.cbor.zst"
            .into(),
        snapshot_cursor: WireChainPoint::Specific(186_076_148, fixed_hash_hex()),
    });
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::SubscribeReply(WireSubscribeReply::SnapshotRedirect {
            snapshot_url,
            snapshot_cursor,
        }) => {
            assert!(snapshot_url.starts_with("r2://"));
            assert!(matches!(snapshot_cursor, WireChainPoint::Specific(_, _)));
        }
        other => panic!("expected SnapshotRedirect, got {other:?}"),
    }
}

#[test]
fn server_apply_roundtrip_preserves_change_bytes_verbatim() {
    let raw_change = vec![0x01, 0x02, 0x03, 0x04, 0x05];
    let original = ServerMessage::Apply {
        emission_id: 7,
        cursor: WireChainPoint::Slot(100),
        change: raw_change.clone(),
    };
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::Apply {
            emission_id,
            cursor,
            change,
        } => {
            assert_eq!(emission_id, 7);
            assert!(matches!(cursor, WireChainPoint::Slot(100)));
            assert_eq!(change, raw_change);
        }
        other => panic!("expected Apply, got {other:?}"),
    }
}

#[test]
fn server_undo_and_mark_roundtrip() {
    for original in [
        ServerMessage::Undo {
            cursor: WireChainPoint::Slot(7),
        },
        ServerMessage::Mark {
            cursor: WireChainPoint::Slot(8),
        },
    ] {
        let bytes = encode_server(&original).unwrap();
        let decoded = decode_server(&bytes).unwrap();
        match (original, decoded) {
            (ServerMessage::Undo { .. }, ServerMessage::Undo { .. }) => {}
            (ServerMessage::Mark { .. }, ServerMessage::Mark { .. }) => {}
            (a, b) => panic!("variant mismatch: {a:?} vs {b:?}"),
        }
    }
}

#[test]
fn server_error_roundtrip() {
    let original = ServerMessage::Error {
        code: "lagged".into(),
        message: "consumer lagged by 5 records; reconnect".into(),
    };
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::Error { code, message } => {
            assert_eq!(code, "lagged");
            assert!(message.contains("reconnect"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn server_recapture_roundtrip() {
    // With reason.
    let original = ServerMessage::Recapture {
        module: "jpg-co".into(),
        reason: Some("manual rebuild after schema migration".into()),
    };
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::Recapture { module, reason } => {
            assert_eq!(module, "jpg-co");
            assert_eq!(
                reason.as_deref(),
                Some("manual rebuild after schema migration")
            );
        }
        other => panic!("expected Recapture, got {other:?}"),
    }

    // Without reason — should round-trip via `skip_serializing_if`
    // + `serde(default)`.
    let original = ServerMessage::Recapture {
        module: "wayup-co".into(),
        reason: None,
    };
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::Recapture { module, reason } => {
            assert_eq!(module, "wayup-co");
            assert!(reason.is_none());
        }
        other => panic!("expected Recapture, got {other:?}"),
    }
}

#[test]
fn client_recapture_ready_roundtrip() {
    let original = ClientMessage::RecaptureReady;
    let bytes = encode_client(&original).unwrap();
    match decode_client(&bytes).unwrap() {
        ClientMessage::RecaptureReady => {}
        other => panic!("expected RecaptureReady, got {other:?}"),
    }
}

#[test]
fn server_recapture_done_roundtrip() {
    let original = ServerMessage::RecaptureDone {
        cursor: WireChainPoint::Specific(186_076_148, fixed_hash_hex()),
        module: "jpg-co".into(),
        events_emitted: 124,
    };
    let bytes = encode_server(&original).unwrap();
    match decode_server(&bytes).unwrap() {
        ServerMessage::RecaptureDone {
            cursor,
            module,
            events_emitted,
        } => {
            match cursor {
                WireChainPoint::Specific(slot, hash) => {
                    assert_eq!(slot, 186_076_148);
                    assert_eq!(hash, fixed_hash_hex());
                }
                other => panic!("expected Specific, got {other:?}"),
            }
            assert_eq!(module, "jpg-co");
            assert_eq!(events_emitted, 124);
        }
        other => panic!("expected RecaptureDone, got {other:?}"),
    }
}

#[test]
fn chain_point_conversion_round_trips() {
    let dolos = ChainPoint::Specific(186_076_148, fixed_hash());
    let wire = chain_point_to_wire(&dolos);
    match &wire {
        WireChainPoint::Specific(slot, hash) => {
            assert_eq!(*slot, 186_076_148);
            assert_eq!(hash, &fixed_hash_hex());
        }
        other => panic!("expected Specific, got {other:?}"),
    }
    let back = chain_point_from_wire(&wire).unwrap();
    assert_eq!(back, dolos);

    assert_eq!(
        chain_point_from_wire(&chain_point_to_wire(&ChainPoint::Origin)).unwrap(),
        ChainPoint::Origin
    );
    assert_eq!(
        chain_point_from_wire(&chain_point_to_wire(&ChainPoint::Slot(42))).unwrap(),
        ChainPoint::Slot(42)
    );
}

#[test]
fn subscribe_reply_to_wire_round_trip() {
    let internal = InternalSubscribeReply::Resume {
        cursor: ChainPoint::Specific(123, fixed_hash()),
    };
    let wire = subscribe_reply_to_wire(&internal);
    match wire {
        WireSubscribeReply::Resume {
            cursor: WireChainPoint::Specific(slot, hash),
        } => {
            assert_eq!(slot, 123);
            assert_eq!(hash, fixed_hash_hex());
        }
        other => panic!("expected Resume, got {other:?}"),
    }
}

// ---- InjectFirst transport ----

mod inject_first {
    use crate::transport::WsTransport;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct ScriptedTransport {
        recv_queue: Mutex<Vec<Vec<u8>>>,
        sent: Mutex<Vec<Vec<u8>>>,
    }

    impl ScriptedTransport {
        fn new(recv_queue: Vec<Vec<u8>>) -> Self {
            Self {
                recv_queue: Mutex::new(recv_queue),
                sent: Mutex::new(Vec::new()),
            }
        }

        #[allow(dead_code)]
        fn sent(&self) -> Vec<Vec<u8>> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WsTransport for ScriptedTransport {
        async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
            let mut q = self.recv_queue.lock().unwrap();
            if q.is_empty() {
                return Ok(None);
            }
            Ok(Some(q.remove(0)))
        }

        async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
            self.sent.lock().unwrap().push(bytes);
            Ok(())
        }
    }

    #[tokio::test]
    async fn inject_first_returns_injected_frame_then_underlying() {
        struct Wrap {
            inner: Box<dyn WsTransport>,
            injected: Option<Vec<u8>>,
        }
        #[async_trait]
        impl WsTransport for Wrap {
            async fn recv_binary(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
                if let Some(b) = self.injected.take() {
                    return Ok(Some(b));
                }
                self.inner.recv_binary().await
            }
            async fn send_binary(&mut self, bytes: Vec<u8>) -> anyhow::Result<()> {
                self.inner.send_binary(bytes).await
            }
        }

        let underlying = ScriptedTransport::new(vec![vec![0xaa], vec![0xbb]]);
        let mut wrapper = Wrap {
            inner: Box::new(underlying),
            injected: Some(vec![0xff]),
        };

        assert_eq!(wrapper.recv_binary().await.unwrap(), Some(vec![0xff]));
        assert_eq!(wrapper.recv_binary().await.unwrap(), Some(vec![0xaa]));
        assert_eq!(wrapper.recv_binary().await.unwrap(), Some(vec![0xbb]));
        assert_eq!(wrapper.recv_binary().await.unwrap(), None);
    }
}
