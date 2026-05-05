//! Integration tests for the companion runtime.
//!
//! These exercise pure-Rust pieces that don't need a `worker::Env`
//! / `State` — wire-protocol round-trips, the channel dispatch
//! contract, and the dyn-trait blanket impl. The full DO-driven WS
//! path needs miniflare-style integration which is out of scope for
//! PR 1; that arrives with PR 7's second-consumer-port work.

use crate::ctx::{Ctx, SqlStorageValue};
use crate::traits::{MitosChannel, MitosChannelDyn};
use crate::wire::{ChainPoint, ClientMessage, InterestOp, ServerMessage};
use serde::{Deserialize, Serialize};

// ============================================================================
// Mock channel — exercises the MitosChannel + MitosChannelDyn shape
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MockEvent {
    Hello(String),
    Number(u64),
}

pub struct MockChannel {
    pub last_seen: std::cell::RefCell<Option<MockEvent>>,
    pub fail_on: Option<MockEvent>,
}

impl Default for MockChannel {
    fn default() -> Self {
        Self {
            last_seen: std::cell::RefCell::new(None),
            fail_on: None,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl MitosChannel for MockChannel {
    const NAME: &'static str = "mock";
    type Event = MockEvent;

    async fn apply_event(&self, _ctx: &Ctx, event: MockEvent) -> crate::Result<()> {
        if matches!(&self.fail_on, Some(want_fail) if want_fail == &event) {
            return Err(crate::CompanionError::Apply {
                channel: Self::NAME,
                source: anyhow::anyhow!("mock requested failure"),
            });
        }
        *self.last_seen.borrow_mut() = Some(event);
        Ok(())
    }
}

// ============================================================================
// MitosChannelDyn blanket impl tests — the meat of the dispatch path
// ============================================================================

/// `MitosChannelDyn::apply_bytes` is the runtime's dispatch entry
/// point. It CBOR-decodes raw payload bytes into the channel's
/// `Event` and forwards to `apply_event`. This test verifies the
/// blanket impl works end-to-end without a runtime DO.
///
/// Skipped in non-wasm builds where `Ctx` would need a real
/// `worker::SqlStorage` (we can't construct one outside the wasm
/// runtime). PR 1 documents this gap; full integration tests come
/// with miniflare in PR 7.
#[cfg(target_arch = "wasm32")]
#[tokio::test]
async fn dispatch_decodes_and_calls_apply_event() {
    // ...lands when we have a miniflare harness in PR 7...
}

#[test]
fn cbor_round_trip_mock_event() {
    let original = MockEvent::Hello("world".into());
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&original, &mut buf).unwrap();
    let decoded: MockEvent = ciborium::de::from_reader(&buf[..]).unwrap();
    assert_eq!(decoded, original);
}

// ============================================================================
// Wire / decode contract tests — runtime-independent
// ============================================================================

#[test]
fn server_apply_decode_then_dispatch_payload() {
    // What the runtime gets from mitos: an Apply frame with an
    // emission_id + a CBOR-encoded change payload. Verify the
    // decode + payload-extract path works. (Channel name comes
    // from the WS Hibernation tag at runtime, not from the frame.)
    let event = MockEvent::Number(42);
    let mut payload_buf = Vec::new();
    ciborium::ser::into_writer(&event, &mut payload_buf).unwrap();

    let frame = ServerMessage::Apply {
        emission_id: 7,
        cursor: ChainPoint::Specific(123, "abcd".into()),
        change: payload_buf.clone(),
    };
    let bytes = crate::wire::encode_server(&frame).unwrap();
    let decoded = crate::wire::decode_server(&bytes).unwrap();
    match decoded {
        ServerMessage::Apply {
            emission_id,
            change,
            ..
        } => {
            assert_eq!(emission_id, 7);
            // Extract the inner Event payload.
            let inner: MockEvent = ciborium::de::from_reader(&change[..]).unwrap();
            assert_eq!(inner, MockEvent::Number(42));
        }
        other => panic!("expected Apply, got {other:?}"),
    }
}

#[test]
fn ack_nack_pair_round_trips() {
    // Tests the Ack/Nack frame shapes in the same PR 5 atomic
    // invariant context — runtime sends one or the other after
    // synchronous cursor advance.
    let ack = ClientMessage::Ack { emission_id: 99 };
    let bytes = crate::wire::encode_client(&ack).unwrap();
    let decoded = crate::wire::decode_client(&bytes).unwrap();
    match decoded {
        ClientMessage::Ack { emission_id } => assert_eq!(emission_id, 99),
        other => panic!("expected Ack, got {other:?}"),
    }

    let nack = ClientMessage::Nack {
        emission_id: 100,
        error: "apply failed: foo".into(),
    };
    let bytes = crate::wire::encode_client(&nack).unwrap();
    let decoded = crate::wire::decode_client(&bytes).unwrap();
    match decoded {
        ClientMessage::Nack { emission_id, error } => {
            assert_eq!(emission_id, 100);
            assert!(error.contains("apply failed"));
        }
        other => panic!("expected Nack, got {other:?}"),
    }
}

#[test]
fn interest_op_variants_round_trip() {
    for op in [InterestOp::Add, InterestOp::Remove, InterestOp::Replace] {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&op, &mut buf).unwrap();
        let decoded: InterestOp = ciborium::de::from_reader(&buf[..]).unwrap();
        assert!(matches!(
            (op, decoded),
            (InterestOp::Add, InterestOp::Add)
                | (InterestOp::Remove, InterestOp::Remove)
                | (InterestOp::Replace, InterestOp::Replace)
        ));
    }
}

#[test]
fn channel_dyn_name_dispatch() {
    // The runtime walks `channels: Vec<Box<dyn MitosChannelDyn>>`
    // and matches by name. Verify a Box<dyn> resolves correctly.
    let ch: Box<dyn MitosChannelDyn> = Box::new(MockChannel::default());
    assert_eq!(ch.name(), "mock");
}

// SqlStorageValue is exercised by the host-side unit tests indirectly
// (the Ctx::exec contract surfaces the type at the public API). We
// re-export it here so callers can construct typed values without
// pulling worker-rs directly.
#[test]
fn sql_value_export_is_usable() {
    let _v = SqlStorageValue::from(42_i64);
    let _w = SqlStorageValue::from("hello");
}
