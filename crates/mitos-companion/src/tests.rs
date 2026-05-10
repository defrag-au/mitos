//! Integration tests for the companion runtime.
//!
//! These exercise pure-Rust pieces that don't need a `worker::Env`
//! / `State` — wire-protocol round-trips, the channel dispatch
//! contract, and the dyn-trait blanket impl. The full DO-driven WS
//! path needs miniflare-style integration which is out of scope for
//! PR 1; that arrives with PR 7's second-consumer-port work.

use crate::ctx::{Ctx, SqlStorageValue};
use crate::traits::{MitosChannel, MitosChannelDyn, MitosCompanion};
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

// ============================================================================
// Dynamic-interest wire shape round-trips (PR 2)
// ============================================================================

#[test]
fn client_interest_frame_round_trips_add_op() {
    use crate::interest::{InterestRow, NO_CHANNEL, kinds, rows_to_interests};
    use crate::wire::{decode_client, encode_client};

    let row = InterestRow {
        kind: kinds::POLICY.into(),
        value: "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6".into(),
        channel: NO_CHANNEL.into(),
        added_at: "2026-05-05T12:00:00Z".into(),
    };
    let items = rows_to_interests(std::slice::from_ref(&row));
    let frame = ClientMessage::Interest {
        op: InterestOp::Add,
        items,
    };
    let bytes = encode_client(&frame).unwrap();
    let decoded = decode_client(&bytes).unwrap();
    match decoded {
        ClientMessage::Interest { op, items } => {
            assert!(matches!(op, InterestOp::Add));
            assert_eq!(items.len(), 1);
        }
        other => panic!("expected Interest, got {other:?}"),
    }
}

// ============================================================================
// Multi-channel companion compile-test (PR 4)
// ============================================================================

/// Second mock channel — paired with `MockChannel` to validate
/// that a single companion impl can return multiple
/// `MitosChannelDyn` from `channels()` without trait-shape
/// breakage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MarketplaceMockEvent {
    Sale { policy: String, price: u64 },
    Offer { policy: String, amount: u64 },
}

pub struct MarketplaceMockChannel;

#[async_trait::async_trait(?Send)]
impl MitosChannel for MarketplaceMockChannel {
    const NAME: &'static str = "marketplace";
    type Event = MarketplaceMockEvent;
    async fn apply_event(&self, _ctx: &Ctx, _event: MarketplaceMockEvent) -> crate::Result<()> {
        Ok(())
    }
}

/// Companion that holds two channels — exercises the
/// `MitosChannel + MitosChannelDyn` blanket impl across multiple
/// concrete channel types in a single `channels()` call.
struct MultiChannelCompanion;

impl crate::MitosCompanion for MultiChannelCompanion {
    const NAME: &'static str = "multi-channel";
    type Config = ();
    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![
            Box::new(MockChannel::default()),
            Box::new(MarketplaceMockChannel),
        ]
    }
}

#[test]
fn multi_channel_companion_returns_two_channels() {
    let companion = MultiChannelCompanion;
    let channels = companion.channels();
    assert_eq!(channels.len(), 2);
    let names: Vec<&str> = channels.iter().map(|c| c.name()).collect();
    assert!(names.contains(&"mock"));
    assert!(names.contains(&"marketplace"));
}

#[test]
fn multi_channel_dyn_dispatch_routes_by_name() {
    // Walking the channels Vec to find a channel by name is
    // exactly what `MitosCompanionRuntime::lookup_channel` does
    // internally on each `Apply` frame. Tests the lookup
    // contract without spinning up a runtime DO.
    let companion = MultiChannelCompanion;
    let channels = companion.channels();
    let ownership_idx = channels.iter().position(|c| c.name() == "mock").unwrap();
    assert_eq!(channels[ownership_idx].name(), "mock");
    let marketplace_idx = channels
        .iter()
        .position(|c| c.name() == "marketplace")
        .unwrap();
    assert_eq!(channels[marketplace_idx].name(), "marketplace");
}

#[test]
fn subscribe_request_carries_full_interest_set() {
    use crate::interest::{InterestRow, NO_CHANNEL, kinds, rows_to_interests};
    use crate::subscribe::{SubscribeRequest, decode_subscribe, encode_subscribe};
    use mitos_protocol::SubscribeTarget;

    let rows = vec![
        InterestRow {
            kind: kinds::POLICY.into(),
            value: "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6".into(),
            channel: NO_CHANNEL.into(),
            added_at: "2026-05-05T12:00:00Z".into(),
        },
        InterestRow {
            kind: kinds::POLICY.into(),
            value: "793aca910dc6a400ced6c94698c6f01d6479d701227fc9a7287ae2a5".into(),
            channel: NO_CHANNEL.into(),
            added_at: "2026-05-05T12:00:01Z".into(),
        },
    ];
    let interests = rows_to_interests(&rows);

    let req = SubscribeRequest {
        targets: vec![SubscribeTarget::Module {
            name: "ownership-indexer".into(),
        }],
        companion_key: "customer_42".into(),
        resume_from: None,
        interests,
        dial_back: None,
    };
    let bytes = encode_subscribe(&req).unwrap();
    let decoded = decode_subscribe(&bytes).unwrap();
    assert_eq!(decoded.interests.len(), 2);
    // Each Interest decodes back into something the host can act on
    // (CBOR-encode → host calls update-interest with Replace semantics
    // → module decodes Vec<Interest>).
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&decoded.interests, &mut buf).unwrap();
    let recovered: Vec<crate::wire::Interest> = ciborium::de::from_reader(&buf[..]).unwrap();
    assert_eq!(recovered.len(), 2);
}
