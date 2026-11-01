//! Contract tests for the fold registry.
//!
//! These use a synthetic block type on purpose: the registry must not depend on the
//! walker's decoder, and a test aggregate that only counts is enough to pin ordering,
//! error naming, consumption and determinism.

use std::sync::{Arc, Mutex};

use mitos_fold::{Aggregate, AggregateResult, Error, Fingerprint, Registry, SchemaId, Stamp};

fn stamp() -> Stamp {
    Stamp::new(42, None, Fingerprint::from_u128(0x1234))
}

/// Records every block it sees and returns the sequence as its artifact.
struct Trace {
    schema: SchemaId,
    seen: Vec<u64>,
    fail_observe: bool,
    fail_finish: bool,
}

impl Trace {
    fn new(kind: &'static str) -> Self {
        Self {
            schema: SchemaId::new(kind, 1),
            seen: Vec::new(),
            fail_observe: false,
            fail_finish: false,
        }
    }

    fn refusing(mut self) -> Self {
        self.fail_observe = true;
        self
    }

    fn unbuildable(mut self) -> Self {
        self.fail_finish = true;
        self
    }
}

impl Aggregate<u64> for Trace {
    type Artifact = Vec<u64>;

    fn schema(&self) -> SchemaId {
        self.schema
    }

    fn observe(&mut self, block: &u64) -> AggregateResult<()> {
        if self.fail_observe {
            return Err(std::io::Error::other("refused").into());
        }
        self.seen.push(*block);
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<Vec<u64>> {
        if self.fail_finish {
            return Err(std::io::Error::other("no artifact").into());
        }
        Ok(self.seen)
    }
}

/// Observes into a log the test still holds, so a test can prove who saw a block.
struct Shared {
    schema: SchemaId,
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Aggregate<u64> for Shared {
    type Artifact = Vec<u64>;

    fn schema(&self) -> SchemaId {
        self.schema
    }

    fn observe(&mut self, block: &u64) -> AggregateResult<()> {
        self.seen.lock().unwrap().push(*block);
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<Vec<u64>> {
        Ok(Vec::new())
    }
}

#[test]
fn registration_order_is_the_run_order() {
    let mut registry = Registry::new();
    registry.register("alpha", Trace::new("alpha")).unwrap();
    registry.register("beta", Trace::new("beta")).unwrap();

    registry.observe(&1).unwrap();
    registry.observe(&2).unwrap();

    let finished = registry.finish(stamp()).unwrap();
    assert_eq!(finished.len(), 2);
    assert_eq!(finished[0].name, "alpha");
    assert_eq!(finished[0].schema, SchemaId::new("alpha", 1));
    assert_eq!(finished[0].artifact, vec![1, 2]);
    assert_eq!(finished[1].name, "beta");
    assert_eq!(finished[1].schema, SchemaId::new("beta", 1));
    assert_eq!(finished[1].artifact, vec![1, 2]);
}

#[test]
fn a_duplicate_name_is_refused() {
    let mut registry = Registry::new();
    registry.register("same", Trace::new("one")).unwrap();

    let error = registry.register("same", Trace::new("two")).unwrap_err();
    assert!(matches!(error, Error::DuplicateName { name } if name == "same"));
}

#[test]
fn a_duplicate_schema_is_refused() {
    let mut registry = Registry::new();
    registry.register("one", Trace::new("same")).unwrap();

    let error = registry.register("two", Trace::new("same")).unwrap_err();
    assert!(matches!(
        error,
        Error::DuplicateSchema { schema } if schema == SchemaId::new("same", 1)
    ));
}

#[test]
fn an_observe_error_names_the_aggregate() {
    let mut registry = Registry::new();
    registry.register("first", Trace::new("first")).unwrap();
    registry
        .register("failing", Trace::new("failing").refusing())
        .unwrap();

    let error = registry.observe(&1).unwrap_err();
    match error {
        Error::Observe { aggregate, .. } => assert_eq!(aggregate, "failing"),
        other => panic!("expected an observe error, got {other:?}"),
    }
}

#[test]
fn observe_stops_at_the_first_error() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register("failing", Trace::new("failing").refusing())
        .unwrap();
    registry
        .register(
            "later",
            Shared {
                schema: SchemaId::new("later", 1),
                seen: Arc::clone(&seen),
            },
        )
        .unwrap();

    assert!(registry.observe(&7).is_err());
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn a_finish_error_names_the_aggregate() {
    let mut registry = Registry::new();
    registry.register("first", Trace::new("first")).unwrap();
    registry
        .register("failing", Trace::new("failing").unbuildable())
        .unwrap();

    let error = registry.finish(stamp()).unwrap_err();
    match error {
        Error::Finish { aggregate, .. } => assert_eq!(aggregate, "failing"),
        other => panic!("expected a finish error, got {other:?}"),
    }
}

#[test]
fn finish_consumes_the_round() {
    let mut registry = Registry::new();
    registry.register("one", Trace::new("one")).unwrap();

    assert_eq!(registry.len(), 1);
    assert!(!registry.is_empty());

    let first = registry.finish(stamp()).unwrap();
    assert_eq!(first.len(), 1);
    assert!(registry.is_empty());

    let second = registry.finish(stamp()).unwrap();
    assert!(second.is_empty());
}

#[test]
fn an_empty_registry_finishes_empty() {
    let mut registry: Registry<u64, Vec<u64>> = Registry::new();
    assert!(registry.is_empty());
    assert!(registry.finish(stamp()).unwrap().is_empty());
}

#[test]
fn an_invalid_schema_is_refused_at_registration() {
    struct VersionZero;

    impl Aggregate<u64> for VersionZero {
        type Artifact = ();

        fn schema(&self) -> SchemaId {
            SchemaId::new("zero", 0)
        }

        fn observe(&mut self, _block: &u64) -> AggregateResult<()> {
            Ok(())
        }

        fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<()> {
            Ok(())
        }
    }

    let mut registry: Registry<u64, ()> = Registry::new();
    let error = registry.register("zero", VersionZero).unwrap_err();
    assert!(matches!(error, Error::InvalidSchema { .. }));
}

#[test]
fn an_empty_name_is_refused() {
    let mut registry = Registry::new();
    let error = registry.register("", Trace::new("one")).unwrap_err();
    assert!(matches!(error, Error::InvalidAggregateName { .. }));
}

#[test]
fn a_padded_name_is_refused() {
    let mut registry = Registry::new();
    let error = registry.register(" padded", Trace::new("one")).unwrap_err();
    assert!(matches!(error, Error::InvalidAggregateName { .. }));
}

#[test]
fn schemas_lists_each_registered_pair_in_order() {
    let mut registry = Registry::new();
    registry.register("alpha", Trace::new("one")).unwrap();
    registry.register("beta", Trace::new("two")).unwrap();

    let pairs: Vec<(&str, SchemaId)> = registry.schemas().collect();
    assert_eq!(
        pairs,
        vec![
            ("alpha", SchemaId::new("one", 1)),
            ("beta", SchemaId::new("two", 1))
        ]
    );
}

#[test]
fn the_same_round_produces_the_same_artifacts() {
    fn run() -> Vec<(SchemaId, Vec<u64>)> {
        let mut registry = Registry::new();
        registry.register("alpha", Trace::new("alpha")).unwrap();
        registry.register("beta", Trace::new("beta")).unwrap();
        for block in 0..4u64 {
            registry.observe(&block).unwrap();
        }
        registry
            .finish(stamp())
            .unwrap()
            .into_iter()
            .map(|finished| (finished.schema, finished.artifact))
            .collect()
    }

    assert_eq!(run(), run());
}
