//! Contract tests for the view registry.
//!
//! The substrate here is a plain `Vec` rather than a live book, on purpose: the registry must
//! not depend on `live-book`, and a view that only reads a slice is enough to pin ordering,
//! error naming, single-call semantics, consumption and determinism.

use std::sync::{Arc, Mutex};

use mitos_fold::{AggregateResult, Error, Fingerprint, SchemaId, Stamp, View, ViewRegistry};

fn stamp() -> Stamp {
    Stamp::new(42, None, Fingerprint::from_u128(0x1234))
}

/// A stand-in substrate: rows of numbers.
type Substrate = Vec<u64>;

/// Reads the substrate once and returns what it saw.
struct Sum {
    schema: SchemaId,
    seen: Option<Vec<u64>>,
    calls: Arc<Mutex<usize>>,
    fail_view: bool,
    fail_finish: bool,
}

impl Sum {
    fn new(kind: &'static str) -> Self {
        Self {
            schema: SchemaId::new(kind, 1),
            seen: None,
            calls: Arc::new(Mutex::new(0)),
            fail_view: false,
            fail_finish: false,
        }
    }

    fn refusing(mut self) -> Self {
        self.fail_view = true;
        self
    }

    fn unbuildable(mut self) -> Self {
        self.fail_finish = true;
        self
    }
}

impl View<Substrate> for Sum {
    type Artifact = Vec<u64>;

    fn schema(&self) -> SchemaId {
        self.schema
    }

    fn view(&mut self, source: &Substrate) -> AggregateResult<()> {
        if self.fail_view {
            return Err(std::io::Error::other("refused").into());
        }
        *self.calls.lock().unwrap() += 1;
        self.seen = Some(source.clone());
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<Vec<u64>> {
        if self.fail_finish {
            return Err(std::io::Error::other("no artifact").into());
        }
        Ok(self.seen.unwrap_or_default())
    }
}

/// Reads into a log the test still holds, so a test can prove who ran.
struct Shared {
    schema: SchemaId,
    ran: Arc<Mutex<bool>>,
}

impl View<Substrate> for Shared {
    type Artifact = Vec<u64>;

    fn schema(&self) -> SchemaId {
        self.schema
    }

    fn view(&mut self, _source: &Substrate) -> AggregateResult<()> {
        *self.ran.lock().unwrap() = true;
        Ok(())
    }

    fn finish(self: Box<Self>, _stamp: Stamp) -> AggregateResult<Vec<u64>> {
        Ok(Vec::new())
    }
}

#[test]
fn registration_order_is_the_run_order() {
    let mut registry = ViewRegistry::new();
    registry.register("alpha", Sum::new("alpha")).unwrap();
    registry.register("beta", Sum::new("beta")).unwrap();

    registry.view(&vec![1, 2, 3]).unwrap();

    let finished = registry.finish(stamp()).unwrap();
    assert_eq!(finished.len(), 2);
    assert_eq!(finished[0].name, "alpha");
    assert_eq!(finished[0].schema, SchemaId::new("alpha", 1));
    assert_eq!(finished[0].artifact, vec![1, 2, 3]);
    assert_eq!(finished[1].name, "beta");
    assert_eq!(finished[1].artifact, vec![1, 2, 3]);
}

#[test]
fn a_view_sees_the_substrate_exactly_once() {
    let calls = Arc::new(Mutex::new(0));
    let mut view = Sum::new("once");
    view.calls = Arc::clone(&calls);

    let mut registry = ViewRegistry::new();
    registry.register("once", view).unwrap();
    registry.view(&vec![1]).unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        1,
        "a substrate is complete before a view runs, so there is nothing to iterate"
    );
}

#[test]
fn a_duplicate_name_is_refused() {
    let mut registry = ViewRegistry::new();
    registry.register("same", Sum::new("one")).unwrap();

    let error = registry.register("same", Sum::new("two")).unwrap_err();
    assert!(matches!(error, Error::DuplicateViewName { name } if name == "same"));
}

#[test]
fn a_duplicate_schema_is_refused() {
    let mut registry = ViewRegistry::new();
    registry.register("one", Sum::new("same")).unwrap();

    let error = registry.register("two", Sum::new("same")).unwrap_err();
    assert!(matches!(
        error,
        Error::DuplicateSchema { schema } if schema == SchemaId::new("same", 1)
    ));
}

#[test]
fn a_view_error_names_the_view() {
    let mut registry = ViewRegistry::new();
    registry.register("first", Sum::new("first")).unwrap();
    registry
        .register("failing", Sum::new("failing").refusing())
        .unwrap();

    let error = registry.view(&vec![1]).unwrap_err();
    match error {
        Error::View { view, .. } => assert_eq!(view, "failing"),
        other => panic!("expected a view error, got {other:?}"),
    }
}

#[test]
fn view_stops_at_the_first_error() {
    let ran = Arc::new(Mutex::new(false));
    let mut registry = ViewRegistry::new();
    registry
        .register("failing", Sum::new("failing").refusing())
        .unwrap();
    registry
        .register(
            "later",
            Shared {
                schema: SchemaId::new("later", 1),
                ran: Arc::clone(&ran),
            },
        )
        .unwrap();

    assert!(registry.view(&vec![7]).is_err());
    assert!(!*ran.lock().unwrap());
}

#[test]
fn a_finish_error_names_the_view() {
    let mut registry = ViewRegistry::new();
    registry.register("first", Sum::new("first")).unwrap();
    registry
        .register("failing", Sum::new("failing").unbuildable())
        .unwrap();

    let error = registry.finish(stamp()).unwrap_err();
    match error {
        Error::ViewFinish { view, .. } => assert_eq!(view, "failing"),
        other => panic!("expected a view-finish error, got {other:?}"),
    }
}

#[test]
fn finish_consumes_the_round() {
    let mut registry = ViewRegistry::new();
    registry.register("one", Sum::new("one")).unwrap();

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
    let mut registry: ViewRegistry<Substrate, Vec<u64>> = ViewRegistry::new();
    assert!(registry.is_empty());
    assert!(registry.finish(stamp()).unwrap().is_empty());
}

#[test]
fn an_empty_name_is_refused() {
    let mut registry = ViewRegistry::new();
    let error = registry.register("", Sum::new("one")).unwrap_err();
    assert!(matches!(error, Error::InvalidViewName { .. }));
}

#[test]
fn a_padded_name_is_refused() {
    let mut registry = ViewRegistry::new();
    let error = registry.register(" padded", Sum::new("one")).unwrap_err();
    assert!(matches!(error, Error::InvalidViewName { .. }));
}

#[test]
fn schemas_lists_each_registered_pair_in_order() {
    let mut registry = ViewRegistry::new();
    registry.register("alpha", Sum::new("one")).unwrap();
    registry.register("beta", Sum::new("two")).unwrap();

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
fn the_same_substrate_produces_the_same_artifacts() {
    fn run() -> Vec<(SchemaId, Vec<u64>)> {
        let mut registry = ViewRegistry::new();
        registry.register("alpha", Sum::new("alpha")).unwrap();
        registry.register("beta", Sum::new("beta")).unwrap();
        registry.view(&vec![4, 5, 6]).unwrap();
        registry
            .finish(stamp())
            .unwrap()
            .into_iter()
            .map(|finished| (finished.schema, finished.artifact))
            .collect()
    }

    assert_eq!(run(), run());
}
