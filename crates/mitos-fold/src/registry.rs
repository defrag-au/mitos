//! The aggregate contract, and the registry that runs it over one walk.
//!
//! An aggregate observes every block of a walk and produces one artifact. The registry owns
//! the set, calls `observe` on each in registration order for every block, then calls
//! `finish` once per aggregate. Registration order is the execution order, deliberately:
//! there is no implicit topological sort, and an aggregate that needs another aggregate's
//! finished artifact is a second registry over that artifact rather than a hidden edge in
//! this one.

use std::fmt;

use crate::schema::SchemaId;
use crate::stamp::Stamp;
use crate::{AggregateResult, Error, Result};

/// One fold over a decoded walk.
///
/// `B` is the walker's block type. This crate does not define it: the same aggregate can be
/// tested against a synthetic block and run against the real decoder without a second
/// implementation.
///
/// The registry calls [`observe`](Aggregate::observe) zero or more times, in walk order,
/// then [`finish`](Aggregate::finish) exactly once. `finish` consumes the aggregate, so
/// nothing resident survives the round.
pub trait Aggregate<B> {
    /// The artifact this aggregate produces.
    type Artifact;

    /// The schema the artifact will be stamped with. It must be stable across the
    /// aggregate's lifetime and unique within a registry.
    fn schema(&self) -> SchemaId;

    /// Observe one block. Returning an error stops the walk and names this aggregate; the
    /// remaining aggregates do not see the block.
    fn observe(&mut self, block: &B) -> AggregateResult<()>;

    /// Produce the artifact and consume the aggregate.
    fn finish(self: Box<Self>, stamp: Stamp) -> AggregateResult<Self::Artifact>;
}

/// One aggregate's output from a finished round.
#[derive(Debug)]
pub struct Finished<A> {
    /// The name it was registered under.
    pub name: String,
    /// The schema it declared.
    pub schema: SchemaId,
    /// What it produced.
    pub artifact: A,
}

struct Entry<B, A> {
    name: String,
    schema: SchemaId,
    aggregate: Box<dyn Aggregate<B, Artifact = A>>,
}

/// A set of aggregates that share one artifact type and one execution order.
///
/// The registry is not `Sync` and does not need to be: a walk is one deterministic pass.
/// All registered aggregates share `A`, so a caller that needs two different artifact types
/// runs two registries — or, more usually, uses the envelope as the common artifact type.
pub struct Registry<B, A> {
    entries: Vec<Entry<B, A>>,
}

impl<B, A> Registry<B, A> {
    /// An empty registry.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Register an aggregate under a name.
    ///
    /// The name and schema must both be unique. The schema is validated here rather than at
    /// publish time so a typo fails before the walk starts.
    pub fn register<AG>(&mut self, name: impl Into<String>, aggregate: AG) -> Result<()>
    where
        AG: Aggregate<B, Artifact = A> + 'static,
    {
        let name = name.into();
        validate_name(&name)?;
        let schema = aggregate.schema();
        schema.validate()?;

        if self.entries.iter().any(|entry| entry.name == name) {
            return Err(Error::DuplicateName { name });
        }
        if self.entries.iter().any(|entry| entry.schema == schema) {
            return Err(Error::DuplicateSchema { schema });
        }

        self.entries.push(Entry {
            name,
            schema,
            aggregate: Box::new(aggregate),
        });
        Ok(())
    }

    /// How many aggregates are registered.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The registered pairs, in execution order.
    pub fn schemas(&self) -> impl Iterator<Item = (&str, SchemaId)> + '_ {
        self.entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.schema))
    }

    /// Observe one block with every aggregate, in registration order.
    ///
    /// Stops at the first error. The error names the aggregate and carries its own error as
    /// the source, so a walk failure is never anonymous. A failed round is discarded: the
    /// aggregates that already saw this block hold partial state, and the caller must drop
    /// the registry rather than ask it to finish.
    pub fn observe(&mut self, block: &B) -> Result<()> {
        for entry in &mut self.entries {
            entry
                .aggregate
                .observe(block)
                .map_err(|source| Error::Observe {
                    aggregate: entry.name.clone(),
                    source,
                })?;
        }
        Ok(())
    }

    /// Finish every aggregate, in registration order, and consume them.
    ///
    /// The registry is empty afterwards, including when an aggregate fails: the round is
    /// over, and a caller that retries starts a fresh walk rather than resuming a partial
    /// set. The error names the aggregate that failed.
    pub fn finish(&mut self, stamp: Stamp) -> Result<Vec<Finished<A>>> {
        let entries = std::mem::take(&mut self.entries);
        let mut finished = Vec::with_capacity(entries.len());

        for entry in entries {
            let Entry {
                name,
                schema,
                aggregate,
            } = entry;
            let artifact = aggregate.finish(stamp).map_err(|source| Error::Finish {
                aggregate: name.clone(),
                source,
            })?;
            finished.push(Finished {
                name,
                schema,
                artifact,
            });
        }

        Ok(finished)
    }
}

impl<B, A> Default for Registry<B, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B, A> fmt::Debug for Registry<B, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.entries.iter().map(|entry| entry.name.as_str()))
            .finish()
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidAggregateName {
            name: name.to_owned(),
            reason: "the name is empty",
        });
    }
    if name.trim() != name {
        return Err(Error::InvalidAggregateName {
            name: name.to_owned(),
            reason: "the name has leading or trailing whitespace",
        });
    }
    if name.chars().any(char::is_control) {
        return Err(Error::InvalidAggregateName {
            name: name.to_owned(),
            reason: "the name contains a control character",
        });
    }
    Ok(())
}
