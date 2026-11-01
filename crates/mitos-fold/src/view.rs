//! The view contract, and the registry that runs it over a substrate.
//!
//! A view is the second shape on the fold plane. An [`Aggregate`](crate::Aggregate) observes
//! every block *during* the walk; a view reads a substrate the walk has *finished*. The book
//! is the substrate v1 defines, and the holder table exists as both — which is the point of
//! the pair, because two shapes that agree are a stronger check on one artifact than either
//! shape alone.
//!
//! The contract is generic over the substrate for the same reason
//! [`Aggregate`](crate::Aggregate) is generic over the block: this crate stays dependency-free,
//! and a view can be exercised against a fixture that never touched a chunk.

use std::fmt;

use crate::registry::Finished;
use crate::schema::SchemaId;
use crate::stamp::Stamp;
use crate::{AggregateResult, Error, Result};

/// One projection over a substrate the walk built.
///
/// [`view`](View::view) is called **exactly once**. Unlike a block, a substrate is complete
/// before a view sees it, so there is nothing to iterate over and no order for a caller to
/// get wrong. A view that needs two passes makes them inside that one call.
///
/// `finish` consumes the view, as [`Aggregate::finish`](crate::Aggregate::finish) does, so
/// nothing resident survives the round.
pub trait View<S> {
    /// The artifact this view produces.
    type Artifact;

    /// The schema the artifact will be stamped with. It must be stable across the view's
    /// lifetime and unique within a registry.
    fn schema(&self) -> SchemaId;

    /// Read the substrate. Returning an error stops the round and names this view.
    fn view(&mut self, source: &S) -> AggregateResult<()>;

    /// Produce the artifact and consume the view.
    fn finish(self: Box<Self>, stamp: Stamp) -> AggregateResult<Self::Artifact>;
}

struct Entry<S, A> {
    name: String,
    schema: SchemaId,
    view: Box<dyn View<S, Artifact = A>>,
}

/// A set of views that share one substrate, one artifact type and one execution order.
///
/// The rule that governs [`Registry`](crate::Registry) governs this one: **registration order
/// is execution order**, with no topological sort and no hidden edge. A view that needs
/// another view's artifact is a second registry over that artifact.
pub struct ViewRegistry<S, A> {
    entries: Vec<Entry<S, A>>,
}

impl<S, A> ViewRegistry<S, A> {
    /// An empty registry.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Register a view under a name.
    ///
    /// The name and schema must both be unique, and both are validated here rather than at
    /// publish time so a typo fails before the substrate is built.
    pub fn register<V>(&mut self, name: impl Into<String>, view: V) -> Result<()>
    where
        V: View<S, Artifact = A> + 'static,
    {
        let name = name.into();
        validate_name(&name)?;
        let schema = view.schema();
        schema.validate()?;

        if self.entries.iter().any(|entry| entry.name == name) {
            return Err(Error::DuplicateViewName { name });
        }
        if self.entries.iter().any(|entry| entry.schema == schema) {
            return Err(Error::DuplicateSchema { schema });
        }

        self.entries.push(Entry {
            name,
            schema,
            view: Box::new(view),
        });
        Ok(())
    }

    /// How many views are registered.
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

    /// Run every view over one substrate, in registration order.
    ///
    /// Stops at the first error, naming the view and carrying its error as the source. A
    /// failed round is discarded: the views that already ran hold partial state, and the
    /// caller must drop the registry rather than ask it to finish.
    pub fn view(&mut self, source: &S) -> Result<()> {
        for entry in &mut self.entries {
            entry.view.view(source).map_err(|source| Error::View {
                view: entry.name.clone(),
                source,
            })?;
        }
        Ok(())
    }

    /// Finish every view, in registration order, and consume them.
    ///
    /// The registry is empty afterwards, including when a view fails — the round is over, and
    /// a caller that retries starts a fresh one.
    pub fn finish(&mut self, stamp: Stamp) -> Result<Vec<Finished<A>>> {
        let entries = std::mem::take(&mut self.entries);
        let mut finished = Vec::with_capacity(entries.len());

        for entry in entries {
            let Entry { name, schema, view } = entry;
            let artifact = view.finish(stamp).map_err(|source| Error::ViewFinish {
                view: name.clone(),
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

impl<S, A> Default for ViewRegistry<S, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, A> fmt::Debug for ViewRegistry<S, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.entries.iter().map(|entry| entry.name.as_str()))
            .finish()
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidViewName {
            name: name.to_owned(),
            reason: "the name is empty",
        });
    }
    if name.trim() != name {
        return Err(Error::InvalidViewName {
            name: name.to_owned(),
            reason: "the name has leading or trailing whitespace",
        });
    }
    if name.chars().any(char::is_control) {
        return Err(Error::InvalidViewName {
            name: name.to_owned(),
            reason: "the name contains a control character",
        });
    }
    Ok(())
}
