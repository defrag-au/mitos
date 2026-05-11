//! Shared event types for mitos community modules.
//!
//! Each community module (`mitos/community-modules/<name>/`) owns
//! a submodule here: `pub mod <name>;`. Consumers depend on this
//! single crate and `use mitos_community_events::<name>::*;` rather
//! than each dApp shipping its own `<module>-events` crate.
//!
//! See `docs/strategy/COMMUNITY_MODULES.md` for the design.

pub mod jpg_co;
