//! Shared event types for mitos community modules.
//!
//! Each community module (`mitos/community-modules/<name>/`) owns
//! a submodule here: `pub mod <name>;`. Consumers depend on this
//! single crate and `use mitos_community_events::<name>::*;` rather
//! than each dApp shipping its own `<module>-events` crate.
//!
//! Submodules are added when their backing community module lands;
//! per `docs/strategy/COMMUNITY_MODULES.md` the first one is
//! `jpg_co` (canonical example).
