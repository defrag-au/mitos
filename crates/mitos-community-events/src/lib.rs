//! Shared event types for mitos community modules.
//!
//! Each community module (`mitos/community-modules/<name>/`) owns
//! a submodule here: `pub mod <name>;`. Consumers depend on this
//! single crate and `use mitos_community_events::<name>::*;` rather
//! than each dApp shipping its own `<module>-events` crate.
//!
//! See `docs/strategy/COMMUNITY_MODULES.md` for the design.

pub mod asset_metadata_update;
pub mod burn_address;
pub mod cip25_mint;
pub mod cip68_mint;
pub mod jpg_co;
pub mod standard_burn;
