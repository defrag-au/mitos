//! Splash DEX decode surface — currently just script addresses,
//! enough for `holder-distribution` to recognise a Splash pool
//! holder as `DexPool` even before Splash decomposition lands.
//!
//! The Splash datum decoder still lives inline in
//! `community-modules/splash-dex/splash_dex.rs`; it migrates here
//! when Splash gets the same decomposition treatment as CSwap
//! (see `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md`).

/// Splash V3 pool address. Single canonical bech32 (script
/// payment + script stake — `addr1x…`); every Splash pool lives
/// here.
pub const POOL_SCRIPT_ADDR: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";

/// SpotOrderV3 script address prefix (51 chars = `addr1z` +
/// header byte + 28-byte payment hash worth of bech32). Each
/// user's order has its own stake credential glued to the same
/// payment script, so consumers prefix-match.
pub const ORDER_SCRIPT_ADDR_PREFIX: &str = "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q";
