//! Files vendored from `github.com/txpipe/balius` (Apache-2.0).
//!
//! Vendoring plan and rationale: see
//! `../../../../docs/strategy/MITOS_PLATFORM_V1.md`
//! §"What we vendor from Balius".
//!
//! This directory will hold (when the vendoring step lands):
//! - `router.rs`  — `MatchKey` router (~140 lines)
//! - `store.rs`   — redb WAL + per-worker cursor (~252 lines)
//! - `kv.rs`      — worker-prefixed KV (subset of `kv/redb.rs`)
//! - `metrics.rs` — OpenTelemetry per-worker metrics (~291 lines)
//!
//! Each will preserve the upstream Apache-2.0 license header
//! verbatim and carry a `// Vendored from txpipe/balius @ <sha>`
//! annotation listing the local modifications. See `NOTICE` in
//! this directory for attribution.
//!
//! Until the vendoring lands, this module is empty.
