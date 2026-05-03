//! Files vendored from `github.com/txpipe/balius` (Apache-2.0).
//!
//! Vendoring plan and rationale: see
//! `../../../../docs/strategy/MITOS_PLATFORM_V1.md`
//! §"What we vendor from Balius".
//!
//! Each file preserves an attribution annotation describing its
//! upstream source + commit + the substantive local
//! modifications. See `NOTICE` in this directory for project-
//! level attribution and `LICENSE-APACHE-2.0` for the upstream
//! license text.
//!
//! Currently vendored:
//! - `kv` — redb-backed per-module KV store (from
//!   `balius-runtime/src/kv/redb.rs`)
//!
//! Planned (will land when mitos-platform needs them):
//! - `router` — match-key routing engine (from
//!   `balius-runtime/src/router.rs`)
//! - `store` — redb WAL + per-worker cursor (from
//!   `balius-runtime/src/store.rs`)
//! - `metrics` — OpenTelemetry per-worker metrics (from
//!   `balius-runtime/src/metrics.rs`)

pub mod kv;
