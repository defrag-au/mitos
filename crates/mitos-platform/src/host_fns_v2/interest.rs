//! v2 `interest` interface — empty marker (no methods on the
//! WIT interface). Predicate decoding happens host-side in
//! `update-interest`'s dispatch path; the interface itself only
//! exists to scope the variants in WIT.

// `InterestHost` is the bindgen-generated empty trait. The
// blanket impl lives in `host_fns_v2/mod.rs` because that's
// where the `HostStateV2` struct is defined; this module
// exists for parity with v1's `host_fns/` layout.
