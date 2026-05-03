//! `PageRequest` / `Page<T>` — cursor-based pagination.
//!
//! Cursor strings are opaque to the caller. The plane defines
//! the encoding (probably compact CBOR `(predicate_hash,
//! last_seen_oref)`) so resumable queries don't require
//! per-cursor server state. Caller treats the token as a black
//! box: pass the previous response's `next_token` back unchanged.

use serde::{Deserialize, Serialize};

use crate::types::ChainTip;

/// Request shape for cursor-paginated queries. `start_token` is
/// `None` for the first page; subsequent pages pass back the
/// previous response's `next_token`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    /// Soft cap on items returned. The server may return fewer.
    /// Server-side hard cap (configurable, default 1000) prevents
    /// foot-gun queries that would return millions.
    pub max_items: u32,
    /// Opaque resume token from a previous response, or `None`
    /// for the first page.
    pub start_token: Option<String>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            max_items: 100,
            start_token: None,
        }
    }
}

impl PageRequest {
    /// Convenience: first page with given size.
    pub fn first(max_items: u32) -> Self {
        Self {
            max_items,
            start_token: None,
        }
    }

    /// Convenience: continue from a previous page's token.
    pub fn next(token: String, max_items: u32) -> Self {
        Self {
            max_items,
            start_token: Some(token),
        }
    }
}

/// A page of results. `next_token == None` indicates no more
/// pages. `tip` is the chain point the response was answered
/// against — caller can detect drift across paginated calls by
/// comparing successive `tip` values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
    pub tip: ChainTip,
}

impl<T> Page<T> {
    pub fn empty(tip: ChainTip) -> Self {
        Self {
            items: Vec::new(),
            next_token: None,
            tip,
        }
    }

    pub fn is_last(&self) -> bool {
        self.next_token.is_none()
    }
}
