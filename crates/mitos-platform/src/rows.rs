//! Client rows, as the hot paths read them.
//!
//! Two of them now ask `mitos_lifecycle::routing` the same question about the
//! same file: the follower's emit fan-out ("do I write this row?") and the
//! dialer's drain ("do I send this row now, or is it a leftover?"). Both need
//! a client's [`ClientRow`] and whether some other instance's directory is
//! still there, and both ask per event / per companion.
//!
//! One cache and one probe, here, rather than a copy in each — because the
//! two answers must agree. `routing::attached_to`'s doc comment is the same
//! argument one level up: were "whose events do I write" and "who keeps me
//! alive" computed two ways, they could disagree.
//!
//! # Why the cache must see a commit
//!
//! The row write IS the switch: from it on, `fan_out` says the new instance
//! serves the client. A cache that missed it would keep delivering from the
//! instance the client has left.
//!
//! The FOLLOWERS' caches are safe by construction — both are stopped across
//! the commit (`LifecycleManager::quiesce_pair`) and each rebuilds its cache
//! when its drain task starts. The DIALER's is not: one drain runs for the
//! whole module and never stops. That is why freshness is keyed on
//! `(mtime, len)` and not mtime alone.

use std::collections::HashMap;

use mitos_lifecycle::client_move::ClientRow;
use mitos_lifecycle::ids::{ClientId, InstanceKey};
use mitos_lifecycle::routing::InstanceDir;

use crate::storage::ModuleStorage;

/// Client rows, cached by path and refreshed on mtime change — exactly what
/// `InterestCache` does for interest files, and for the same reason: the
/// fan-out asks per emitted EVENT, and re-reading plus TOML-parsing a row
/// per companion per event would be the loop's dominant cost.
#[derive(Default)]
pub struct RowCache {
    entries: HashMap<String, CachedRow>,
}

struct CachedRow {
    /// Mtime AND length. Mtime alone is not enough: a row goes
    /// `Committing` → committed in two writes that can land inside one
    /// timestamp tick, and the commit is the routing change — a cache that
    /// missed it would keep delivering from the instance the client has
    /// left. A committed row has lost its `pending` table, so its LENGTH
    /// always changes even when its mtime does not.
    stamp: Option<(std::time::SystemTime, u64)>,
    row: Option<ClientRow>,
}

impl RowCache {
    /// The client's row, or `None` — which covers three cases the fan-out
    /// treats identically: no row, an unparseable one, and a directory name
    /// that is not a `ClientId`. A row that cannot be read must never stop
    /// delivery; `routing::fan_out` reads `None` as "follows the module's
    /// current instance", which is what the client was doing anyway.
    pub fn get(
        &mut self,
        storage: &ModuleStorage,
        module: &str,
        client_id: &str,
    ) -> Option<&ClientRow> {
        let Ok(client) = ClientId::parse(client_id) else {
            return None;
        };
        let path = storage
            .module_dir_for_rows(module)
            .join(format!("{client_id}.toml"));
        let stamp = std::fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok().map(|mtime| (mtime, m.len())));
        let key = path.to_string_lossy().into_owned();
        let fresh = matches!(self.entries.get(&key), Some(e) if e.stamp == stamp);
        if !fresh {
            let row = match storage.read_row(module, &client) {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(
                        module = %module,
                        client_id = %client_id,
                        error = %e,
                        "client row unreadable; this client follows the module's current instance",
                    );
                    None
                }
            };
            self.entries.insert(key.clone(), CachedRow { stamp, row });
        }
        self.entries.get(&key).and_then(|e| e.row.as_ref())
    }
}

/// The `dir_of` every `routing` call takes — one `stat`, asked lazily and
/// only about an instance other than the one asking.
pub fn instance_dir_of(storage: &ModuleStorage, key: &InstanceKey) -> InstanceDir {
    if storage.instance_dir(key).is_dir() {
        InstanceDir::Present
    } else {
        InstanceDir::Gone
    }
}
