//! Spike host. Loads the guest .wasm component, runs through:
//!  - module-version check (ABI handshake)
//!  - trap-policy read
//!  - init
//!  - one synthetic handle-event with a `resolved-block` resource
//!    pushed into the ResourceTable, passed as `borrow`, deleted
//!    after dispatch
//!
//! All host fns are async; one of them sleeps to prove the
//! future flows through wasmtime's executor cleanly.

use std::path::PathBuf;

use anyhow::{Context, Result};
use wasmtime::component::{Component, HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Config, Engine, Store};

wasmtime::component::bindgen!({
    path: "../wit",
    world: "mitos-module",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        "mitos:platform/block-context.resolved-block": ResolvedBlockHost,
    },
});

use mitos::spike::block_context::{Host as BlockContextHost, HostResolvedBlock};
use mitos::spike::state_kv::Host as StateKvHost;
use mitos::spike::types::{Host as TypesHost, TypedOutput};

/// Host-side state for one `resolved-block` resource. In the
/// real platform this would carry the decoded block + a lazy
/// resolution cache; here we just stub the fields.
pub struct ResolvedBlockHost {
    slot: u64,
    tx_count: u32,
}

/// Per-Store state. One `ResourceTable` lives the lifetime of
/// the instance; resources push/delete through it per block.
pub struct HostState {
    table: ResourceTable,
    kv: std::collections::HashMap<String, Vec<u8>>,
}

impl HostState {
    fn new() -> Self {
        Self {
            table: ResourceTable::new(),
            kv: std::collections::HashMap::new(),
        }
    }
}

// ---- Host trait impls ----

// Empty interface-level marker traits — bindgen requires impls
// even when the interface only contains free functions / records.
impl TypesHost for HostState {}
impl BlockContextHost for HostState {}

impl StateKvHost for HostState {
    async fn get_value(&mut self, key: String) -> wasmtime::Result<Option<Vec<u8>>> {
        // Simulate I/O: a real impl would do
        // `tokio::task::spawn_blocking(move || db.read(...))`.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(self.kv.get(&key).cloned())
    }

    async fn set_value(&mut self, key: String, value: Vec<u8>) -> wasmtime::Result<()> {
        self.kv.insert(key, value);
        Ok(())
    }
}

impl HostResolvedBlock for HostState {
    async fn slot(&mut self, self_: Resource<ResolvedBlockHost>) -> wasmtime::Result<u64> {
        Ok(self.table.get(&self_)?.slot)
    }

    async fn tx_count(&mut self, self_: Resource<ResolvedBlockHost>) -> wasmtime::Result<u32> {
        Ok(self.table.get(&self_)?.tx_count)
    }

    async fn get_consumed_input(
        &mut self,
        _self_: Resource<ResolvedBlockHost>,
        _tx_idx: u32,
        _input_idx: u32,
    ) -> wasmtime::Result<Option<TypedOutput>> {
        // Spike: stub. Real impl: lazy lookup against data plane,
        // memoised in the ResolvedBlockHost.
        Ok(None)
    }

    async fn drop(&mut self, rep: Resource<ResolvedBlockHost>) -> wasmtime::Result<()> {
        // Called when the guest drops the borrow. With `borrow<>`
        // the host retains ownership; we still implement drop so
        // wasmtime can wire up the resource's lifecycle.
        let _ = self.table.delete(rep);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let wasm_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: spike <path-to-guest.wasm>")?;

    let mut config = Config::new();
    config.wasm_component_model(true);
    config.epoch_interruption(true);
    config.consume_fuel(true);

    let engine = Engine::new(&config)?;
    let component = Component::from_file(&engine, &wasm_path)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("loading component from {}", wasm_path.display()))?;

    let mut linker = Linker::<HostState>::new(&engine);
    MitosModule::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s| s)?;

    let mut store = Store::new(&engine, HostState::new());
    store.set_fuel(100_000_000)?;
    store.fuel_async_yield_interval(Some(10_000))?;
    // Generous epoch deadline; in production a background thread
    // would tick epoch periodically and expire after N ticks.
    store.set_epoch_deadline(1_000_000);

    let bindings = MitosModule::instantiate_async(&mut store, &component, &linker).await?;

    // 1. ABI version handshake.
    let (major, minor) = bindings.call_module_version(&mut store).await?;
    println!("module-version: {major}.{minor}");
    if major != 1 {
        anyhow::bail!("ABI version mismatch: host wants 1.x, got {major}.{minor}");
    }

    // 2. Trap policy.
    let (strategy, retry) = bindings.call_trap_policy(&mut store).await?;
    println!("trap-policy: {strategy:?}, retry={retry:?}");

    // 3. init.
    bindings.call_init(&mut store, &[]).await?;
    println!("init: ok");

    // 4. Push a resolved-block resource into the table, dispatch.
    let block_id = store
        .data_mut()
        .table
        .push(ResolvedBlockHost {
            slot: 12_345_678,
            tx_count: 42,
        })?;

    bindings
        .call_handle_event(&mut store, 0, block_id)
        .await?;
    println!("handle-event: ok");

    // 5. (No explicit delete — `borrow<>` semantics let the guest
    // drop, and our `HostResolvedBlock::drop` impl reaps. If we
    // pushed many blocks we'd verify the table doesn't leak.)

    Ok(())
}
