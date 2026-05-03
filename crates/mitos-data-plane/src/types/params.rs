//! `ProtocolParameters` — narrowed subset of Cardano protocol
//! parameters most relevant to dApps.
//!
//! Cardano's full protocol-parameter set has dozens of fields
//! across multiple eras (Shelley / Alonzo / Babbage / Conway).
//! Most dApps only need a handful: min UTxO value, fee
//! coefficients, ex-units prices for Plutus execution. This
//! struct exposes that subset; callers needing the full era-
//! specific shape can use `read_full_params` (TODO — not in
//! Phase A).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolParameters {
    /// Minimum lovelace per UTxO byte. Used in min-UTxO-value
    /// calculations. Conway-era field.
    pub coins_per_utxo_byte: u64,
    /// Fee per byte of tx body (Cardano's a*x + b model — this
    /// is the `a` coefficient).
    pub min_fee_a: u64,
    /// Constant fee component (`b` coefficient).
    pub min_fee_b: u64,
    /// Maximum tx size in bytes.
    pub max_tx_size: u32,
    /// Plutus execution-unit prices: lovelace per memory unit.
    pub price_mem: f64,
    /// Plutus execution-unit prices: lovelace per CPU step.
    pub price_step: f64,
    /// Maximum execution units per tx (memory).
    pub max_tx_ex_mem: u64,
    /// Maximum execution units per tx (CPU steps).
    pub max_tx_ex_steps: u64,
    /// Current epoch number when these params apply.
    pub epoch: u64,
}
