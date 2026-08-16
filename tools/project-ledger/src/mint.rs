//! What a mint tx reveals about the project, read off the chain — the two
//! seeds the registry is forbidden to declare.
//!
//! - **The policy signer(s):** the native script whose hash IS the policy id
//!   is in the mint tx's witnesses; every `sig <keyhash>` in it is a credential
//!   that controls the policy. Any address with that payment credential is the
//!   minter, observed. `before <slot>` (InvalidHereafter) is the mint window's
//!   ceiling, also observed.
//! - **The CIP-27 royalty address:** label 777 in the on-mint metadata, `addr`
//!   as one string or a list of ≤64-char chunks to be joined; `rate` as text.

use pallas_primitives::alonzo::{Metadatum, NativeScript};
use pallas_traverse::{MultiEraTx, OriginalHash};

/// The policy's native script, decoded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyScript {
    /// `sig` key hashes anywhere in the script tree.
    pub signers: Vec<[u8; 28]>,
    /// The tightest `before` (InvalidHereafter) slot, if any.
    pub before_slot: Option<u64>,
    /// The loosest `after` (InvalidBefore) slot, if any.
    pub after_slot: Option<u64>,
}

/// Find the native script for `policy` among the tx's witnesses and decode it.
pub fn policy_script(tx: &MultiEraTx<'_>, policy: &[u8; 28]) -> Option<PolicyScript> {
    tx.native_scripts()
        .iter()
        .find(|s| s.original_hash().as_ref() == policy)
        .map(|s| {
            let mut out = PolicyScript::default();
            collect(s, &mut out);
            out.signers.sort();
            out.signers.dedup();
            out
        })
}

fn collect(s: &NativeScript, out: &mut PolicyScript) {
    match s {
        NativeScript::ScriptPubkey(h) => {
            let mut k = [0u8; 28];
            k.copy_from_slice(h.as_ref());
            out.signers.push(k);
        }
        NativeScript::ScriptAll(v)
        | NativeScript::ScriptAny(v)
        | NativeScript::ScriptNOfK(_, v) => {
            for c in v {
                collect(c, out);
            }
        }
        NativeScript::InvalidHereafter(slot) => {
            out.before_slot = Some(out.before_slot.map_or(*slot, |b| b.min(*slot)));
        }
        NativeScript::InvalidBefore(slot) => {
            out.after_slot = Some(out.after_slot.map_or(*slot, |a| a.max(*slot)));
        }
    }
}

/// CIP-27 royalty declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Royalty {
    pub addr: String,
    pub rate: Option<String>,
}

/// Label 777, if this tx carries it.
pub fn cip27_royalty(tx: &MultiEraTx<'_>) -> Option<Royalty> {
    let m = tx.metadata();
    let Metadatum::Map(kv) = m.find(777)? else {
        return None;
    };
    let mut addr: Option<String> = None;
    let mut rate: Option<String> = None;
    for (k, v) in kv.iter() {
        let Metadatum::Text(key) = k else { continue };
        match key.as_str() {
            "addr" => addr = text_or_joined(v),
            "rate" | "pct" => rate = text_or_joined(v),
            _ => {}
        }
    }
    addr.map(|addr| Royalty { addr, rate })
}

fn text_or_joined(v: &Metadatum) -> Option<String> {
    match v {
        Metadatum::Text(s) => Some(s.clone()),
        Metadatum::Array(parts) => {
            let mut s = String::new();
            for p in parts {
                if let Metadatum::Text(t) = p {
                    s.push_str(t);
                }
            }
            (!s.is_empty()).then_some(s)
        }
        Metadatum::Int(i) => Some(format!("{i:?}")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_signers_and_window() {
        let a = pallas_primitives::Hash::<28>::new([1u8; 28]);
        let b = pallas_primitives::Hash::<28>::new([2u8; 28]);
        let script = NativeScript::ScriptAll(vec![
            NativeScript::ScriptPubkey(a),
            NativeScript::ScriptAny(vec![
                NativeScript::ScriptPubkey(b),
                NativeScript::ScriptPubkey(a),
            ]),
            NativeScript::InvalidHereafter(1_000),
            NativeScript::InvalidHereafter(900),
            NativeScript::InvalidBefore(10),
        ]);
        let mut out = PolicyScript::default();
        collect(&script, &mut out);
        out.signers.sort();
        out.signers.dedup();
        assert_eq!(out.signers, vec![[1u8; 28], [2u8; 28]]);
        assert_eq!(out.before_slot, Some(900));
        assert_eq!(out.after_slot, Some(10));
    }

    #[test]
    fn joins_chunked_addr() {
        let v = Metadatum::Array(vec![
            Metadatum::Text("addr1qx".into()),
            Metadatum::Text("yz".into()),
        ]);
        assert_eq!(text_or_joined(&v).as_deref(), Some("addr1qxyz"));
        assert_eq!(
            text_or_joined(&Metadatum::Text("r".into())).as_deref(),
            Some("r")
        );
    }
}
