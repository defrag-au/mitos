# wallet-trace

Who is the same hand?

Given a `$handle`, an address or a stake key, produce the cluster of
credentials operated by one signer — with the transaction hash that joined each
pair.

Family B (see the root README): reads Mithril immutable chunk files directly.

## The one fact it rests on

Cardano requires **every key-locked input to be authorised by a signature in
the transaction's own witness set**, so the co-signing group is readable from
block bytes alone.

That means: no input resolution, no outref ladder. The walk is stateless and
forward-only — which is what makes a **chain-wide** index affordable. Built
once and reused by every case, rather than re-walked per investigation the way
`project-ledger` is.

MEASURED: full-chain index ≈ **12.8 GB**.

## ⚠️ A cluster is not a person

The chain shows that two credentials co-signed. It does not show why. Shared
custody, a batching service, a multisig, an exchange sweeping — all produce
co-signature. See `WALLET_TRACE.md` §"What the chain cannot tell you" before
putting a name to a cluster, and `suppress` for the credentials that must never
be allowed to merge two clusters.

## Commands

```bash
wallet-trace bootstrap   # fetch/prepare the snapshot
wallet-trace probe       # what the witness sets look like before committing
wallet-trace index       # build the chain-wide co-signing index
wallet-trace suppress    # exclude a credential from clustering
wallet-trace trace       # the cluster for one target
```

## ⚠️ Not guarded against phase-2 failures, deliberately

Every other walker in this workspace skips transactions the block declared
invalid, because their outputs never existed. **wallet-trace must not.**

An invalid transaction's *signatures are real* — it was signed and submitted by
real keys and paid a real fee — so its co-signing is genuine clustering
evidence. Skipping it would discard true information.

Its `stake_events` handling is the mixed case: certificates in an invalid
transaction never took effect, so those alone ought to be dropped. That is
unfixed, and doing it means a 12.8 GB rebuild. Recorded here rather than
half-done.
