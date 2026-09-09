# policy-archive

The sealed Parquet archive of one policy's movements — writer, reader, and the
derived tiers over it.

Consumed by `tools/token-ledger` on the box **and by a Cloudflare Worker
reading the same files out of R2**. That second consumer is why the crate is
shaped the way it is.

Designs: `cnft.dev-workers/docs/design/POLICY_ARCHIVE_AND_SCALE.md`,
`POLICY_ARCHIVE_OBSERVATIONS.md`, `POLICY_PROFILE.md`.

## The four properties that constrain everything

**No arrow, no async, no I/O.** The crate is pure over byte ranges: a caller
fetches what `reader` asks for and hands the bytes back. That is what lets the
same code run on cardano-infra against a file, in a Worker against R2, and in a
browser bundle — and what keeps the wasm cost at 0.15 MB rather than the 2.5 MB
the convenient API costs.

**The footer is the density tier.** Row groups are time-aligned (nominally one
calendar day, quiet days merged, busy days split on a transaction boundary), so
a reader that fetches only the footer gets a daily histogram of the policy's
whole life from one range request and no decoded data pages.

**The stamp travels WITH the file.** Which policy, which slot range, how far the
producing walk had reached, and whether it was complete — all in the footer. A
partition that cannot say whether it is a window or an archive is a liability.

**Point lookups without a database.** `tx_hash` carries a split-block bloom
filter per row group.

## Modules

| module | what |
|---|---|
| `schema` / `writer` / `reader` | the movement rows and the sealed file |
| `manifest` | the archive's record of itself — passes, coverage, completeness, profile |
| `groups` / `density` | time-aligned row groups and the histogram read from footers |
| `feed` | rows → `FeedRow`; the sum-merge that folds corrections onto a transaction |
| `observation` | what a script output HELD, decoded or not — the tier that lets a decoder added *later* re-derive history from the archive instead of from chunks |
| `price` | as far as ONE policy's archive can honestly go, plus a name for everything it cannot |
| `supply` | the archive reconciled against **itself** |
| `trade` | movements → fills / placements / cancellations |
| `profile` | what a policy's units ARE, and therefore what is worth recording |
| `graph` | party-to-party movement graph |
| `bundle` / `multi` | manifest + every footer as one blob for KV; reading across files |

## Traps

- ⚠️ **A reader merges EVERY file the manifest names**, summing per
  `(transaction, unit, party)`. Rollup and compaction are that same sum applied
  ahead of time. Reading one file is not reading the archive.
- ⚠️ **Corrections are a separate `FileKind`.** A later pass records the source
  it finally resolved there. Anything that reconciles over movements alone
  reports every one of those as still below the floor.
- ⚠️ **`net_mint` is assigned, not added.** Every row of a mint carries the
  transaction's figure, so summing them multiplies the mint by its output
  count. Amounts *are* summed — that is how corrections apply. Getting either
  backwards yields a number that looks like a reconciliation and is not one.
- ⚠️ **`Manifest` deserialises with serde ignoring unknown fields.** An older
  binary reads a newer manifest happily and then silently drops the new field
  on its next write.
- 🔑 **`supply` grades rather than asserts, and the SIGN is the
  discriminator.** A positive gap is normal on a partial archive (a source not
  yet descended to) and damning on a complete one. A negative gap — supply
  minted that reached nobody — fails at any completeness, because a mint's
  recipients are outputs of the minting transaction itself.
- 🔑 **`trade` takes `Roles` as a parameter** so this crate stays free of any
  decode stack. And roles must be built from the decode crates, not
  `address-registry`, because only the former distinguishes a pool from an
  order contract.
