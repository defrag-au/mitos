# policy-index

`(policy, asset)` → the mint that created it, and **its CIP-25 metadata span**,
over a Mithril immutable DB.

The companion `tx-index` could never be: that index is keyed by the first 8
bytes of a *transaction hash* and carries no policy dimension at all. "Where is
transaction H" and "which transaction first minted policy P" are different
questions, and only the first was indexed.

Design and every measurement below:
`cnft.dev-workers/docs/design/POLICY_INDEX.md`.

## 🔑 The shared pass IS the design

This crate owns **no I/O**. It never opens a chunk, never splits CBOR, never
decides what a block is. It is pure over blocks that `tx-index`'s extractor
already decoded, reached through
`tx_index::extract::extract_chunk_observed`'s callback.

That is not tidiness. MEASURED 2026-09-09: harvesting mints alongside costs
**+1.8%**, because the block decode is already paid — and the cost does not
grow with mint density (the NFT-boom sample carries 15× the mints per byte for
the same 1.3%). **A second independent pass over the same chunks would cost
+100%.** Give this crate its own walk and the entire economic argument
evaporates.

Neither crate depends on the other: the seam is a callback over primitives, and
the caller (`tools/tx-index`) wires them together.

## Built, whole chain

| | |
|---|---|
| mint records | **19,021,375** |
| distinct policies | **231,515** |
| base file | **666 MiB** |
| extract / compact / structural verify | 1,125 s / 8.1 s / 0.2 s |
| floor probe | **~3 µs** |

## Layout

```
<index-dir>/
  segments/NNNNN.seg      ← tx-index's, untouched
  base.idx                ← tx-index's, untouched
  policy/
    segments/NNNNN.pseg   one per chunk, append-only, tmp+rename
    base.pidx             directory + records + time permutation + policy table
```

Record = **32 bytes**: `policy_prefix u64`, `name_prefix u64`, `chunk u16`,
`offset u32`, `len u16`, `aux_offset u32`, `aux_len u16`, `flags u8`, `rsv u8`.

⚠️ **Not 24 like the tx-index entry, and it cannot be.** A tx entry's key is
one hash; a mint record must identify a policy *and* an asset and still carry a
location and an aux span. Dropping the policy makes records unmergeable across
chunks (a segment holds many policies); dropping the aux span throws away the
metadata lookup that is half the point.

## Two orderings, one copy of the records

- **asset order** — `(policy, name, chunk)`, the array itself
- **time order** — a `u32` **permutation** over the same records, 4 bytes each
  rather than 32

So the two views cannot disagree about what a record says, because there is
only one record.

## Traps

- 🔑 **`name_prefix` is hashed; `policy_prefix` is not.** A policy id is
  already blake2b-224, so its own bytes bucket evenly. Asset names are
  human-chosen and differ in their *last* bytes — `SolJourney1-0001`,
  `-0002` — so raw prefixes would put a whole collection in one bucket. A test
  asserts the unhashed prefix really is degenerate.
- 🔑 **`DIR_BITS = 21`, not the tx-index's 24.** At 19M records, 2²⁴ buckets
  would hold ~1 entry each and a 64 MB directory would outweigh a sixth of the
  data it indexes. The width follows the corpus, not the sibling.
- ⚠️ **Every answer is a CANDIDATE.** 8-byte prefixes collide. Confirm against
  the transaction the record points at, exactly as `tx-index` re-hashes a
  located body. This crate cannot do that itself — no chunk access, by design.
  Collisions degrade toward *more work*, never toward *wrong and silent*.
- ⚠️ **MINTS ONLY.** Never an occurrence index. "Every transaction that touched
  policy P" is bounded by every transfer ever ($SNEK alone is 3.3M) and already
  exists as the policy archive. A mint index makes that archive's job smaller;
  an occurrence index would duplicate its reason to exist.
- ⚠️ **A mint in a phase-2 failed transaction never happened**, and its body
  still decodes perfectly. Skipped at the only place a record is created, and
  **counted** — "nothing was invalid" and "we never looked" must not read the
  same. An index that baked this in would be worse than the walker bug it came
  from: every consumer inherits a shared index's mistakes with no way to see
  them.

## Verification

`Base::verify_structure()` — ordering, fences, the permutation really being a
permutation confined to its policy's run, and each run's `first_chunk` against
its own records. 0.2 s over 19M.

`verify::verify_sample()` — go back to the chunks, decode the body's mint field
**independently of the extractor**, confirm the prefixes match. 3,001 sampled,
3,001 confirmed. Sampled by **stride**, not first-N and not random: a stride
hits every era and bucket, which is where a systematic fault lives.

⚠️ Its first run reported 342/3,000 faulty and **the fault was in the checker** —
`minicbor::Decoder::map()` returns `Ok(None)` for an indefinite-length map,
which the draft read as "no map". A verifier is code too.
