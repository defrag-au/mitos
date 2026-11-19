# Pinned registry artefacts

`behaviour.bin` and `stake-services.bin` are copies of `resolved/` from
[`theyexist/address-registry`](https://github.com/theyexist/address-registry) at

    rev 45ff9557d0f98ed17fddcf7a8f6edcbfccf2b995

They are here for consumers that cannot fetch: the `holder-distribution` wasm
module, and tests that must run offline. A native tool fetches the same files
from the rev-pinned URL instead. The source of truth is the registry repository;
this is a pinned view of it, and the `pinned` feature is what puts it in a build.

## The runtime cache

A native tool fetches those two files at start and writes what it read to a
cache, so a run whose fetch fails classifies with the last verified copy rather
than not at all.

- Location: `ADDRESS_REGISTRY_CACHE` (a root; the rev is appended as a
  subdirectory), else `$XDG_CACHE_HOME/address-registry`, else
  `$HOME/.cache/address-registry`. A systemd service has no `HOME`, so the
  token-ledger units set it in the unit file.
- The rev is a directory level on purpose: a cache written for another revision
  is never read. Inside it, `SHA256SUMS` records both digests and is written
  last, so a copy that does not match its record — truncated, edited, or caught
  mid-write — is ignored rather than served.
- A cached read is reported at WARN by every native consumer: the contracts are
  the pinned revision's, but anything added to the registry since the cache was
  written reads as unnamed.
- `ADDRESS_REGISTRY_DIR` wins over both. It is the explicit "use exactly these
  bytes, do not fetch" escape, for a checkout or an air-gapped run.

| file | sha256 |
| --- | --- |
| `behaviour.bin` | `32daa9bcd9773a0730b749b698401852bca310f3abd81d53e50867eccb03c55a` |
| `stake-services.bin` | `70d741d98bc603f7c223b16df96a21eefc5ea6586fa06518f79f469f8725229c` |

## Refreshing

From a checkout of `address-registry` at the new rev:

```sh
cp resolved/behaviour.bin resolved/stake-services.bin \
   ../../crates/mitos-registry/data/
```

Then update the rev and both digests above, and the rev in the workspace
dependency. `registry_drift` fails while the copy disagrees with the contracts
mitos names, so a stale pin does not pass silently.
