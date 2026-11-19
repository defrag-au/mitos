# Pinned registry artefacts

`behaviour.bin` and `stake-services.bin` are copies of `resolved/` from
[`theyexist/address-registry`](https://github.com/theyexist/address-registry) at

    rev 45ff9557d0f98ed17fddcf7a8f6edcbfccf2b995

They are here for consumers that cannot fetch: the `holder-distribution` wasm
module, and tests that must run offline. A native tool fetches the same files
from the rev-pinned URL instead. The source of truth is the registry repository;
this is a pinned view of it, and the `pinned` feature is what puts it in a build.

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
