# Module versioning — SUPERSEDED

This was the working document for the 2026-09-19 architecture review. It
has been consolidated, with every open question closed, into:

**`docs/strategy/MITOS_PLATFORM_V3.md`**

Two positions recorded here during the review were later REVERSED and
must not be revived from memory of this file:

- "Versions coexist for cutover only" → replaced by **instance per
  digest** (any two clients may diverge; cutover is the one-client case).
  Forced by the discovery that `jpg-store-*` has two prod consumers.
- "Split the deployments" (a third `mitos-mainnet-dev` instance) →
  measured and rejected in favour of namespaces inside two hosts.
