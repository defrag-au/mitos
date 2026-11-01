#!/usr/bin/env bash
# Mekka S2 — SEEDED re-run, 2026-08-30.
#
# Successor to s2-pipeline.sh (cold start, 2026-08-23). Two changes, and only
# two, so the delta is attributable:
#   1. registry-s2-seeded.toml declares the project boundary (treasury +
#      $mekkalabs + $mekkaops project-side; $jprigs33 and the Yepple/BankFi
#      rails terminal) instead of bootstrapping from the chain alone.
#   2. `score` + `provenance` now run — the cold-start pipeline stopped at
#      classify, so the team-share figure was never refreshed by it.
#
# --max-hops is passed EXPLICITLY as 2. The flag's default changed to 1, and
# the Octaverse run established that at hop 1 the off-ramp is invisible.
#
# Numeric terminal thresholds stay at default: changing seeding and thresholds
# in the same run would confound the two.
set -euo pipefail

cd /opt/project-ledger
BIN=/opt/mitos/src/target/release/project-ledger
SNAP=/opt/market-ledger/snapshot-full/db
DB=mekka-s2.db
REG=registry-s2-seeded.toml
HOPS=2

step() { echo; echo "=== [$(date -u +%FT%TZ)] $* ==="; }

step "snapshot coverage (what this run can see)"
ls "$SNAP/immutable" | grep -E '^[0-9]+\.chunk$' | sort | tail -1

# Pass 1 records which inputs it wants but cannot resolve; resolve-local then
# fills them from the snapshot; pass 2 re-walks with a warm cache so every leg
# books correctly. reset keeps outref_cache + discovered_holder, so the
# resolution layer and the holder set survive between passes.
step "reset (keeps outref_cache + discovered_holder)"
$BIN reset --db $DB --yes

step "seed pass 1 (declared boundary)"
$BIN seed --registry $REG --db $DB

step "walk pass 1 (records wanted refs)"
$BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS

step "resolve-local (genesis scan)"
$BIN resolve-local --db $DB --data-dir $SNAP

step "reset + seed + walk pass 2 (books correctly)"
$BIN reset --db $DB --yes
$BIN seed --registry $REG --db $DB
$BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS

step "enrich (venue sales from market-ledger)"
$BIN enrich --db $DB --market /opt/market-ledger/market-ledger.db

# --infer-service is REQUIRED for the sweep-target rule to run at all; without
# it relay_hop rows are collected and interpreted by nothing.
step "classify (with inference — needed for sweep targets)"
$BIN classify --db $DB --infer-service

step "score (investigative interest)"
$BIN score --db $DB --report 20

# Two provenance passes, reported separately and never merged. The first is
# anchored on the REGISTRY seeds (asserted roots — the wallets declared above).
# The second additionally roots coreness on the derived dominant mint-proceeds
# destination. There is no annotations sidecar for this ledger, so neither pass
# has human assertions behind it; the tool says so itself.
step "provenance — registry-anchored roots"
$BIN provenance --db $DB --report 25

step "provenance — WITH derived roots (comparison, reported separately)"
$BIN provenance --db $DB --report 25 --derived-roots

step "relay_hop summary"
sqlite3 $DB "SELECT COUNT(*)||' hops, '||COUNT(DISTINCT relay_addr)||' relays, '||COUNT(DISTINCT to_addr)||' targets, '||ROUND(SUM(quantity)/1e6,0)||' ADA' FROM relay_hop;" || true

step "THE QUESTION: where did the S2 mint treasury's bare-address money go?"
sqlite3 -header -column $DB "
SELECT substr(h.relay_addr,1,26) AS relay, ROUND(h.quantity/1e6,0) AS ada,
       substr(h.to_addr,1,34) AS landed_at, h.out_slot-h.in_slot AS held_s
FROM relay_hop h
WHERE h.from_party LIKE 'stake1u9xsvje8ewnzpts3tdj6mm89%'
ORDER BY h.quantity DESC;" || true

step "sweep targets overall"
sqlite3 -header -column $DB "
SELECT substr(to_addr,1,34) AS target, COUNT(DISTINCT relay_addr) AS relays,
       COUNT(DISTINCT from_party) AS payers, ROUND(SUM(quantity)/1e6,0) AS ada
FROM relay_hop WHERE unit='lovelace'
GROUP BY to_addr HAVING COUNT(DISTINCT relay_addr) >= 2
ORDER BY SUM(quantity) DESC LIMIT 12;" || true

step "S2 SEEDED PIPELINE DONE"
