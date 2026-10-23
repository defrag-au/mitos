#!/usr/bin/env bash
# Mekka S1 (Dynasty) — SEEDED run, 2026-08-30. Mirrors s2-seeded-pipeline.sh.
#
# S1 is ~9x the walk S2 is: floor 160,419,671 against a tip near 196.4M, so
# ~36M slots per pass rather than ~3.9M. Budget 1.5–2 hours, most of it in the
# two walks and the genesis scan.
#
# The question this run exists to answer: how much S1 supply was effectively
# team-funded, ROYALTIES INCLUDED. `provenance` already treats a `royalty` role
# as a coreness root, so royalty money funding a mint is caught without anyone
# asserting that it did.
set -euo pipefail

cd /opt/project-ledger
BIN=/opt/mitos/src/target/release/project-ledger
SNAP=/opt/market-ledger/snapshot-full/db
DB=mekka.db
REG=registry-s1-seeded.toml
HOPS=2

step() { echo; echo "=== [$(date -u +%FT%TZ)] $* ==="; }

step "snapshot coverage"
ls "$SNAP/immutable" | grep -E '^[0-9]+\.chunk$' | sort | tail -1

step "reset (keeps outref_cache + discovered_holder)"
$BIN reset --db $DB --yes

step "seed pass 1 (declared boundary)"
$BIN seed --registry $REG --db $DB

step "walk pass 1 (records wanted refs) — the long one"
$BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS

step "resolve-local (genesis scan)"
$BIN resolve-local --db $DB --data-dir $SNAP

step "reset + seed + walk pass 2 (books correctly)"
$BIN reset --db $DB --yes
$BIN seed --registry $REG --db $DB
$BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS

step "enrich (venue sales from market-ledger)"
$BIN enrich --db $DB --market /opt/market-ledger/market-ledger.db

step "classify (with inference — needed for sweep targets)"
$BIN classify --db $DB --infer-service

step "score"
$BIN score --db $DB --report 20

step "provenance — royalty wallet is a coreness root by role"
$BIN provenance --db $DB --report 25

step "distributions + deep-dive export"
$BIN distributions --db $DB --export /opt/project-ledger/mekka-s1.deep-dive.json

step "royalty outflow — where did S1 royalties actually go?"
sqlite3 -header -column $DB "
SELECT COALESCE((SELECT value FROM party_alias a WHERE a.party=v.counterparty AND a.kind='handle' LIMIT 1),
                substr(v.counterparty,1,26)) AS paid_to,
       COUNT(*) legs, ROUND(-SUM(v.delta)/1e6,0) AS ada
FROM value_event v
WHERE v.party IN (SELECT key FROM party WHERE role='royalty') AND v.delta<0
GROUP BY v.counterparty ORDER BY SUM(v.delta) LIMIT 12;" || true

step "S1 SEEDED PIPELINE DONE"
