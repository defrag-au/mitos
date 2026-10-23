#!/usr/bin/env bash
set -euo pipefail
cd /opt/project-ledger
BIN=/opt/mitos/src/target/release/project-ledger
SNAP=/opt/market-ledger/snapshot-full/db
DB=octaverse.db
REG=registry-octaverse.toml

# Pass 2 watched 57 parties where pass 1 watched 1, so it TOUCHED more txs and
# asked for 3,461 refs the first genesis scan was never told to want. 142,958
# ADA of net movement sits on those rows, and until they resolve the change
# rule cannot tell an incoming payment from the wallet's own change on any of
# them. `outref_cache` is append-only, so this scan only adds.
echo "=== resolve-local (round 2: the refs pass 2 newly wanted) ==="
$BIN resolve-local --db $DB --data-dir $SNAP
echo "=== reset + seed + walk (pass 3) ==="
$BIN reset --db $DB --yes
$BIN seed --registry $REG --db $DB
$BIN walk --db $DB --data-dir $SNAP --remote offline
echo "=== enrich ==="
$BIN enrich --db $DB --market /opt/market-ledger/market-ledger.db
echo "=== classify ==="
$BIN classify --db $DB --infer-service
echo "=== stats ==="
$BIN stats --db $DB
echo "=== residual unresolved ==="
sqlite3 -header -column $DB "SELECT COUNT(*) txs, SUM(unresolved_inputs) refs, ROUND(SUM(ABS(delta))/1e6,0) ada FROM tx_delta WHERE unresolved_inputs > 0;"
echo "=== mint fund split ==="
sqlite3 -header -column $DB "SELECT destination, COUNT(*) legs, ROUND(SUM(lovelace)/1e6,2) ada, MIN(date(block_time,'unixepoch')) first, MAX(date(block_time,'unixepoch')) last FROM mint_payment GROUP BY destination ORDER BY SUM(lovelace) DESC;"
echo "=== OCTAVERSE PASS 3 DONE ==="
