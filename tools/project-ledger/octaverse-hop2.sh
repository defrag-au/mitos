#!/usr/bin/env bash
set -euo pipefail
cd /opt/project-ledger
BIN=/opt/mitos/src/target/release/project-ledger
SNAP=/opt/market-ledger/snapshot-full/db
DB=octaverse.db
REG=registry-octaverse.toml

# Re-walk at --max-hops 2.
#
# WHY: the frontier seeds from the CIP-27 royalty address, so the project
# treasury sits at hop 1 and every wallet the treasury PAYS sits at hop 2 —
# outside the default bound. Those payees are exactly the subject of the
# investigation, and because they were never watched, the relay detector (which
# only arms on payments FROM a watched party) never looked at them. A 6,000 ADA
# off-ramp through three single-use relays into the Mekka swap desk went
# unflagged as a result.
#
# Widening the bound grows the watch set, which makes the walk TOUCH more txs,
# which makes it WANT input refs the earlier genesis scans were never told to
# fetch. So resolve+re-walk is iterated until the residual is zero rather than
# run a fixed number of times — a walk that cannot resolve its inputs books
# change as income, and 6.7% residual was enough to matter last round.
HOPS=2

echo "=== seed + walk (pass 1 at hop $HOPS) ==="
$BIN reset --db $DB --yes
$BIN seed --registry $REG --db $DB
$BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS

for i in 2 3 4 5; do
  RESIDUAL=$(sqlite3 $DB "SELECT COALESCE(SUM(unresolved_inputs),0) FROM tx_delta;")
  echo "=== residual unresolved refs before pass $i: $RESIDUAL ==="
  if [ "$RESIDUAL" = "0" ]; then echo "closed — no further pass needed"; break; fi
  echo "=== resolve-local + re-walk (pass $i) ==="
  $BIN resolve-local --db $DB --data-dir $SNAP
  $BIN reset --db $DB --yes
  $BIN seed --registry $REG --db $DB
  $BIN walk --db $DB --data-dir $SNAP --remote offline --max-hops $HOPS
done

echo "=== enrich ==="
$BIN enrich --db $DB --market /opt/market-ledger/market-ledger.db
echo "=== classify (--infer-service) ==="
$BIN classify --db $DB --infer-service
echo "=== stats ==="
$BIN stats --db $DB

echo "=== residual unresolved (must be 0) ==="
sqlite3 -header -column $DB "SELECT COUNT(*) txs, COALESCE(SUM(unresolved_inputs),0) refs FROM tx_delta WHERE unresolved_inputs > 0;"

echo "=== RELAY HOPS — the thing hop 1 could not see ==="
sqlite3 -header -column $DB "
SELECT substr(relay_addr,1,22) AS relay, substr(from_party,1,22) AS payer,
       substr(to_addr,1,46) AS landed_at, ROUND(quantity/1e6,1) AS ada,
       out_slot-in_slot AS dwell_s
FROM relay_hop WHERE unit='lovelace' ORDER BY quantity DESC LIMIT 25;"

echo "=== sweep targets (>=2 independent relays) ==="
sqlite3 -header -column $DB "
SELECT substr(to_addr,1,50) AS target, COUNT(DISTINCT relay_addr) AS relays,
       COUNT(DISTINCT from_party) AS payers, ROUND(SUM(quantity)/1e6,1) AS ada
FROM relay_hop WHERE unit='lovelace'
GROUP BY to_addr HAVING COUNT(DISTINCT relay_addr) >= 2
ORDER BY SUM(quantity) DESC;"

echo "=== treasury outflow, now with watched destinations ==="
sqlite3 -header -column $DB "
SELECT datetime(v.block_time,'unixepoch') t, ROUND(-v.delta/1e6,2) ada_out,
       substr(v.counterparty,1,24) AS dest, p.role, COALESCE(a.value,'') AS handle
FROM value_event v
LEFT JOIN party p ON p.key = v.counterparty
LEFT JOIN party_alias a ON a.party = v.counterparty AND a.kind='handle'
WHERE v.party='stake1uyk6rcqyvkxejzhcy5dghj9a5jf4egk38ak87ng9esl8jacu6hv03'
  AND v.delta < 0 ORDER BY v.slot;"

echo "=== party count + frontier size (watch for explosion) ==="
sqlite3 -header -column $DB "SELECT role, COUNT(*) n, SUM(expand) expanding FROM party GROUP BY role ORDER BY n DESC;"
echo "=== OCTAVERSE HOP2 DONE ==="
