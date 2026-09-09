#!/usr/bin/env python3
"""Measure jpg.store's buy/delist redeemer convention SEPARATELY per contract version.

The question this answers: for a spend of a jpg listing at version V with
redeemer constructor C, were the listing datum's payouts actually paid?

A *buy* settles the datum: every payout target receives at least its stated
lovelace. A *delist* settles nothing — the asset and its min-UTxO go back to
the owner, so the payouts are unpaid (the owner may still receive change, but
far less than the ask).

Bins results by (contract version, constructor) so a version-dependent
convention shows up as two cells with opposite settlement rates. Reads tx
hashes (one per line) from stdin or a file and talks to Koios.
"""

import json
import subprocess
import sys
from collections import defaultdict

KOIOS = "https://api.koios.rest/api/v1/tx_info"

# From mitos-marketplace-decode::sales — the addresses `classify_jpg_address`
# recognises. V2 and V3 are the same validator in two bech32 forms.
JPG_ADDRS = {
    "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm": "V1",
    "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx": "V2",
    "addr1w8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7eg0fcr8k": "V3",
}

BATCH = 20


def fetch(hashes):
    payload = json.dumps({"_tx_hashes": hashes, "_inputs": True, "_scripts": True})
    out = subprocess.run(
        [
            "/usr/bin/curl", "-s", "-X", "POST", KOIOS,
            "-H", "content-type: application/json",
            "-d", payload,
        ],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        print(f"  ! koios returned non-JSON ({out[:120]!r})", file=sys.stderr)
        return []


def walk_bytes(node):
    """Yield every `bytes` leaf under a PlutusData node, in order."""
    if isinstance(node, dict):
        if "bytes" in node:
            yield node["bytes"]
        for key in ("fields", "list", "map"):
            for child in node.get(key, []) or []:
                yield from walk_bytes(child)
        for key in ("k", "v"):
            if key in node:
                yield from walk_bytes(node[key])
    elif isinstance(node, list):
        for child in node:
            yield from walk_bytes(child)


def max_int(node):
    """Largest integer under a node — the lovelace, whether bare or Value-mapped.

    V2/V3 payouts carry a bare `int`; V1 payouts carry a
    `{policy: {asset: lovelace}}` Value map. Both reduce to "the big number".
    """
    best = 0
    if isinstance(node, dict):
        if "int" in node:
            best = max(best, int(node["int"]))
        for key in ("fields", "list", "map"):
            for child in node.get(key, []) or []:
                best = max(best, max_int(child))
        for key in ("k", "v"):
            if key in node:
                best = max(best, max_int(node[key]))
    elif isinstance(node, list):
        for child in node:
            best = max(best, max_int(child))
    return best


def parse_listing(datum):
    """(owner_cred, [(payout_cred, lovelace)]) from a jpg listing datum.

    Field order differs by version (V1 is owner-first, V2/V3 payouts-first), so
    locate each part by SHAPE — the list is the payouts, the bare bytes is the
    owner. Mirrors `decode_listing_datum`'s field-order-agnostic approach.
    """
    fields = datum.get("fields") or []
    payout_list, owner = None, None
    for f in fields:
        if isinstance(f, dict) and "list" in f:
            payout_list = f["list"]
        elif isinstance(f, dict) and "bytes" in f:
            owner = f["bytes"]
    if payout_list is None:
        return owner, []

    payouts = []
    for entry in payout_list:
        creds = list(walk_bytes(entry))
        if not creds:
            continue
        # First bytes leaf = the payout address's PAYMENT credential; the
        # following one (when present) is its stake part.
        payouts.append((creds[0], max_int(entry)))
    return owner, payouts


def settled(payouts, outputs):
    """Did every payout target receive at least its stated lovelace?"""
    if not payouts:
        return None
    received = defaultdict(int)
    for o in outputs:
        cred = (o.get("payment_addr") or {}).get("cred")
        if cred:
            received[cred] += int(o["value"])
    return all(received.get(cred, 0) >= amount for cred, amount in payouts)


def main():
    src = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin
    hashes = [line.strip() for line in src if line.strip()]

    # (version, constructor) -> [settled bools]
    cells = defaultdict(list)
    examples = {}

    for i in range(0, len(hashes), BATCH):
        batch = hashes[i:i + BATCH]
        print(f"  … {i + len(batch)}/{len(hashes)}", file=sys.stderr)
        for tx in fetch(batch):
            for pc in tx.get("plutus_contracts") or []:
                version = JPG_ADDRS.get(pc.get("address"))
                if version is None:
                    continue
                inp = pc.get("input") or {}
                redeemer = (inp.get("redeemer") or {}).get("datum", {}).get("value")
                datum = (inp.get("datum") or {}).get("value")
                if not isinstance(redeemer, dict) or not isinstance(datum, dict):
                    continue
                constr = redeemer.get("constructor")
                _owner, payouts = parse_listing(datum)
                verdict = settled(payouts, tx.get("outputs") or [])
                if verdict is None:
                    continue
                key = (version, constr)
                cells[key].append(verdict)
                examples.setdefault((key, verdict), tx["tx_hash"])

    print()
    print(f"{'version':<8} {'constr':>6} {'spends':>7} {'payouts settled':>16}  reading")
    print("-" * 74)
    for (version, constr) in sorted(cells, key=lambda k: (k[0], k[1] if k[1] is not None else -1)):
        rows = cells[(version, constr)]
        hits = sum(rows)
        pct = 100.0 * hits / len(rows)
        reading = "BUY" if pct >= 80 else "DELIST" if pct <= 20 else "MIXED — investigate"
        print(f"{version:<8} {constr:>6} {len(rows):>7} {hits:>6}/{len(rows):<5} {pct:5.1f}%  {reading}")

    print()
    for (key, verdict), tx in sorted(examples.items()):
        print(f"  example {key[0]} constr={key[1]} settled={verdict}: {tx}")


if __name__ == "__main__":
    main()
