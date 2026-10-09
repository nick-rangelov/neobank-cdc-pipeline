#!/usr/bin/env python3
"""
Reference implementation of the correctness rule for the CDC events (standard library only).

Correct rule:
  1. start from the snapshot
  2. drop events with seq <= snapshot_seq (already contained in the snapshot)
  3. drop duplicate deliveries (same seq)
  4. apply the remaining events in seq order (never in arrival order)

The result must equal expected/customers_expected.csv.

Usage:
    python3 reference_apply.py --data data           # correct rule, should PASS
    python3 reference_apply.py --data data --naive   # arrival order, no dedupe: should FAIL
"""
import argparse
import csv
import glob
import json
import os
import sys

COLS = ["customer_id", "first_name", "last_name", "email", "country", "segment", "marketing_opt_in"]


def norm(row):
    return {c: (row.get(c) or "").strip() for c in COLS}


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_events(data_dir):
    """All change rows in arrival order: partition by partition, file by file, row by row."""
    events = []
    for path in sorted(glob.glob(os.path.join(data_dir, "events", "dt=*", "*.csv"))):
        events += read_csv(path)
    return events


def apply_event(state, event):
    cid = int(event["customer_id"])
    if event["op"] == "D":
        state.pop(cid, None)
    else:
        state[cid] = norm(event)


def apply_correct(snapshot, snapshot_seq, events):
    state = {int(r["customer_id"]): norm(r) for r in snapshot}
    seen, fresh = set(), []
    for e in events:
        seq = int(e["seq"])
        if seq in seen or seq <= snapshot_seq:
            continue
        seen.add(seq)
        fresh.append(e)
    for e in sorted(fresh, key=lambda e: int(e["seq"])):
        apply_event(state, e)
    return state


def apply_naive(snapshot, events):
    state = {int(r["customer_id"]): norm(r) for r in snapshot}
    for e in events:                       # arrival order, no dedupe, no overlap handling
        apply_event(state, e)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", default="data", help="folder made by generate_events.py")
    parser.add_argument("--naive", action="store_true", help="apply events in arrival order")
    args = parser.parse_args()

    with open(os.path.join(args.data, "snapshot", "snapshot_manifest.json")) as f:
        snapshot_seq = json.load(f)["snapshot_seq"]
    snapshot = read_csv(os.path.join(args.data, "snapshot", "customers_snapshot.csv"))
    events = load_events(args.data)
    expected = {int(r["customer_id"]): norm(r)
                for r in read_csv(os.path.join(args.data, "expected", "customers_expected.csv"))}

    state = apply_naive(snapshot, events) if args.naive else apply_correct(snapshot, snapshot_seq, events)

    wrong = [cid for cid in sorted(set(state) | set(expected)) if state.get(cid) != expected.get(cid)]
    mode = "naive (arrival order)" if args.naive else "correct (dedupe + seq order)"
    print(f"mode: {mode}")
    print(f"snapshot rows: {len(snapshot)} | event rows read: {len(events)}")
    print(f"result rows: {len(state)} | expected rows: {len(expected)} | mismatching customers: {len(wrong)}")
    if wrong:
        print(f"FAIL (first few ids: {wrong[:10]})")
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
