#!/usr/bin/env python3
"""
Synthetic CDC event generator for a `customers` table (standard library only).

It simulates what a change-data-capture (CDC) tool delivers: an initial snapshot plus
daily files of change events (insert / update / delete), with deliberately messy delivery:
duplicates, out-of-order rows, late files, a snapshot/stream overlap and a schema change.

All data is randomly generated. The same seed always produces identical output.

Usage:
    python3 generate_events.py --out data --customers 500 --days 10 --seed 42
"""
import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta

FIRST_NAMES = ["Anna", "Boris", "Clara", "Daan", "Elena", "Felix", "Greta", "Hugo", "Iva", "Jan",
               "Katya", "Lars", "Maya", "Nikola", "Olga", "Pieter", "Quinn", "Rosa", "Stefan", "Tess"]
LAST_NAMES = ["Petrov", "de Vries", "Muller", "Rossi", "Novak", "Janssen", "Dimitrov", "Bakker",
              "Garcia", "Schmidt", "Ivanova", "Visser", "Martin", "Kowalski", "Silva"]
COUNTRIES = ["Netherlands", "Germany", "France", "Belgium", "Italy", "Spain", "Austria"]
SEGMENTS = ["retail", "premium", "business"]

BASE_COLS = ["customer_id", "first_name", "last_name", "email", "country", "segment"]
OPTIN_COL = "marketing_opt_in"          # added later: this is the "schema change"
EVENT_PREFIX = ["seq", "op", "commit_ts"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


class Source:
    """A tiny pretend production table. Every change gets the next sequence number (seq)."""

    def __init__(self, rng, schema_change_date):
        self.rng = rng
        self.schema_change_date = schema_change_date
        self.state = {}        # customer_id -> current row (the ground truth)
        self.seq = 0
        self.next_id = 1
        self.events = []       # the change log, in true order

    def _optin_enabled(self, ts):
        return ts.date() >= self.schema_change_date

    def _emit(self, op, row, ts):
        self.seq += 1
        event = {"seq": self.seq, "op": op, "commit_ts": ts.strftime(TS_FORMAT)}
        if op == "D":
            event["customer_id"] = row["customer_id"]   # deletes carry the key only
        else:
            event.update(row)                           # inserts/updates carry the full new row
        self.events.append(event)

    def insert(self, ts):
        rng = self.rng
        cid = self.next_id
        self.next_id += 1
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        row = {
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": f"{first}.{last}{cid}@example.com".lower().replace(" ", ""),
            "country": rng.choice(COUNTRIES),
            "segment": rng.choices(SEGMENTS, weights=[70, 20, 10])[0],
            OPTIN_COL: rng.choice(["true", "false"]) if self._optin_enabled(ts) else "",
        }
        self.state[cid] = row
        self._emit("I", row, ts)

    def update(self, ts):
        rng = self.rng
        cid = rng.choice(sorted(self.state))
        row = dict(self.state[cid])
        field = rng.choice(["country", "segment", "email"])
        if field == "country":
            row["country"] = rng.choice([c for c in COUNTRIES if c != row["country"]])
        elif field == "segment":
            row["segment"] = rng.choice([s for s in SEGMENTS if s != row["segment"]])
        else:
            row["email"] = (f"{row['first_name']}.{row['last_name']}{cid}.{rng.randint(1, 99)}"
                            "@example.com").lower().replace(" ", "")
        if self._optin_enabled(ts) and row[OPTIN_COL] == "":
            row[OPTIN_COL] = rng.choice(["true", "false"])
        self.state[cid] = row
        self._emit("U", row, ts)

    def delete(self, ts):
        cid = self.rng.choice(sorted(self.state))
        del self.state[cid]
        self._emit("D", {"customer_id": cid}, ts)


def write_csv(path, columns, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore",
                                restval="", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def generate(out_dir, n_customers, days, seed, overlap):
    rng = random.Random(seed)
    stream_start = date(2026, 2, 1)
    schema_change_date = stream_start + timedelta(days=days * 6 // 10)
    src = Source(rng, schema_change_date)

    # ---- Phase A: history that exists before the snapshot is taken ----
    ts = datetime(2026, 1, 1, 8, 0, 0)
    for _ in range(n_customers):
        ts += timedelta(seconds=rng.randint(1, 30))
        src.insert(ts)
    for _ in range(max(1, n_customers // 12)):
        ts += timedelta(seconds=rng.randint(1, 600))
        src.update(ts)
    snapshot_seq = src.seq
    snapshot_ts = ts + timedelta(minutes=1)
    snapshot_rows = [dict(src.state[c]) for c in sorted(src.state)]
    overlap_events = src.events[-overlap:] if overlap else []
    history_count = len(src.events)

    # ---- Phase B: live changes after the snapshot ----
    for d in range(days):
        day = stream_start + timedelta(days=d)
        n_events = rng.randint(35, 60)
        for s in sorted(rng.sample(range(86400), n_events)):
            ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=s)
            op = rng.choices(["I", "U", "D"], weights=[25, 70, 5])[0]
            if op in ("U", "D") and len(src.state) < 10:
                op = "I"
            {"I": src.insert, "U": src.update, "D": src.delete}[op](ts)
    live_events = src.events[history_count:]

    # ---- Delivery: decide which file each event arrives in (and make it messy) ----
    deliveries = [(stream_start, ev) for ev in overlap_events]   # replay of pre-snapshot events
    late = 0
    for ev in live_events:
        commit_date = datetime.strptime(ev["commit_ts"], TS_FORMAT).date()
        delivery = commit_date
        if rng.random() < 0.04:                                  # late delivery
            delivery = commit_date + timedelta(days=rng.randint(1, 2))
            late += 1
        deliveries.append((delivery, ev))
    duplicates = []
    for delivery, ev in deliveries:
        if rng.random() < 0.03:                                  # delivered twice
            duplicates.append((delivery + timedelta(days=rng.choice([0, 1])), ev))
    deliveries += duplicates

    by_day = {}
    for delivery, ev in deliveries:
        by_day.setdefault(delivery, []).append(ev)

    arrival_seqs = []
    for delivery in sorted(by_day):
        rows = by_day[delivery]
        rng.shuffle(rows)                                        # out-of-order within the day
        n_files = min(len(rows), rng.randint(1, 3))
        cuts = sorted(rng.sample(range(1, len(rows)), n_files - 1)) if n_files > 1 else []
        bounds = [0] + cuts + [len(rows)]
        columns = EVENT_PREFIX + BASE_COLS
        if delivery >= schema_change_date:
            columns = columns + [OPTIN_COL]                      # schema change: new column
        for i in range(n_files):
            chunk = rows[bounds[i]:bounds[i + 1]]
            path = os.path.join(out_dir, "events", f"dt={delivery.isoformat()}",
                                f"customers_changes_{i + 1:03d}.csv")
            write_csv(path, columns, chunk)
            arrival_seqs += [r["seq"] for r in chunk]

    max_seen, out_of_order = 0, 0
    for s in arrival_seqs:
        if s < max_seen:
            out_of_order += 1
        max_seen = max(max_seen, s)

    # ---- Snapshot, manifest and ground truth ----
    write_csv(os.path.join(out_dir, "snapshot", "customers_snapshot.csv"), BASE_COLS, snapshot_rows)
    with open(os.path.join(out_dir, "snapshot", "snapshot_manifest.json"), "w") as f:
        json.dump({"snapshot_seq": snapshot_seq,
                   "snapshot_ts": snapshot_ts.strftime(TS_FORMAT)}, f, indent=2)

    expected_rows = [src.state[c] for c in sorted(src.state)]
    write_csv(os.path.join(out_dir, "expected", "customers_expected.csv"),
              BASE_COLS + [OPTIN_COL], expected_rows)
    stats = {
        "seed": seed,
        "snapshot_seq": snapshot_seq,
        "stream_start": stream_start.isoformat(),
        "schema_change_date": schema_change_date.isoformat(),
        "overlap_events_replayed": len(overlap_events),
        "live_events": len(live_events),
        "live_inserts": sum(e["op"] == "I" for e in live_events),
        "live_updates": sum(e["op"] == "U" for e in live_events),
        "live_deletes": sum(e["op"] == "D" for e in live_events),
        "duplicate_deliveries": len(duplicates),
        "late_events": late,
        "rows_arriving_after_a_higher_seq": out_of_order,
        "final_customer_count": len(src.state),
    }
    with open(os.path.join(out_dir, "expected", "injected_cases.json"), "w") as f:
        json.dump(stats, f, indent=2)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data", help="output folder (default: data)")
    parser.add_argument("--customers", type=int, default=500, help="customers in the snapshot")
    parser.add_argument("--days", type=int, default=10, help="days of change files")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--overlap", type=int, default=30,
                        help="pre-snapshot events replayed in the first change file")
    args = parser.parse_args()
    stats = generate(args.out, args.customers, args.days, args.seed, args.overlap)
    print(f"Wrote snapshot + change files to {args.out}")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
