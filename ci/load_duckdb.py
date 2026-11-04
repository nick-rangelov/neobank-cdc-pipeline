#!/usr/bin/env python3
"""
CI stand-in for load_raw.py: loads the generated CDC files into a local DuckDB file, so the dbt
project can be built and tested on every pull request without any cloud credentials.

It creates the same raw tables as redshift/cdc_raw_tables.sql (schema raw_cdc) and fills the
lineage columns (_source_file, _loaded_at) and the load log, like the real loader does.
Like the real loader, it reads each file's header to decide which columns to load.

Usage:
    python ci/load_duckdb.py --data data --db ci/ci.duckdb
"""
import argparse
import csv
import glob
import os
from datetime import datetime, timedelta, timezone

import duckdb

SNAPSHOT_TABLE = "raw_cdc.customers_snapshot"
EVENTS_TABLE = "raw_cdc.customer_events"

DDL = [
    "create schema raw_cdc",
    """create table raw_cdc.customers_snapshot (
        customer_id integer, first_name varchar(100), last_name varchar(100),
        email varchar(255), country varchar(100), segment varchar(50),
        _source_file varchar(512), _loaded_at timestamp)""",
    """create table raw_cdc.customer_events (
        seq bigint, op char(1), commit_ts timestamp, customer_id integer,
        first_name varchar(100), last_name varchar(100), email varchar(255),
        country varchar(100), segment varchar(50), marketing_opt_in varchar(10),
        _source_file varchar(512), _loaded_at timestamp)""",
    """create table raw_cdc.load_log (
        source_file varchar(512), row_count integer, loaded_at timestamp)""",
]


def load_file(con, path, key, table, loaded_at):
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames
        rows = [[row[c] if row[c] != "" else None for c in columns] for row in reader]
    placeholders = ", ".join("?" for _ in range(len(columns) + 2))
    sql = f"insert into {table} ({', '.join(columns)}, _source_file, _loaded_at) values ({placeholders})"
    for row in rows:
        con.execute(sql, row + [key, loaded_at])
    con.execute("insert into raw_cdc.load_log values (?, ?, ?)", [key, len(rows), loaded_at])
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", default="data", help="folder made by generate_events.py")
    parser.add_argument("--db", default="ci/ci.duckdb", help="DuckDB file to create")
    args = parser.parse_args()

    if os.path.exists(args.db):
        os.remove(args.db)
    con = duckdb.connect(args.db)
    for statement in DDL:
        con.execute(statement)

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    files = [(os.path.join(args.data, "snapshot", "customers_snapshot.csv"), SNAPSHOT_TABLE)]
    files += [(path, EVENTS_TABLE) for path in sorted(glob.glob(os.path.join(args.data, "events", "dt=*", "*.csv")))]

    total = 0
    for i, (path, table) in enumerate(files):
        key = "cdc/" + os.path.relpath(path, args.data).replace(os.sep, "/")
        total += load_file(con, path, key, table, started + timedelta(seconds=i))
    con.close()
    print(f"Loaded {len(files)} files, {total} rows, into {args.db}")


if __name__ == "__main__":
    main()
