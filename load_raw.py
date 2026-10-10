#!/usr/bin/env python3
"""
Idempotent raw loader: S3 -> Redshift Serverless, using the Redshift Data API.

For every CSV file under <prefix>/snapshot/ and <prefix>/events/ in the bucket it:
  1. skips the file if it is already recorded in raw_cdc.load_log
  2. reads the file's header line and builds the COPY column list from it
     (so files with and without the later `marketing_opt_in` column both load)
  3. runs ONE transaction: COPY into a temp stage table, insert into the raw table
     with the source file name, and write the load_log entry.
     If any step fails, the whole transaction rolls back, so nothing is half-loaded
     and the file can simply be retried.

Run it as many times as you like: files already loaded are skipped.

Usage:
    export CDC_BUCKET=<your-bucket>
    python3 load_raw.py --dry-run --limit 2     # show what would run, change nothing
    python3 load_raw.py --limit 3               # load the first 3 new files
    python3 load_raw.py                         # load everything not loaded yet
"""
import argparse
import os
import sys
import time

SNAPSHOT_TARGET = "raw_cdc.customers_snapshot"
EVENTS_TARGET = "raw_cdc.customer_events"
LOAD_LOG = "raw_cdc.load_log"

SNAPSHOT_REQUIRED = ["customer_id", "first_name", "last_name", "email", "country", "segment"]
EVENTS_REQUIRED = ["seq", "op", "commit_ts"] + SNAPSHOT_REQUIRED
EVENTS_OPTIONAL = ["marketing_opt_in"]     # appears in files delivered from the schema change on


class SchemaError(Exception):
    """A file's header does not match any layout we know about."""


class LoadError(Exception):
    """Redshift reported a failure (or the statement timed out)."""


def sql_literal(value):
    """Quote a value as a SQL string literal (doubles any single quotes)."""
    return "'" + str(value).replace("'", "''") + "'"


def kind_of(key):
    if "/snapshot/" in key:
        return "snapshot"
    if "/events/" in key:
        return "events"
    raise ValueError(f"cannot tell what kind of file this is: {key}")


def list_csv_keys(s3, bucket, prefix):
    """CSV keys to consider: the snapshot first, then events in partition/file order."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for sub in ("snapshot", "events"):
        found = []
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{sub}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    found.append(obj["Key"])
        keys += sorted(found)
    return keys


def read_header(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-4095")["Body"].read()
    lines = body.decode("utf-8").splitlines()
    return [c.strip() for c in lines[0].split(",")] if lines else []


def validate_header(kind, header):
    required = SNAPSHOT_REQUIRED if kind == "snapshot" else EVENTS_REQUIRED
    optional = [] if kind == "snapshot" else EVENTS_OPTIONAL
    missing = [c for c in required if c not in header]
    unknown = [c for c in header if c not in required + optional]
    repeated = sorted({c for c in header if header.count(c) > 1})
    if missing or unknown or repeated:
        raise SchemaError(f"unexpected header {header} "
                          f"(missing={missing}, unknown={unknown}, repeated={repeated})")


def plan_statements(kind, key, header, bucket, bucket_region):
    """The SQL for one file. All of it runs as a single transaction."""
    target = SNAPSHOT_TARGET if kind == "snapshot" else EVENTS_TARGET
    cols = ", ".join(header)                  # safe: validated against a fixed allow-list
    file_literal = sql_literal(key)
    s3_literal = sql_literal(f"s3://{bucket}/{key}")
    return [
        "drop table if exists load_stage",
        f"create temp table load_stage as select {cols} from {target} where 1 = 0",
        f"copy load_stage ({cols}) from {s3_literal} iam_role default "
        f"region {sql_literal(bucket_region)} csv ignoreheader 1 emptyasnull "
        f"compupdate off statupdate off",
        f"insert into {target} ({cols}, _source_file) select {cols}, {file_literal} from load_stage",
        f"insert into {LOAD_LOG} (source_file, row_count) select {file_literal}, count(*) from load_stage",
    ]


def wait(rsd, statement_id, timeout=300, poll=1.0):
    start = time.time()
    while True:
        described = rsd.describe_statement(Id=statement_id)
        status = described["Status"]
        if status == "FINISHED":
            return described
        if status in ("FAILED", "ABORTED"):
            raise LoadError(described.get("Error", status))
        if time.time() - start > timeout:
            raise LoadError(f"timed out after {timeout}s (status {status})")
        time.sleep(poll)


def fetch_loaded_files(rsd, cfg):
    statement_id = rsd.execute_statement(
        WorkgroupName=cfg.workgroup, Database=cfg.database,
        Sql=f"select source_file from {LOAD_LOG}")["Id"]
    wait(rsd, statement_id)
    files, token = set(), None
    while True:
        kwargs = {"Id": statement_id}
        if token:
            kwargs["NextToken"] = token
        page = rsd.get_statement_result(**kwargs)
        for record in page["Records"]:
            files.add(record[0]["stringValue"])
        token = page.get("NextToken")
        if not token:
            return files


def load_all(s3, rsd, cfg, dry_run=False, limit=None, out=print):
    keys = list_csv_keys(s3, cfg.bucket, cfg.prefix)
    loaded = fetch_loaded_files(rsd, cfg)
    action = "planned" if dry_run else "loaded"
    summary = {"found": len(keys), "loaded": 0, "planned": 0, "skipped": 0, "failed": 0}
    for key in keys:
        if key in loaded:
            out(f"SKIP  {key}  (already in load_log)")
            summary["skipped"] += 1
            continue
        if limit is not None and summary[action] >= limit:
            out(f"STOP  limit of {limit} file(s) reached")
            break
        try:
            kind = kind_of(key)
            header = read_header(s3, cfg.bucket, key)
            validate_header(kind, header)
            statements = plan_statements(kind, key, header, cfg.bucket, cfg.bucket_region)
            if dry_run:
                out(f"PLAN  {key}  columns={len(header)}")
                for statement in statements:
                    out(f"        {statement}")
            else:
                statement_id = rsd.batch_execute_statement(
                    WorkgroupName=cfg.workgroup, Database=cfg.database, Sqls=statements)["Id"]
                wait(rsd, statement_id)
                out(f"LOAD  {key}  columns={len(header)}")
            summary[action] += 1
        except (SchemaError, LoadError) as error:
            out(f"FAIL  {key}  {error}")
            summary["failed"] += 1
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", default=os.environ.get("CDC_BUCKET"),
                        help="S3 bucket (or set CDC_BUCKET)")
    parser.add_argument("--prefix", default="cdc", help="folder in the bucket (default: cdc)")
    parser.add_argument("--bucket-region", default="eu-central-1")
    parser.add_argument("--workgroup", default=os.environ.get("CDC_WORKGROUP", "neobank-wg"))
    parser.add_argument("--database", default="dev")
    parser.add_argument("--redshift-region", default="us-east-1")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    parser.add_argument("--limit", type=int, default=None, help="load at most N new files")
    args = parser.parse_args(argv)
    if not args.bucket:
        parser.error("--bucket (or the CDC_BUCKET environment variable) is required")

    import boto3                               # imported here so the tests need no AWS libraries
    s3 = boto3.client("s3", region_name=args.bucket_region)
    rsd = boto3.client("redshift-data", region_name=args.redshift_region)
    summary = load_all(s3, rsd, args, dry_run=args.dry_run, limit=args.limit)
    print("summary:", summary)
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
