"""Tests for load_raw.py using fake S3 and Redshift Data API clients (no AWS needed).

Run:  python3 -m unittest discover -s tests -v
"""
import os
import re
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import load_raw  # noqa: E402

HEADER_9 = "seq,op,commit_ts,customer_id,first_name,last_name,email,country,segment"
HEADER_10 = HEADER_9 + ",marketing_opt_in"
SNAPSHOT_HEADER = "customer_id,first_name,last_name,email,country,segment"

OBJECTS = {
    "cdc/snapshot/customers_snapshot.csv": SNAPSHOT_HEADER + "\n1,A,B,a@x.com,Spain,retail\n",
    "cdc/snapshot/snapshot_manifest.json": "{}",
    "cdc/events/dt=2026-02-01/customers_changes_001.csv": HEADER_9 + "\n",
    "cdc/events/dt=2026-02-09/customers_changes_001.csv": HEADER_10 + "\n",
}
CFG = SimpleNamespace(bucket="b", prefix="cdc", bucket_region="eu-central-1",
                      workgroup="wg", database="dev")


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, name):
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}
        return Paginator()

    def get_object(self, Bucket, Key, Range=None):
        data = self.objects[Key].encode()[:4096]

        class Body:
            def read(self):
                return data
        return {"Body": Body()}


class FakeRedshiftData:
    def __init__(self, loaded=(), fail_keys=()):
        self.loaded = set(loaded)
        self.fail_keys = set(fail_keys)
        self.batches = []
        self._statuses = {}
        self._counter = 0

    def _new(self, status="FINISHED", error=None):
        self._counter += 1
        statement_id = f"id{self._counter}"
        self._statuses[statement_id] = (status, error)
        return statement_id

    def execute_statement(self, WorkgroupName, Database, Sql):
        return {"Id": self._new()}

    def get_statement_result(self, Id, NextToken=None):
        return {"Records": [[{"stringValue": f}] for f in sorted(self.loaded)]}

    def batch_execute_statement(self, WorkgroupName, Database, Sqls):
        self.batches.append(Sqls)
        key = re.search(r"select '(.*)', count\(\*\)", Sqls[-1]).group(1).replace("''", "'")
        if key in self.fail_keys:                  # a failed batch rolls back: nothing is logged
            return {"Id": self._new("FAILED", "simulated COPY failure")}
        self.loaded.add(key)
        return {"Id": self._new()}

    def describe_statement(self, Id):
        status, error = self._statuses[Id]
        described = {"Status": status}
        if error:
            described["Error"] = error
        return described


def run(s3=None, rsd=None, **kwargs):
    lines = []
    s3 = s3 or FakeS3(OBJECTS)
    rsd = rsd or FakeRedshiftData()
    summary = load_raw.load_all(s3, rsd, CFG, out=lines.append, **kwargs)
    return summary, rsd, lines


class LoaderTests(unittest.TestCase):
    def test_header_decides_the_copy_column_list(self):
        _, rsd, _ = run()
        copies = [s for batch in rsd.batches for s in batch if s.startswith("copy ")]
        early = next(c for c in copies if "dt=2026-02-01" in c)
        late = next(c for c in copies if "dt=2026-02-09" in c)
        self.assertNotIn("marketing_opt_in", early)
        self.assertIn("segment, marketing_opt_in)", late)

    def test_snapshot_is_loaded_before_events(self):
        _, rsd, _ = run()
        self.assertIn("raw_cdc.customers_snapshot", rsd.batches[0][1])
        self.assertIn("raw_cdc.customer_events", rsd.batches[1][1])

    def test_non_csv_files_are_ignored(self):
        summary, _, _ = run()
        self.assertEqual(summary["found"], 3)

    def test_second_run_loads_nothing(self):
        rsd = FakeRedshiftData()
        first, _, _ = run(rsd=rsd)
        batches_after_first = len(rsd.batches)
        second, _, lines = run(rsd=rsd)
        self.assertEqual(first["loaded"], 3)
        self.assertEqual(second["loaded"], 0)
        self.assertEqual(second["skipped"], 3)
        self.assertEqual(len(rsd.batches), batches_after_first)
        self.assertTrue(all(line.startswith("SKIP") for line in lines))

    def test_unknown_column_fails_and_runs_no_sql(self):
        objects = dict(OBJECTS)
        objects["cdc/events/dt=2026-02-09/customers_changes_001.csv"] = HEADER_10 + ",surprise\n"
        summary, rsd, lines = run(s3=FakeS3(objects))
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["loaded"], 2)
        self.assertTrue(any(line.startswith("FAIL") and "surprise" in line for line in lines))
        self.assertFalse(any("dt=2026-02-09" in s for batch in rsd.batches for s in batch))

    def test_failed_load_is_not_logged_and_can_be_retried(self):
        bad = "cdc/events/dt=2026-02-01/customers_changes_001.csv"
        rsd = FakeRedshiftData(fail_keys=[bad])
        summary, _, _ = run(rsd=rsd)
        self.assertEqual(summary["failed"], 1)
        self.assertNotIn(bad, rsd.loaded)
        rsd.fail_keys.clear()
        retry, _, _ = run(rsd=rsd)
        self.assertEqual(retry["loaded"], 1)
        self.assertIn(bad, rsd.loaded)

    def test_dry_run_executes_nothing(self):
        summary, rsd, lines = run(dry_run=True)
        self.assertEqual(rsd.batches, [])
        self.assertEqual(summary["planned"], 3)
        self.assertTrue(any(line.startswith("PLAN") for line in lines))

    def test_limit_stops_after_n_new_files(self):
        summary, rsd, _ = run(limit=1)
        self.assertEqual(summary["loaded"], 1)
        self.assertEqual(len(rsd.batches), 1)

    def test_single_quotes_are_escaped(self):
        self.assertEqual(load_raw.sql_literal("a'b"), "'a''b'")

    def test_all_statements_run_in_one_batch_per_file(self):
        _, rsd, _ = run()
        for batch in rsd.batches:
            self.assertEqual(len(batch), 5)
            self.assertTrue(batch[2].startswith("copy "))
            self.assertTrue(batch[-1].startswith("insert into raw_cdc.load_log"))


if __name__ == "__main__":
    unittest.main()
