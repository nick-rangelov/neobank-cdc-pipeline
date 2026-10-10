# Neobank CDC Pipeline (learning project)

A learning project about keeping a warehouse in sync with a source that keeps changing:
change data capture (CDC), incremental loads and backfills, using **synthetic** customer data.

> All data in this repository is randomly generated. It contains no real customer data and
> is not derived from any employer's systems.

## Why

A first project (batch ELT with S3, Redshift and dbt) loaded a one-time snapshot. Real
pipelines have to handle changes arriving continuously: duplicates, out-of-order events,
late files, deletes, schema changes and the handover from a historical backfill to a live
stream. This project builds that on a small scale, with a known correct answer to test against.

## Status

- [x] Event design ([docs/event_design.md](docs/event_design.md))
- [x] Synthetic CDC event generator (snapshot, change files, injected edge cases)
- [x] Reference check of the correctness rule
- [x] Load raw events into the warehouse (append-only, idempotent)
- [ ] dbt: dedupe, incremental model, history snapshot
- [ ] Reconciliation and freshness checks
- [ ] Scheduling and a backfill experiment
- [ ] PII handling layer

## Quick start

Standard library only, no installs needed:

```bash
python3 generate_events.py --out data --customers 500 --days 10 --seed 42
python3 reference_apply.py --data data           # correct rule: PASS
python3 reference_apply.py --data data --naive   # arrival order, no dedupe: FAIL
```

The same seed always produces identical output.

## Loading into Redshift

Raw tables are defined in `redshift/cdc_raw_tables.sql`. Copy the generated files to S3
(excluding `expected/`), then:

    pip install boto3
    export CDC_BUCKET=<your-bucket>
    python3 load_raw.py                        # loads every file not yet in raw_cdc.load_log
    python3 -m unittest discover -s tests -v   # logic tests with fake AWS clients

Each file is loaded in a single transaction: COPY into a temp stage table, insert into the
raw table with its source file name, write the load log entry. A failed load leaves nothing
behind, and a re-run skips files that are already loaded.

Limits: files are loaded one at a time, so two runs at once are not safe. Unknown columns
fail on purpose instead of being loaded silently.
