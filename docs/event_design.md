# CDC event design: `customers`

**Goal:** simulate what a change-data-capture (CDC) tool delivers for one table, so I can design
and verify an incremental pipeline against a known correct answer.
All data is synthetic and randomly generated.

## 1. What the pipeline receives

A pretend production table `customers` keeps a change log. Every change gets the next
sequence number `seq`. The pipeline never sees the source database, only:

- one **snapshot** (the table as it was at `snapshot_seq`)
- **change files**, delivered per day, with imperfect delivery (section 4)

## 2. Change event contract

| Field | Meaning |
|---|---|
| `seq` | Unique, increasing log position. **Defines the true order of changes.** |
| `op` | `I` insert, `U` update, `D` delete |
| `commit_ts` | When the change was committed in the source |
| `customer_id` | Primary key |
| `first_name`, `last_name`, `email`, `country`, `segment` | Full row *after* the change. Empty for deletes. |
| `marketing_opt_in` | Added later (schema change, section 4) |

Rules: inserts and updates carry the **full new row**; deletes carry **only the key**.
True order is `seq`. Arrival order and file order carry no meaning.

## 3. Snapshot and handover

`snapshot_manifest.json` holds `snapshot_seq`. Rule: **apply only events with `seq > snapshot_seq`**.
The stream deliberately replays the last 30 events from before the snapshot, so the
handover between "history" and "live" has to be handled on purpose.

## 4. Delivery problems injected on purpose (deterministic via the seed)

| Case | What happens | Correct handling |
|---|---|---|
| Duplicates (~3%) | The same event (same `seq`) is delivered twice | Deduplicate by `seq` |
| Out of order | Rows are shuffled within and across files | Order by `seq`, never by arrival |
| Late events (~4%) | Delivered 1-2 days after commit | Do not assume partition date = commit date |
| Snapshot overlap | The first file replays events already in the snapshot | Ignore `seq <= snapshot_seq` |
| Deletes (~5% of changes) | `op = D`, key only | Remove from the current state |
| Schema change | From a certain delivery date files have an extra column | Loader must tolerate the new column |

## 5. Definition of "correct"

- **Current state** = snapshot + all unique events with `seq > snapshot_seq`, applied in `seq` order.
- It must equal `expected/customers_expected.csv`.
- **Idempotent:** loading a file twice, or re-running a day, gives the same result.
- A **full rebuild** from the raw events equals the incrementally built table.

`reference_apply.py` is the plain-Python version of this rule. Applying events in arrival
order without deduplication produces wrong customers, which is exactly what the pipeline
must avoid.

## 6. Layout produced by `generate_events.py`

```
data/
  snapshot/customers_snapshot.csv
  snapshot/snapshot_manifest.json
  events/dt=YYYY-MM-DD/customers_changes_001.csv ...
  expected/customers_expected.csv      (ground truth, not visible to the pipeline)
  expected/injected_cases.json         (how much mess was injected)
```

## 7. Open design questions

- How should deletes appear downstream: removed rows, or a soft-delete flag with history?
- Where should deduplication live: in the load step or in dbt staging?
- How do I prove idempotency with a test?

## 8. Out of scope for now

Multiple tables, before-images, transactions spanning tables, a real CDC tool, PII handling
(planned as a later milestone).
