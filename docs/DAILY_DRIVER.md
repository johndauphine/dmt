# Daily-Driver Workflow

DMT can run as a scheduled incremental sync by combining an initial
`drop_recreate` load with later `upsert` runs that use
`migration.date_updated_columns`.

## Workflow

1. Run the first migration with `target_mode: drop_recreate`.
2. Include the same `date_updated_columns` list you plan to use for daily
   runs. The baseline run loads all rows and records per-table sync
   timestamps.
3. Run later migrations with `target_mode: upsert` and the same
   `data_dir`/checkpoint backend.
4. DMT filters each table to rows where the first matching date column is
   strictly newer than the last successful source-side high watermark.

Example:

```yaml
migration:
  target_mode: upsert
  date_updated_columns:
    - LastAccessDate
    - LastActivityDate
    - ModifiedDate
    - CreationDate
```

The date-column list is ordered. For each table, DMT uses the first column
that exists on that source table. Tables with no matching date column still
run as full-table upserts on each daily run.

## Preview and Summary

Before enabling a scheduled run, preview the plan without writing data:

```sh
dmt -c config.yaml run --dry-run
```

The dry run performs connection/preflight checks, extracts the filtered source
schema, reports schema drift using the configured fail/log policy, estimates
row counts, and shows the planned pagination strategy. When recent same
source/target throughput history exists, the preview includes an estimated
duration.

Every normal `run` and `resume` prints a completion summary for terminal logs:
run ID, status, timestamps, duration, table counts, transferred rows,
throughput, per-table results, failed tables, and the final error when present.
JSON output modes keep their machine-readable result instead of printing the
human summary.

Slack notifications reuse the same completion data in a structured message.
Configure the webhook under `slack` or `~/.secrets/dmt-config.yaml`, then use
`migration.notify` to control completion alerts:

```yaml
migration:
  notify:
    on_success: true
    on_failure: true
```

`on_success` controls successful completion notifications. `on_failure`
controls hard failures, partial runs, and per-table transfer failure alerts.
The migration-start notification is sent when either completion policy is
enabled.

## Watermark Semantics

Incremental reads use a strict `>` comparison:

```sql
date_column > last_successful_source_high_watermark
```

Rows exactly equal to the previous watermark are not replayed. This avoids
repeat processing across back-to-back daily runs. The persisted watermark is
the maximum non-NULL source timestamp DMT saw before the successful table run,
not the application server's clock. Rows updated during a run with timestamps
newer than that captured high watermark may replay once on the next run, which
preserves at-least-once behavior without full-table rescans.

The timestamp column should advance whenever a source row changes. A changed
row whose timestamp is manually set equal to an already persisted watermark is
treated as already covered and will be skipped by design. Source systems should
also avoid writing future-dated update timestamps; a future timestamp remains
newer than subsequent run watermarks until wall-clock time catches up.

Watermarks are persisted in DMT state by source schema, table name, and target
schema. Reuse the same `migration.data_dir` or state backend between the
baseline and daily runs.

## Deletes

The date-column upsert path captures inserts and updates. It does not
propagate hard deletes from the source. In `upsert` mode, count-only
validation permits target row counts greater than source row counts so a
source-side delete does not fail the daily run.

Delete handling is staged separately. `migration.deletes` is now parsed and
validated with `mode: off` as the default, and dry-run output reports whether
the configured reconciliation interval is due. `mode: reconcile` remains
reserved for the key-set reconciliation implementation tracked in #351. See
[DELETE_HANDLING.md](DELETE_HANDLING.md) for the configuration and rollout
plan.

## Validation

Run the integration check locally with real MSSQL and Postgres containers:

```sh
make test-dbs-up
make integration-test-daily-driver
```

The test proves:

- a baseline `drop_recreate` run seeds sync timestamps
- one source row update transfers exactly one row during the next upsert run
- a source update whose timestamp exactly equals the saved watermark is skipped
- an unchanged follow-up upsert transfers zero rows
- the target retains the expected row count

For a broader production validation pass, run two consecutive upserts against
the same source and target. The second run should transfer zero rows for
unchanged tables, and tables with changed rows should advance their
watermarks.
