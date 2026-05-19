# Delete Handling

Incremental upsert mode currently captures inserts and updates, but it does not
detect source-side deletes. A row hard-deleted from the source can remain on the
target forever unless the target is rebuilt with `drop_recreate` or another
process removes it.

This document compares the design options for delete propagation and records
the staged implementation plan. The `migration.deletes` config surface is parsed
and validated as of #351's first slice, but runtime reconciliation and target
mutation still land in follow-up work. DMT also records the latest successful
delete reconciliation in checkpoint state and includes due/not-due scheduling
metadata in dry-run output when reconciliation is enabled. The shared
reconciliation package can already scan source/target primary-key sets,
identify target-only keys, and execute parameter-bounded target hard deletes;
the runtime path now invokes that primitive before validation when interval
scheduling says reconciliation is due. Reconciliation results are persisted per
table and included in run summaries so operators can see candidate, deleted, and
skipped counts after a reconciliation pass. The SQLite integration test now
proves an upsert run removes a source-side hard delete from the target and
records the per-table delete counts in checkpoint state.

## Recommendation

Keep the shipped default as no delete propagation:

```yaml
migration:
  deletes:
    mode: off
```

This avoids surprising target data loss for existing upsert users. For operators
who opt in, the recommended first implementation is periodic key-set
reconciliation. It works without source schema changes or database-native CDC
setup, gives predictable correctness, and keeps the daily incremental path fast
when reconciliation is scheduled less frequently than normal upserts.

Tombstone support should ship as a low-cost opt-in for schemas that already model
soft deletes. CDC should be treated as a separate larger feature, implemented per
source engine after the simpler modes are stable.

## Option Comparison

| Option | How it works | Strengths | Costs and limits | Best fit |
|--------|--------------|-----------|------------------|----------|
| Tombstone column | Source rows carry a delete marker such as `deleted_at` or `is_deleted`; DMT reads marked rows during incremental sync and applies configured target behavior. | Cheapest runtime path; no full-table diff; no engine CDC infrastructure; delete latency follows normal upsert cadence. | Requires source schema cooperation; cannot detect rows that are physically deleted before DMT sees a tombstone; needs clear per-table column configuration. | Applications that already use soft deletes and retain tombstoned rows long enough for migration runs. |
| Periodic key-set reconciliation | On a schedule, scan source primary keys and target primary keys, then remove or mark target rows whose keys are absent from source. | Works for hard deletes; engine-agnostic; no source schema changes; no replication privileges; simple correctness model. | O(row count) key scans on both sides; delete visibility lags the reconciliation schedule; requires primary keys or a configured stable key. | Default opt-in mode for daily-driver convergence where near-real-time delete latency is not required. |
| CDC | Read source database change logs and apply delete events from native change streams. | Lowest latency; captures hard deletes directly; avoids periodic full key diff. | Engine-specific readers and state tracking; source-side DBA setup; additional permissions; operational complexity around log retention and restart positions. | High-freshness pipelines where database teams can support native change capture. |

## Configuration

The supported first-slice config block is nested under `migration`:

```yaml
migration:
  target_mode: upsert

  deletes:
    mode: off              # off | reconcile
    target_behavior: hard  # hard

    reconcile:
      schedule: interval   # interval
      interval: 168h
      batch_size: 10000
      require_primary_key: true
```

Defaults:

| Field | Default | Notes |
|-------|---------|-------|
| `migration.deletes.mode` | `off` | Existing upsert behavior remains unchanged. |
| `migration.deletes.target_behavior` | `hard` | When delete propagation is enabled, the target converges by physically deleting target rows unless configured otherwise. |
| `reconcile.schedule` | `interval` | Prefer elapsed time over "every Nth run" because it behaves better when runs are retried, skipped, or manually triggered. |
| `reconcile.interval` | `168h` | Weekly reconciliation is a conservative starting point. |
| `reconcile.batch_size` | `10000` | Applies target deletes or updates in bounded batches. |
| `reconcile.require_primary_key` | `true` | Reconciliation should fail fast unless DMT has a stable key to compare. |

`migration.deletes` should only be valid with `target_mode: upsert`.
`drop_recreate` already removes target-only rows by rebuilding the table from the
current source snapshot.

The broader design still reserves these future options, but current config
validation rejects them until implemented:

- `mode: tombstone`
- `mode: cdc`
- `target_behavior: soft`
- `reconcile.schedule: every_run|every_n_runs|manual`
- `reconcile.require_primary_key: false`

### Reconciliation Scheduling

Use an interval-based schedule for the first reconciliation implementation:

- `interval`: run reconciliation when the last successful reconciliation for a
  table is older than the configured duration.
- `manual`: only run from a future explicit command such as `dmt reconcile`.
- `every_run`: reconcile after every successful upsert run.
- `every_n_runs`: reconcile every N successful runs.

The interval schedule should be the default because it is easy to reason about
for daily operations and less brittle than counting runs. If a run fails before
the reconciliation step completes, the next successful run should still consider
the table due.

## Target Behavior

Delete detection and target behavior are separate choices.

### Hard-delete target behavior

`target_behavior: hard` physically deletes matching rows from the target:

```sql
DELETE FROM target_table WHERE pk IN (...);
```

This is the default when delete propagation is enabled because it makes the
target converge with a hard-deleting source and keeps validation semantics
simple. It is also the least surprising behavior for reconciliation, where a key
missing from source normally means the target row should not exist.

Hard deletes should be batched, logged in structured output, and counted per
table. Operators should be able to dry-run reconciliation before enabling writes
in a later implementation issue.

### Soft-delete target behavior

`target_behavior: soft` marks target rows instead of removing them. This requires
target-side column configuration in the implementation slice, for example:

```yaml
migration:
  deletes:
    mode: reconcile
    target_behavior: soft
    soft_delete:
      column: deleted_at
      value: now
```

Soft-delete target behavior is useful when the target is an analytical store,
audit store, or downstream application that needs to retain deleted rows. It
should not be silently inferred from tombstone source mode; a source tombstone
can still drive a hard delete on the target, and a reconciliation miss can still
drive a target tombstone.

Implementation should require an explicit target soft-delete column and should
fail configuration validation if the column is missing or not writable.

## CDC Setup Notes

CDC should not be part of the first delete-handling implementation. It needs
engine-specific design for setup, privileges, state, and retention.

Initial setup expectations:

| Source engine | Candidate mechanism | Setup considerations |
|---------------|---------------------|----------------------|
| SQL Server | Change Tracking or CDC | Change Tracking is lighter and can capture primary-key delete facts; CDC is richer but needs SQL Server Agent and source-side DDL. Both require retention monitoring and last-version/LSN state. |
| PostgreSQL | Logical replication slot with `pgoutput` | Requires `wal_level=logical`, replication privileges, slot lifecycle management, publication/table selection, and restart LSN persistence. |
| MySQL | Row-based binlog | Requires row binlog format, replication privileges, server id handling, binlog retention monitoring, and filename/position or GTID persistence. |

## Validation and Observability

Runtime implementations should expose delete handling clearly:

- config validation rejects unsupported combinations before transfer
- dry-run output reports whether reconciliation is due, the prior successful
  reconciliation time, and primary-key table eligibility
- run logs and completion summaries should report delete mode, target behavior,
  tables evaluated, rows marked or deleted, and skipped tables
- reconciliation should distinguish "not due" from "ran and found zero deletes"
- validation in upsert mode requires source/target count parity after the
  current run completes delete reconciliation, while still allowing extra target
  rows when reconciliation is disabled or not due
- dry-run output lists candidate delete counts without mutating the target when
  reconciliation is due

## Follow-up Implementation Issues

Track implementation in separate issues:

1. Add notification-specific delete metrics if Slack or other sinks need a more
   structured payload than the shared completion summary.
2. Add a lower-cost dry-run estimate mode if full key scans are too expensive
   for very large reconciliation tables.
3. Add `target_behavior: soft` with explicit target soft-delete column
   validation and batched updates.
4. Add tombstone source support for `deleted_at IS NOT NULL` and boolean marker
   columns, with per-table override design if global config is insufficient.
5. Add stricter validation behavior for non-count checks after delete
   reconciliation if sample/full validation later needs delete-aware behavior.
6. Design CDC state and setup requirements per engine, then file one
   implementation issue each for SQL Server, PostgreSQL, and MySQL.
