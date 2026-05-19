# Delete Handling

Incremental upsert mode currently captures inserts and updates, but it does not
detect source-side deletes. A row hard-deleted from the source can remain on the
target forever unless the target is rebuilt with `drop_recreate` or another
process removes it.

This document compares the design options for delete propagation and proposes a
configuration surface for a future implementation. It is design-only; no delete
handling is implemented yet.

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

## Proposed Configuration

The proposed config block is nested under `migration`:

```yaml
migration:
  target_mode: upsert

  deletes:
    mode: off              # off | reconcile | tombstone | cdc
    target_behavior: hard  # hard | soft

    soft_delete:
      column: deleted_at
      value: now

    reconcile:
      schedule: interval   # every_run | every_n_runs | interval | manual
      every_n_runs: 7
      interval: 168h
      batch_size: 10000
      require_primary_key: true

    tombstone:
      column: deleted_at
      marker: non_null     # non_null | boolean_true | value
      value: null

    cdc:
      engine: auto         # auto | sqlserver_change_tracking | sqlserver_cdc | postgres_logical | mysql_binlog
      state_name: default
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
| `tombstone.marker` | `non_null` | Fits timestamp-style `deleted_at` columns. Boolean and sentinel-value markers are opt-in. |
| `cdc.engine` | `auto` | DMT chooses the source engine's configured CDC reader once CDC support exists. |

`migration.deletes` should only be valid with `target_mode: upsert`.
`drop_recreate` already removes target-only rows by rebuilding the table from the
current source snapshot.

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

Future implementations should expose delete handling clearly:

- config validation should reject unsupported combinations before transfer
- run logs should report delete mode, target behavior, tables evaluated, rows
  marked or deleted, and skipped tables
- reconciliation should distinguish "not due" from "ran and found zero deletes"
- validation in upsert mode should account for enabled delete propagation instead
  of always permitting extra target rows
- dry-run output should list candidate delete counts without mutating the target

## Follow-up Implementation Issues

File separate implementation issues after this design lands:

1. Implement `migration.deletes` config parsing and validation for `mode: off`,
   `mode: reconcile`, and `target_behavior: hard`.
2. Implement interval-based key-set reconciliation for primary-key tables in
   upsert mode, including checkpointed last-success metadata.
3. Add reconciliation dry-run reporting and structured per-table delete metrics.
4. Add `target_behavior: soft` with explicit target soft-delete column
   validation and batched updates.
5. Add tombstone source support for `deleted_at IS NOT NULL` and boolean marker
   columns, with per-table override design if global config is insufficient.
6. Update upsert validation semantics when delete propagation is enabled.
7. Design CDC state and setup requirements per engine, then file one
   implementation issue each for SQL Server, PostgreSQL, and MySQL.
