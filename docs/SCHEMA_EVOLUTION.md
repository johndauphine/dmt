# Schema Evolution

Schema evolution builds on source schema drift detection. Drift detection
compares the current source schema to the last successful source snapshot; schema
evolution can apply compatible changes to the target before transfer.

## Current Scope

The current implementation supports two safe change types:

- `added_column` in `target_mode: upsert`
- `nullability_change` in `target_mode: upsert`, only when the source relaxes a
  column from `NOT NULL` to `NULL`

When enabled with `auto`, DMT applies compatible target ALTERs before transfer:

```yaml
migration:
  target_mode: upsert
  schema_evolution:
    added_column: auto
    nullability_change: auto
```

Omitting `migration.schema_evolution` keeps drift reporting read-only. If the
section is present and a supported policy is omitted, that policy defaults to
`auto`.

## Added Column Policy

| Policy | Behavior |
|--------|----------|
| `auto` | Add each new source column to the target as nullable, then continue transfer. |
| `log` | Leave the target unchanged and continue with the normal drift report. |
| `fail` | Abort before transfer if any added source column is detected. |

Added columns are always created nullable in this slice, even when the source
column is `NOT NULL`. Existing target rows need a value, and DMT does not
backfill historical rows during schema evolution. A later slice can add
backfill-aware policies.

## Nullability Change Policy

| Policy | Behavior |
|--------|----------|
| `auto` | Relax target columns from `NOT NULL` to `NULL`, then continue transfer. |
| `log` | Leave the target unchanged and continue with the normal drift report. |
| `fail` | Abort before transfer if any nullability change is detected. |

`auto` does not tighten `NULL` columns to `NOT NULL`. Tightening requires
proving every existing target row has a non-NULL value and coordinating with
indexes, constraints, and application writes. DMT reports tightening as an
unsupported auto-apply change and stops rather than risking a partially applied
schema.

## Guardrails

- Schema evolution is evaluated during a fresh migration run. `dmt resume`
  continues to report drift but does not alter target tables mid-resume.
- Schema evolution only runs in `upsert` mode. `drop_recreate` already rebuilds
  tables from the current source schema.
- Identity columns and primary-key columns are not auto-added.
- Identity columns and primary-key columns are not auto-relaxed.
- Other drift categories remain report-only, including dropped columns, type
  changes, defaults, indexes, foreign keys, checks, and table-level changes.
- Existing type mapping is reused for the new target column type and for
  engines that require the type while relaxing nullability.
- The target table must already exist.
- SQLite targets cannot relax `NOT NULL` in place; that requires a table
  rebuild and is intentionally not part of this slice.

## Rollback

For the first slice, rollback is manual and database-specific:

- remove the newly added target column if it should not exist
- set `migration.schema_evolution.added_column: log` or remove
  `migration.schema_evolution`
- rerun after confirming the target schema

Use `fail_on_schema_drift: true` when you want drift to remain a hard gate
instead of allowing schema evolution to decide per change type.
