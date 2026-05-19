# Schema Evolution

Schema evolution builds on source schema drift detection. Drift detection
compares the current source schema to the last successful source snapshot; schema
evolution can apply compatible changes to the target before transfer.

## Current Scope

The first implementation slice supports one safe change type:

- `added_column` in `target_mode: upsert`

When enabled with `auto`, DMT adds the new target column before transfer:

```yaml
migration:
  target_mode: upsert
  schema_evolution:
    added_column: auto
```

Omitting `migration.schema_evolution` keeps drift reporting read-only. If the
section is present and `added_column` is omitted, the added-column policy
defaults to `auto`.

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

## Guardrails

- Schema evolution is evaluated during a fresh migration run. `dmt resume`
  continues to report drift but does not alter target tables mid-resume.
- Schema evolution only runs in `upsert` mode. `drop_recreate` already rebuilds
  tables from the current source schema.
- Identity columns and primary-key columns are not auto-added.
- Other drift categories remain report-only in this slice, including dropped
  columns, type changes, nullability changes, defaults, indexes, foreign keys,
  checks, and table-level changes.
- Existing type mapping is reused for the new target column type.
- The target table must already exist.

## Rollback

For the first slice, rollback is manual and database-specific:

- remove the newly added target column if it should not exist
- set `migration.schema_evolution.added_column: log` or remove
  `migration.schema_evolution`
- rerun after confirming the target schema

Use `fail_on_schema_drift: true` when you want drift to remain a hard gate
instead of allowing schema evolution to decide per change type.
