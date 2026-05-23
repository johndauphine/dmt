# Schema Evolution

> Deprecated: `migration.schema_evolution` is a legacy configuration surface.
> It remains functional for existing migrations, but DMT will replace it with
> DLT-style `migration.schema_contract` settings for tables, columns, and
> data_type in a future release. New configs should avoid adding
> `migration.schema_evolution`; track the replacement work in
> [#403](https://github.com/johndauphine/dmt/issues/403).

Schema evolution builds on source schema drift detection. Drift detection
compares the current source schema to the last successful source snapshot; schema
evolution can apply compatible changes to the target before transfer.

## Current Scope

The current implementation supports three safe change types:

- `added_column`; `auto` applies target ALTERs in `target_mode: upsert`, while
  `discard_value` omits added columns from the effective target DDL, transfer,
  validation, and success snapshot
- `nullability_change` in `target_mode: upsert`, only when the source relaxes a
  column from `NOT NULL` to `NULL`
- `type_change` in `target_mode: upsert`, only when explicitly set to `auto`
  and DMT classifies the source change as a widening

When enabled with `auto`, DMT applies compatible target ALTERs before transfer:

```yaml
migration:
  target_mode: upsert
  schema_evolution:
    added_column: auto
    nullability_change: auto
    type_change: auto
```

Omitting `migration.schema_evolution` keeps drift reporting read-only. If the
section is present and a supported policy is omitted, that policy defaults to
`auto`, except `type_change`, which defaults to `log` because target type
ALTERs can rewrite storage and must be explicitly enabled.

To keep migrating rows while ignoring newly added source columns, use the
DLT-compatible `discard_value` policy:

```yaml
migration:
  schema_evolution:
    added_column: discard_value
```

`discard` is accepted as a short alias for `discard_value`.

## Added Column Policy

| Policy | Behavior |
|--------|----------|
| `auto` | Add each new source column to the target as nullable, then continue transfer. |
| `log` | Leave the target unchanged and continue with the normal drift report. |
| `fail` | Abort before transfer if any added source column is detected. |
| `discard_value` | Leave the target unchanged, omit each added source column from the read/write/validation plan, and continue transferring the rest of each row. |

Added columns are always created nullable in this slice, even when the source
column is `NOT NULL`. Existing target rows need a value, and DMT does not
backfill historical rows during schema evolution. A later slice can add
backfill-aware policies.

`discard_value` is intentionally a data-plane policy, not only a logging policy.
DMT removes the discarded column from the effective table metadata before target
DDL, transfer, validation, and snapshot capture. The persisted success snapshot
therefore stays aligned with the target schema, so the same source column will
continue to be treated as newly added and discarded on later runs until the
operator switches the policy to `auto`.

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

## Type Change Policy

| Policy | Behavior |
|--------|----------|
| `auto` | Alter widened source column types on the target, then continue transfer. |
| `log` | Leave the target unchanged and continue with the normal drift report. |
| `fail` | Abort before transfer if any type change is detected. |

`auto` only applies changes DMT classifies as `type_widened`, such as increasing
integer rank, increasing string/binary length, increasing float rank, or
expanding decimal precision/scale without reducing integer or fractional
capacity. Narrowed and lossy type changes remain hard errors under `auto`; set
`type_change: log` to report them without applying target ALTERs.

Type widening is not auto-applied when the same column also has nullability or
default drift, when the table has primary-key drift, or when the column is an
identity or primary-key column. DMT uses the existing source-to-target type
mapper to choose the new target type.

## Guardrails

- Target ALTER schema evolution is evaluated during a fresh migration run.
  `dmt resume` continues to report drift but does not alter target tables
  mid-resume. `discard_value` may still prune the transfer plan on resume
  because it does not mutate the target.
- Target ALTER schema evolution only runs in `upsert` mode. `drop_recreate`
  already rebuilds tables from the effective source schema. When
  `added_column: discard_value` is active, the rebuilt table also omits the
  discarded column.
- Identity columns and primary-key columns are not auto-added.
- Identity columns and primary-key columns are not auto-relaxed.
- Identity columns and primary-key columns are not auto-widened.
- Added primary-key columns cannot be discarded; changing the key changes the
  upsert identity contract and must be handled explicitly.
- Nullability is not auto-relaxed when the same column also has type/default
  drift, or when the table has primary-key drift.
- Type widening is not auto-applied when the same column also has
  nullability/default drift, or when the table has primary-key drift.
- Other drift categories remain report-only, including dropped columns,
  narrowed/lossy type changes, defaults, indexes, foreign keys, checks, and
  table-level changes.
- Existing type mapping is reused for added target columns, widened target
  column types, and engines that require the type while relaxing nullability.
- The target table must already exist.
- SQLite targets cannot relax `NOT NULL` or alter declared column types in
  place; those changes require a table rebuild.

## Rollback

For the first slice, rollback is manual and database-specific:

- remove the newly added target column if it should not exist
- set `migration.schema_evolution.added_column: log` or remove
  `migration.schema_evolution`
- rerun after confirming the target schema

Use `fail_on_schema_drift: true` when you want drift to remain a hard gate
instead of allowing schema evolution to decide per change type.

For `discard_value`, rollback is changing the policy to `auto` and rerunning.
DMT will then add the target column and include values in future transfers.

For `type_change: auto`, rollback is database-specific. Confirm the target
engine supports narrowing back to the previous type without truncation before
running manual DDL.
