# Schema Contract

`migration.schema_contract` is DMT's DLT-style replacement for the deprecated
`migration.schema_evolution` settings. It controls schema drift by entity:

```yaml
migration:
  target_mode: upsert
  schema_contract:
    tables: report
    columns: discard_value
    data_type: report
```

You can also use shorthand. This expands to all three entities:

```yaml
migration:
  schema_contract: report
```

## Modes

| Mode | DMT behavior |
|------|--------------|
| `evolve` | Apply compatible changes where DMT can do so safely. For tables in `upsert` mode this creates newly detected target tables before transfer when deterministic target DDL is available, then adds configured indexes, foreign keys, and check constraints after transfer. For columns this maps to nullable added-column evolution; for `data_type` this maps to safe nullability relaxation and widened type changes. |
| `freeze` | Abort before transfer when that entity changes. |
| `discard_row` | For `tables`, skip newly added source tables for this run. For `columns`, skip source tables with newly added columns for this run because the added column exists on every source row. Dropped source tables and columns are reported and retained on the target. |
| `discard_value` | For `columns`, omit newly added source columns from target DDL, transfer, validation, and schema snapshots. |
| `report` | DMT-specific mode. Report drift and do not apply target schema changes. This preserves the old report-only behavior. |

Non-table/non-column `discard_row` and non-column `discard_value` are not implemented yet.
DMT rejects those modes at config load time instead of pretending to support
row-level or value-level discard semantics that the relational transfer engine
cannot enforce today.

## Entity Mapping

| Entity | Supported modes | Notes |
|--------|-----------------|-------|
| `tables` | `evolve`, `freeze`, `discard_row`, `report` | `evolve` creates newly added target tables before `upsert` transfer and finalizes configured secondary DDL after transfer; `drop_recreate` already recreates filtered source tables. `freeze` fails on table add/drop drift. `discard_row` skips newly added source tables for the run. Dropped source tables are reported and retained on the target. |
| `columns` | `evolve`, `freeze`, `discard_row`, `discard_value`, `report` | `discard_value` keeps migrating while ignoring newly added source columns. `discard_row` skips the affected table for the run. DMT does not drop target columns for dropped source columns; source writes and validation use the current source column list, so retained target columns are not written by DMT. |
| `data_type` | `evolve`, `freeze`, `report` | Covers source type drift and nullability drift. `evolve` only applies changes DMT already classifies as safe. |

Omitted entities default to `evolve` when `schema_contract` is present. Omitting
the entire `schema_contract` section keeps the baseline read-only drift report.

## Compatibility

Do not combine `migration.schema_contract` with deprecated
`migration.schema_evolution`; DMT rejects configs that specify both so the
effective contract is unambiguous.

DLT's official [schema contract](https://dlthub.com/docs/general-usage/schema-contracts)
surface uses the same `tables`, `columns`, and `data_type` entities and the
same `evolve`/`freeze`/`discard_row`/`discard_value` mode names. DMT adds
`report` because operators already rely on report-only schema drift detection.
