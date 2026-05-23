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
| `discard_row` | For `tables`, skip newly added source tables for this run. For `columns`, skip source tables with newly added columns for this run because the added column exists on every source row. For `data_type`, skip tables with data type or nullability drift for this run. Dropped source tables and columns are reported and retained on the target. |
| `discard_value` | For `columns`, omit newly added source columns from target DDL, transfer, validation, and schema snapshots. For `data_type`, omit affected non-key, non-identity, non-date-tracking columns from transfer and validation while retaining the previous snapshot metadata so the same drift remains ignored on later runs. |
| `report` | DMT-specific mode. Report drift and do not apply target schema changes. This preserves the old report-only behavior. |

Table-level `discard_value` is not implemented because there is no safe
whole-table value-only interpretation. DMT rejects that mode at config load
time instead of pretending to support semantics the relational transfer engine
cannot enforce today.

## Entity Mapping

| Entity | Supported modes | Notes |
|--------|-----------------|-------|
| `tables` | `evolve`, `freeze`, `discard_row`, `report` | `evolve` creates newly added target tables before `upsert` transfer and finalizes configured secondary DDL after transfer; `drop_recreate` already recreates filtered source tables. `freeze` fails on table add/drop drift. `discard_row` skips newly added source tables for the run. Dropped source tables are reported and retained on the target. |
| `columns` | `evolve`, `freeze`, `discard_row`, `discard_value`, `report` | `evolve` can add compatible source columns to existing targets in `upsert` mode; `drop_recreate` accepts the current source columns by rebuilding the target table. `discard_value` keeps migrating while ignoring newly added source columns. `discard_row` skips the affected table for the run. DMT does not drop target columns for dropped source columns; source writes and validation use the current source column list, so retained target columns are not written by DMT. |
| `data_type` | `evolve`, `freeze`, `discard_row`, `discard_value`, `report` | Covers source type drift and nullability drift. In `upsert` mode, `evolve` only applies changes DMT already classifies as safe: nullability relaxation and widened source types. Narrowed/lossy type changes still abort under `upsert` `evolve`; DMT does not create DLT-style variant columns for relational targets. `drop_recreate` accepts source type/nullability drift by rebuilding the target table. `discard_row` skips affected tables. `discard_value` omits affected non-key, non-identity, non-date-tracking columns and prunes dependent indexes, foreign keys, and checks from the effective plan. |

Omitted entities default to `evolve` when `schema_contract` is present. Omitting
the entire `schema_contract` section keeps the baseline read-only drift report.

## Reporting

When schema drift is detected, DMT records structured schema contract decisions
in the audit log and includes the latest decisions in JSON result/status output
when the run audit log is available. Each decision includes the entity
(`tables`, `columns`, or `data_type`), configured mode, drift kind,
schema/table/object, previous/current evidence, action (`evolved`, `frozen`,
`blocked`, `discarded_row`, `discarded_value`, or `reported`), and reason.
Unsafe `data_type: evolve` changes use the `blocked`
action because they are stopped by deterministic safety gates rather than by
`freeze` mode. Freeze failures also include the exact blocked object and point
operators toward `report`, `evolve`, or discard modes where supported.
When `discard_row` skips a table, non-freeze decisions for other drift on that
same table are reported without target changes so skipped-table drift does not
abort the run before DMT can omit the affected table.

## Compatibility

Do not combine `migration.schema_contract` with deprecated
`migration.schema_evolution`; DMT rejects configs that specify both so the
effective contract is unambiguous.

DLT's official [schema contract](https://dlthub.com/docs/general-usage/schema-contracts)
surface uses the same `tables`, `columns`, and `data_type` entities and the
same `evolve`/`freeze`/`discard_row`/`discard_value` mode names. DMT adds
`report` because operators already rely on report-only schema drift detection.

## DLT Parity Matrix

| DLT contract behavior | DMT schema_contract behavior | Intentional relational differences |
|-----------------------|------------------------------|------------------------------------|
| Omitted entity defaults to `evolve` when a contract is configured. | Same. `schema_contract: report` is also available as DMT shorthand for report-only drift handling. | `report` is DMT-only and does not exist in DLT. |
| `tables: evolve` accepts newly detected tables. | Creates added target tables before `upsert` transfer when deterministic DDL is available; `drop_recreate` rebuilds from the current filtered source tables. | `upsert` requires a primary key for newly evolved tables. Dropped source tables are reported and retained on the target. |
| `tables: freeze` blocks table changes. | Same; added or dropped source tables abort before transfer. | None beyond DMT's pre-transfer error wording and audit decisions. |
| `tables: discard_row` skips rows from new tables. | Skips newly added source tables for the run and omits them from validation and success snapshots. | Dropped source tables are reported; target tables are retained. |
| `tables: discard_value` is not meaningful for whole tables. | Rejected at config load time. | DMT has no safe relational value-only interpretation for a whole table. |
| `columns: evolve` accepts newly detected columns. | Adds compatible source columns before `upsert` transfer; `drop_recreate` rebuilds target tables from the current source shape. | Added identity or primary-key columns are blocked in `upsert` because they change write identity. Dropped source columns are reported and retained on the target. |
| `columns: freeze` blocks column changes. | Same for added and dropped source columns. | None beyond DMT's pre-transfer error wording and audit decisions. |
| `columns: discard_row` skips rows affected by new columns. | Skips the affected table for the run because every row in a relational table carries the added column. | This is table-granular, not individual-row granular. |
| `columns: discard_value` omits newly detected column values. | Omits added source columns from target DDL, transfer, validation, and success snapshots. | Primary-key, identity, and date-tracking columns cannot be discarded. |
| `data_type: evolve` accepts compatible type changes. | Applies deterministic safe changes in `upsert`: nullability relaxation and widened source types. `drop_recreate` accepts current source types by rebuilding the table. | DMT does not create DLT-style variant columns on relational targets. Narrowing, lossy conversion, nullability tightening, and PK/default-coupled drift are blocked. |
| `data_type: freeze` blocks type changes. | Same for source type and nullability drift. | None beyond DMT's pre-transfer error wording and audit decisions. |
| `data_type: discard_row` skips rows affected by type drift. | Skips the affected table for the run. | This is table-granular, not individual-row granular. |
| `data_type: discard_value` omits affected values. | Omits affected non-key, non-identity, non-date-tracking columns from transfer and validation while retaining previous snapshot metadata for those columns. | Target column definitions are left unchanged; the same drift remains visible until the operator chooses another policy. |

## Copyable Examples

Report drift without applying target changes:

```yaml
migration:
  target_mode: upsert
  schema_contract: report
```

Automatically evolve deterministic relational changes:

```yaml
migration:
  target_mode: upsert
  schema_contract:
    tables: evolve
    columns: evolve
    data_type: evolve
```

Block drift before transfer:

```yaml
migration:
  target_mode: upsert
  schema_contract:
    tables: freeze
    columns: freeze
    data_type: freeze
```

Keep migrating but omit new column values and safe non-key type-drift values:

```yaml
migration:
  target_mode: upsert
  schema_contract:
    tables: report
    columns: discard_value
    data_type: discard_value
```

Keep migrating unaffected tables while skipping tables touched by drift:

```yaml
migration:
  target_mode: upsert
  schema_contract:
    tables: discard_row
    columns: discard_row
    data_type: discard_row
```
