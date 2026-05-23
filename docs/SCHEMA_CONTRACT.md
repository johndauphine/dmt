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
| `evolve` | Apply compatible changes where DMT can do so safely. For columns this maps to nullable added-column evolution; for `data_type` this maps to safe nullability relaxation and widened type changes. |
| `freeze` | Abort before transfer when that entity changes. |
| `discard_value` | For `columns`, omit newly added source columns from target DDL, transfer, validation, and schema snapshots. |
| `report` | DMT-specific mode. Report drift and do not apply target schema changes. This preserves the old report-only behavior. |

`discard_row` and non-column `discard_value` are not implemented yet. DMT rejects
those modes at config load time instead of pretending to support row-level
discard semantics that the relational transfer engine cannot enforce today.

## Entity Mapping

| Entity | Supported modes | Notes |
|--------|-----------------|-------|
| `tables` | `evolve`, `freeze`, `report` | `freeze` fails when a new source table is detected. `evolve` follows the current target mode behavior. |
| `columns` | `evolve`, `freeze`, `discard_value`, `report` | `discard_value` is the safe way to keep migrating while ignoring newly added source columns. |
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
