# Dialect template-expressibility audit (#478)

Disposition of every `driver.Dialect` method for the catalog-driven
generic driver (#191): can a YAML catalog express it, and how?

**Dispositions:**
- **data** — a literal or format template; goes straight in the catalog.
- **parameterized** — a template plus declared structure (variants,
  argument order); the catalog carries both, the generic driver renders.
- **strategy-selected** — genuinely imperative; the generic driver ships
  named Go strategies and the catalog selects one (the same pattern as
  the `bulk_write.strategy` field in #191's sketch).

| Method | Disposition | Catalog expression |
|---|---|---|
| `DBType` | data | catalog `name` |
| `QuoteIdentifier` | data | quote format (`"%s"`, `[%s]`, `` `%s` ``) |
| `QualifyTable` | data | qualify format + a schema-ignored flag (sqlite-style engines) |
| `ParameterPlaceholder` | data | placeholder format (`?`, `$%d`, `@p%d`, `:%d`) |
| `BuildDSN` | data | `connection.url_template` (already in the #191 sketch) |
| `TableHint` | data | two literals keyed by `strict_consistency` (e.g. `WITH (NOLOCK)` / empty) |
| `ColumnList` | data | derived: join of quoted identifiers |
| `ColumnListForSelect` | **strategy-selected** | per-type select wrappers (spatial→WKT today); catalog declares `select_wrap: {geometry: "...", geography: "..."}` templates or names a strategy |
| `BuildKeysetQuery` | parameterized | keyset template with declared conditional sections (`max_pk`, `date_filter`) — render variants, don't string-build |
| `BuildKeysetArgs` | **parameterized (argument-order list)** | the catalog declares parameter order explicitly, e.g. `[last_pk, max_pk?, date_from?, limit]`. **This is the highest-risk surface**: arg order is where templated SQL fails at runtime instead of review time, and dialects genuinely differ here |
| `BuildRowNumberQuery` | parameterized | same pattern as keyset |
| `BuildRowNumberArgs` | parameterized (argument-order list) | same pattern as keyset args |
| `PartitionBoundariesQuery` | data | NTILE/quantile query template |
| `RowCountQuery` | data | two templates: exact + stats-based |
| `DateColumnQuery` | data | information-schema query template |
| `ValidDateTypes` | data | list of type names |
| `ValueConverters` (#477) | **strategy-selected** | catalog declares per-type conversion rules from a named set (`mixed_endian_uuid`, `bit_to_bool`, `min_datetime_null`, …); the generic driver maps rules → Go funcs |
| `AIPromptAugmentation` | data | literal text (empty for most catalogs) |
| `AIDropTablePromptAugmentation` | data | literal text |

**Summary**: 13 of 19 methods are plain data, 4 are parameterized
templates (the two pagination query/args pairs — with argument order as
the explicitly declared, conformance-pinned part), and 2 are
strategy-selected (`ColumnListForSelect`, `ValueConverters`). Nothing in
the surface is unexpressible; no Dialect method needs to change shape
before #191.

## The catalog contract: conformance `DriverCase`

A new catalog's definition of done is a passing
`conformance.DriverCase` — the same harness the four built-in engines
run:

- quoting, qualification, and placeholder expectations pin the *data*
  rows of the table above
- `PaginationCase` pins **exact SQL and argument order** for the keyset
  and ROW_NUMBER paths — written from the engine's documentation
  *before* the catalog is wired, so arg-order mistakes fail in unit
  tests rather than mid-migration
- `WriterCapabilities` / `ReaderCapabilities` (#474/#476) pin what the
  catalog declares it can do

Rule of thumb: **write the `DriverCase` first**, from the engine's
docs, then make the catalog satisfy it.
