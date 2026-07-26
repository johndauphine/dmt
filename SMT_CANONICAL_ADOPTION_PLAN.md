# SMT Public DDL Adoption

**Status**: completed 2026-07-26
**SMT dependency**: `github.com/johndauphine/smt v1.4.0`

## Outcome

DMT consumes SMT as a normal versioned Go module and still ships as one
statically linked executable. SMT is the single production renderer for the
target-schema DDL that DMT currently emits. DMT retains migration policy,
database discovery, dependency scheduling, idempotency checks, execution,
retry/error handling, and data transfer.

The public SMT boundary has two layers:

- `github.com/johndauphine/smt/schema/canonical` owns cross-dialect type
  normalization for PostgreSQL, SQL Server, MySQL, SQLite, and ClickHouse.
- `github.com/johndauphine/smt/schema` owns deterministic create, side-object,
  and schema-evolution rendering.

Unknown or unsupported schema features return SMT's typed policy errors. DMT
does not fall back to a second local or AI target-DDL renderer.

## Production ownership

| DMT operation | SMT v1.4.0 API |
|---|---|
| Create schema/database and table | `PlanCreate` |
| Create inline or standalone primary key | `PlanCreate` / `CreatePrimaryKey` |
| Create secondary/unique index | `CreateIndex` |
| Create foreign key | `CreateForeignKey` |
| Create check constraint | `CreateCheckConstraint` |
| Add column | `AddColumn` |
| Relax column nullability | `AlterColumnNullability` |
| Widen column type | `AlterColumnType` |
| Drop table | `DropTable` |
| Truncate table | `TruncateTable` |

SMT also exports drop-schema, drop-column, drop-index, drop-constraint,
set/drop-default, and named-unique-constraint rendering. DMT does not currently
schedule those operations; adopting them requires explicit DMT policy and
metadata rather than another renderer.

## Execution contract

SMT evolution methods return `schema.Batch`. DMT executes each statement
verbatim and in order:

- `RequiresSingleConnection` pins setup, operation, and cleanup to one
  physical connection.
- Required failures stop the batch.
- Failure cleanup runs on the same connection with an independent bounded
  context.
- `BestEffortStatementIndexes` are logged and do not fail the batch.
- Cleanup errors are logged without replacing the primary error.

This is required for MySQL foreign-key-check toggles, SQLite foreign-key
pragmas and sequence cleanup, and future multi-statement dialect operations.

## Metadata and identifier fidelity

DMT passes base catalog type names plus structured metadata to SMT. MySQL
`COLUMN_TYPE` is parsed for unsigned/zerofill, `TINYINT(1)`, `BIT(N)`, and
escaped `ENUM`/`SET` members. MySQL `TIME`, `DATETIME`, and `TIMESTAMP`
fractional-second precision is carried as an explicit pointer so FSP 0 remains
distinguishable from an unspecified value. A declaration such as
`varchar(100)` is never misclassified as a raw catalog type.

PostgreSQL identifiers are pre-normalized with DMT's established sanitizer
before calling SMT. Target schema normalization and catalog probes share the
same physical identity, including default `public` suppression and custom
schema names.

## Deliberately DMT-owned SQL

The SMT ownership boundary is migrated target-schema DDL. These operational or
internal statements remain in DMT:

- temporary staging tables and staging-column rewrites;
- identity/sequence reseeding;
- SQL Server database snapshots;
- the private SQLite checkpoint/state schema and its migrations;
- database tuning and session-control commands used by the transfer engine.

They manage DMT runtime state or data-transfer mechanics and are not a second
renderer for the migrated target schema.

## Verification

The adoption is guarded by:

- public-boundary tests that forbid local catalog target-DDL templates and
  production imports of the legacy renderer;
- exact create and side-object parity tests for all five target dialects;
- evolution batch ordering, connection-affinity, cleanup, and best-effort
  tests;
- PostgreSQL create/evolution identifier and schema parity tests;
- MySQL full-type metadata regression tests;
- unit, race, lint, build, live integration, and 12-pair cross-engine gates.

`internal/typemap/ddl` remains only as a test oracle for pre-SMT parity. It has
no production consumer.
