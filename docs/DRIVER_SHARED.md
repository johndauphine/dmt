# Shared Driver Refactor

Issue #338 tracks the refactor to reduce repeated driver infrastructure before
returning to catalog-driven engine work. The goal is to make database engines
easier to add and maintain by sharing common code while preserving each
engine's native behavior.

## Goals

- Centralize repeated SQL-shape, batching, validation, reader, and preflight
  scaffolding when the behavior is identical across engines.
- Keep concrete drivers responsible for database-specific semantics and fast
  paths.
- Make each refactor slice small enough to review and test without changing
  migration behavior accidentally.
- Keep production and test code in separate files, with human-readable modules
  that avoid growing beyond the project's readability target.

## Package Boundary

Shared driver code belongs under `internal/driver/shared`.

Concrete drivers may import:

- `internal/driver`
- `internal/driver/shared`

The base `internal/driver` package must not import `internal/driver/shared`.
This keeps the core interfaces independent from optional helper code and avoids
package cycles as the shared package grows.

## What Belongs In Shared

Good candidates for `internal/driver/shared` are helpers that are useful across
multiple drivers and do not hide engine-specific behavior:

- quoted identifier lists built from a dialect's existing quoting rules
- multi-row placeholder groups built from a dialect's placeholder rules
- row-shape validation and argument flattening
- ordered primary-key scan statements for engine-agnostic key-set comparison
- bounded primary-key delete predicates for reconciliation-style cleanup
- raw `database/sql` exec/query wrappers where drivers already expose the same
  behavior
- exact `COUNT(*)` query construction and execution, including optional table
  hint suffixes
- fast-count-then-exact-count fallback control flow
- sampled column-value and sample-row scan loops after concrete readers own the
  dialect-specific query strings
- partition-boundary result scanning when concrete readers already return the
  same row shape
- reader channel lifecycle, pagination loops, query timing, and batch `Done`
  semantics when query construction is already delegated to concrete dialects
- batch slicing and effective batch-size selection
- stable preflight finding builders, connection-check plumbing, backup-ack
  gating, and pool-headroom decisions after concrete drivers own the probe SQL
- reader control-flow helpers when pagination semantics are already delegated
  to the dialect or concrete driver

Shared helpers should stay small and composable. Prefer several focused
functions over a generic base driver type.

Some existing `internal/driver` interfaces already contain duplicated helper
behavior, such as `Dialect.ColumnList`. Later PRs may retire or narrow those
methods once concrete drivers are safely routed through shared helpers; those
interface changes should be called out explicitly in the PR that makes them.

## What Stays In Concrete Drivers

The following behavior should remain in the database-specific packages unless a
later PR proves that the shared form is clearer and behavior-preserving:

- DSN construction and connection setup
- native write paths such as PostgreSQL COPY and SQL Server bulk copy
- upsert syntax and change-detection SQL
- type conversion and binary/text handling
- engine-specific identifier, placeholder, schema, and table qualification
  rules
- parameter limits, packet limits, and runtime target probes
- transaction boundaries where engines have different safety or performance
  requirements
- preflight probes that require engine-specific catalog queries

## PR Sequence

1. Add `internal/driver/shared` with tested SQL-shape primitives and this
   architecture note. Do not migrate concrete driver behavior in this first
   slice.
2. Add reconciliation-oriented SQL-shape helpers for ordered key scans and
   bounded key deletes so delete propagation (#351) can reuse one tested shape
   across target engines.
3. Migrate one low-risk writer path, likely MySQL or SQLite multi-row INSERT
   construction, to the shared helpers.
4. Extract shared batch slicing and row-shape validation once the first writer
   migration proves the API shape.
5. Centralize raw writer helpers and row-count control flow where concrete
   drivers already behave identically.
6. Centralize reader sampling scan loops once each concrete reader still owns
   its engine-specific casts and query shape.
7. Centralize partition-boundary result scanning for engines whose partition
   queries return the same row shape.
8. Move the closest reader streaming paths onto shared pagination control flow
   once the dialect query builders already own the database-specific SQL.
9. Centralize preflight framework helpers where driver-specific probe SQL stays
   concrete.
10. Revisit catalog-driven engine work after the native driver code is drier, so
   any configuration or catalog approach builds on a cleaner foundation.

## Acceptance Criteria For Each Slice

- Behavior remains unchanged unless the PR explicitly says otherwise.
- Tests cover the shared helper and at least one migrated call path when a
  concrete driver starts using it.
- Documentation is updated when the shared boundary changes.
- The change avoids new public config surface unless the issue explicitly calls
  for it.
