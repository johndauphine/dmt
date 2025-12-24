# Plan: expand migrations (PG→PG, MSSQL→MSSQL), upsert/bulk modes, stability fixes

Goals
- Support same-engine migrations: PostgreSQL→PostgreSQL, SQL Server→SQL Server.
- Add upsert/bulk-insert modes for same-engine paths.
- Fix known stability issues (MSSQL metadata on older compat, DSN encoding, resume docs).
- Keep current cross-engine behavior intact.

Assumptions
- No breaking changes to existing CLI unless flagged clearly.
- Maintain single binary workflow and resume semantics.
- Timeframe: staged delivery; start with unblockers, then features, then polish/tests/docs.

Work streams & steps

1) Unblock same-engine validation
- Relax `config.validate` to allow same-source/target when an explicit mode is set (e.g., `target_mode: drop_recreate|truncate|upsert|bulk_insert_copy`) and both ends are the same engine.
- Add a guardrail: default still rejects same-to-same unless `allow_same_engine` or the target_mode is in an allowlist.
- Add unit test for validation.

2) Metadata extraction compatibility (MSSQL)
- Detect SQL Server compatibility level. If <130, fall back from `STRING_AGG … WITHIN GROUP` to a STUFF/FOR XML aggregator for indexes/FKs/checks.
- Surface a clear warning if metadata can’t be fully loaded; ensure we don’t silently skip object creation.
- Add a small test (or at least a package-level function test) for the fallback SQL.

3) DSN encoding hardening
- URL-encode user/pass/database/SPN components in `buildMSSQLDSN` / `buildPostgresDSN`.
- Add a tiny test to cover special chars (`@:/?`).

4) Define same-engine DDL strategy
- PostgreSQL→PostgreSQL:
  - Default: `drop_recreate` (matching current cross-engine), optional `truncate`, and a new `upsert`/`merge` mode using `INSERT … ON CONFLICT DO UPDATE`.
  - Optionally honor existing schema without drop (skip DDL) if a flag is set (e.g., `preserve_schema`).
- SQL Server→SQL Server:
  - Default: `drop_recreate` or `truncate`.
  - Upsert mode: staging + batch UPDATE/INSERT (avoid one giant MERGE).
  - Consider a `preserve_schema` option to skip drops and only upsert data.

5) Implement same-engine transfer paths
- Reuse existing transfer pipeline; add target behaviors:
  - PG target: upsert write path (batching—multi-row VALUES or COPY to temp + merge; avoid row-by-row).
  - MSSQL target: batch upsert via staging table + UPDATE/INSERT by PK ranges (avoid single massive MERGE).
- Ensure resume logic aligns: no truncation in upsert mode; cleanup/validation for partial writes.

6) Resume semantics & docs
- Document current checkpoint granularity (per table/partition).
- If feasible, add periodic checkpointing (every N chunks) in upsert modes; if not, clearly state cost of restarts.

7) Testing
- Add unit/small integration tests:
  - Config validation for same-engine + modes.
  - DSN encoding.
  - MSSQL metadata SQL fallback generator.
  - Simple upsert writer tests with a stub target (if possible).
- Add a smoke test script for PG→PG upsert and MSSQL→MSSQL truncate (manual if CI DBs not available).

8) Docs & examples
- Update README/examples:
  - New `target_mode` options and same-engine use cases.
  - Upsert expectations (PK required, no deletes).
  - Metadata compat warning for older SQL Server.
  - Resume granularity and behavior in upsert modes.
  - Airflow note (state-file, JSON logging) if relevant.

9) Rollout
- Ship in stages:
  - Stage 1: blockers/fixes (validation guardrails, DSN encoding, MSSQL metadata fallback).
  - Stage 2: PG→PG upsert/truncate support.
  - Stage 3: MSSQL→MSSQL truncate/upsert with batched UPDATE+INSERT.
  - Stage 4: docs/examples/tests polish.

Risks to watch
- Performance regressions if upsert implementations are per-row; ensure batching/COPY is used.
- MERGE behavior on older SQL Server/compat levels; prefer UPDATE+INSERT batching.
- Schema drift when reusing target tables; define and document expectations.
- Resume correctness in upsert modes; ensure idempotence and cleanup are safe.
