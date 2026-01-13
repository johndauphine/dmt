# Review: data-transfer-tool

Findings (ordered by severity)
- MSSQL metadata queries use `STRING_AGG ... WITHIN GROUP`, which fails on SQL Server compatibility levels < 130 (error “Incorrect syntax near '('” seen during runs). Impact: index/foreign key/check metadata is not loaded, so recreate phases may skip those objects. Source: `internal/source/pool.go` (LoadIndexes/LoadForeignKeys/LoadCheckConstraints). Proposal: detect server/compat level and fall back to a STUFF/FOR XML aggregation for 2014/2012, or gate `CreateIndexes/CreateForeignKeys/CreateCheckConstraints` when metadata query fails and surface a clearer warning.
- DSN builders do not URL-encode username/password/database fields, so credentials containing `@:/?` can produce invalid connection strings or misroute authentication. Source: `internal/config/config.go` (`buildMSSQLDSN`, `buildPostgresDSN`). Proposal: apply `url.PathEscape`/`url.QueryEscape` to user/pass/db components (and SPN) before formatting, and add a short test.
- Docs vs CLI mismatch for `profile save`: README says `--name` can be omitted and inferred, but the CLI flag is `Required`. Source: `README.md` vs `cmd/migrate/main.go`. Proposal: either make the flag optional with inference logic, or tighten the docs to match current behavior.
- Resume checkpoints are coarse (saved at end of a table/partition). An interruption mid-partition forces a full partition restart, even for keyset pagination. This is safe but expensive and not documented. Proposal: document the granularity and consider periodic checkpoints (e.g., every N chunks) with idempotent cleanup.

Notes / confirmations
- Resume path verified in practice: interrupted run `7329ae46` resumed and validated all tables successfully; no data loss observed with current changes.
- Untracked local item: `.claude/` in the repo (left untouched).
