# Code-review handoff: issues #536–#568

Thirty-three verified findings from a multi-agent code review (Claude Code), each filed
as a GitHub issue with an **🤖 AI implementation brief** in the issue body (primary files,
test pointers, exact verify command, acceptance criteria, scope guardrails). This file is
the index and coordination plan.

**How to work an issue:** open the GitHub issue, follow its AI implementation brief. Branch
`fix/<slug>` off `main` (never commit to `main`), Conventional Commit ending `(#<issue>)`,
run `make lint`, include a regression test that fails before and passes after the fix.

**Build/test commands:** unit `go test -short ./internal/<pkg>/...` · full `make test` ·
race `go test -race ./internal/<pkg>/...`. Integration tests are gated by `testing.Short()`
and need live DBs: `make test-dbs-up` (MSSQL :1433, Postgres :5432); MySQL/ClickHouse have
their own `make *-up` targets.

## ⚠️ File-conflict clusters (do not parallelize within a cluster)

Agents working these issues touch the same file — assign each cluster to one agent, or
sequence and rebase.

| Cluster | Issues | Shared surface |
|---|---|---|
| Resume flow | #536, #549, #565, #566 | `internal/orchestrator/resume.go` |
| ROW_NUMBER transfer | #538, #540 | `internal/transfer/row_number.go` + orchestrator transfer path |
| MSSQL bulk | #547, #548, #567 | `internal/driver/generic/bulk_mssql.go` |
| MySQL catalog | #542, #545 | `internal/driver/generic/catalogs/mysql.yaml` (different sections) |
| MySQL infile | #543, #544 | `internal/driver/generic/bulk_mysql_infile.go` |
| TUI Esc/tick switch | #558, #559 | `internal/tui/model_update.go` |
| Template expansion | #552, #554 | `config.LoadBytes` — same root cause, fix together |

## Suggested order

1. **P1 data loss / corruption:** #536, #537, #538+#540, #539, #545, #543
2. **P1 security / secrets:** #549, #554, #555, #542, #550
3. **P2**, then **P3**.
4. **Easy warm-ups** (mechanical, low-risk): #564 (reorder check), #567 (add `rows.Err()`), #562 (slice fix), #546 (SQL `CASE`), #560 (cache-key field).

## All issues

### P1 — data loss, corruption, or secret exposure

| # | Title | Primary file |
|---|---|---|
| [536](../../issues/536) | resume in upsert mode truncates pre-existing target tables | `internal/orchestrator/resume.go` |
| [537](../../issues/537) | delete reconciliation compares raw driver key encodings → mass hard-delete | `internal/reconcile/keys.go` |
| [538](../../issues/538) | ROW_NUMBER pagination bounded by stats-estimated RowCount drops tail rows | `internal/orchestrator/job_builder.go`, `internal/transfer/row_number.go` |
| [539](../../issues/539) | keyset MIN/MAX query failure treated as empty table → truncated table marked success | `internal/transfer/keyset.go` |
| [540](../../issues/540) | in-run retry of ROW_NUMBER jobs replays committed rows without idempotent writes | `internal/transfer/row_number.go` |
| [542](../../issues/542) | mysql FOREIGN_KEY_CHECKS toggles run on different pooled connections | `internal/driver/generic/writer.go`, `catalogs/mysql.yaml` |
| [543](../../issues/543) | LOAD DATA INFILE renders time.Time without UTC conversion | `internal/driver/generic/bulk_mysql_infile.go` |
| [545](../../issues/545) | mysql introspection never reads COLUMN_TYPE (unsigned corrupt; enum/bit degrade) | `catalogs/mysql.yaml`, `internal/typemap/mysql.go` |
| [549](../../issues/549) | Sanitized() leaves AI API key plaintext in checkpoint DB; breaks resume hash | `internal/config/types.go`, `internal/checkpoint/schema.go` |
| [550](../../issues/550) | FileState task-ID counter resets across processes → progress corruption | `internal/checkpoint/filestate_tasks.go` |
| [554](../../issues/554) | aicopilot validates AI config through template expansion → reads/leaks secrets | `internal/aicopilot/config_change_validate.go` |
| [555](../../issues/555) | TUI /wizard echoes database passwords in plaintext | `internal/tui/wizard.go` |

### P2 — correctness / robustness

| # | Title | Primary file |
|---|---|---|
| [541](../../issues/541) | chunk-size retry after partial autocommit duplicates rows (mysql) | `internal/transfer/writers.go`, `internal/driver/generic/bulk.go` |
| [544](../../issues/544) | LOAD DATA LOCAL implicitly IGNOREs bad/duplicate rows; RowsAffected unchecked | `internal/driver/generic/bulk_mysql_infile.go` |
| [546](../../issues/546) | postgres catalog misses GENERATED AS IDENTITY columns | `catalogs/postgres.yaml` |
| [547](../../issues/547) | mssql upsert MERGE `<>` under CI collation misses case-only changes | `internal/driver/generic/bulk_mssql.go` |
| [548](../../issues/548) | DBCC CHECKIDENT interpolates table name into quoted literal (injection) | `internal/driver/generic/bulk_mssql.go` |
| [551](../../issues/551) | secrets.Save ignores YAML parse errors → wipes encryption master key | `internal/secrets/config_file.go` |
| [552](../../issues/552) | secret template expansion splices raw values into YAML text | `internal/config/load.go` |
| [553](../../issues/553) | SanitizePG identifier collisions undetected; no 63-byte truncation | `internal/ident/ident.go`, `internal/target/identifiers.go` |
| [556](../../issues/556) | TUI CaptureToString deadlocks on large output; os.Stdout swap races | `internal/tui/capture.go`, `commands_run.go` |
| [557](../../issues/557) | TUI /run running-guard races → two concurrent migrations | `internal/tui/commands.go`, `commands_run.go` |
| [558](../../issues/558) | Esc quits TUI during a running migration without cancel/flush | `internal/tui/model_update.go` |
| [559](../../issues/559) | TUI runs blocking git subprocesses inside Update() every tick | `internal/tui/model_update.go`, `git.go` |
| [560](../../issues/560) | AI table-DDL cache key omits IsIdentity → stale DDL | `internal/driver/ai_typemapper_tableddl.go` |
| [561](../../issues/561) | AI finalization DDL executed with prefix-only validation (injection) | `internal/driver/ai_typemapper_finalizationddl.go` |

### P3 — smaller correctness / observability

| # | Title | Primary file |
|---|---|---|
| [562](../../issues/562) | sanitizeErrorResponse slices with lowercased offsets → panic on non-ASCII | `internal/driver/ai_typemapper_http.go` |
| [563](../../issues/563) | AI type-cache saved with non-atomic unsynchronized WriteFile | `internal/driver/ai_typemapper_cache.go` |
| [564](../../issues/564) | GetLastSyncTimestamp checks NullString.Valid before scan error | `internal/checkpoint/state.go` |
| [565](../../issues/565) | Resume() summary/tuning history use RowCount estimates (#498 regression) | `internal/orchestrator/resume.go` |
| [566](../../issues/566) | transient preflight failure during resume marks run failed → unresumable | `internal/orchestrator/resume.go`, `internal/checkpoint/runs.go` |
| [567](../../issues/567) | mssql staging/spatial introspection missing rows.Err() checks | `internal/driver/generic/bulk_mssql.go` |
| [568](../../issues/568) | registry claims TUI confirms backup interactively but no surface exists | `internal/command/registry.go` |
