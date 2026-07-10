# Epic #679 handoff: strict_consistency parallel readers for non-PostgreSQL sources

Epic [#679](../../issues/679) extends the PG-only strict-parallel work (#662/#672, #663/#673) to
MySQL/MariaDB and SQL Server via engine-native mechanisms, and re-enables partitioning/composite
parallelism where a strategy's stable view spans jobs. SQLite is explicitly out of scope. Each
child issue carries a full spec (evidence at `main` 83e7f64f file:line, design steps, tests,
acceptance criteria); this file is the coordination index plus the environment/measurement
knowledge that doesn't fit in an issue. Remove this file when the epic closes (precedent: #670).

**Stakes (measured 2026-07-10, SO2010 19.3M rows, mssql→pg, same config, strict toggled):**

| Path | Relaxed | Strict today | Penalty |
|---|---|---|---|
| Host-side (Docker proxy-bound) | 598K rows/s | 614K rows/s | ~0% — the ~222MB/s host↔VM proxy saturates with one reader and masks the clamp |
| In-VM (headline methodology) | 1,008K rows/s (19s) | 877K rows/s (22s) | **~13%** |

Single-dominant-table migrations lose far more (PG analog measured **+34%** restored). Never
size this work from host-side runs — see "Docker Host↔VM Proxy Ceiling" in `docs/BENCHMARKS.md`.

## How to work an issue

Open the GitHub issue and follow its spec. Branch `feat/<slug>` (or `refactor/` for #680) off
`main` — never commit to `main`. Conventional Commit ending `(#<issue>)`. Run `make lint`, add
tests that fail before and pass after, and run `codex review --base main` before declaring the
PR ready. Revalidate all file:line refs against current `main` before implementing.

**Build/test:** unit `go test -short ./internal/<pkg>/...` · full `make test` · race
`go test -race ./internal/transfer/...`. Integration tests gate on `testing.Short()` and need
live DBs: `make test-dbs-up` (MSSQL :1433 `sa/TestPass2024`, Postgres :5432); fixtures via
`scripts/load-fixture-so2010-minimal.sh [--source mssql|pg|mysql]`.

## Sequencing and conflict clusters

`#680 → {#681, #682} → #683 → #684 → #685`

| Order | Issue | Depends on | Shared-surface cautions |
|---|---|---|---|
| 1 | [#680](../../issues/680) strategy seam (pure refactor) | — | Blocks everything: creates the `internal/transfer` strategy registry + `strict_parallel_strategy` catalog field + conformance pins |
| 2a | [#681](../../issues/681) MySQL `lock_window_sessions` | #680 | Parallelizable with #682; both register in the same strategy map and touch keyset-plan messaging — trivial rebase, otherwise disjoint (mysql.yaml, preflight_mysql.go) |
| 2b | [#682](../../issues/682) MSSQL `table_shared_lock` | #680 | Counterpart of #681 (mssql.yaml, preflight_mssql.go) |
| 3 | [#683](../../issues/683) MSSQL `database_snapshot` (migration scope) | #682 | Shares `preflight_mssql.go` with #682 and relaxes `internal/config/validation.go` scope gate — sequence after #682 |
| 4 | [#684](../../issues/684) unclamp partitioning + composite | #680 (PG half), #683 (partition half) | `composite_parallel.go`, `job_builder.go`, `transfer.go`; PG-composite half can start right after #680 |
| 5 | [#685](../../issues/685) proofs + docs + diagnosis | #681, #682 (#683 for snapshot variant) | Closes the epic with evidence; mirrors `internal/transfer/consistency_snapshot_pg_integration_test.go` (#678) |

## Key code seams (main 83e7f64f — revalidate)

- `internal/transfer/source_snapshot.go:28` — `sourceQueryerFactory`: the per-worker pinned-queryer plumbing every strategy reuses; `:232-258` — the engine string switches #680 replaces.
- `internal/transfer/keyset.go:181-198` — `strictKeysetReaderPlan` (the 1-reader clamp); `:100-106` — factory wiring to replicate in composite.
- `internal/transfer/composite_parallel.go:48` — unconditional strict bail-out (even PG epoch); `:55` — single-queryer gap.
- `internal/orchestrator/job_builder.go:330-336` — partitioning disabled for table-scoped strict; `internal/transfer/transfer.go:36-44` — partition guard; `:111-143` — strict snapshot begin + row-count persistence.
- `internal/config/validation.go:161-170` — migration scope hard-locked to PG (relaxed by #683).
- `internal/driver/generic/catalog.go` — named-strategy fields to imitate (`dsn_strategy` :72, `validate_strategy` :84, `preflight_strategy` :60); `catalogs/mssql.yaml:49-51` — NOLOCK strict/relaxed table hints (unchanged by this epic).
- `internal/driver/conformance/driver.go:469-471` — `CheckReaderCapabilities` to extend.
- `internal/pool/factory.go:17` — `NewSourcePool` (#683 builds a second pool pointed at the snapshot DB).

## Mechanism gotchas (verified, not in git history)

- **go-sql-driver/mysql never sends `WITH CONSISTENT SNAPSHOT`** from `BeginTx`. Reader sessions
  must be raw `*sql.Conn`s issuing `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` then
  `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` via `ExecContext`, all inside the
  coordinator's `LOCK TABLES <t> READ` window. Plain `START TRANSACTION` snapshots lazily at
  first read — outside the window, guarantee void.
- **`LOCK TABLES` implicitly commits** any open transaction on that session: the MySQL
  coordinator conn must carry none. Timeout knob: `SET SESSION lock_wait_timeout` (error 1205);
  privilege errors 1044/1142 → loud single-reader fallback.
- **go-mssqldb rejects `TxOptions.ReadOnly`** (existing comment at `source_snapshot.go:245-249`).
- **Today's MSSQL serializable strict is NOT one point-in-time** — progressive key-range locks
  admit commits ahead of the scan within the initial bounds. #682's frozen table (TABLOCK +
  HOLDLOCK on a coordinator txn, `SET LOCK_TIMEOUT`, error 1222 on timeout) is a strict upgrade.
- **Database snapshot spike (mssql-bench, SQL Server 2022 Linux, Developer)**: snapshot of the
  9GB StackOverflow2010 created in **96ms**; post-snapshot writes invisible in it; sparse `.ss`
  file = 16KB on disk until COW. Syntax needs logical file names from `sys.master_files`.
  In-container client: `/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P TestPass2024`.
- **Edition matrix**: database snapshots on all editions since 2016 SP1 (Enterprise-only
  before); `SERVERPROPERTY('EngineEdition') = 5` (Azure SQL Database) = unsupported → preflight
  finding, fail closed for migration scope.
- **PG-vs-MSSQL resume asymmetry**: PG's exported snapshot dies with the lead session; an MSSQL
  database snapshot survives a dmt crash and can be reused on resume of the same run.

## Benchmark reproduction (for #685's before/after)

Local containers: source `mssql-bench` :1433 (`sa/TestPass2024`, StackOverflow2010), target
`postgres-target` :5433 (`postgres/PostgresPassword123`, fresh DB per run). Pin the config so
runs are comparable — `runtime_tuning: false`, `workers: 8`, `write_ahead_writers: 4`,
`chunk_size: 50000`, `parallel_readers: 4`, `read_ahead_buffers: 4`, `max_partitions: 8`,
`max_source_connections: 20`, `max_target_connections: 30` — and toggle only
`strict_consistency`. Compare the `Transfer complete:` rows/sec line.

In-VM (required for honest numbers): `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o dmt-linux ./cmd/migrate`,
run it in a container attached to the target's network plus the default bridge
(`docker run -d --name dmt-bench --network <pg-net> -v <dir>:/bench alpine:3 sleep 3600 &&
docker network connect bridge dmt-bench`); reach mssql-bench via its bridge IP
(`docker inspect mssql-bench` → 172.17.x.x), postgres by container name.

## Done when

All six children closed; in-VM strict-vs-relaxed gap on SO2010 within benchmark noise;
`docs/BENCHMARKS.md` records before/after per the in-VM methodology; this file deleted.
