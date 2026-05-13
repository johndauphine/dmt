# Changelog

All notable changes to this project will be documented in this file.

> CHANGELOG entries between v3.0.0 and v3.60.0 (Jan 2026 → May 2026)
> weren't maintained; the per-PR commit messages are the canonical
> record for that window. Tracking #233 to formalize CHANGELOG
> discipline going forward.

## [Unreleased]

### Added

- **Preflight health checks** (#228). New `TaskPreFlight` phase 0
  runs BEFORE schema extraction or DDL: connection ping, supported
  DB version (PG 12+, MSSQL 2016+, MySQL 5.7+/MariaDB 10.3+),
  encoding/collation, pool headroom (`max_connections - current_connections
  ≥ workers + 5`), per-driver privilege probes via information-schema
  introspection, and a backup-acknowledgment guard that requires
  `--confirm-backup` when `drop_recreate` mode runs against a
  non-empty target schema. Misconfigured environments now fail in
  ~0.5s with an actionable remedy instead of dying minutes into a
  partial run. Unified into a single `dmt preflight` subcommand
  (with `health-check` kept as an alias for existing Airflow/k8s
  probes). Opt-out per check via `--skip-preflight=name1,name2`
  or `--skip-preflight=all`.

- **Observability surface** (#229). Three coordinated surfaces, all
  off by default, all sharing the same dimension names (`run_id`,
  `phase`, `table`, `source_db`, `target_db`):
  - `--log-format=json` now emits structured fields on every line
    via slog-style base attributes. Existing printf-style log calls
    work unchanged; new `logging.Event` API for hot-path structured
    calls.
  - `--metrics-addr=:9090` binds a Prometheus `/metrics` endpoint
    with 11 metrics (`rows_total`, `bytes_total`, `errors_total`,
    `retries_total`, `chunk_duration_seconds`, `phase_duration_seconds`,
    `writer_queue_depth`, `writers_active`, `runtime_tuning_adjustments_total`,
    `ai_fallback_total`, `migration_info`). Per-run-labeled gauges
    cleared on RunComplete to bound cardinality.
  - `--otel-endpoint=URL` exports OTLP HTTP traces — one root span
    per run, child spans per orchestrator phase. Chunk milestones
    emit as span events on the phase span rather than separate spans
    (avoids flooding tracing backends on 100M-row migrations).
  - New `docs/OBSERVABILITY.md` + Grafana dashboard JSON.

- **CI gating on every PR** (#230). `.github/workflows/ci.yml` runs
  `go build`, `go vet`, unit tests, `go test -race`, golangci-lint
  v2.12.2 (only-new-issues — pre-existing lint debt isn't a PR
  blocker), and govulncheck v1.1.4. `.github/workflows/integration.yml`
  spins up MSSQL 2022 + Postgres 16 service containers, loads the
  SO2010-minimum fixture, runs a real `dmt mssql → postgres` migration,
  and asserts row-count parity. dmt logs uploaded as artifacts on
  failure. Local reproduction documented in `CONTRIBUTING.md`
  (`make integration-test` runs the exact same script CI does, with
  isolated state at `./.dmt-ci-state/` so the operator's
  `~/.dmt/migrate.db` is never touched). govulncheck is currently
  informational (`continue-on-error: true`) until stdlib `net`
  findings have a released Go toolchain fix.

- **Release policy + process** (#233). New `VERSIONING.md` documents
  SemVer triggers (MAJOR/MINOR/PATCH), stability commitments
  (stable vs experimental fields), and the deprecation cycle for
  breaking changes. New `RELEASE.md` documents how to cut a release,
  release cadence, the hotfix path, and pre-release/RC tagging.
  Goes binding at v1.0.0; best-effort before.

- **Production migration runbook + failure-mode catalog** (#234). New
  `docs/RUNBOOK.md` is the one-document operational reference for an
  SRE taking a dmt migration to green: pre-run checklist (privileges,
  disk, encoding, backup ack, `dmt preflight` walk-through), a
  symptom-keyed failure-mode catalog covering preflight / connection /
  privilege / disk / deadlock / hung-shutdown / validation /
  partial-exit-3 / config-hash / stale-resume / torn-state-file /
  MySQL TLS / content-divergence (one entry per closed issue from the
  production-readiness epic), a one-paragraph pointer at each of the
  three observability surfaces (cross-links to `OBSERVABILITY.md`),
  the operational-command reference (`run` / `resume` / `preflight` /
  `status` / `history` / `validate` / `analyze` / `profile`), and an
  emergency-procedures section (graceful kill, SIGKILL escalation,
  target-stuck-in-bad-state recovery, downstream-data-already-consumed
  triage). Exit-code table included for automation predicates.
  Cross-links `RESTARTABILITY.md`, `OBSERVABILITY.md`, `PRIVILEGES.md`
  (sibling, #232), `FIXTURES.md`, and `CONTRIBUTING.md` rather than
  duplicating them.

- **Production migration runbook + failure-mode catalog** (#234). New
  `docs/RUNBOOK.md` is the one-document operational reference for an
  SRE taking a dmt migration to green: pre-run checklist (privileges,
  disk, encoding, backup ack, `dmt preflight` walk-through), a
  symptom-keyed failure-mode catalog covering preflight / connection /
  privilege / disk / deadlock / hung-shutdown / validation /
  partial-exit-3 / config-hash / stale-resume / torn-state-file /
  MySQL TLS / content-divergence (one entry per closed issue from the
  production-readiness epic), a one-paragraph pointer at each of the
  three observability surfaces (cross-links to `OBSERVABILITY.md`),
  the operational-command reference (`run` / `resume` / `preflight` /
  `status` / `history` / `validate` / `analyze` / `profile`), and an
  emergency-procedures section (graceful kill, SIGKILL escalation,
  target-stuck-in-bad-state recovery, downstream-data-already-consumed
  triage). Exit-code table included for automation predicates.
  Cross-links `RESTARTABILITY.md`, `OBSERVABILITY.md`, `PRIVILEGES.md`
  (sibling, #232), `FIXTURES.md`, and `CONTRIBUTING.md` rather than
  duplicating them.

### Fixed

- **ROW_NUMBER pagination resume safety** (#227, #266, #267). Four
  layered correctness bugs that previously caused silent data loss
  or duplication on resume of composite/varchar-PK tables, all
  closed:
  1. Resume preflight was partition-blind: large ROW_NUMBER tables
     have partition-level checkpoints, but the truncation decision
     checked only the table-level checkpoint and used
     `SupportsKeysetPagination()` (which excludes ROW_NUMBER) to
     gate partition-awareness. Fixed: use `HasPK()` to match the
     actual partitioning decision in `job_builder.go`, and clear
     partition-level checkpoints whenever a target is truncated.
  2. Per-table checkpoints lag acked rows by up to
     `checkpoint_freq - 1` chunks. On crash + resume those chunks
     would replay, and a plain INSERT would crash on duplicate PK.
     Fixed: when the orchestrator dispatches a job as part of a
     Resume() call, the writer routes through driver-specific
     idempotent paths (PG: temp staging + COPY + `INSERT...SELECT
     ON CONFLICT DO NOTHING`; MySQL: `ON DUPLICATE KEY UPDATE
     pk_col = pk_col` — NOT `INSERT IGNORE`, which masks
     data-conversion errors; MSSQL: per-partition staging + insert-only
     `MERGE`). Replayed rows become silent no-ops; non-resume
     `drop_recreate` runs are unchanged.
  3. Stale-checkpoint clears in resume preflight were silently
     swallowing errors, so a failed clear could leave the system in
     the exact pre-#227 state. Fixed: clears are now fatal — the
     run aborts with `ConfigError` if either `ClearTransferProgress`
     or `ClearPartitionTransferProgress` fails.
  4. Resume preflight now also verifies that partition progress
     records are consistent with the partition task graph (#267) —
     a stale partition_id whose task no longer exists in the run
     surfaces as a resume-time error rather than silent skip.

### Breaking Changes

- **Partial migrations exit non-zero by default** (#248). When one or
  more tables fail to transfer, `dmt run` and `dmt resume` now exit
  with code 3 (`TransferError`) instead of 0. The run is still
  recorded in state as `partial` and the JSON result still includes
  the per-table breakdown, so consumers that inspect `result.status`
  and `result.failed_tables` see the same information they did before
  — only the exit code changed. Set `migration.allow_partial: true`
  in your config to restore the pre-#248 behavior (exit 0 on
  partial). This closes a silent automation hazard where Airflow/k8s
  jobs treated partial migrations as successful.

- **MySQL TLS defaults to require+verify instead of preferred** (#252).
  When `ssl_mode` is unset (or set to an unrecognized value), the
  MySQL DSN now uses `tls=true` (TLS required, CA + hostname
  verification) instead of the pre-#252 `tls=preferred` (TLS
  attempted, silent plaintext fallback). The MySQL driver's default
  and the setup wizard's default both moved from `"preferred"` to
  `"require"`. Operators who still want downgradeable TLS can
  explicitly set `ssl_mode: preferred` — that branch maps to
  `tls=preferred` as before, but it's now an explicit opt-in, not a
  silent default. Operators connecting to non-TLS MySQL instances
  (e.g. local Docker test containers) must set `ssl_mode: disable`.
  Additionally, `verify-ca` no longer maps to `skip-verify` (no
  verification — the direct inverse of the operator's intent); it
  now maps to `tls=true` (verify CA + hostname), which is strictly
  safer.

- **Validation no longer reports false-positive success on missing
  evidence** (#253). Three policy gaps closed:

  1. Row-count validation **timeouts** were warnings, not failures.
     A `COUNT(*)` that exceeded `ValidationTimeout` (30s default)
     was reported as a warning and the run could still finish
     "successful." Now timeouts fail the run by default; opt out
     via `migration.validation.fail_on_timeout: false`.
  2. **Estimated-count mismatches** were warnings, not failures.
     When the exact `COUNT(*)` timed out on one or both sides dmt
     fell back to estimated counts; if those disagreed, the
     discrepancy logged a warning. Now mismatches under the
     estimated-counts fallback fail the run by default; opt out
     via `migration.validation.fail_on_estimate_mismatch: false`.
  3. **MSSQL exact-count ignored `strict_consistency`**. The
     `GetRowCountExact` query always included `WITH (NOLOCK)`,
     silently overriding any operator who explicitly asked for
     read-committed counts. `strict_consistency: true` now drops
     the NOLOCK hint.

  Combined with #248, this closes both layers of silent acceptance
  between a broken migration and a green status.

### Removed

- **Kerberos auth descoped pending a verifiable test environment**
  (#251). The README and example configs advertised Kerberos /
  SPNEGO support for SQL Server and PostgreSQL, but the runtime
  drivers go through `Dialect.BuildDSN(..., cfg.DSNOptions())` and
  `DSNOptions()` doesn't carry `auth`/`keytab`/`realm`/`SPN`, so an
  `auth: kerberos` config silently fell back to password auth.
  `examples/config-mssql-to-pg-kerberos.yaml` and
  `examples/config-pg-to-mssql-kerberos.yaml` are removed, the
  README no longer claims Kerberos as a supported auth method, and
  config-load now rejects `auth: kerberos` with an error pointing at
  #251. The DSN-builder code path that would emit a correct
  Kerberos DSN is left in place (with its tests) for the eventual
  re-enable.

### Fixed

- **Date-based incremental sync now works on the file backend**
  (#255). Pre-#255 the file backend's `GetLastSyncTimestamp` and
  `UpdateSyncTimestamp` were explicit no-ops, so date-based
  incremental sync configured against the Airflow/k8s-recommended
  file backend silently degraded to a full-table copy every run.
  The cost scaled linearly with table size instead of delta size —
  the inverse of what incremental sync is supposed to deliver.
  Timestamps now persist in a `sync_timestamps:` map in the YAML
  state file, keyed by (source schema, table, target schema) to
  match the SQLite backend's UNIQUE constraint. Writes go through
  the crash-safe `atomicWriteFile` path established in #254.

- **File-state writes are now crash-safe** (#254). The YAML file
  backend used by the Airflow/k8s headless mode previously wrote via
  `os.WriteFile`, which is not atomic: a SIGKILL, OOM-kill, or pod
  eviction partway through the write would leave a truncated YAML
  file that fails to parse on resume — exactly the failure mode the
  file backend was added to handle. `FileState.save()` now uses the
  standard tmp + fsync + rename + dir-fsync pattern, so the state
  file is always either the pre-write or post-write version, never
  torn. Also `MkdirAll`s missing parent directories with 0700 perms.

- **Reader goroutines no longer leak when writers fail mid-transfer**
  (#250). In both the keyset and ROW_NUMBER pagination paths, reader
  goroutines used bare `chunkChan <- result` sends. On writer
  failure the consumer would break out of its loop, but blocked
  readers would never unblock — leaking the reader goroutines (and
  the close-channel goroutine waiting on them) and holding source
  DB cursors until the process exited. Readers now run under a
  per-transfer child context and send via a select that also
  watches `ctx.Done()`, and the consumer cancels that context after
  it stops draining. Aborts in-flight source-side queries via
  `QueryContext` as well.

## [4.0.0] - 2026-05-12

Major release: the **AI-optional architecture epic (#167)** ships
end-to-end. dmt no longer requires an AI provider for any migration
path; deterministic catalogs cover type mapping, error diagnosis, DB
tuning, runtime parameter adjustment, and smart-config selection. AI
remains available as an opt-in enhancement for vendor-specific edge
cases (e.g. Oracle `hierarchyid`, MSSQL `geography`).

### Breaking Changes

- **Setup defaults flipped**: `dmt init-secrets` no longer seeds an
  AI provider section; `dmt setup` wizard's AI prompt defaults to
  "skip" rather than "configure". Existing users with AI configured
  in `~/.secrets/dmt-config.yaml` continue to work; the change is
  visible only on fresh installs. (#174)
- **Driver interface additions**: anyone implementing a custom
  `driver.Driver` must now provide `HardChunkLimit(avgRowBytes int64) int`
  and `ProbeTarget(ctx, db) TargetProbe`. Built-in drivers updated.
  (#166)
- **Removed exported types**:
  - `driver.AIErrorDiagnoser`, `driver.GetAIErrorDiagnoser`,
    `driver.NewAIErrorDiagnoser` (replaced by deterministic
    `errordiag` package + dispatch helpers). (#173)
  - `dbtuning.AIQuerier`, `dbtuning.AITuningAnalyzer`,
    `dbtuning.NewAITuningAnalyzer`. (#172)
- **`dbtuning.Analyze` signature**: removed the trailing `aiMapper interface{}`
  parameter. Callers that previously passed `nil` should drop the
  argument. (#172)

### Added

- **Deterministic error diagnosis catalog** with 76 regex-matched
  patterns across PG (26), MSSQL (25), MySQL (25). Replaces the
  AI-driven `ai_errordiag.go` (382 LOC removed). Catalog growth via
  the unmatched-error log signal. (#173)
- **Deterministic DB tuning catalog** with 30 settings across PG (11),
  MSSQL (9), MySQL (10). Replaces the AI-driven `ai_analyzer.go`
  (482 LOC removed). Each setting has a hardcoded SQL query plus a
  pure Go rule producing recommendations. (#172)
- **MySQL `@@max_allowed_packet` probe** drives the `chunk_size` hard
  cap so MySQL targets with default 4MB packet no longer crash on
  wide rows. Probe-derived cap threaded through to the runtime
  controller (`MaxChunkSize`/`MinChunkSize`) so growth rules can't
  exceed the packet limit mid-transfer. (#166)
- **Deep validation passes** layered after the existing row-count
  check: `validation.mode: null_parity` adds per-column NULL count
  parity; `validation.mode: sample` adds value-level row comparison
  via a deterministic MD5(pk)-ordered sample. Default mode unchanged
  (`count_only`); cross-DB canonicalizer normalizes types so source
  and target produce identical bytes for the same value. (#226)
- **CI-loadable fixture loaders**: `make load-fixture-pgbench` +
  `make load-fixture-so2010-minimal`. SO2010-minimal synthesizes
  byte-for-byte-compatible schema and seed for the public Brent Ozar
  dataset; `make test-fixtures-load` chains both in ~5s. Manual
  procedures for full SO2010/SO2013/WWI documented in
  `docs/FIXTURES.md`. (#178)
- **Tier 1 exact-identity workload classifier** for the deterministic
  tuner: matches historical runs by 8-tuple identity
  (source+target host/port/db/schema) before falling back to regime
  filtering. Improved R² on stable-workload runs. (#215)
- **Setup wizard `--with-ai` flag** + `secrets.GenerateTemplateWithAI()`
  for users who explicitly opt into AI features at template creation.
  Default `dmt init-secrets` produces an AI-free file. (#174)
- **Optional AI Enhancements** README section reframes AI as opt-in
  rather than required; Quick Start at the top of the README is
  explicit about no AI being needed. (#174)
- **Pattern of `codex review --base main` after every substantial PR**
  caught real bugs across the epic (~25 P1/P2 findings between Codex
  and Copilot — schema sanitization, integer-division-to-zero on
  packet caps, MySQL parameter syntax mismatches, UTF-8 rune-vs-byte
  handling, `time.Duration` YAML parsing, unbounded goroutine fan-out,
  more). See per-PR commit messages.

### Changed

- **Runtime parameter adjustment** is fully deterministic. The
  field `migration.ai_adjust` is preserved for config backward
  compatibility but no longer involves AI; it controls a rule-based
  controller (4 deterministic rules). A rename to `runtime_tuning`
  with deprecation cycle is tracked in #211.
- **Regression-tier tuner** learns `parallel_readers` and
  `read_ahead_buffers` in addition to `write_ahead_writers` and
  `chunk_size`. Argmax now skips uncovered cube-corner cells
  (refuses to extrapolate beyond training-data support). (#219, #221)
- **MySQL text/blob tiers** preserved via canonical `MaxBytes`,
  fixing nvarchar(max)/text/varbinary(max) → LONGTEXT/LONGBLOB. (#196, #206)

### Removed

- **AI-driven error diagnoser** (`internal/driver/ai_errordiag.go`).
  Error messages no longer egress to third-party LLMs, closing a
  PII-leak surface. (#173)
- **AI-driven DB tuning analyzer** (`internal/driver/dbtuning/ai_analyzer.go`).
  Server-configurable settings no longer require LLM round-trips. (#172)
- **AI smartconfig prompt machinery** (~1500 LOC across earlier PRs
  during the epic): replaced by the deterministic tuner in
  `internal/tuning/`.

### Production-Readiness Tracking

The production-readiness epic #236 was opened and made progress on:
- **#178** CI-loadable fixtures (this release)
- **#226** deep validation passes (this release; full row-hash mode
  reserved for a future iteration after the cost analysis concluded
  it didn't justify its bandwidth)

Remaining work: #227 (ROW_NUMBER resume safety, P0), #228 (preflight
health checks, P1), #229 (structured logs + Prometheus + OTLP, P1),
#230 (CI gating, P1, unblocked by this release's #178), and others.

### Closed-as-not-pursued

- **#244** — same-engine in-DB row hashing was filed during the
  #226 PR and closed after analysis: dmt's headline use case is
  cross-engine, where row-level proof is fundamentally Go-side;
  same-engine migrations have first-party tools (pg_dump, native
  backup-restore) that already provide higher-fidelity guarantees
  than a SQL row-hash sum.

## [3.0.0] - 2026-01-13

### Breaking Changes
- **Renamed environment variable** - `DATA_TRANSFER_TOOL_MASTER_KEY` → `DMT_MASTER_KEY` for profile encryption
- **Default Slack username** - Changed from `data-transfer-tool` to `dmt`

### Changed
- **Updated TUI branding** - New ASCII art logo and version display
- **Centralized version** - Version now managed in `internal/version` package
- **Updated all references** - Makefile, README, docs, and config examples now use `dmt`

### Added
- **Tests for notify package** - 37 test cases covering Slack notifications, error handling, and formatting
- **Tests for progress package** - 32 test cases covering tracker, JSON reporter, and throttling

### Maintenance
- Removed unused dependencies (`lib/pq`, `go-sqlmock`)
- Promoted `gopsutil/v3` to direct dependency

## [2.27.0] - 2026-01-12

### Changed
- **Pluggable driver architecture** - Consolidated Dialect into driver package, eliminated switch statements
- **Project renamed** - `mssql-pg-migrate` → `dmt`

## [2.26.0] - 2026-01-12

### Changed
- **AI-first type mapping** - AI determines all type mappings with aggressive caching
- Static mappings only used as fallback when AI fails

## [2.25.0] - 2026-01-12

### Added
- **Analyze command** - `dmt analyze` for database analysis and config suggestions
- Auto-tuned performance parameters based on system specs
- AI-suggested alternatives with reasoning

## [2.24.0] - 2026-01-12

### Added
- **AI-powered type mapping** - Support for Claude, OpenAI, and Gemini providers
- Intelligent inference for unknown database types
- Row sampling for better context
- Persistent caching to minimize API calls

## [2.23.0] - 2026-01-12

### Maintenance
- Removed legacy pool implementations (4,473 lines of dead code)
- Completed Phase 7 of pluggable database architecture

## [2.22.0] - 2026-01-12

### Changed
- **WriteAheadWriters tuning** - Moved to driver interface for full pluggability
- PostgreSQL scales writers with CPU cores (2-4)
- MSSQL fixed at 2 writers (TABLOCK serialization)

## [2.21.0] - 2026-01-12

### Changed
- **Driver defaults** - Use driver registry instead of hardcoded fallbacks
- Each driver returns its own defaults via `Defaults()` method

## [2.2.0] - 2026-01-12

### Fixed
- **PG→MSSQL geography upsert** - Use DROP/ADD COLUMN instead of ALTER COLUMN for geography types

## [2.1.0] - 2026-01-12

### Changed
- **Pluggable factory** - Factory calls driver methods directly, no switch statements
- **BuildRowNumberQuery fix** - Extract column aliases for outer SELECT in CTE

## [1.43.0] - 2026-01-11

### Security
- **DSN injection fix** - URL-encode credentials in connection strings
- **SQL injection fix** - Whitelist validation for SQLite table names

### Fixed
- Spatial column detection for same-engine migrations
- SRID preservation for PostGIS columns

## [1.42.0] - 2026-01-11

### Fixed
- Security vulnerabilities identified during code review

## [1.41.0] - 2026-01-11

### Fixed
- **PG→MSSQL geography staging** - Query staging table directly for spatial columns

## [1.40.0] - 2026-01-11

### Added
- **packet_size config** - TDS packet size for MSSQL (default: 32KB)

### Performance
- MSSQL→PG: +27% throughput
- MSSQL→MSSQL: +162% throughput

## [1.32.0] - 2026-01-11

### Fixed
- **PG→MSSQL geography upsert** - Convert WKT text via STGeomFromText in MERGE

## [1.31.0] - 2026-01-10

### Added
- **Incremental sync** - Date-based highwater marks for fast delta transfers
- `date_updated_columns` configuration option

## [1.21.0] - 2025-12-30

### Performance
- **Direct Bulk API for MSSQL Inserts** - 73% throughput improvement using `CreateBulkContext` directly (#20)
  - Bypasses `database/sql` prepared statement overhead by using `conn.Raw()` to access driver directly
  - Previous: ~15,500 rows/sec → Now: ~27,000 rows/sec
  - Explicit transaction wrapper for atomicity with proper `defer tx.Rollback()` pattern

## [1.20.0] - 2025-12-30

### Performance
- **Worker Cap at 12** - Capped max workers at 12 for optimal performance (#17)
- **Removed Row Size Adjustment** - Removed chunk_size reduction based on row size that was hurting throughput (#16)

## [1.19.0] - 2025-12-30

### Performance
- **Removed Memory Safety Loop** - Removed conservative memory reduction loop that was limiting throughput (#15)

## [1.18.0] - 2025-12-30

### Features
- **Auto-tune Parallel Readers and Writers** - Automatic tuning of `parallel_readers` and `write_ahead_writers` based on system resources (#14)

## [1.17.0] - 2025-12-30

### Features
- Auto-tuning improvements for migration settings

## [1.16.0] - 2025-12-24

### Features
- Performance improvements and bug fixes

## [1.15.0] - 2025-12-24

### Features
- Additional migration optimizations

## [1.14.0] - 2025-12-23

### Features
- Migration enhancements

## [1.13.0] - 2025-12-21

### Features
- Core migration functionality improvements

## [1.12.0] - 2025-12-21

### Features
- Initial stable release with bidirectional migration support
