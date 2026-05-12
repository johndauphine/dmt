# Changelog

All notable changes to this project will be documented in this file.

> CHANGELOG entries between v3.0.0 and v3.60.0 (Jan 2026 → May 2026)
> weren't maintained; the per-PR commit messages are the canonical
> record for that window. Tracking #233 to formalize CHANGELOG
> discipline going forward.

## [Unreleased]

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
