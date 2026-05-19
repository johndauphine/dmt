# Changelog

All notable changes to this project will be documented in this file.

> CHANGELOG entries between v3.0.0 and v3.60.0 (Jan 2026 → May 2026)
> weren't maintained; the per-PR commit messages are the canonical
> record for that window. Tracking #233 to formalize CHANGELOG
> discipline going forward.

## [Unreleased]

### Added

- Added read-only source schema drift detection with persisted snapshots,
  structured change reports, and an optional `migration.fail_on_schema_drift`
  pre-transfer safety gate (#305).
- Added initial shared driver SQL-shape helpers and a shared-driver
  architecture note to guide behavior-preserving DRY work across database
  engines (#338).
- Added opt-in schema evolution for `added_column` drift in upsert mode,
  including per-policy config, nullable target `ADD COLUMN` support across
  drivers, and operator documentation (#306).
- Added the MSSQL → Postgres pair to the nightly cross-engine integration
  matrix so the primary migration path has scheduled coverage as well as
  per-PR coverage (#342).

### Changed

- Split the P1 oversized production files into focused same-package
  files to improve code readability without changing behavior (#212).
- Split the PostgreSQL writer into focused same-package files for DDL,
  row counts, COPY batching, upsert, raw SQL helpers, and writer lifecycle
  so the first P2 readability story is easier to review and maintain (#212).
- Split the setup wizard state machine into focused same-package files for
  prompts, input processing, connection config expansion, and helper defaults
  so the next P2 readability story is easier to maintain (#212).
- Split the tuning history selector into focused same-package files for
  dispatch, regression argmax, formatting, retry filters, outlier filters,
  and WAW-bin selection as the next P2 readability story (#212).
- Split the SQL Server reader into focused same-package files for schema
  extraction, constraints, row counts, partitioning, sampling, reads, and
  compatibility checks as the next P2 readability story (#212).
- Split the file checkpoint backend into focused same-package files for
  persistence, runs, tasks/progress, sync timestamps, AI-history no-ops,
  fallback events, and lifecycle helpers as the next P2 readability story
  (#212).
- Split the tuning regression model into focused same-package files for
  model definitions, fitting, prediction intervals, math helpers, and
  residual outlier filtering as the next P2 readability story (#212).
- Split the MySQL writer into focused same-package files for DDL,
  batch/upsert writes, row counts, raw SQL helpers, and value conversion
  as the next P2 readability story (#212).
- Split the SQLite reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split secrets configuration handling into focused same-package files for
  config file persistence, validation/defaults, and generated templates as
  the next P2 readability story (#212).
- Split the MySQL reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split the PostgreSQL reader into focused same-package files for schema
  introspection, streaming reads, row counts, partitioning, and sampling
  as the next P2 readability story (#212).
- Split orchestrator healthcheck and analysis code into focused
  same-package files for dry-run health checks, config analysis, offline
  system suggestions, tuning history, and DB tuning recommendations as the
  next P2 readability story (#212).
- Split the runtime monitor controller into focused same-package files for
  decision evaluation, action application, and controller helpers as the
  next P2 readability story (#212).
- Split the orchestrator transfer runner into focused same-package files for
  pre-transfer setup, job execution, error diagnosis, and transfer logging as
  the final original P2 readability story (#212).
- Split orchestrator status rendering into focused same-package files for
  summary output, detailed status, fallback reporting, history display, and
  result reconstruction as an additional readability cleanup (#212).
- Split the audit logger into focused same-package files for logger
  lifecycle, path resolution, field scrubbing, canonical JSON, and hash
  chaining as an additional readability cleanup (#212).

### Fixed

- Setup edit mode now raw-loads configs with templated non-string scalars
  such as `${env:DB_PORT}` while still preserving placeholders in string
  fields for safe round-tripping (#283).
- Setup edit mode now distinguishes omitted `create_indexes` and
  `create_foreign_keys` from explicit `false`, preserving the documented
  default when raw-loading and round-tripping existing configs (#282).
- SQL Server bulk-copy writes now convert UTF-8 text values scanned as
  `[]byte` into strings while preserving binary bytes, avoiding odd-byte
  Unicode failures in MySQL-to-SQL Server transfers (#303).
- SQLite incremental sync now accepts explicitly configured `TEXT` date
  columns so ISO-8601 timestamp strings do not silently fall back to full
  sync (#311).
- Diagnosis boxes now truncate long non-ASCII messages on UTF-8 boundaries
  instead of slicing through multibyte runes (#238).
- Runtime Slack notifications now honor the effective per-config
  `slack` settings instead of rereading only global secrets (#284).
- `dmt analyze --apply` now writes `migration.max_memory_mb` alongside
  the other analyzed tuning fields so applied config files preserve the
  full memory-budget recommendation (#246).

### Added

- **Nightly cross-engine integration matrix in CI** (closes #291).
  New `.github/workflows/integration-nightly.yml` runs 12 cross-engine
  directed pairs once a night (`cron: 0 5 * * *`) and on manual
  `workflow_dispatch`. Pairs cover every {mssql, postgres, mysql, sqlite}
  combination and exclude only the four same-engine round-trips (which
  don't exercise the cross-DB type-mapping surface). Matrix runs with
  `fail-fast: false` and `max-parallel: 12` so one driver's failure
  doesn't mask the rest, and the whole matrix wraps in roughly the
  wall-clock of a single pair. New generic runner
  `scripts/integration-test-pair.sh --pair <src>-<tgt>` dispatches
  source-side fixture loading and target-side DB prep per engine,
  then validates via `dmt run` + `dmt validate` (trusts dmt's own
  validate logic as the canonical row-count check rather than
  re-encoding case sanitization per target). Per-pair configs live
  under `scripts/fixtures/ci-{src}-{tgt}.yaml`. Local repro:
  `make integration-test-pair PAIR=mssql-pg`.

- **SO2010-minimal posts.body text aligned across all four engine
  fixtures** (part of #291). Previously the mssql, pg, sqlite, and
  mysql fixtures each used engine-specific phrasing for the row-1
  `posts.body` value (`NVARCHAR(MAX) path`, `TEXT path`, `LONGTEXT
  path`). The per-engine type-mapping context lives in the file
  header comments where it belongs; the seeded data itself is now
  engine-neutral (`testing the wide-text path`) so cross-engine
  row-parity assertions in the upcoming integration matrix won't
  fail on documentary text alone. The sqlite fixture drift predated
  this PR but is fixed here to keep the four files in lockstep.

- **SO2010-minimal PG + MySQL source-side fixtures** (part of #291).
  New `scripts/fixtures/so2010-minimal-pg.sql` and
  `scripts/fixtures/so2010-minimal-mysql.sql` mirror the existing MSSQL
  fixture's 9 tables, 61 columns, and 47 seed rows — translated to
  each engine's dialect (PG: `TIMESTAMP`/`TEXT`/`VARCHAR`; MySQL:
  `DATETIME`/`LONGTEXT`/`VARCHAR` on InnoDB+utf8mb4). Same canonical
  lookup-table values (including the canonical `TagWikiExerpt` typo)
  so cross-engine round-trip tests can assert byte-identical row
  parity regardless of which engine is the source. `scripts/load-fixture-so2010-minimal.sh`
  gained a `--source mssql|pg|mysql` flag (default `mssql` preserves
  the original signature) and discovers a running container for each
  engine (`pg-test`/`pg-bench` for PG, `mysql-bench` for MySQL).
  Three new Make targets — `load-fixture-so2010-minimal-{mssql,pg,mysql}`
  — make the choice explicit at the command line; the bare
  `load-fixture-so2010-minimal` target stays as an alias for the
  mssql case. This is the fixture half of #291; the CI matrix wiring
  that actually exercises these is the follow-up PR.

- **SQLite → SQLite integration test in CI.** New `sqlite-to-sqlite`
  job in `.github/workflows/integration.yml` runs an end-to-end dmt
  migration between two on-disk SQLite databases on every PR. No
  service containers, no client tools beyond the preinstalled
  `sqlite3` CLI, and finishes in seconds — the fastest signal in the
  integration matrix. Fixture (`scripts/fixtures/so2010-minimal-sqlite.sql`)
  mirrors the existing MSSQL SO2010-minimal fixture schema and seed
  rows so the two CI jobs cover the same surface area from different
  driver angles. Includes value spot-checks (UTF-8 round-trip, NULL
  preservation, negative integer PK, canonical lookup-table contents)
  beyond the row-count parity check. Local repro: `make integration-test-sqlite`.

- **SQLite driver for test-only migrations** (#298). New driver under
  `internal/driver/sqlite/` uses the pure-Go `modernc.org/sqlite`
  (already a transitive dependency of the checkpoint store) so dmt can
  be exercised end-to-end without an external database server. SQLite
  works as both source and target. The deterministic typemap learned a
  fourth dialect via `internal/typemap/sqlite.go` plus targeted
  updates in `internal/typemap/ddl/`, so cross-engine type mapping
  (`sqlite ↔ {mssql, postgres, mysql}`) goes through the same path
  the production drivers use. Single-writer pool, single-partition
  reads, `INSERT OR IGNORE` for idempotent replay, and
  `INSERT … ON CONFLICT … DO UPDATE SET … = excluded.*` for upserts.
  `INTEGER PRIMARY KEY AUTOINCREMENT` is emitted only for sole
  integer-PK columns; composite PKs preserve `NOT NULL` on identity
  columns (SQLite, unlike PG/MSSQL/MySQL, does not implicitly
  NOT NULL table-level PK columns). FK and CHECK constraints can
  only be declared inline at CREATE TABLE on SQLite, so the post-
  load Create FK / Create Check phases log a warning and skip — use
  sqlite as source rather than target when FK enforcement matters.

- **TUI `/explore` control** (#182). Adds a slash command to the
  interactive TUI for the tuner's exploration policy. `/explore on`
  arms a one-shot probe consumed by the next `/run` (sets
  `cfg.Migration.Explore=true` for that run only). `/explore
  low|balanced|high` sets the steady-state ε strength for the
  session, mirroring `cfg.Migration.ExploreMode`. `/explore off`
  clears both. Bare `/explore` reports current state. Empty mode
  leaves the loaded config / secrets value alone — the TUI overrides
  only when the operator explicitly sets a mode this session.
  Completes the deferred TUI surface from #179 (PR2).

- **Type cache: partition by source + `cache clear --ai-only`** (#177).
  The on-disk `~/.dmt/type-cache.json` format now records per-entry
  provenance (`source`, `model`, `cached_at`) wrapped in a versioned
  envelope (`{"version": 2, "mappings": {...}}`). Read path is
  backward-compatible: pre-#177 flat-map files are sniffed and
  migrated as `source: "ai"` on the next load (defensive — the old
  code only ever wrote AI mappings). AI write sites (column-level,
  table DDL, drop DDL) record the current effective model so
  `dmt cache clear --ai-only` can invalidate after a model upgrade
  without disturbing other entries. Entries tagged `source:
  "deterministic"` are bypassed on read as defense-in-depth — the
  deterministic mapper is the source of truth for those mappings. New
  CLI: `dmt cache clear` (full wipe) and `dmt cache clear --ai-only`;
  the file-level helper operates directly on the JSON so the CLI
  doesn't need an AI provider configured to invalidate.

### Changed

- **Deterministic tuning searches reader settings and composes epsilon probes** (#294, #295).
  Regression selection now has explicit coverage proving the argmax path
  searches learned `parallel_readers` / `read_ahead_buffers` cells rather
  than carrying baseline reader settings through unchanged. Steady-state
  epsilon exploration can also apply a second nudge on a different knob
  with conditional probability `epsilon`, making multi-knob neighbors
  reachable after cold-start. Composite perturbation reasoning now uses
  an unambiguous comma-separated direction list.

- **Config validation: skip `host` requirement for file-based drivers.**
  `config.validate()` previously required `source.host` and
  `target.host` unconditionally, which rejected valid sqlite configs
  whose connection identity is a file path on `database`. Added an
  `isFileBasedDriver` helper that branches on canonical driver name
  (currently just `sqlite`) so file-driver configs validate cleanly.
  Caught by the new sqlite → sqlite integration test on first local
  run — the network-driver assumption was load-bearing for the
  3-driver world but invalid once a file driver landed.

- **Drop `R²=` from regression-tier reasoning line** (#293). The
  operator-facing log line emitted from
  `internal/tuning/history.go::applyHistoryRegression` no longer
  includes the model's training-set R². On noisy real workloads R²
  is structurally capped low — within-cell variance dominates
  between-cell signal — so the line read `R²=0.13` while the
  regression's actual decisions were near-optimal (1.4% top-1
  regret on the SO2010 → PG sweep that motivated this change, with
  Spearman ρ = +0.68 and 95% CI coverage at 88%). Operators reading
  the log thought the tuner was broken when it wasn't. The 95%
  prediction interval still in the same line carries the relevant
  point-level confidence in units operators understand (MB/s). The
  `model.r2` field is still computed and remains available to debug
  logs and the existing `regression_test.go` assertions. A negative-
  guard assertion in `TestApplyHistory_RegressionTier` catches
  accidental re-introduction.

- **Studentized-residual outlier filter** (#225). Replaces the
  feature-blind `0.5×median` row-level outlier rule in
  `internal/tuning/history.go` with a leverage-adjusted
  studentized-residual filter that scores each row against the
  regression's prediction at its own features. The marginal floor kept
  a host-throttled 575K rows/s run at a config the model predicted to
  be 1.06M (because 575K > 0.5×median = 500K), which dragged R² from
  0.20 down to 0.09 across the next two runs. The residual filter
  reuses the `σ̂²` and `(XᵀX + λI)⁻¹` cached from #216 — no second
  matrix solve. Drops rows with `|t| > 3` AND `ChunkRetryCount == 0`,
  cap at 10% of rows per pass ordered by `|t|` descending. Gated to
  ≥ 2 × `minRowsForRegression` (60 rows) — below that, the marginal
  filter remains as the safety net. Reasoning emission is deferred so
  that when Tune's Tier 1 (identity) cohort drives the selection, only
  the identity cohort's drops appear in `Output.Reasoning` — the
  regime cohort's filter pass is silent (avoids double-reporting
  when the identity cohort is a subset of the regime cohort).

- **Packet cap and memory budget scoped to filtered tables** (#241).
  When `applyAITuning` runs, the orchestrator's
  include/exclude-filtered table set is now passed into the
  smartconfig analyzer via a new `SetTableNameFilter`. Every
  workload-wide derivation — `@@max_allowed_packet`-derived chunk
  cap, avg/max row size, `TotalRows`, `TotalTables`, `LargestTables`,
  `EstimatedMemMB`, the deterministic tuner's `HardChunkLimit` —
  now reflects only the tables that will actually be transferred.
  Previously an excluded wide table (e.g. an archive blob at 16 KB
  rows) still drove the packet cap and clamped chunk_size for the
  narrow tables that DO ship. The `analyze` CLI subcommand keeps its
  pre-#241 unscoped behavior (no filter context to apply).

- **Physical-regime buckets for ClassifyRegime** (#214). Replaces the
  per-field ratio gates (3× total_rows, 2× avg_row_bytes) the
  workload-similarity check used to drop historical rows from the
  regression's training set. Three-axis bucket gating now applies:
  total-bytes band (Tiny < 100 MB / Small < 10 GB / Medium < 100 GB
  / Large < 1 TB / Huge), then largest-table-share skew tier, then
  total-table-count tier. Each axis only triggers when the higher
  axis matches and either side has a defined value. Fixes the two
  failure modes the ratios produced: the boundary discontinuity
  (100M and 305M rows artificially partitioned even though they're
  on the same physical cliff) and the asymmetry (1M→3M and 30M→90M
  have the same ratio but very different physics).
  `LargestTableBytes` and `TotalTables` are added to the
  `tuning.HistoryRecord` IR for the new axes; persistence of
  `LargestTableBytes` to `ai_tuning_history` is deferred to a
  follow-up schema migration. Historical rows that don't carry the
  field fall through as `skewUnknown` — neutral on the secondary
  axis until they get backfilled.

- **Regression predicts bytes/sec, not rows/sec** (#224). The
  deterministic tuner's quadratic regression in
  `internal/tuning/regression.go` now trains on
  `HistoryRecord.FinalThroughputBytes` (rows/sec × avg_row_bytes,
  populated by the smartconfig adapter with the existing
  `safeAvgRowBytes` fallback). Aligns the dependent variable with
  the chunk_size_bytes input feature, drops the dual role
  `log(avg_row_bytes)` was playing as an implicit unit conversion,
  and should measurably improve cross-workload R² in Tier 2.
  Within-workload Tier 1 R² is unchanged (linear y rescaling). The
  reasoning log now reads "predicted 537 MB/s [95% CI: 412 MB/s–663
  MB/s]" via a new `formatBytesPerSec` helper that picks GB/s for
  ≥1 GB/s rates. Other readers of `FinalThroughput` (filterOutliers,
  smoothed-bins aggregator, regime_drift, selectWAW) stay in
  rows/sec — they're within-workload comparators where rows/sec is
  the natural unit.

### Added

- **AI fallback observability** (#176). Every AI-fallback call site
  (column- and table-level type mapping, finalization DDL, errordiag
  catalog miss) now flows through `observability.RecordFallback`,
  which fans an event out to three surfaces in lockstep: the existing
  `dmt_ai_fallback_total{surface=...}` Prometheus counter (#229), an
  in-process counter for the running migration, and a new per-run
  `fallback_events` row on both the SQLite and YAML/FileState
  checkpoint backends. Persisting to the backend means a separate-
  process `dmt status` poll (Airflow's documented workflow) sees the
  running migration's counts. UPSERT keys are
  `(run_id, surface, fingerprint)` so a 10K-Raw-column migration
  produces O(distinct types) rows, not O(occurrences). Errordiag
  fingerprints are normalized to `driver|scrubbed_prefix` (the
  SHA-256 hash stays in the debug log) so a bulk load that fails
  on row-specific error details doesn't explode the table. The
  detailed-status view sorts fingerprints by count and caps the
  inline display at 10 with an "and N more" suffix.
  `CleanupOldRuns` purges the new table alongside the rest of the
  run-scoped state.

- **Setup wizard prompts for Slack webhook URL** (#281). New Phase 1b
  step (`StepSlackWebhook`) interposes between the AI prompts and the
  Phase 2 source database configuration. Runs unconditionally — the
  webhook is independent of AI, so users who skip AI still see the
  prompt. Edit mode pre-populates from the existing
  `~/.secrets/dmt-config.yaml` so Enter preserves the current value;
  blank input on a fresh setup skips writing entirely; `-` or `none`
  explicitly clears the stored webhook. The webhook lives only in
  the global secrets file (matches existing CLAUDE.md guidance: AI
  and Slack are global-only), not in the per-migration config.

- **`secrets.SaveSlackWebhook`** (#281). New helper that sets the
  Slack webhook URL with explicit-clear semantics (empty string
  writes through), unlike `secrets.Save` whose merge logic skips
  zero values. Preserves all other sections of the secrets file
  (AI, Encryption, MigrationDefaults).

- **Setup wizard loads existing config and offers Edit-vs-New** (#279).
  `/setup` (TUI) and `dmt setup -o existing.yaml` (CLI) used to always
  start from a blank slate even when the underlying state machine
  already wired existing values into every prompt's Default. Now both
  entry points load the target config if it exists, route through a
  new `StepEditOrNew` preflight, and seed each prompt with the
  user's prior values so Enter preserves them. New configs
  (`/setup @newfile.yaml`) still start fresh and save to the
  requested path.

- **Wizard prompts for `date_updated_columns` when target_mode=upsert**
  (#279). Without a watermark column, upsert silently degraded to
  full scans on every re-run. The new `StepDateColumns` prompt only
  appears in upsert mode and accepts a comma-separated list (Enter
  keeps the existing list, `-` clears it). drop_recreate bypasses
  this step.

- **`config.LoadRaw` and `config.Expand`** (#279). `LoadRaw` reads a
  config YAML without secret-template expansion / defaults / validation,
  so `${env:DB_PASSWORD}` placeholders survive a wizard round-trip and
  are never written back to disk in cleartext. `Expand` is a public
  single-string template expander used by the wizard's per-side
  connection tests, which now resolve only the side being tested so
  an unrelated missing template on the other side never poisons the
  test (issue noted by codex review pass 3).

### Deprecated

- **`migration.ai_adjust` → `migration.runtime_tuning`** (#211). The
  knob controls a deterministic rule-based controller (post-#172);
  the old name implied AI involvement that no longer exists, which
  was recurring user-facing confusion. Companion field
  `ai_adjust_interval` is renamed to `runtime_tuning_interval`. Both
  names are accepted during the v5-to-v6 deprecation window per the
  policy in [VERSIONING.md](VERSIONING.md): a config that still uses
  `ai_adjust` continues to work and emits a WARN log per migration
  pointing at the new name. When both names are present with
  conflicting values, the new field wins and a clarifying WARN names
  both. The legacy fields will be removed in v6.0.0. The same rename
  applies to `migration_defaults` in `~/.secrets/dmt-config.yaml`.
  The stored resume config hash uses the legacy JSON wire shape (via
  JSON tags), so an in-flight migration started before the upgrade
  resumes cleanly without `--force-resume`.

### Fixed

- **Analyze tuning no longer pollutes global secrets or training history**
  (#246, #247). `dmt analyze --apply` now writes workload-specific
  tuning values into the analyzed config file's `migration:` section
  instead of `~/.secrets/dmt-config.yaml`, preserving unrelated
  config sections and file permissions. Analyze mode is also advisory
  only: it can read prior `ai_tuning_history` rows for recommendations,
  but only completed migration runs persist new training rows.

- **TUI viewport now auto-scrolls to follow new output** (#280). The
  first `WindowSizeMsg` wrote the welcome message into the viewport
  but never called `GotoBottom`, so `YOffset` stayed at 0 with the
  welcome overflowing. From that moment `viewport.AtBottom()` returned
  false for the whole session, and `appendOutput`'s auto-follow check
  skipped `GotoBottom` on every new line — users had to manually
  scroll to see migration progress, wizard prompts, etc. Init path
  now anchors at the bottom so subsequent appends follow. Resize path
  preserves at-bottom-ness across dimension changes. As a bonus
  hardening, viewport width/height are clamped to a minimum of 1 so a
  tiny terminal (fewer rows than the 7-row footer) doesn't panic
  inside bubbles' `visibleLines()` with `slice bounds out of range`.

- **Setup wizard honored loaded values for source/target fields but
  not Phase 6 / TargetMode** (#279). `StepTargetMode` hardcoded
  `drop_recreate` even for configs that set `target_mode: upsert`,
  and `StepCreateIndexes`/`CreateForeignKeys`/`Workers` ignored the
  loaded values entirely. In EditMode the wizard now honors the
  loaded `target_mode`, preserves explicit-false bools, and leaves
  `Workers=0` (auto-tune) intact when the loaded YAML omitted the
  key. Two narrower follow-ups tracked in the LoadRaw godoc:
  `create_indexes`/`create_foreign_keys` should become `*bool` to
  match the AIAdjust precedent, and templated non-string scalars
  (e.g. `port: ${env:DB_PORT}`) still fail `LoadRaw` due to typed
  unmarshal.

## [5.0.0] - 2026-05-13

Production-readiness milestone: closes the [production-readiness
epic #236](https://github.com/johndauphine/dmt/issues/236) end-to-end.
All ten gates green — see `VERSIONING.md` for the gate list and
the note on why this lands as `v5.0.0` rather than re-numbering
to `v1.0.0`.

### Added

- **Per-run immutable audit log** (#235). dmt now writes an append-only
  NDJSON record of every `dmt run` / `dmt resume` to
  `$HOME/.dmt/audit/<run_id>.ndjson` (override with `--audit-dir`).
  Compliance-regime-friendly: each line is one event, the file is
  `chmod 0444` after a successful or hard-failed run (cancelled /
  resumable runs keep the file 0600 so `dmt resume` can append to
  it), and `--audit-tamper-evident` opts
  into hash-chained events (each event carries `seq` / `prev_hash` /
  `hash` so retroactive modification is detectable via a one-liner
  shell verification). Sensitive values flow through the same scrubber
  established by #231 — DSN passwords, API keys, webhook URLs, and
  any field whose key name matches `password` / `api_key` / `token` /
  etc. are redacted before write. Row content is never logged by
  design. Disable entirely with `--no-audit` for environments with
  another compliance mechanism. New `docs/AUDIT-LOG.md` documents the
  event schema, the hash-chain verification procedure, and retention
  recommendations per compliance regime (SOC 2 / HIPAA / PCI-DSS / SOX).

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

- **Sensitive-value scrubbing audit** (#231). New
  `internal/logging/scrub.go` exposes `Scrub(s)` /
  `ScrubError(err)` with a centralized regex set covering
  URL-style and libpq-style DSN passwords (PG, MSSQL, MySQL
  shapes), `password=`/`passwd=`/`pwd=`/`api_key=`/`secret=`/
  `token=` key/value forms (with `:` and `=` separators both
  preserved), `Authorization: Bearer` headers, `sk-` and
  `sk-ant-` API keys, and Slack incoming-webhook URLs. Threaded
  through every site that wraps a driver-library error: the
  PG / MSSQL / MySQL Reader and Writer constructors plus their
  Ping calls, the setup wizard's `TestConnection`, the
  orchestrator's `dmt preflight` JSON output, and the Slack
  notifier's HTTP error path. New `docs/SECURITY.md`
  documents the threat model, what's scrubbed, what's
  never logged (row content), and how an operator verifies.

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
