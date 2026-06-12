# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DMT is a high-performance CLI tool for database migrations between SQL Server, PostgreSQL, and MySQL. Written in Go, it achieves 222K-717K rows/sec throughput through bulk copy protocols, parallel I/O pipelines, deterministic smartconfig, and rule-based runtime tuning.

## Build and Test Commands

```bash
make build                    # Build binary to ./dmt
make test                    # Run all tests with verbose output
make test-short              # Run tests with -short flag (skips integration tests)
make lint                    # Run golangci-lint
make check                   # Format + test
make run                     # Build and run with config.yaml

# Run a specific test
go test -v ./internal/driver -run TestTypeMappingCache
go test -race ./...          # Race detector

# Docker test databases
make test-dbs-up             # Start MSSQL (port 1433) and PostgreSQL (port 5432)
make test-dbs-down           # Stop test database containers

# Pre-commit hooks (runs go fmt + go test ./... -short)
make setup-hooks
```

## Architecture

### Entry Point

`cmd/migrate/main.go` — CLI using urfave/cli/v2. No-args launches TUI (`tui.Start()`). Commands: `run`, `resume`, `status`, `validate`, `diagnose`, `history`, `profile`, `preflight` (alias `health-check`), `analyze`, `ai`, `setup`, `init`, `init-secrets`, `cache`. The TUI mirrors these (parity registry in `internal/command/registry.go`; table in `docs/TUI_COMMANDS.md`).

### Core Packages

All packages live under `internal/`. The key ones:

| Package | Purpose |
|---------|---------|
| `driver/` | Pluggable database drivers + AI integrations (type mapping, smart config, error diagnosis) |
| `driver/postgres/`, `mssql/` | Hand-written oracle implementations awaiting removal (#509 cleanup; mysql and sqlite already removed) |
| `driver/generic/` | Catalog-driven engine (#191): one Reader/Writer/Dialect, per-engine YAML catalogs (`sqlite` runs on it) |
| `orchestrator/` | Migration workflow coordinator (9 task phases, retry logic, health checks) |
| `transfer/` | Data transfer pipeline with read-ahead buffering, runtime tuning, checkpoint coordination |
| `pool/` | WriterPool goroutine pool, driver factory |
| `checkpoint/` | State persistence — SQLite (default) or YAML file (Airflow/headless) |
| `config/` | YAML parsing, secret expansion (`${env:VAR}`, `${file:/path}`), auto-tuning, driver validation |
| `dbconfig/` | SourceConfig/TargetConfig structs — exists to break circular import between `config` and `driver` |
| `monitor/` | Rule-based runtime controller: polls metrics, adjusts parameters via `RuntimeTuner` |
| `tui/` | Interactive terminal UI (Bubble Tea framework) |
| `secrets/` | Loads `~/.secrets/dmt-config.yaml` for AI keys and provider settings |

### Driver Plugin System

Drivers self-register via `init()` in each sub-package with the global registry (`driver/registry.go`). Lookup is case-insensitive. Drivers are activated by blank imports in `config/drivers.go` and `pool/factory.go`.

Each driver implements:
- **`Driver`** (`driver/driver.go`) — Factory: `Name()`, `Aliases()`, `Defaults()`, `Dialect()`, `NewReader()`, `NewWriter()`
- **`Reader`** (`driver/reader.go`) — Schema extraction, row counts, partition boundaries (data streaming itself lives in `transfer/` — see pipeline below); optional capability: `IncrementalDateReader` (date-column sync tracking)
- **`Writer`** (`driver/writer.go`) — DDL ops, `WriteBatch()` (bulk insert); optional capabilities: `Upserter` (staging+MERGE — required for `target_mode: upsert`), `SequenceResetter`, `ConstraintWriter` (post-transfer FK/CHECK)
- **`TypeMapper`** (`driver/typemapper.go`) — Column-level type mapping; `TableTypeMapper` for full-table DDL via AI
- **`Dialect`** (`driver/dialect.go`) — SQL syntax: `QuoteIdentifier()`, `BuildKeysetQuery()`, `BuildRowNumberQuery()`, `AIPromptAugmentation()`

Driver aliases: `mssql` (sqlserver, sql-server), `postgres` (postgresql, pg), `mysql` (mariadb, maria), `sqlite` (sqlite3, sqlitedb).

MSSQL sets `ScaleWritersWithCores: true`: the target writer uses parallel BCP without TABLOCK by default. PostgreSQL/MySQL also set `true`; SQLite sets `false` and pins to a single writer (file-based, single-writer constraint). MSSQL `drop_recreate` builds non-PK indexes after transfer; for upsert or other loads into existing indexed tables, reduce `migration.write_ahead_writers` or rebuild secondary indexes around the migration if contention appears.

SQLite is intended primarily for testing dmt end-to-end without external database servers — fixtures live in `.db` files and round-trip through the same pipeline. Cross-engine type mapping is supported: sqlite→{mssql,postgres,mysql} and {mssql,postgres,mysql}→sqlite both go through the deterministic typemap. `GetPartitionBoundaries` always returns a single partition for SQLite (no parallelism benefit from splitting). FK and CHECK constraints can only be declared inline at CREATE TABLE time on SQLite, so the sqlite catalog declares `constraint_writer: false` (#460, #191) — finalization skips FK/CHECK creation with one audited message; users who need FK enforcement should run sqlite as source rather than target.

**Capability interfaces (#460)**: a writer surface an engine cannot honor must be an optional interface (e.g. `driver.ConstraintWriter` for post-transfer FK/CHECK creation) with caller-side type assertion and uniform degradation — never a silent or warning no-op stub on the engine. The conformance harness pins each driver's capability matrix (`conformance.CheckWriterCapabilities`); a new engine declares what it supports there.

### Data Transfer Pipeline

```
Source DB → [parallel reader goroutines in transfer/keyset.go|row_number.go,
             querying via Dialect.BuildKeysetQuery/BuildRowNumberQuery] →
chunkChan (buffered) → Consumer loop → WriterPool (N goroutines) →
WriteBatch/UpsertBatch → Target DB
                       → ackChan → checkpoint coordinator → SQLite
```

**Pagination strategy** (determined by PK type in `driver.Table`):
- **Keyset**: Single-column integer PK — supports parallel readers via `splitPKRange()`
- **ROW_NUMBER**: Composite/varchar PKs — single reader only, no partial-data cleanup on resume

**Dynamic tuning**: `RuntimeTuner` (`transfer/runtime_tuner.go`) allows AI monitor to adjust `ChunkSize` and `WriteAheadWriters` mid-migration. `chunkSizeFn` closure reads tuner snapshot on each iteration. `WriterPool.ScaleWorkers()` adds/removes goroutines at runtime.

### Migration Task Flow (Orchestrator)

The orchestrator (`orchestrator/orchestrator.go`) sequences 9 phases:
1. `TaskExtractSchema` — Read schema from source
2. `TaskCreateTables` — Create tables in target (with AI type mapping)
3. `TaskTransfer` — Stream data via pipeline (parallel table jobs via semaphore)
4. `TaskResetSequences` — Reset identity/sequence values
5. `TaskCreatePKs` — Create primary keys
6. `TaskCreateIndexes` — Create non-PK indexes (parallel)
7. `TaskCreateFKs` — Create foreign keys (parallel)
8. `TaskCreateChecks` — Create check constraints (parallel)
9. `TaskValidate` — Row count validation

**Target mode strategies** (`orchestrator/target_mode.go`): `TargetModeStrategy` interface with `dropRecreateStrategy` and `upsertStrategy` implementations.

**Resume flow**: Verifies config hash (SHA256 of sanitized config), skips completed tables, loads chunk-level progress, cleans up partial data (keyset only).

### AI And Tuning Integration

AI fallback features share `AITypeMapper.CallAI()` with provider abstraction (Claude, OpenAI, Gemini, Ollama, LMStudio):

1. **AI fallback type/DDL mapper** (`driver/ai_typemapper*.go`) — Handles Raw vendor types and unsupported DDL surfaces after the deterministic mapper declines. Local cache at `~/.dmt/type-cache.json` records source/provider/model/schema-hash metadata plus a checksum. In-flight dedup uses `sync.Map`; provider HTTP uses exponential backoff and honors `Retry-After`.
2. **Smart Config** (`driver/smartconfig.go`, `internal/tuning/`) — Deterministic tuner based on source stats, system resources, driver profiles, and completed-run history. `config.ApplyTunerSuggestions()` pins per-config and secrets-default values; generated defaults remain overrideable and debug output reports provenance.
3. **Runtime Controller** (`monitor/`) — Rule-based controller that adjusts runtime parameters through `RuntimeTuner`; it does not call an LLM or mutate `*config.Config` during transfer.
4. **Error Diagnosis** (`driver/diagnosis*.go`) — Deterministic catalog. Unmatched errors are counted as `errordiag` fallback-observability events for catalog growth, not sent to an AI provider.

### Config System

`config/load.go` loads YAML with secret expansion → driver defaults → auto-tuning.

**Secret expansion** (before YAML parse): `${file:/path}`, `${env:VAR}`, `${VAR}` (legacy).

**Secrets layering**: `~/.secrets/dmt-config.yaml` provides global defaults (AI keys, Slack, migration defaults), overridden by per-config-file settings. Loaded in `applyGlobalDefaults()`.

**Config loading precedence**: `--profile name` (from SQLite) > `--config file` > default `config.yaml`.

### Checkpoint System

**SQLite** (`checkpoint/state.go`): WAL mode, single connection (`MaxOpenConns(1)`), auto-migration on `New()`. Tables: `runs`, `tasks`, `transfer_progress`, `ai_adjustments`, `ai_tuning_history`, `profiles`, `sync_timestamps`.

**File-based** (`checkpoint/filestate.go`): Single YAML file for Airflow. No profile or AI history support.

**Encrypted profiles** (`checkpoint/profiles.go`): AES-GCM with `DMT_MASTER_KEY`, profile name as AAD, version byte prefix for algorithm migration.

## Key Patterns

- **Plugin Registry**: `init()` self-registration in each driver, case-insensitive lookup, panic on duplicate
- **Circular Import Prevention**: `dbconfig` package holds SourceConfig/TargetConfig used by both `config` and `driver`
- **Producer-Consumer Pipeline**: Multiple reader goroutines → buffered chunkChan → WriterPool → ackChan → checkpoint coordinator
- **Strategy Pattern**: `TargetModeStrategy` interface for drop-recreate vs upsert
- **Runtime Controller**: rule-based adjustments flow through `RuntimeTuner`; config is treated as the pre-transfer baseline
- **Dynamic Tuning**: `RuntimeTuner` + `chunkSizeFn` closure allow mid-migration parameter changes
- **Identifier Sanitization**: `target.SanitizePGIdentifier()` converts SQL Server identifiers to valid PostgreSQL names

## Environment Setup

**Go Version**: Requires Go 1.24+

**GOROOT Issue**: If you encounter "cannot find GOROOT directory", set `GOROOT` to your Go installation path.

## Development Workflow

**File organization**: Every file should own a nameable concern (e.g. `dialect.go`, `preflight.go`, `writer_ddl.go`). Prefer 200-700 lines per file with a hard cap of 1000; when a file outgrows the cap, split it at a concern boundary, never by sharding a type's methods into `foo_helpers.go` / `foo_part2.go`-style buckets just to hit a size target. Platform build-tag files (`memory_darwin.go` etc.) are exempt from the size floor.

**Testing conventions**:
- Integration tests use `_integration_test.go` suffix and `testing.Short()` skip
- `make test-short` skips integration tests; `make test` runs everything
- Docker test DBs: MSSQL on port 1433 (`SA_PASSWORD=TestPass2024`), PostgreSQL on port 5432 (`POSTGRES_PASSWORD=TestPass2024`)

**Branch strategy**: Feature branches (`feat/`, `fix/`, `refactor/`), never commit directly to `main`.

**Commit style**: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`) with PR numbers like `(#12)`.

**Debug TUI mode**: `./dmt 2>debug.log` (TUI captures stdout, logs go to stderr).

**Configuration & secrets**: Keep real credentials out of the repo. Store API keys, webhook URLs, and encryption keys in `~/.secrets/dmt-config.yaml`. See `config.yaml.example` for the baseline template.
