# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DMT is a high-performance CLI tool for database migrations between SQL Server, PostgreSQL, and MySQL. Written in Go, it achieves 222K-717K rows/sec throughput through bulk copy protocols, parallel I/O pipelines, and AI-driven parameter tuning.

## Build and Test Commands

```bash
# Build
make build                    # Build binary to ./dmt
make build-all               # Build for all platforms (linux, darwin, windows)

# Test
make test                    # Run all tests with verbose output
make test-short              # Run tests with -short flag (faster)
make test-coverage           # Generate coverage report (coverage.html)

# Run specific test
go test -v ./internal/driver -run TestTypeMappingCache
go test -v ./internal/orchestrator/...
go test -race ./...          # Race detector

# Development
make run                     # Build and run with config.yaml
make fmt                     # Format code
make lint                    # Run golangci-lint
make check                   # Format + test

# Test databases (Docker)
make test-dbs-up             # Start MSSQL and PostgreSQL containers
make test-dbs-down           # Stop test database containers

# Git hooks (pre-commit runs go fmt + go test ./... -short)
make setup-hooks             # Configure pre-commit hooks (.githooks/)
```

## Architecture

### Entry Point

`cmd/migrate/main.go` - CLI using urfave/cli/v2. No-args launches TUI (`tui.Start()`). Commands: `run`, `resume`, `status`, `validate`, `history`, `profile`, `health-check`, `analyze`, `init`, `init-secrets`.

### Core Packages

All packages live under `internal/`.

| Package | Purpose |
|---------|---------|
| `driver/` | Pluggable database drivers + AI integrations (type mapping, smart config, error diagnosis) |
| `driver/postgres/`, `mssql/`, `mysql/` | Database-specific Reader/Writer/Dialect implementations |
| `driver/dbtuning/` | AI-driven DB parameter analysis |
| `orchestrator/` | Migration workflow coordinator (9 task types, retry logic, health checks) |
| `transfer/` | Data transfer pipeline with read-ahead buffering, runtime tuning, checkpoint coordination |
| `pool/` | WriterPool goroutine pool, driver factory, interface type aliases |
| `checkpoint/` | State persistence - SQLite (default) or YAML file (Airflow/headless) |
| `config/` | YAML parsing, secret expansion, auto-tuning, driver validation |
| `dbconfig/` | SourceConfig/TargetConfig structs - exists to break circular import between `config` and `driver` |
| `monitor/` | Real-time AI performance monitoring with runtime adjustments |
| `tui/` | Interactive terminal UI (Bubble Tea framework) |
| `secrets/` | Loads `~/.secrets/dmt-config.yaml` for AI keys and provider settings |
| `logging/` | Leveled logger (debug/info/warn/error), text and JSON formats |
| `progress/` | Progress bar (schollz/progressbar/v3) and JSON reporter |
| `notify/` | Slack webhook notifications |
| `exitcodes/` | Exit code constants (0-7) with error classification |
| `stats/` | WriterPool stats collection |
| `version/` | Version string injected via `-ldflags` at build time |
| `source/` | Type aliases: `source.Table` = `driver.Table` etc. |
| `target/` | PostgreSQL identifier sanitization (lowercasing, special char replacement) |

### Driver Plugin System

Drivers self-register via `init()` in each sub-package with the global registry (`driver/registry.go`). Lookup is case-insensitive. Drivers are activated by blank imports in `config/config.go` and `pool/factory.go`.

Each driver implements:
- **`Driver`** (`driver/driver.go`) - Factory: `Name()`, `Aliases()`, `Defaults()`, `Dialect()`, `NewReader()`, `NewWriter()`
- **`Reader`** (`driver/reader.go`) - Schema extraction, partitioned streaming via `ReadTable() <-chan Batch`, row counts, sampling
- **`Writer`** (`driver/writer.go`) - DDL ops, `WriteBatch()` (bulk insert), `UpsertBatch()` (staging+MERGE)
- **`TypeMapper`** (`driver/typemapper.go`) - Column-level type mapping; `TableTypeMapper` for full-table DDL via AI
- **`Dialect`** (`driver/dialect.go`) - SQL syntax: `QuoteIdentifier()`, `BuildKeysetQuery()`, `BuildRowNumberQuery()`, `AIPromptAugmentation()`

Driver aliases: `mssql` (sqlserver, sql-server), `postgres` (postgresql, pg), `mysql` (mariadb, maria).

Driver defaults control per-DB behavior: MSSQL sets `ScaleWritersWithCores: false` (TABLOCK serializes bulk inserts), PostgreSQL/MySQL set `true`.

### Data Transfer Pipeline

```
Source DB → ReadTable() → [parallel reader goroutines] → chunkChan (buffered) →
Consumer loop → WriterPool (N goroutines) → WriteBatch/UpsertBatch → Target DB
                                          → ackChan → checkpoint coordinator → SQLite
```

**Pagination strategy** (determined by PK type in `driver.Table`):
- **Keyset**: Single-column integer PK - supports parallel readers via `splitPKRange()`
- **ROW_NUMBER**: Composite/varchar PKs - single reader only, no partial-data cleanup on resume

**Dynamic tuning**: `RuntimeTuner` (`transfer/runtime_tuner.go`) allows AI monitor to adjust `ChunkSize` and `WriteAheadWriters` mid-migration. `chunkSizeFn` closure reads tuner snapshot on each iteration. `WriterPool.ScaleWorkers()` adds/removes goroutines at runtime.

**WriterPool** (`pool/writer_pool.go`): Reusable parallel writer infrastructure with ordered ack processing, runtime scaling, and buffered job/ack channels.

### Migration Task Flow (Orchestrator)

The orchestrator (`orchestrator/orchestrator.go`) sequences 9 phases:
1. `TaskExtractSchema` - Read schema from source
2. `TaskCreateTables` - Create tables in target (with AI type mapping)
3. `TaskTransfer` - Stream data via pipeline (parallel table jobs via semaphore)
4. `TaskResetSequences` - Reset identity/sequence values
5. `TaskCreatePKs` - Create primary keys
6. `TaskCreateIndexes` - Create non-PK indexes (parallel)
7. `TaskCreateFKs` - Create foreign keys (parallel)
8. `TaskCreateChecks` - Create check constraints (parallel)
9. `TaskValidate` - Row count validation

Before transfer: AI pre-migration tuning (`SmartConfigAnalyzer.Analyze()`), row-size-based memory refinement, table filtering (glob include/exclude).

**Target mode strategies** (`orchestrator/target_mode.go`): `TargetModeStrategy` interface with `dropRecreateStrategy` and `upsertStrategy` implementations.

**Resume flow**: Verifies config hash (SHA256 of sanitized config), skips completed tables, loads chunk-level progress, cleans up partial data (keyset only).

### AI Integration

All AI features share `AITypeMapper.CallAI()` with provider abstraction (Claude, OpenAI, Gemini, Ollama, LMStudio):

1. **Type Mapper** (`driver/ai_typemapper.go`) - Cross-database type inference. Local cache at `~/.dmt/type-cache.json`. In-flight dedup via `sync.Map`. Exponential backoff (1s base, 10s max, 3 retries).
2. **Smart Config** (`driver/ai_smartconfig.go`) - Queries source stats + system resources (gopsutil), recommends config. `config.ApplyAISuggestions()` only overrides params the user didn't explicitly set (tracked via `AutoConfig.Original*` fields).
3. **Runtime Monitor** (`monitor/ai_monitor.go` + `ai_adjuster.go`) - 30s ticker. Collects throughput, CPU%, memory%, queue depth, error count, transfer time breakdown. Circuit breakers: API failure (3 fails → 5min pause), effectiveness (3 negative effects → 10min pause). 90s cooldown between adjustments. Skips when >90% done. Falls back to heuristic rules on AI failure.
4. **Error Diagnosis** (`driver/ai_errordiag.go`) - Singleton, initialized on first error. Caches diagnoses by SHA256 of error message. Called from `orchestrator/target_mode.go` for DDL failures.

### Config System

`config/config.go` loads YAML with secret expansion → driver defaults → auto-tuning.

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
- **Circuit Breaker**: AI adjuster has two breakers (API failures + effectiveness) with auto-reset
- **Dynamic Tuning**: `RuntimeTuner` + `chunkSizeFn` closure allow mid-migration parameter changes
- **Config Hash**: SHA256 of sanitized config stored at run start, compared on resume to detect drift
- **Identifier Sanitization**: `target.SanitizePGIdentifier()` converts SQL Server identifiers to valid PostgreSQL names

## Environment Setup

**Go Version**: Requires Go 1.24+

**GOROOT Issue**: If you encounter "cannot find GOROOT directory", set:
```bash
export GOROOT=/home/johnd/.local/go
```

## Development Workflow

**Testing conventions**:
- Integration tests use `_integration_test.go` suffix and `testing.Short()` skip
- `make test-short` skips integration tests; `make test` runs everything
- Docker test DBs: MSSQL on port 1433 (`SA_PASSWORD=TestPass123!`), PostgreSQL on port 5432 (`POSTGRES_PASSWORD=TestPass123!`)

**Branch strategy**: Feature branches (`feat/`, `fix/`, `refactor/`), never commit directly to `main`.

**Commit style**: Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`) with PR numbers like `(#12)`.

**Debug TUI mode**: `./dmt 2>debug.log` (TUI captures stdout, logs go to stderr).
