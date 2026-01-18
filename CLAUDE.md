# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DMT is a high-performance CLI tool for database migrations between SQL Server, PostgreSQL, MySQL, and Oracle. Written in Go, it achieves 222K-645K rows/sec throughput through bulk copy protocols and parallel I/O pipelines.

## Build and Test Commands

```bash
# Build
make build                    # Build binary to ./dmt
make build-all               # Build for all platforms (linux, darwin, windows)

# Test
make test                    # Run all tests with verbose output
make test-short              # Run tests with -short flag (faster)
make test-coverage           # Generate coverage report (coverage.html)

# Development
make fmt                     # Format code
make lint                    # Run golangci-lint
make check                   # Format + test

# Test databases (Docker)
make test-dbs-up             # Start MSSQL and PostgreSQL containers
make test-dbs-down           # Stop test database containers

# Git hooks
make setup-hooks             # Configure pre-commit hooks (.githooks/)
```

## Architecture

### Directory Structure

- `cmd/migrate/main.go` - CLI entry point using urfave/cli/v2
- `internal/` - All internal packages

### Core Packages

| Package | Purpose |
|---------|---------|
| `driver/` | Pluggable database drivers (postgres, mssql, mysql, oracle) |
| `orchestrator/` | Migration workflow coordinator (9 task types) |
| `transfer/` | Data transfer pipeline with read-ahead buffering |
| `pipeline/` | Configurable producer-consumer pipeline with runtime tuning |
| `checkpoint/` | State persistence (SQLite or YAML for Airflow) |
| `config/` | YAML parsing, secret expansion, driver validation |
| `tui/` | Interactive terminal UI (Bubble Tea framework) |
| `monitor/` | Real-time performance monitoring with AI adjustments |

### Driver Plugin System

Each database driver registers via `init()` with the global registry in `driver/registry.go`. Drivers implement:
- `Reader` - Schema extraction, partitioned streaming, batch reads
- `Writer` - Table creation, bulk insert, upsert, constraint management
- `TypeMapper` - Cross-database type conversion (with AI fallback)

Driver packages: `driver/postgres/`, `driver/mssql/`, `driver/mysql/`, `driver/oracle/`

### Migration Task Flow

The orchestrator sequences 9 tasks:
1. `TaskExtractSchema` - Read schema from source
2. `TaskCreateTables` - Create tables in target (with AI type mapping)
3. `TaskTransfer` - Stream data via pipeline
4. `TaskResetSequences` - Reset identity/sequence values
5. `TaskCreatePKs` - Create primary keys
6. `TaskCreateIndexes` - Create non-PK indexes
7. `TaskCreateFKs` - Create foreign keys
8. `TaskCreateChecks` - Create check constraints
9. `TaskValidate` - Row count validation

### Data Transfer Pipeline

```
Source (Reader) → ReadAhead Buffer → Pipeline → WriteAhead Writers → Target (Writer)
```

Features:
- Keyset pagination for integer PKs, ROW_NUMBER for composite/varchar PKs
- Chunk-level checkpointing for resumable migrations
- AI monitor can adjust chunk_size/workers mid-migration

### AI Integration

AI features with shared provider abstraction (Claude, OpenAI, Gemini):
1. **Type Mapper** (`driver/ai_typemapper.go`) - Cross-database type inference with caching
2. **Smart Config** (`driver/ai_smartconfig.go`) - Analyze source and recommend config
3. **Runtime Monitor** (`monitor/ai_monitor.go`) - Adjust parameters during migration
4. **Error Diagnosis** (`driver/ai_errordiag.go`) - Analyze migration failures and suggest fixes

### TUI Mode

Interactive mode uses Bubble Tea. Launch with `./dmt` (no args). Supports:
- Slash commands: `/run`, `/resume`, `/analyze`, `/wizard`, etc.
- File completion with `@` prefix
- Real-time migration progress

## Key Patterns

- **Plugin Registry**: Drivers self-register in `init()`, looked up by name/alias
- **Interface Segregation**: Reader/Writer/TypeMapper in `pool/interfaces.go`
- **State Machine**: TUI wizard progression in `tui/model.go`
- **Producer-Consumer**: Pipeline with configurable read-ahead/write-ahead
- **Strategy Pattern**: Drop-recreate vs upsert in `orchestrator/target_mode.go`

## Configuration

YAML config with environment variable expansion (`${VAR_NAME}`, `${env:VAR}`, `${file:/path}`). Example configs in `examples/` directory.

Required environment variables for AI features:
- `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `GEMINI_API_KEY`
- `DMT_MASTER_KEY` (base64-encoded 32-byte key for encrypted profiles)
