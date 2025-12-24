# AI Context File - mssql-pg-migrate

This file provides context for AI assistants working on this project. Read this before making changes.

## Project Overview

**mssql-pg-migrate** is a high-performance CLI tool for bidirectional database migration between Microsoft SQL Server and PostgreSQL. Written in Go, it achieves 575K+ rows/sec for MSSQL→PG and 419K+ rows/sec for PG→MSSQL.

It features a modern **Terminal User Interface (TUI)** powered by Bubble Tea, offering an interactive wizard, real-time monitoring, encrypted profile storage, and easy configuration management.

**Repository**: https://github.com/johndauphine/mssql-pg-migrate

## Architecture

```
cmd/migrate/main.go          # CLI entry point (urfave/cli + TUI trigger)
internal/
├── config/                  # Configuration loading and validation
│   ├── config.go           # Config structs, YAML parsing, DSN building, LoadBytes, DefaultDataDir
│   ├── permissions_unix.go # File permission check (Linux/macOS)
│   └── permissions_windows.go # File permission check (Windows)
├── tui/                     # Interactive Terminal User Interface (Bubble Tea)
│   ├── model.go            # Main TUI loop, command handling, wizard logic, profile commands
│   ├── styles.go           # Lip Gloss styles (Gemini-style: purple accents, gold input border)
│   ├── capture.go          # Output redirection (io.Writer -> TUI)
│   └── git.go              # Git status bar integration
├── orchestrator/           # Migration coordinator
│   └── orchestrator.go     # Main workflow: extract → create → transfer → validate
├── checkpoint/             # State persistence (SQLite)
│   ├── state.go           # Run/task tracking, progress saving, resume support, run origin
│   └── profiles.go        # Encrypted profile storage (AES-GCM)
├── source/                 # Source database abstraction
│   ├── pool.go            # MSSQL source pool
│   ├── postgres_pool.go   # PostgreSQL source pool
│   └── types.go           # Table, Column, Index, FK structs
├── target/                 # Target database abstraction
│   ├── pool.go            # PostgreSQL target pool (COPY protocol)
│   └── mssql_pool.go      # MSSQL target pool (TDS bulk copy)
├── transfer/              # Data transfer engine
│   └── transfer.go        # Chunked transfer with read-ahead/write-ahead pipelining
├── pool/                  # Connection pool factory
│   └── factory.go         # Creates source/target pools based on config
├── progress/              # Progress bar display
│   └── tracker.go
└── notify/                # Slack notifications
    └── notifier.go
examples/                   # Example configuration files
```

## Key Concepts

### Interactive Mode (TUI)
- Launched when no arguments are provided: `./mssql-pg-migrate`
- **Framework**: Bubble Tea + Lip Gloss + Bubbles
- **Commands**: Slash commands (e.g., `/run`, `/wizard`, `/status`, `/resume`) with tab completion
- **Output Capture**: Redirects `stdout`/`stderr` to a persistent, scrollable viewport
- **File Picker**: Dynamic `@filename` completion for config files
- **Wizard**: Step-by-step interactive configuration generator with SSL support

### Encrypted Profiles
- Profiles stored in SQLite with AES-GCM encryption
- Master key via `MSSQL_PG_MIGRATE_MASTER_KEY` environment variable (base64 32-byte key)
- Profile CRUD: `/profile save`, `/profile list`, `/profile load`, `/profile delete`
- Run history tracks whether run came from profile or config file

### Transfer Pipeline
1. **Read-ahead**: Async goroutines pre-fetch chunks into buffered channel
2. **Write-ahead**: Multiple parallel writers consume from channel
3. **Chunk-level checkpointing**: Progress saved every 10 chunks for resume

### Target Modes
- **drop_recreate** (default): Drop and recreate target tables
- **truncate**: Truncate existing tables, create if missing
- **upsert**: Incremental sync - INSERT new rows, UPDATE changed rows, preserve target-only rows

### Pagination Strategies
- **Keyset pagination**: For single-column integer PKs (fastest)
- **ROW_NUMBER pagination**: For composite/varchar PKs
- Tables without PKs are rejected

### Authentication
- **Password**: Traditional user/password (default)
- **Kerberos**: Enterprise SSO via krb5 (MSSQL) or GSSAPI (PostgreSQL)

## Current State (December 2025)

### Latest Commits
```
21147b2 Improve TUI UX and profile metadata handling
1122042 Add profile metadata and description display
6ecc69a Add encrypted SQLite profiles and record run origin
9aaacd9 Support custom configuration files in TUI and fix PG partition query
5275226 Update AI context file with TUI implementation details
```

### Major Features

#### Interactive TUI (Gemini-Style)
- Purple/dark theme with gold input border, transparent background
- Rounded borders, scrollable viewport with scrollbar
- Git status bar shows branch and uncommitted changes
- Line-buffered log capture handles progress bars (`\r`)

#### Encrypted Profile Storage
- Profiles table in SQLite: `name`, `description`, `config_enc`, timestamps
- Encryption logic in `internal/checkpoint/profiles.go`
- `SaveProfile(name, description, config)` / `GetProfile` / `ListProfiles` / `DeleteProfile`
- `profile.name` and `profile.description` fields in config schema

#### Run History Origin Tracking
- `runs` table stores `profile_name` and `config_path`
- History output includes Origin column (profile or config file)
- Run detail shows `Origin: profile:<name>` or `config:<path>`

#### TUI Commands
- `/run @config.yaml` or `/run --profile myprofile` - Run migration
- `/resume` - Resume interrupted migration
- `/validate @config.yaml` - Validate row counts
- `/status` - Show current run status
- `/history` - Show run history
- `/history --run <id>` - Show run details with config
- `/wizard` - Interactive config builder
- `/profile save <name>`, `/profile list`, `/profile load`, `/profile delete`
- `/logs` - Show log buffer
- `/about` - Show version info
- `/help` - Show help

### Security Features
- Credentials sanitized before storing in SQLite state database
- Config file permission warnings (chmod 600 recommended)
- Encrypted profile storage with AES-GCM
- Kerberos authentication support

## Configuration

Config files use YAML with environment variable support (`${VAR_NAME}`).

Key fields:
- `profile.name`: Name for profile storage
- `profile.description`: Description for profile
- `migration.data_dir`: Directory for state database (auto-created)
- `migration.target_mode`: `drop_recreate` (default), `truncate`, or `upsert`

### Upsert Mode
Incremental synchronization that preserves target-only data:
- **Requirements**: All tables must have primary keys
- **PostgreSQL**: Uses batched `INSERT...ON CONFLICT DO UPDATE` with `IS DISTINCT FROM` change detection
- **SQL Server**: Uses staging table + `UPDATE`/`INSERT` with `EXCEPT` change detection
- **No deletes**: Rows only in target are preserved
- **Performance**: 2-5x slower than bulk copy due to index maintenance and conflict detection
- **Auto-tuning**: `upsert_merge_chunk_size` scales with available memory (5K-20K rows)

See `examples/config-upsert.yaml` for a complete example.

## Performance Benchmarks

### Test Environment
- **Hardware**: MacBook Pro M3 Max, 36GB RAM, 14 cores
- **Dataset**: Stack Overflow 2010 (19.3M rows, 9 tables)
- **Databases**: PostgreSQL 15 and SQL Server 2022 (both in Docker)

### Bulk Copy (drop_recreate mode)
| Direction | Throughput | Notes |
|-----------|------------|-------|
| MSSQL → PostgreSQL | 575K+ rows/sec | COPY protocol |
| PostgreSQL → MSSQL | 419K+ rows/sec | TDS bulk copy |

### Upsert Mode (PG → MSSQL)
| Scenario | Time | Throughput | Notes |
|----------|------|------------|-------|
| Initial load (empty target) | 6m26s | 50K rows/sec | All inserts via staging table |
| Re-run (no changes) | 3m49s | 84K rows/sec | Change detection skips updates |
| Re-run (1 row modified) | 3m40s | 88K rows/sec | Only modified row updated |

### Upsert Mode (MSSQL → PG)
| Scenario | Time | Throughput | Notes |
|----------|------|------------|-------|
| Initial load (empty target) | 1m27s | 222K rows/sec | INSERT ON CONFLICT batches |
| Re-run (no changes) | 1m23s | 233K rows/sec | IS DISTINCT FROM skips updates |
| Re-run (1 row modified) | 1m59s | 163K rows/sec | Only modified row updated |

**Key observations**:
- PostgreSQL upsert is ~4x faster than SQL Server upsert due to efficient `INSERT...ON CONFLICT`
- MSSQL→PG upsert is only ~2.5x slower than bulk COPY (vs 8x for PG→MSSQL)
- `IS DISTINCT FROM` provides efficient NULL-safe change detection
- SQL Server staging table + UPDATE/INSERT pattern has higher overhead

### Same-Engine Migrations (PG → PG)
| Mode | Time | Throughput | Notes |
|------|------|------------|-------|
| drop_recreate | ~47s | ~410K rows/sec | Fresh table creation |
| truncate | ~47s | ~410K rows/sec | Truncate and reload |
| upsert | ~56s | ~345K rows/sec | IS DISTINCT FROM change detection |

### Same-Engine Migrations (MSSQL → MSSQL)
| Mode | Time | Throughput | Notes |
|------|------|------------|-------|
| drop_recreate | ~2m7s | ~152K rows/sec | Fresh table creation |
| truncate | ~1m50s | ~176K rows/sec | Truncate and reload |
| upsert | ~4m18s | ~75K rows/sec | IDENTITY_INSERT + staging table |

**Same-engine notes**:
- Type normalization preserves canonical types (e.g., `int4` → `integer` for PG)
- MSSQL→MSSQL upsert requires IDENTITY_INSERT for identity columns
- Useful for database cloning, environment sync, or disaster recovery

## Building

```bash
# Build with TUI support
CGO_ENABLED=0 go build -o mssql-pg-migrate ./cmd/migrate

# Cross-compile
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o mssql-pg-migrate.exe ./cmd/migrate
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o mssql-pg-migrate-darwin ./cmd/migrate
```

## Dependencies
- `github.com/charmbracelet/bubbletea`: TUI runtime
- `github.com/charmbracelet/lipgloss`: Styling and layout
- `github.com/charmbracelet/bubbles`: UI components (viewport, textinput)
- `github.com/urfave/cli/v2`: Command-line argument parsing
- `modernc.org/sqlite`: Pure-Go SQLite driver

## Development Workflow

### Git Branching
- **Always create a feature/fix branch before making code changes**
- Never commit directly to main
- Merge to main only after testing
- Delete branches after merging

### Building
- **Always build the binary to the repo root after making changes**
- Command: `go build -o mssql-pg-migrate ./cmd/...`
- This ensures the user has an up-to-date binary for testing

### Testing
- Run `go build ./...` before committing
- Run `golangci-lint run --timeout=5m` to check for lint errors
- Test changes manually before pushing

## Code Patterns

### Config Loading
- `config.Load(path)` - Load from file with permission check
- `config.LoadBytes(data)` - Load from bytes (used by TUI wizard)
- `config.DefaultDataDir()` - Get default data directory

### Profile Encryption
- Master key from `MSSQL_PG_MIGRATE_MASTER_KEY` (base64 encoded 32-byte key)
- Generate key: `openssl rand -base64 32`
- Profiles encrypted at rest, decrypted only when loaded

### Error Handling
- Wrap errors with context: `fmt.Errorf("doing X: %w", err)`
- Log warnings but continue for non-fatal issues

## Session History

### Session 3: Encrypted Profiles (Codex - December 20, 2025)
1. Added encrypted profile storage in SQLite (AES-GCM)
2. Added `profile.name` and `profile.description` to config schema
3. Run history now tracks origin (profile vs config file)
4. TUI `/profile` commands with descriptions
5. `/status` improved to suggest `/resume` when applicable
6. Styling updated to Gemini look: darker palette, purple accents

### Session 2: TUI Implementation (Gemini - December 20, 2025)
1. Implemented Bubble Tea TUI with Gemini-style aesthetics
2. Added slash commands, auto-completion, file picker
3. Added wizard for interactive config creation
4. Git status bar integration
5. Line-buffered output capture

### Session 1: Security & Kerberos (Claude - December 20, 2025)
1. Fixed credential storage (sanitization)
2. Added config file permission checks
3. Added Kerberos authentication support
4. Added SSL/TLS configuration options
5. Comprehensive documentation and examples

## Known Issues / TODOs

1. **Kerberos not tested in production** - Implementation complete but needs real environment testing
2. **Windows ACL check is heuristic** - Uses icacls output parsing
3. **Profile encryption key management** - Currently environment variable only

## Contact

Project maintainer: John Dauphine (jdauphine@gmail.com)
