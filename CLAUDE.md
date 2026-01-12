# AI Context File - mssql-pg-migrate

This file provides context for AI assistants working on this project. Read this before making changes.

## Project Overview

**mssql-pg-migrate** is a high-performance CLI tool for bidirectional database migration between Microsoft SQL Server and PostgreSQL. Written in Go, it supports all 4 migration directions (MSSQL↔PG, PG↔PG, MSSQL↔MSSQL) with throughput ranging from 79K-472K rows/sec depending on direction and mode.

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
├── dbconfig/               # Database configuration types (breaks circular import)
│   └── dbconfig.go         # SourceConfig, TargetConfig structs
├── driver/                  # Pluggable database driver abstractions (NEW)
│   ├── driver.go           # Driver interface, registry pattern
│   ├── reader.go           # Reader interface (source abstraction)
│   ├── writer.go           # Writer interface (target abstraction)
│   ├── types.go            # Table, Column, Partition, Index types
│   ├── typemapper.go       # TypeMapper interface for type conversion
│   ├── postgres/           # PostgreSQL driver implementation
│   │   ├── driver.go       # Driver registration via init()
│   │   ├── reader.go       # PostgresReader (pgx-based)
│   │   ├── writer.go       # PostgresWriter (COPY protocol)
│   │   ├── dialect.go      # PostgreSQL SQL syntax
│   │   └── types.go        # PG type mappings
│   └── mssql/              # SQL Server driver implementation
│       ├── driver.go       # Driver registration via init()
│       ├── reader.go       # MSSQLReader (go-mssqldb)
│       ├── writer.go       # MSSQLWriter (TDS bulk copy)
│       ├── dialect.go      # MSSQL SQL syntax
│       └── types.go        # MSSQL type mappings
├── pipeline/               # Transfer orchestration (NEW)
│   ├── pipeline.go         # Pipeline struct (Reader → Queue → Writer)
│   ├── job.go              # Job, DateFilter types
│   ├── stats.go            # Transfer statistics
│   ├── writer_pool.go      # Parallel writer pool
│   └── checkpoint.go       # Checkpoint coordinator for parallel readers
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
├── source/                 # Source database abstraction (legacy, being replaced by driver/)
│   ├── pool.go            # MSSQL source pool
│   ├── postgres_pool.go   # PostgreSQL source pool
│   └── types.go           # Table, Column, Index, FK structs
├── target/                 # Target database abstraction (legacy, being replaced by driver/)
│   ├── pool.go            # PostgreSQL target pool (COPY protocol)
│   └── mssql_pool.go      # MSSQL target pool (TDS bulk copy)
├── transfer/              # Data transfer engine
│   └── transfer.go        # Chunked transfer with read-ahead/write-ahead pipelining
├── pool/                  # Connection pool factory
│   └── factory.go         # Creates source/target pools using driver registry
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
- **upsert**: Incremental sync - INSERT new rows, UPDATE changed rows, preserve target-only rows

### Pagination Strategies
- **Keyset pagination**: For single-column integer PKs (fastest)
- **ROW_NUMBER pagination**: For composite/varchar PKs
- Tables without PKs are rejected

### Authentication
- **Password**: Traditional user/password (default)
- **Kerberos**: Enterprise SSO via krb5 (MSSQL) or GSSAPI (PostgreSQL)

## Current State (January 2026)

### Latest Commits
```
e263d89 Merge pull request #61 - fix PG→MSSQL upsert with geography columns
df28b7a fix: PG→MSSQL upsert with geography columns
a5bf6c7 docs: update README with v2.1.0 benchmark results
e1b3ccd chore: bump version to 2.1.0
bdcf722 Merge pull request #60 - make factory truly pluggable with driver registry
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
- `migration.target_mode`: `drop_recreate` (default) or `upsert`

### Upsert Mode
Incremental synchronization that preserves target-only data:
- **Requirements**: All tables must have primary keys; target tables must already exist
- **Workflow**: Run `drop_recreate` first for initial load, then `upsert` for incremental syncs
- **PostgreSQL**: Uses batched `INSERT...ON CONFLICT DO UPDATE` with `IS DISTINCT FROM` change detection
- **SQL Server**: Uses staging table + `UPDATE`/`INSERT` with `EXCEPT` change detection
- **No deletes**: Rows only in target are preserved
- **Performance**: 2-5x slower than bulk copy due to index maintenance and conflict detection
- **Auto-tuning**: `upsert_merge_chunk_size` scales with available memory (5K-20K rows)

### Date-Based Incremental Loading (v1.31.0+)
For fast delta transfers using highwater marks:
- **Configuration**: `date_updated_columns` lists column names to check (e.g., `UpdatedAt`, `ModifiedDate`)
- **Highwater marks**: Stored in `table_sync_timestamps` table in state database
- **First run**: Full load, records sync timestamp per table
- **Subsequent runs**: Only fetches rows where `date_column > last_sync_timestamp`
- **Performance**: Reduces sync time from minutes to seconds when data hasn't changed

See `examples/config-upsert.yaml` for a complete example.

## Performance Benchmarks

### Test Environment
- **Hardware**: MacBook Pro M3 Max, 36GB RAM, 14 cores
- **Dataset**: Stack Overflow 2010 (19.3M rows, 9 tables)
- **Databases**: PostgreSQL 15 and SQL Server 2022 (both in Docker)
- **Last tested**: January 2026

### Complete Benchmark Matrix

All permutations of source, target, and mode (19.3M rows each):

| Direction | drop_recreate | upsert |
|-----------|---------------|--------|
| **MSSQL → PG** | 400-425K rows/sec | 267-430K rows/sec |
| **PG → MSSQL** | 196K rows/sec | 148K rows/sec |
| **MSSQL → MSSQL** | 464-499K rows/sec | 350-381K rows/sec |
| **PG → PG** | 472K rows/sec | 337K rows/sec |

*Note: MSSQL source performance improved significantly in v1.40.0 with 32KB TDS packet size (default).*

### Key Observations

**Cross-engine migrations:**
- MSSQL → PG is faster than PG → MSSQL (PG COPY protocol is more efficient than MSSQL bulk insert)
- PostgreSQL's `INSERT...ON CONFLICT` with `IS DISTINCT FROM` provides efficient upsert
- MSSQL upsert uses staging tables + MERGE which has higher overhead

**Same-engine migrations:**
- PG → PG is fastest overall due to COPY protocol on both ends
- MSSQL → MSSQL upsert is slowest due to IDENTITY_INSERT handling and MERGE complexity
- Useful for database cloning, environment sync, or disaster recovery

**Upsert mode overhead:**
- PG targets: ~8-29% slower than drop_recreate
- MSSQL targets: ~24-57% slower than drop_recreate
- MSSQL → MSSQL upsert has highest overhead due to staging table + MERGE pattern

**Column name case handling:**
- PG → MSSQL upsert correctly maps lowercase PG columns to mixed-case MSSQL columns
- Uses case-insensitive column name mapping via staging table introspection

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

### Session 16: PG→MSSQL Geography Upsert Fix (Claude - January 12, 2026)
1. **PR #61 - Fix PG→MSSQL upsert with geography columns**:
   - Error: "Implicit conversion from data type geography to nvarchar(max) is not allowed"
   - Root cause: SQL Server doesn't allow ALTER COLUMN from geography to nvarchar(max)
   - Previous approach tried `ALTER TABLE ... ALTER COLUMN` which failed
   - Solution: Use `DROP COLUMN` + `ADD COLUMN` instead of `ALTER COLUMN`
   - Staging table geography columns are dropped and re-added as `nvarchar(max)` for WKT text
   - MERGE still uses `geography::STGeomFromText()` to convert back to geography type
2. **WWI Benchmark Results (all 4 directions, both modes)**:
   - Test environment: WSL2 on Windows, 32GB RAM, PostgreSQL 15 and SQL Server 2022 in Docker
   - Dataset: WideWorldImporters Sales schema (9 tables, 701K rows)

   | Direction | drop_recreate | upsert |
   |-----------|---------------|--------|
   | PG → PG | 563K rows/sec | 694K rows/sec |
   | PG → MSSQL | 645K rows/sec | 492K rows/sec |
   | MSSQL → PG | 248K rows/sec | 295K rows/sec |
   | MSSQL → MSSQL | 222K rows/sec | 507K rows/sec |

   - Upsert mode is faster for 3/4 directions (pre-existing tables, no DDL overhead)
   - PG→MSSQL upsert 24% slower due to staging table + MERGE overhead
3. Released v2.2.0

### Session 15: Pluggable Factory & BuildRowNumberQuery Fix (Claude - January 12, 2026)
1. **PR #60 - Make Factory Truly Pluggable**:
   - Addressed Codex feedback: factory still had switch statements on mssql/postgres
   - Refactored `pool/factory.go` to call `d.NewReader()` and `d.NewWriter()` directly
   - No switch statements - adding new driver requires zero changes to factory
   - Made `source.Table`, `source.Column` type aliases for `driver.*` types
   - Made `pool.SourcePool`, `pool.TargetPool` type aliases for `driver.Reader`, `driver.Writer`
   - Added `ExecRaw()` and `QueryRowRaw()` to Writer interface for raw SQL operations
   - Refactored transfer.go and validator.go to use `DBType()` checks instead of type assertions
2. **BuildRowNumberQuery CTE Fix**:
   - Critical bug: outer SELECT in CTE used full expressions like `[Col].STAsText() AS [Col]`
   - CTE only exposes aliases, not original expressions - caused "column not found" errors
   - Added `extractColumnAliases()` function to parse column aliases for outer SELECT
   - Fixed in: `driver/mssql/dialect.go`, `driver/postgres/dialect.go`, `dialect/dialect.go`
3. **PostgreSQL Identifier Sanitization**:
   - New `driver/postgres/writer.go` wasn't sanitizing identifiers like old code path
   - Added `sanitizePGIdentifier()` function (lowercase, replace special chars)
   - Updated all DDL methods: generateDDL, DropTable, TruncateTable, CreatePrimaryKey, etc.
   - Ensures consistency between DDL generation and data transfer
4. Test results (WWI MSSQL→PG):
   - 9/9 tables migrated successfully
   - 701,604 rows at 269K rows/sec
   - Customers table (663 rows with geography) transferred correctly
5. Reviews:
   - Copilot: Approved, no comments (reviewed 13/18 files)
   - Codex: Found false positive (WriteBatch sanitization) - caller sanitizes in transfer.go
6. Released v2.1.0

### Session 14: Config-Driver Circular Import Fix (Claude - January 12, 2026)
1. Addressed remaining pluggable driver blockers identified by Codex review:
   - Config validation was hard-coding mssql/postgres (config.go:660-672)
   - Factory used legacy switch instead of driver registry (factory.go:16-69)
2. **PR #58 - Driver Alias Validation**:
   - Initially tried importing driver package in config.go - hit circular import error
   - Workaround: Created `supportedDrivers` map in config.go with hardcoded aliases
   - Added helper functions: `canonicalDriverName()`, `isValidDriverType()`, `availableDriverTypes()`
   - Updated factory.go to use `driver.Get()` then switch on canonical `d.Name()`
   - Codex review caught case-sensitivity bug: fixed with `strings.ToLower()` in registry
3. **PR #59 - Breaking Circular Import**:
   - Gemini review noted `supportedDrivers` map duplicates driver registry info
   - Created `internal/dbconfig/dbconfig.go` with `SourceConfig` and `TargetConfig` types
   - Both driver and config packages can now import dbconfig without cycles
   - Updated config.go to use type aliases: `type SourceConfig = dbconfig.SourceConfig`
   - Removed `supportedDrivers` workaround - now uses driver registry directly
   - Config validation uses `driver.IsRegistered()` for validation
   - Factory uses `driver.Get()` to resolve aliases to canonical names
4. Architecture improvements:
   - Driver registry is now single source of truth for driver validation
   - Adding new driver only requires implementing in `internal/driver/<name>/`
   - No changes needed to config.go or factory.go for new drivers
5. Pre-commit test runner: All tests pass, no regressions

### Session 13: Pluggable Database Architecture (Claude - January 11, 2026)
1. Implemented pluggable database architecture (PR #56) - Phases 1-5 of refactoring plan:
   - Enables adding new databases (MySQL, Oracle, etc.) with zero changes to core code
2. **Phase 1-2: Driver Package & PostgreSQL Driver**:
   - Created `internal/driver/` with core interfaces (Driver, Reader, Writer, Dialect, TypeMapper)
   - Driver registry pattern with `init()` registration and alias support
   - PostgreSQL driver: `internal/driver/postgres/` (~1,500 lines)
   - Reader uses pgx for streaming, Writer uses COPY protocol
3. **Phase 3: MSSQL Driver**:
   - SQL Server driver: `internal/driver/mssql/` (~2,000 lines)
   - Reader uses go-mssqldb, Writer uses TDS bulk copy
   - Full dialect support (bracket quoting, @p parameters, NOLOCK hints)
   - Type mapper for PG→MSSQL and MSSQL→MSSQL conversions
4. **Phase 4: Pipeline Package**:
   - Created `internal/pipeline/` (~1,500 lines) for Reader → Writer orchestration
   - `Pipeline` struct with `Execute()` method
   - Writer pool for parallel writes
   - Checkpoint coordinator for multi-reader keyset pagination
   - Stats tracking (query time, scan time, write time)
5. **Phase 5: Factory Integration**:
   - Updated `internal/pool/factory.go` to use driver registry
   - Validates database types via `driver.Get()`
   - Supports aliases (postgresql, pg, sqlserver, sql-server)
   - Backward compatible with existing pool interfaces
6. All tests pass, binary builds successfully
7. Remaining phases for follow-up PRs:
   - Phase 6: AI type mapper with configurable providers (optional)
   - Phase 7: Cleanup deprecated packages

### Session 12: SRID Hardcoding Fix (Claude - January 11, 2026)
1. Addressed remaining SRID hardcoding issue from Gemini CLI review (PR #47):
   - Previous implementation hardcoded SRID 4326 (WGS84) for all geography/geometry conversions
   - PostGIS columns with custom SRIDs (e.g., NAD83, Web Mercator) would be incorrectly converted
2. Added SRID field to Column struct (`internal/source/schema.go`):
   - New `SRID int` field stores spatial reference ID for geography/geometry columns
3. Query SRID from PostGIS metadata (`internal/source/postgres_pool.go`):
   - Added `loadSpatialSRIDs()` function
   - Queries `geometry_columns` and `geography_columns` views during schema extraction
   - Gracefully handles missing PostGIS (SRID remains 0)
4. Pass SRID through transfer pipeline:
   - Added `colSRIDs []int` parameter to `UpsertChunkWithWriter` interface
   - Updated `executeKeysetPagination`, `executeRowNumberPagination`, `writeChunkUpsertWithWriter`
   - SRID values parallel to colTypes array
5. Updated `buildMSSQLMergeWithTablock()`:
   - `SpatialColumn` struct now includes `SRID int` field
   - Uses actual SRID from source in `STGeomFromText(source.col, SRID)`
   - Defaults to 4326 only when SRID is 0 (unset)
6. Added unit tests for custom SRID handling:
   - Test for SRID 2163 (NAD83 / US National Atlas Equal Area)
   - Test for SRID 3857 (Web Mercator)
   - Test for SRID 0 fallback to default 4326
7. All CI checks passed, PR merged to main

### Session 11: Security & Correctness Fixes (Claude - January 11, 2026)
1. Ran Gemini CLI code review on codebase - identified security vulnerabilities
2. Fixed DSN injection vulnerabilities (PR #46):
   - Added `url.QueryEscape()` for user, password, database in connection strings
   - Fixed in `source/pool.go`, `source/postgres_pool.go`, `target/mssql_pool.go`
   - Prevents credential injection via special characters
3. Fixed SQL injection in `tableColumns()` (checkpoint/state.go):
   - SQLite PRAGMA doesn't support parameterized queries
   - Added whitelist validation for allowed table names (runs, tasks, profiles, table_sync_timestamps)
4. Improved `isASCIINumeric()` robustness:
   - Rewrote to properly validate numeric format
   - Validates sign placement, single decimal point, scientific notation
   - Prevents false positives like ".", "+-1", "1.2.3"
5. Fixed spatial column detection for same-engine migrations:
   - Previous refactor only detected spatial columns for cross-engine (PG→MSSQL)
   - MSSQL→MSSQL with geography columns would fail change detection
   - Now always detects spatial columns via `getSpatialColumns()`
   - Only alters columns to nvarchar(max) for cross-engine
6. Added `SpatialColumn` struct to track column type (geography vs geometry):
   - Ensures correct STGeomFromText prefix (`geography::` vs `geometry::`)
   - Updated `buildMSSQLMergeWithTablock()` signature to accept `[]SpatialColumn`
7. Senior-data-engineer agent review: Grade A-
8. Pre-commit test runner validated all 8 migration paths:
   - All passed: MSSQL↔PG, PG↔PG, MSSQL↔MSSQL × drop_recreate/upsert
   - 5.6M rows transferred, 389K rows/sec average
   - Geography columns working in all scenarios
9. Released v1.42.0

### Session 10: PG→MSSQL Geography Staging Table Fix (Claude - January 11, 2026)
1. Fixed PG→MSSQL upsert failing for tables with geography columns (PR #45):
   - Previous fix (PR #43) only worked when `colTypes` contained target types
   - For PG→MSSQL, source type is `text` (WKT), not `geography`
   - Staging table created from target had `geography` type, but `colTypes` had `text`
   - Bulk insert failed: `invalid type for Binary column: string POINT...`
2. Solution: Query staging table directly to find spatial columns:
   - Query `tempdb.sys.columns` to find geography/geometry columns in staging table
   - Alter those columns to `nvarchar(max)` to accept WKT text
   - Return list of spatial column names to MERGE builder
   - MERGE uses `geography::STGeomFromText(source.col, 4326)` to convert back
3. Refactored `alterSpatialColumnsToText()`:
   - No longer takes `colTypes` parameter (was unreliable for cross-engine)
   - Queries staging table directly for spatial column detection
   - Returns list of altered columns for use in MERGE
4. Refactored `buildMSSQLMergeWithTablock()`:
   - Takes `spatialCols []string` instead of `colTypes` and `isCrossEngine`
   - Uses spatial column list for WKT→geography conversion
5. Test results (WWI Sales with geography):
   - PG→MSSQL upsert: **9/9 tables ✓** (was 8/9), 403-428K rows/sec
6. Released v1.41.0

### Session 9: MSSQL Performance Optimization (Claude - January 11, 2026)
1. Added `packet_size` config parameter for MSSQL connections (PR #44):
   - TDS protocol packet size in bytes (default: 32767, max: 32767)
   - go-mssqldb driver defaults to 4KB which limits throughput
   - 32KB packets significantly improve MSSQL read/write performance
2. Changed `encrypt` config from string to `*bool`:
   - More intuitive: `encrypt: false` instead of `encrypt: "false"`
   - Default: `true` (secure by default)
   - Nil-safe: Added nil checks in SourceDSN(), TargetDSN(), DebugDump()
3. Performance improvements with 32KB packet size:
   - **MSSQL→PG**: 323K → 400-425K rows/sec (+27%)
   - **MSSQL→MSSQL**: 183K → 464-499K rows/sec (+162%)
   - **MSSQL→MSSQL upsert**: 79K → 350-381K rows/sec (+380%)
4. Released v1.40.0 with MSSQL performance optimizations

### Session 8: PG→MSSQL Geography Upsert Fix (Claude - January 11, 2026)
1. Fixed PG→MSSQL upsert for tables with geography/geometry columns (PR #43):
   - PostgreSQL sends spatial data as WKT text (e.g., `POINT (-108.55 39.04)`)
   - SQL Server bulk insert expected binary geography data, causing error:
     `invalid type for Binary column: string POINT (-108.5523153 39.0430375)`
   - Solution: Alter staging table spatial columns to `nvarchar(max)` for WKT text
   - In MERGE, convert WKT back using `geography::STGeomFromText(source.col, 4326)`
   - SRID 4326 is WGS84 (standard GPS coordinates)
2. Updated `buildMSSQLMergeWithTablock` to accept `isCrossEngine` parameter:
   - Detects cross-engine via `sourceType == "postgres"`
   - Only applies WKT conversion for PG→MSSQL, not MSSQL→MSSQL
3. Added unit tests for cross-engine geography/geometry conversion
4. Released v1.32.0 with PG→MSSQL geography upsert support
5. Complete WWI upsert test results (all 4 directions with geography columns):
   - **Test environment**: WSL2 with 32GB RAM, PostgreSQL 15 and SQL Server 2022 in Docker
   - **Dataset**: WideWorldImporters Sales schema (9 tables, 701K rows, includes Customers with geography)

   | Direction | Status | Throughput |
   |-----------|--------|------------|
   | MSSQL→MSSQL | ✓ 9/9 tables | 202K rows/sec |
   | MSSQL→PG | ✓ 9/9 tables | 219K rows/sec |
   | PG→MSSQL | ✓ 9/9 tables | 388K rows/sec |
   | PG→PG | ✓ 9/9 tables | 482K rows/sec |

   All directions now fully support upsert mode with geography/geometry columns.

### Session 7: MSSQL to MSSQL Fixes & Geography Support (Claude - January 11, 2026)
1. Fixed MSSQL→MSSQL decimal column bulk copy (PR #39):
   - SQL Server returns decimal/money as `[]byte` (ASCII string like "3000.00")
   - go-mssqldb bulk copy doesn't handle `[]byte` for decimal columns
   - Added `convertRowForBulkCopy()` to convert `[]byte` to string
2. Fixed binary data preservation in bulk copy (PR #40):
   - Initial fix converted ALL `[]byte` to string, breaking geography columns
   - Added `isASCIINumeric()` to only convert numeric byte arrays
   - Geography/varbinary data now passes through unchanged
3. Added geography/geometry support for upsert mode (PR #42):
   - SQL Server doesn't support `<>` operator on spatial types
   - MERGE change detection was failing for tables with geography columns
   - Solution: Pass column types through pipeline, skip spatial columns from change detection
   - Spatial columns are still updated (SET clause) but not compared
   - Updated `UpsertChunkWithWriter` interface to accept `colTypes`
   - Added unit tests for geography/geometry exclusion
4. Added helpful warning for geography upsert failures (PR #41):
   - Shows HINT message when spatial comparison error detected
   - Suggests using `drop_recreate` or `exclude_tables`
5. Test results with WideWorldImporters:
   - MSSQL→MSSQL drop_recreate: 9/9 tables, 701K rows at 264K rows/sec
   - MSSQL→MSSQL upsert: 9/9 tables (including Customers with geography), 202K rows/sec
6. Updated README: Added then removed "Known Limitations" for geography (now fixed)

### Session 6: Incremental Logging & WideWorldImporters Testing (Claude - January 11, 2026)
1. Investigated why 25 rows transferred during incremental sync - discovered lookup tables (`posttypes`, `linktypes`, `votetypes`) without date columns sync fully each run
2. Improved incremental sync logging for all migration paths (PR #37):
   - Incremental: `Table X: incremental - syncing rows where column > timestamp`
   - First sync: `Table X: first sync - loading all N rows, will use column for future incremental syncs`
   - Full sync: `Table X: full sync - no date column, syncing all N rows (repeated each run)`
   - Added summary: `Incremental sync summary: 6 tables incremental, 0 tables first sync, 3 tables full sync`
3. Set up WideWorldImporters sample database for testing:
   - Downloaded from Microsoft GitHub releases (122MB backup)
   - Created Docker containers: `mssql-wwi` (port 1435), `pg-wwi` (port 5435)
   - Database has 48 tables, 4.7M rows across 4 schemas
4. Discovered limitations with WideWorldImporters:
   - **Geography columns**: SQL Server spatial data causes UTF-8 errors (needs PostGIS or WKT conversion)
   - **Archive tables**: Temporal history tables (`*_Archive`) don't have primary keys
5. Created benchmark configs for WWI:
   - `examples/benchmark-wwi-sales.yaml`: 8 tables, 700K rows, 428K rows/sec
   - `examples/benchmark-wwi-warehouse.yaml`: 9 tables, 300K rows, 172K rows/sec

### Session 5: Incremental Sync & Upsert Improvements (Claude - January 10, 2026)
1. Ran full benchmark suite with SO2010 dataset (downloaded via aria2, restored to Docker containers)
2. Tested all 4 migration directions: MSSQL→PG, PG→MSSQL, PG→PG, MSSQL→MSSQL
3. Tested upsert mode with `date_updated_columns` for incremental loading
4. Modified upsert mode to require existing tables (fail if missing)
5. Improved validation to collect all errors (missing tables, missing PKs) before failing
6. Updated `examples/config-upsert.yaml` with workflow documentation
7. Created PR #36, addressed Copilot review feedback, merged to main
8. Released v1.31.0 with incremental sync as headline feature
9. Updated README with new "Incremental Sync" section and workflow guide
10. Built and uploaded binaries for Linux, macOS (Intel/ARM), Windows

### Session 4: Same-Engine Migrations (Claude - December 23, 2025)
1. Implemented PG→PG and MSSQL→MSSQL migrations with source-type aware DDL generation
2. Added `sourceType` parameter to pool factory and target pools
3. Updated DDL generation to use unified `typemap.MapType()` for all 4 directions
4. Added IDENTITY_INSERT handling for MSSQL→MSSQL upsert mode
5. Fixed SQL injection vulnerability in identity column detection (parameterized query)
6. Added unit tests for DDL generation with all migration directions
7. Tested all modes (drop_recreate, upsert) with SO2010 dataset
8. Released v1.16.0 with same-engine migration support
9. Updated README with new feature documentation and download URLs

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

1. **Tables without PKs rejected** - Temporal archive tables (`*_Archive`) and other PK-less tables cannot be migrated
2. **Kerberos not tested in production** - Implementation complete but needs real environment testing
3. **Windows ACL check is heuristic** - Uses icacls output parsing
4. **Profile encryption key management** - Currently environment variable only
5. **Geography/geometry upsert behavior** - Spatial columns are always updated in upsert mode (not compared for changes) since SQL Server doesn't support `<>` on spatial types

## Contact

Project maintainer: John Dauphine (jdauphine@gmail.com)
