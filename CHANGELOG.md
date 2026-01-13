# Changelog

All notable changes to this project will be documented in this file.

## [2.28.0] - 2026-01-13

### Changed
- **Renamed environment variable** - `DATA_TRANSFER_TOOL_MASTER_KEY` → `DMT_MASTER_KEY` for profile encryption
- **Updated TUI branding** - New ASCII art logo and version display
- **Centralized version** - Version now managed in `internal/version` package

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
