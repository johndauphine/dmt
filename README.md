# mssql-pg-migrate

High-performance CLI tool for migrating data from Microsoft SQL Server to PostgreSQL.

## Features

- **Fast transfers** using PostgreSQL COPY protocol
- **Keyset pagination** for single-column integer PKs (no OFFSET performance degradation)
- **ROW_NUMBER pagination** for composite/varchar PKs
- **Parallel partitioning** - Large tables split via NTILE for concurrent transfer
- **Chunked reads** to handle large tables without memory issues
- **Resume capability** via SQLite state database
- **Idempotent retries** - Partition cleanup on retry prevents duplicates
- **Row count validation** after transfer
- **Sample data validation** - Random row verification (optional)
- **Slack notifications** - Get notified on start, completion, and failures
- **Progress bar** with real-time throughput stats
- **Index creation** - Non-PK indexes (optional)
- **Foreign key creation** (optional)
- **Check constraint creation** (optional)
- **Strict consistency mode** - Disable NOLOCK for consistent reads
- **YAML configuration** with environment variable support
- **Single binary** - no runtime dependencies

## Benchmarks

Tested on StackOverflow database dumps (Docker containers, same host):

| Dataset | Rows | Duration | Throughput |
|---------|------|----------|------------|
| SO2010 | 19.3M | 2m 21s | 137,000 rows/sec |
| SO2013 | 106.5M | 12m 25s | 143,000 rows/sec |

## Installation

### From source

Requires Go 1.21+

```bash
git clone https://github.com/johndauphine/mssql-pg-migrate.git
cd mssql-pg-migrate
go build -o mssql-pg-migrate ./cmd/migrate
```

### Cross-compile

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o mssql-pg-migrate-linux ./cmd/migrate

# macOS
GOOS=darwin GOARCH=arm64 go build -o mssql-pg-migrate-mac ./cmd/migrate

# Windows
GOOS=windows GOARCH=amd64 go build -o mssql-pg-migrate.exe ./cmd/migrate
```

## Configuration

Create a `config.yaml` file:

```yaml
source:
  host: localhost
  port: 1433
  database: MyDatabase
  user: sa
  password: ${MSSQL_PASSWORD}  # Environment variable
  schema: dbo

target:
  host: localhost
  port: 5432
  database: mydb
  user: postgres
  password: ${PG_PASSWORD}
  schema: public

migration:
  max_connections: 12      # Connection pool size
  chunk_size: 200000       # Rows per chunk
  max_partitions: 8        # For large table splitting
  workers: 8               # Parallel workers
  large_table_threshold: 5000000
  exclude_tables:
    - __*                  # Glob patterns
    - temp_*
  data_dir: ~/.mssql-pg-migrate

  # Schema objects (optional)
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true

  # Consistency (optional)
  strict_consistency: false  # Set true to disable NOLOCK

  # Validation (optional)
  sample_validation: false
  sample_size: 100

# Slack notifications (optional)
slack:
  enabled: true
  webhook_url: ${SLACK_WEBHOOK_URL}
  channel: "#data-engineering"
  username: mssql-pg-migrate
```

## Usage

### Run a migration

```bash
./mssql-pg-migrate run --config config.yaml
```

### Resume interrupted migration

```bash
./mssql-pg-migrate resume
```

### Check status

```bash
./mssql-pg-migrate status
```

### Validate row counts

```bash
./mssql-pg-migrate validate
```

### View history

```bash
./mssql-pg-migrate history
```

## How it works

1. **Extract schema** - Reads table structure, indexes, FKs, and check constraints from SQL Server
2. **Create tables** - Generates and executes PostgreSQL DDL (as UNLOGGED for speed)
3. **Transfer data** - Uses optimal pagination strategy:
   - **Keyset pagination** for single-column integer PKs (fastest)
   - **ROW_NUMBER pagination** for composite/varchar PKs
4. **Finalize** - Converts to LOGGED tables, resets sequences, creates PKs
5. **Create indexes** - Non-PK indexes (if enabled)
6. **Create foreign keys** - FK constraints (if enabled)
7. **Create check constraints** - CHECK constraints (if enabled)
8. **Validate** - Compares row counts and optionally samples random rows

State is persisted to SQLite (`~/.mssql-pg-migrate/migrate.db`) enabling resume after failures.

## Pagination Strategies

The tool automatically selects the best pagination strategy per table:

| PK Type | Strategy | Performance |
|---------|----------|-------------|
| Single integer (int, bigint) | Keyset (`WHERE pk > @last`) | Fastest |
| Composite PK | ROW_NUMBER | Good |
| VARCHAR PK | ROW_NUMBER | Good |
| No PK | ROW_NUMBER with arbitrary order | Slowest |

## Type Mapping

| SQL Server | PostgreSQL |
|------------|------------|
| int | integer |
| bigint | bigint |
| smallint | smallint |
| tinyint | smallint |
| bit | boolean |
| decimal/numeric | numeric |
| float | double precision |
| real | real |
| money | numeric(19,4) |
| char/nchar | char |
| varchar/nvarchar | varchar |
| text/ntext | text |
| date | date |
| time | time |
| datetime/datetime2 | timestamp |
| datetimeoffset | timestamptz |
| uniqueidentifier | uuid |
| varbinary/image | bytea |
| xml | xml |

## Advanced Options

### Strict Consistency Mode

By default, the tool uses `WITH (NOLOCK)` for maximum throughput. Enable strict consistency for consistent reads (slower):

```yaml
migration:
  strict_consistency: true
```

### Sample Validation

Enable random row sampling to verify data integrity beyond row counts:

```yaml
migration:
  sample_validation: true
  sample_size: 100  # Rows per table
```

### Idempotent Retries

When a partition transfer fails and retries, the tool automatically cleans up any partial data in that partition's PK range before re-transferring. This prevents duplicate rows on retry.

## Roadmap

- [x] NTILE partitioning for large tables
- [x] Parallel partition transfers
- [x] Slack notifications
- [x] Keyset pagination (vs OFFSET)
- [x] ROW_NUMBER pagination for composite PKs
- [x] Unlogged tables during transfer
- [x] Foreign key creation
- [x] Index creation
- [x] Check constraint creation
- [x] Sample data validation
- [x] Strict consistency mode
- [ ] Connection pooling for MSSQL (semaphore-based)
- [ ] Default value extraction
- [ ] Computed column support

## License

MIT
