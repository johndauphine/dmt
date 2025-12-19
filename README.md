# mssql-pg-migrate

[![CI](https://github.com/johndauphine/mssql-pg-migrate/actions/workflows/ci.yml/badge.svg)](https://github.com/johndauphine/mssql-pg-migrate/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/johndauphine/mssql-pg-migrate)](https://github.com/johndauphine/mssql-pg-migrate/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/johndauphine/mssql-pg-migrate)](https://go.dev/)
[![License](https://img.shields.io/github/license/johndauphine/mssql-pg-migrate)](LICENSE)

High-performance CLI tool for bidirectional database migration between Microsoft SQL Server and PostgreSQL.

## Performance

- **158,000 rows/sec** throughput (tested with 106M rows)
- **2-3x faster** than equivalent Python/Airflow solutions
- 106M rows migrated in **11 minutes**

## Supported Directions

| Source | Target | Write Method |
|--------|--------|--------------|
| SQL Server | PostgreSQL | COPY protocol (fastest) |
| PostgreSQL | SQL Server | BULK INSERT or batch INSERT |

**Note:** Same-to-same migrations (MSSQL→MSSQL, PG→PG) are not supported. Use native tools for those.

## Features

- **Bidirectional migration** - SQL Server ↔ PostgreSQL
- **Fast transfers** using PostgreSQL COPY protocol (MSSQL→PG) or BULK INSERT (PG→MSSQL)
- **Keyset pagination** for single-column integer PKs (no OFFSET performance degradation)
- **ROW_NUMBER pagination** for composite/varchar PKs
- **Parallel partitioning** - Large tables split via NTILE for concurrent transfer
- **Chunk-level resume** - Progress saved every 10 chunks, resume from exact position
- **Table-level resume** - Skip already-completed tables on restart
- **Idempotent retries** - Partition cleanup on retry prevents duplicates
- **Identity column support** - Preserves auto-increment behavior with sequence reset
- **Row count validation** after transfer
- **Sample data validation** - Random row verification with composite PK support
- **Slack notifications** - Get notified on start, completion, and failures
- **Progress bar** with real-time throughput stats
- **Index creation** - Non-PK indexes (optional)
- **Foreign key creation** (optional)
- **Check constraint creation** (optional)
- **Table filtering** - Include/exclude tables with glob patterns
- **Strict consistency mode** - Disable NOLOCK for consistent reads
- **YAML configuration** with environment variable support
- **Single binary** - no runtime dependencies

## Installation

### Download pre-built binaries

Download from [GitHub Releases](https://github.com/johndauphine/mssql-pg-migrate/releases/latest):

```bash
# Linux x64
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.0.0/mssql-pg-migrate-v1.0.0-linux-amd64.tar.gz
tar -xzf mssql-pg-migrate-v1.0.0-linux-amd64.tar.gz
chmod +x mssql-pg-migrate-linux-amd64
./mssql-pg-migrate-linux-amd64 --version

# macOS Apple Silicon
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.0.0/mssql-pg-migrate-v1.0.0-darwin-arm64.tar.gz
tar -xzf mssql-pg-migrate-v1.0.0-darwin-arm64.tar.gz

# macOS Intel
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.0.0/mssql-pg-migrate-v1.0.0-darwin-amd64.tar.gz
tar -xzf mssql-pg-migrate-v1.0.0-darwin-amd64.tar.gz

# Windows (PowerShell)
Invoke-WebRequest -Uri https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.0.0/mssql-pg-migrate-v1.0.0-windows-amd64.tar.gz -OutFile mssql-pg-migrate.tar.gz
tar -xzf mssql-pg-migrate.tar.gz
```

### Build from source

Requires Go 1.21+

```bash
git clone https://github.com/johndauphine/mssql-pg-migrate.git
cd mssql-pg-migrate
CGO_ENABLED=0 go build -o mssql-pg-migrate ./cmd/migrate
```

### Go install

```bash
go install github.com/johndauphine/mssql-pg-migrate/cmd/migrate@latest
```

## Quick Start

### SQL Server to PostgreSQL

1. Create a `config.yaml`:

```yaml
source:
  type: mssql              # optional, default for source
  host: sqlserver.example.com
  port: 1433
  database: MyDatabase
  user: sa
  password: ${MSSQL_PASSWORD}
  schema: dbo

target:
  type: postgres           # optional, default for target
  host: postgres.example.com
  port: 5432
  database: mydb
  user: postgres
  password: ${PG_PASSWORD}
  schema: public

migration:
  workers: 8
  chunk_size: 200000
```

### PostgreSQL to SQL Server

```yaml
source:
  type: postgres
  host: postgres.example.com
  port: 5432
  database: mydb
  user: postgres
  password: ${PG_PASSWORD}
  schema: public

target:
  type: mssql
  host: sqlserver.example.com
  port: 1433
  database: MyDatabase
  user: sa
  password: ${MSSQL_PASSWORD}
  schema: dbo
  # BULK INSERT configuration (optional but ~10x faster)
  bulk_insert_temp_dir: "/path/on/host"           # Where tool writes CSV files
  bulk_insert_sql_path: "\\\\server\\share\\path" # Path as seen by SQL Server

migration:
  workers: 8
  chunk_size: 200000
```

**Write methods for SQL Server target:**

| Method | Throughput | Configuration |
|--------|-----------|---------------|
| BULK INSERT | ~150,000 rows/sec | Set `bulk_insert_temp_dir` |
| Bulk Copy (TDS) | ~106,000 rows/sec | Default (no temp dir) |
| Batch INSERT | ~16,000 rows/sec | Fallback only |

**Write method selection (automatic):**
1. **BULK INSERT**: Used when `bulk_insert_temp_dir` is configured. Fastest but requires file system access.
2. **Bulk Copy (TDS)**: Default method. Uses native TDS bulk copy protocol (`mssql.CopyIn`). No file system required.
3. **Batch INSERT**: Fallback if bulk copy fails. Slower but most compatible.

**BULK INSERT notes:**
- Requires shared file access between the migration tool and SQL Server
- For Docker: mount a shared volume and set both paths:
  - `bulk_insert_temp_dir`: host path where files are written
  - `bulk_insert_sql_path`: path as seen by SQL Server container
- Falls back to bulk copy if file creation fails

2. Run the migration:

```bash
./mssql-pg-migrate -c config.yaml run
```

3. If interrupted, resume:

```bash
./mssql-pg-migrate -c config.yaml resume
```

## Configuration

Full configuration options:

```yaml
source:
  type: mssql                 # "mssql" (default) or "postgres"
  host: localhost
  port: 1433                  # Default: 1433 for mssql, 5432 for postgres
  database: MyDatabase
  user: sa
  password: ${MSSQL_PASSWORD} # Environment variable
  schema: dbo                 # Default: dbo for mssql, public for postgres

target:
  type: postgres              # "postgres" (default) or "mssql"
  host: localhost
  port: 5432                  # Default: 5432 for postgres, 1433 for mssql
  database: mydb
  user: postgres
  password: ${PG_PASSWORD}
  schema: public              # Default: public for postgres, dbo for mssql
  bulk_insert_temp_dir: ""    # For MSSQL target: local path for temp CSV files
  bulk_insert_sql_path: ""    # For MSSQL target: path as seen by SQL Server (if different)

migration:
  # Connection pools
  max_connections: 12         # Connection pool size (both MSSQL and PG)

  # Transfer settings
  chunk_size: 200000          # Rows per chunk (default: 200000)
  max_partitions: 8           # Max partitions for large tables
  workers: 8                  # Parallel workers
  large_table_threshold: 5000000  # Tables larger than this get partitioned

  # Table filtering (glob patterns)
  include_tables:             # Only migrate these tables (optional)
    - Users
    - Orders*
  exclude_tables:             # Skip these tables
    - __*
    - temp_*
    - audit_*

  # State persistence
  data_dir: ~/.mssql-pg-migrate

  # Target table handling
  target_mode: drop_recreate  # "drop_recreate" (default) or "truncate"

  # Schema objects (all optional, default: true)
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true

  # Consistency
  strict_consistency: false   # Set true to disable NOLOCK

  # Validation
  sample_validation: false    # Enable random row sampling
  sample_size: 100            # Rows per table to sample

# Slack notifications (optional)
slack:
  enabled: true
  webhook_url: ${SLACK_WEBHOOK_URL}
  channel: "#data-engineering"
  username: mssql-pg-migrate
```

## Usage

### Commands

```bash
# Run a new migration
./mssql-pg-migrate -c config.yaml run

# Resume an interrupted migration (continues from last checkpoint)
./mssql-pg-migrate -c config.yaml resume

# Check status of current/last run
./mssql-pg-migrate -c config.yaml status

# Validate row counts between source and target
./mssql-pg-migrate -c config.yaml validate

# View migration history
./mssql-pg-migrate -c config.yaml history
```

### Example Output

```
Starting migration run: a1b2c3d4
Connection pools: MSSQL=12, PostgreSQL=12
Extracting schema...
Found 11 tables
Pagination: 9 keyset, 1 ROW_NUMBER, 1 no PK
Creating target tables (drop and recreate)...
Transferring data...
Transferring 100% |███████████| (106535072/106535072, 158384 rows/s)
Transferred 106535070 rows in 11m13s (158384 rows/sec)

Transfer Profile (per table):
------------------------------
Votes                     query=2.8s (3%), scan=50.6s (55%), write=38.9s (42%), rows=52928720
Comments                  query=1.6s (0%), scan=87.1s (20%), write=346.5s (80%), rows=24534730
Posts                     query=3.2s (0%), scan=2183.3s (73%), write=823.5s (27%), rows=17142169
...

Validation Results:
-------------------
Badges                         OK 8042005 rows
Comments                       OK 24534730 rows
Posts                          OK 17142169 rows
Users                          OK 2465713 rows
Votes                          OK 52928720 rows
```

## How It Works

1. **Extract schema** - Reads table structure, PKs, indexes, FKs, and check constraints from SQL Server
2. **Create tables** - Generates PostgreSQL DDL with proper type mapping and identity columns
3. **Transfer data** - Uses optimal pagination strategy per table:
   - **Keyset pagination** for single-column integer PKs (fastest)
   - **ROW_NUMBER pagination** for composite/varchar PKs
4. **Save progress** - Checkpoints every 10 chunks to SQLite for resume capability
5. **Finalize** - Resets identity sequences, creates primary keys
6. **Create indexes** - Non-PK indexes (if enabled)
7. **Create foreign keys** - FK constraints (if enabled)
8. **Create check constraints** - CHECK constraints (if enabled)
9. **Validate** - Compares row counts and optionally samples random rows

## Resume Capability

The tool saves progress to SQLite (`~/.mssql-pg-migrate/migrate.db`), enabling efficient resume after failures:

### Table-level resume
- Completed tables are skipped entirely on resume
- Verified by comparing row counts between source and target

### Chunk-level resume
- Progress saved every 10 chunks during transfer
- On resume, continues from the exact last successful chunk
- Partial data from interrupted chunks is cleaned up automatically

```bash
# Resume shows what's being skipped/continued
./mssql-pg-migrate -c config.yaml resume

# Output:
# Resuming run: a1b2c3d4 (started 2025-01-15T10:30:00Z)
# Skipping 5 already-complete tables: [Users, Posts, Comments, Badges, Votes]
# Resuming transfer of 2 tables
# Resuming Orders from chunk (lastPK=1234567, rows=5000000)
```

## Pagination Strategies

The tool automatically selects the best pagination strategy per table:

| PK Type | Strategy | Performance |
|---------|----------|-------------|
| Single integer (int, bigint) | Keyset (`WHERE pk > @last`) | Fastest |
| Composite PK | ROW_NUMBER | Good |
| VARCHAR PK | ROW_NUMBER | Good |
| No PK | Rejected | - |

Tables without primary keys are rejected to ensure data correctness.

## Type Mapping

### SQL Server → PostgreSQL

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

Identity columns are mapped to `GENERATED BY DEFAULT AS IDENTITY` with proper sequence reset.

### PostgreSQL → SQL Server

| PostgreSQL | SQL Server |
|------------|------------|
| integer | int |
| bigint | bigint |
| smallint | smallint |
| boolean | bit |
| numeric/decimal | decimal |
| double precision | float |
| real | real |
| char | char |
| varchar/character varying | nvarchar |
| text | nvarchar(max) |
| date | date |
| time | time |
| timestamp | datetime2 |
| timestamptz | datetimeoffset |
| uuid | uniqueidentifier |
| bytea | varbinary(max) |
| json/jsonb | nvarchar(max) |

Serial/identity columns are mapped to `IDENTITY(1,1)` with proper seed reset.

## Benchmarks

Tested on StackOverflow database dumps (Docker containers, same host):

| Dataset | Rows | Duration | Throughput |
|---------|------|----------|------------|
| SO2013 | 106.5M | 11m 13s | 158,000 rows/sec |
| SO2010 | 19.3M | 51s | 378,000 rows/sec |

Performance varies based on:
- Network latency between source and target
- Table structure (wide tables are slower)
- Data types (LOBs are slower)
- Available CPU and memory

## Comparison with Airflow DAG

| Feature | mssql-pg-migrate (Go) | Airflow DAG (Python) |
|---------|----------------------|---------------------|
| Throughput | 158k rows/sec | ~50-80k rows/sec |
| Memory usage | ~50MB | ~200-400MB |
| Resume granularity | Chunk-level | Partition-level |
| Dependencies | None (single binary) | Python, Airflow, etc. |
| Scheduling | External (cron, etc.) | Built-in |
| Monitoring | Slack, CLI | Airflow UI |

Use the Go version for:
- One-time migrations
- Maximum performance
- Minimal dependencies

Use the Airflow DAG for:
- Scheduled/recurring migrations
- Integration with existing Airflow infrastructure
- Complex workflow orchestration

## License

MIT
