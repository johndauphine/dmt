# mssql-pg-migrate

High-performance CLI tool for migrating data from Microsoft SQL Server to PostgreSQL.

## Features

- **Fast transfers** using PostgreSQL COPY protocol
- **Chunked reads** to handle large tables without memory issues
- **Resume capability** via SQLite state database
- **Row count validation** after transfer
- **Progress bar** with real-time throughput stats
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

1. **Extract schema** - Reads table structure from SQL Server
2. **Create tables** - Generates and executes PostgreSQL DDL
3. **Transfer data** - Reads chunks from MSSQL, writes via COPY to PostgreSQL
4. **Finalize** - Resets sequences, creates constraints
5. **Validate** - Compares row counts between source and target

State is persisted to SQLite (`~/.mssql-pg-migrate/migrate.db`) enabling resume after failures.

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

## Roadmap

- [ ] NTILE partitioning for large tables
- [ ] Parallel partition transfers
- [ ] Keyset pagination (vs OFFSET)
- [ ] Unlogged tables during transfer
- [ ] Connection pooling for MSSQL
- [ ] Foreign key creation
- [ ] Index creation
- [ ] Composite primary key support

## License

MIT
