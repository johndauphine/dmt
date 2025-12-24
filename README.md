# mssql-pg-migrate

[![CI](https://github.com/johndauphine/mssql-pg-migrate/actions/workflows/ci.yml/badge.svg)](https://github.com/johndauphine/mssql-pg-migrate/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/johndauphine/mssql-pg-migrate)](https://github.com/johndauphine/mssql-pg-migrate/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/johndauphine/mssql-pg-migrate)](https://go.dev/)
[![License](https://img.shields.io/github/license/johndauphine/mssql-pg-migrate)](LICENSE)

High-performance CLI tool for bidirectional database migration between Microsoft SQL Server and PostgreSQL.

## Interactive Mode (New!)

Launch the tool without arguments to enter the **Interactive Shell**, a modern TUI designed for ease of use.

```bash
./mssql-pg-migrate
```

### Features
*   **Slash Commands**: Type `/` to see all available commands (e.g., `/run`, `/wizard`, `/status`).
*   **Resume**: Use `/resume` to continue interrupted migrations.
*   **Auto-Completion**:
    *   **Commands**: Tab-complete commands like `/validate` or `/history`.
    *   **Files**: Type `@` to browse and select configuration files from your current directory (e.g., `/run @conf<TAB>`).
*   **Configuration Wizard**: Type `/wizard` to interactively create or edit your `config.yaml`. It guides you through connection details, SSL settings, and performance tuning.
*   **Live Monitoring**: Watch migration progress with real-time logs and visual status indicators.
*   **Git Integration**: View your current branch and repository status directly in the status bar.

## Security Notice

**Versions prior to v1.10.0** stored database credentials in plaintext in the SQLite state database (`~/.mssql-pg-migrate/migrate.db`). If you used an earlier version, your passwords may be stored in this file.

**Recommended actions:**
1. **Upgrade to v1.10.0 or later** - The tool now automatically sanitizes any stored credentials on startup
2. **Rotate your database passwords** if you used earlier versions with sensitive credentials
3. **Delete the state database** if you want to ensure all traces are removed: `rm ~/.mssql-pg-migrate/migrate.db`

Starting with v1.10.0, credentials are always redacted before being stored.

## Encrypted Profiles (SQLite)

You can store full configuration profiles (including secrets) encrypted at rest inside the same SQLite database used for run history.

**Master key**
- Set `MSSQL_PG_MIGRATE_MASTER_KEY` to a **base64-encoded 32-byte key**.
- Example key generation (POSIX):
  ```bash
  openssl rand -base64 32
  ```
- Without this key, profile operations will fail, but YAML-based workflows continue to work.

**CLI workflow**
```bash
# Save a profile from YAML (encrypts and stores in SQLite)
MSSQL_PG_MIGRATE_MASTER_KEY=... ./mssql-pg-migrate profile save --name prod --config config.yaml

# List profiles
MSSQL_PG_MIGRATE_MASTER_KEY=... ./mssql-pg-migrate profile list

# Run using a profile
MSSQL_PG_MIGRATE_MASTER_KEY=... ./mssql-pg-migrate run --profile prod

# Export a profile back to YAML
MSSQL_PG_MIGRATE_MASTER_KEY=... ./mssql-pg-migrate profile export --name prod --out config.yaml
```

**YAML profile name (optional)**
```yaml
profile:
  name: prod
  description: |
    Production profile for nightly migrations.
    Uses MSSQL source and PostgreSQL target.
```

If `profile.name` is present, `profile save` can infer the name when `--name` is omitted.
Descriptions are shown in `profile list`.

**TUI workflow**
```
/profile save prod @config.yaml
/profile save @config.yaml      # infers name from profile.name or filename
/profile list
/run --profile prod
/profile export prod @config.yaml
```

**Airflow note**
- Profiles are stored as encrypted blobs in the same SQLite DB (`~/.mssql-pg-migrate/migrate.db` by default).
- In Airflow, you can set `MSSQL_PG_MIGRATE_MASTER_KEY` via your secrets backend and run `profile save` at deploy time, or stick with YAML + env vars for CI/CD.
- You can relocate the SQLite DB by setting `migration.data_dir` in your config (e.g., to a shared volume).
- On first run, the default data directory (`~/.mssql-pg-migrate`) is created automatically if it does not exist.

## State File Backend (Airflow/Kubernetes)

For headless environments like Airflow or Kubernetes where SQLite may be impractical, you can use a YAML-based state file instead.

```bash
# Use a YAML state file instead of SQLite
./mssql-pg-migrate -c config.yaml --state-file /tmp/migration-state.yaml run

# Resume using the same state file
./mssql-pg-migrate -c config.yaml --state-file /tmp/migration-state.yaml resume

# Check status
./mssql-pg-migrate -c config.yaml --state-file /tmp/migration-state.yaml status

# View history
./mssql-pg-migrate -c config.yaml --state-file /tmp/migration-state.yaml history
```

**State file features:**
- **Portable** - Single YAML file, easy to store in cloud storage or shared volumes
- **Human-readable** - Inspect and debug migration state directly
- **Chunk-level resume** - Same resume granularity as SQLite backend
- **Error tracking** - Failed runs store the error message for debugging

**Example state file:**
```yaml
run_id: a1b2c3d4
started_at: 2025-01-15T10:30:00Z
completed_at: 2025-01-15T10:45:00Z
status: success
source_schema: dbo
target_schema: public
config_hash: 2bd314ff9b5251d5
config_path: /path/to/config.yaml
tables:
  transfer:dbo.Users:
    status: success
    last_pk: 2465713
    rows_done: 2465713
    rows_total: 2465713
    task_id: 1001
  transfer:dbo.Posts:
    status: success
    last_pk: 17142169
    rows_done: 17142169
    rows_total: 17142169
    task_id: 1002
```

**When to use state file vs SQLite:**

| Feature | SQLite (default) | State File (`--state-file`) |
|---------|------------------|----------------------------|
| History | Full run history | Current run only |
| Profiles | Encrypted storage | Not supported |
| Best for | Desktop, TUI | Airflow, Kubernetes, CI/CD |
| Persistence | Local database | Any storage (S3, NFS, etc.) |

## Airflow Integration

The CLI provides first-class support for Airflow with machine-readable outputs and deterministic run IDs.

### Airflow CLI Flags

| Flag | Description |
|------|-------------|
| `--run-id <id>` | Explicit run ID (default: auto-generated UUID). Use `{{ dag_run.run_id }}` in Airflow. |
| `--output-json` | Output JSON result to stdout on completion (logs go to stderr) |
| `--output-file <path>` | Write JSON result to file on completion |
| `--log-format=json` | Structured JSONL logging (one JSON object per line) |
| `status --json` | Output current status as JSON (for Airflow sensors) |
| `--force-resume` | Bypass config hash validation on resume |

### BashOperator Example

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('mssql_to_pg_migration', start_date=datetime(2025, 1, 1)) as dag:

    migrate = BashOperator(
        task_id='migrate_data',
        bash_command='''
            /opt/mssql-pg-migrate \
                --run-id "{{ dag_run.run_id }}" \
                --output-json \
                --output-file /tmp/{{ dag_run.run_id }}_result.json \
                --log-format=json \
                --state-file /tmp/{{ dag_run.run_id }}_state.yaml \
                -c /opt/configs/migration.yaml \
                run
        ''',
        do_xcom_push=True,  # Captures stdout JSON for downstream tasks
    )
```

### KubernetesPodOperator Example

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

migrate = KubernetesPodOperator(
    task_id='migrate_data',
    name='mssql-pg-migrate',
    image='your-registry/mssql-pg-migrate:latest',
    cmds=['/mssql-pg-migrate'],
    arguments=[
        '--run-id', '{{ dag_run.run_id }}',
        '--output-json',
        '--log-format', 'json',
        '--state-file', '/data/state.yaml',
        '-c', '/config/migration.yaml',
        'run'
    ],
    volumes=[...],
    volume_mounts=[...],
    get_logs=True,
    do_xcom_push=True,
)
```

### JSON Output Format

**Migration Result** (`--output-json` / `--output-file`):
```json
{
  "run_id": "dag_2025_01_15",
  "status": "success",
  "started_at": "2025-01-15T10:00:00Z",
  "completed_at": "2025-01-15T10:01:34Z",
  "duration_seconds": 94,
  "tables_total": 9,
  "tables_success": 9,
  "tables_failed": 0,
  "rows_transferred": 19310703,
  "rows_per_second": 205432,
  "failed_tables": [],
  "table_stats": [
    {"name": "Users", "rows": 299398, "status": "success"},
    {"name": "Posts", "rows": 3729195, "status": "success"}
  ]
}
```

**Status Result** (`status --json` - for Airflow sensors):
```json
{
  "run_id": "dag_2025_01_15",
  "status": "running",
  "phase": "transferring",
  "started_at": "2025-01-15T10:00:00Z",
  "tables_total": 9,
  "tables_complete": 5,
  "tables_running": 2,
  "tables_pending": 2,
  "rows_transferred": 12500000,
  "progress_percent": 65
}
```

**JSON Log Format** (`--log-format=json` - JSONL to stderr):
```json
{"ts":"2025-01-15T10:00:01Z","level":"info","msg":"Starting migration run: dag_2025_01_15"}
{"ts":"2025-01-15T10:00:02Z","level":"info","msg":"Found 9 tables"}
{"ts":"2025-01-15T10:00:03Z","level":"info","msg":"Transferring data..."}
```

### Config Hash Validation

When using `--state-file`, the tool stores a hash of the config at run start. On `resume`:

- If the config has changed, resume is blocked with an error showing both hashes
- Use `--force-resume` to bypass this check (useful for intentional config tweaks)
- This prevents accidentally resuming with mismatched source/target settings

```bash
# Config changed error
Error: config changed since run started (hash 6abfe692 != 1cddb8e0), use --force-resume to override

# Force resume anyway
./mssql-pg-migrate --state-file state.yaml -c config.yaml resume --force-resume
```

### Airflow Sensor Pattern

Poll migration status from a separate task:

```python
from airflow.sensors.python import PythonSensor
import subprocess
import json

def check_migration_status(**context):
    result = subprocess.run([
        '/opt/mssql-pg-migrate',
        '--state-file', f"/tmp/{context['dag_run'].run_id}_state.yaml",
        '-c', '/opt/configs/migration.yaml',
        'status', '--json'
    ], capture_output=True, text=True)

    status = json.loads(result.stdout)
    if status['status'] == 'success':
        return True
    elif status['status'] == 'failed':
        raise Exception(f"Migration failed: {status.get('error')}")
    return False  # Still running

sensor = PythonSensor(
    task_id='wait_for_migration',
    python_callable=check_migration_status,
    poke_interval=60,
    timeout=3600,
)
```

## Performance

- **575,000 rows/sec** MSSQL → PostgreSQL (auto-tuned, 19M rows in 34s)
- **419,000 rows/sec** PostgreSQL → MSSQL (8 writers, 19M rows in 46s)
- **Auto-tuning** based on CPU cores and available RAM
- **3-4x faster** than equivalent Python/Airflow solutions

## Supported Directions

| Source | Target | Write Method |
|--------|--------|--------------|
| SQL Server | PostgreSQL | COPY protocol (fastest) |
| PostgreSQL | SQL Server | TDS bulk copy |
| PostgreSQL | PostgreSQL | COPY protocol |
| SQL Server | SQL Server | TDS bulk copy |

### Same-Engine Migrations (New in v1.16.0)

Same-engine migrations (PG→PG, MSSQL→MSSQL) are now supported with all target modes:

```yaml
# PostgreSQL to PostgreSQL
source:
  type: postgres
  host: source-pg.example.com
  # ...

target:
  type: postgres
  host: target-pg.example.com
  # ...

migration:
  target_mode: upsert  # or drop_recreate, truncate
```

**Use cases:**
- Database cloning
- Environment sync (dev → staging → prod)
- Disaster recovery
- Data center migrations

## Features

- **Bidirectional migration** - SQL Server ↔ PostgreSQL
- **Auto-tuning** - Workers, connection pools, and buffers sized based on CPU/RAM
- **Fast transfers** using PostgreSQL COPY protocol (MSSQL→PG) or TDS bulk copy (PG→MSSQL)
- **Pipelined I/O** - Read-ahead buffering and parallel writers for maximum throughput
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
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.16.0/mssql-pg-migrate-v1.16.0-linux-amd64.tar.gz
tar -xzf mssql-pg-migrate-v1.16.0-linux-amd64.tar.gz
chmod +x mssql-pg-migrate-linux-amd64
./mssql-pg-migrate-linux-amd64 --version

# macOS Apple Silicon
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.16.0/mssql-pg-migrate-v1.16.0-darwin-arm64.tar.gz
tar -xzf mssql-pg-migrate-v1.16.0-darwin-arm64.tar.gz

# macOS Intel
curl -LO https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.16.0/mssql-pg-migrate-v1.16.0-darwin-amd64.tar.gz
tar -xzf mssql-pg-migrate-v1.16.0-darwin-amd64.tar.gz

# Windows (PowerShell)
Invoke-WebRequest -Uri https://github.com/johndauphine/mssql-pg-migrate/releases/download/v1.16.0/mssql-pg-migrate-v1.16.0-windows-amd64.tar.gz -OutFile mssql-pg-migrate.tar.gz
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

migration:
  workers: 8
  chunk_size: 200000
```

**SQL Server target uses TDS Bulk Copy protocol** (`mssql.CopyIn`) for optimal performance (~130,000 rows/sec).

2. Run the migration:

```bash
./mssql-pg-migrate -c config.yaml run
```

3. If interrupted, resume:

```bash
./mssql-pg-migrate -c config.yaml resume
```

## Configuration Reference

The configuration file uses YAML format. Environment variables can be used with `${VAR_NAME}` syntax.

### Source Database Settings

The `source` section configures the database to migrate FROM.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `type` | No | `mssql` | Database type: `mssql` or `postgres` |
| `host` | **Yes** | - | Database server hostname or IP address |
| `port` | No | 1433 (mssql) / 5432 (postgres) | Database server port |
| `database` | **Yes** | - | Database name to connect to |
| `user` | Yes* | - | Username for authentication (*not required for Kerberos) |
| `password` | Yes* | - | Password for authentication (*not required for Kerberos). Supports `${ENV_VAR}` syntax |
| `schema` | No | `dbo` (mssql) / `public` (postgres) | Schema containing tables to migrate |

**SSL/TLS Settings (source):**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `ssl_mode` | No | `require` | PostgreSQL SSL mode: `disable`, `require`, `verify-ca`, `verify-full` |
| `encrypt` | No | `true` | SQL Server encryption: `disable`, `false`, `true` |
| `trust_server_cert` | No | `false` | SQL Server: Skip certificate validation (use only for testing) |

**Kerberos Settings (source):**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `auth` | No | `password` | Authentication method: `password` or `kerberos` |
| `krb5_conf` | No | System default | Path to krb5.conf file (e.g., `/etc/krb5.conf`) |
| `keytab` | No | Credential cache | Path to keytab file for service account authentication |
| `realm` | No | Auto-detected | Kerberos realm (e.g., `EXAMPLE.COM`) |
| `spn` | No | Auto-detected | SQL Server Service Principal Name (e.g., `MSSQLSvc/host.example.com:1433`) |
| `gssencmode` | No | `prefer` | PostgreSQL GSSAPI encryption: `disable`, `prefer`, `require` |

### Target Database Settings

The `target` section configures the database to migrate TO. It uses the same parameters as `source`.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `type` | No | `postgres` | Database type: `mssql` or `postgres` |
| `host` | **Yes** | - | Database server hostname or IP address |
| `port` | No | 5432 (postgres) / 1433 (mssql) | Database server port |
| `database` | **Yes** | - | Database name to connect to |
| `user` | Yes* | - | Username for authentication |
| `password` | Yes* | - | Password for authentication |
| `schema` | No | `public` (postgres) / `dbo` (mssql) | Target schema for migrated tables |

The same SSL/TLS and Kerberos settings are available for `target`.

### Migration Settings

The `migration` section controls how data is transferred.

**Connection Pool Settings:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `max_mssql_connections` | No | Auto-sized | Maximum SQL Server connection pool size |
| `max_pg_connections` | No | Auto-sized | Maximum PostgreSQL connection pool size |

**Parallelism Settings:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `workers` | No | CPU cores - 2 | Number of parallel transfer workers (min: 2, max: 32) |
| `chunk_size` | No | Auto-scaled by RAM | Rows per chunk (100,000 - 500,000) |
| `max_partitions` | No | Same as `workers` | Maximum partitions for large table parallelism |
| `large_table_threshold` | No | 5,000,000 | Tables with more rows than this are partitioned |

**Table Filtering:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `include_tables` | No | All tables | List of glob patterns for tables to include (e.g., `Users`, `Order*`) |
| `exclude_tables` | No | None | List of glob patterns for tables to exclude (e.g., `temp_*`, `__*`) |

**Target Table Handling:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `target_mode` | No | `drop_recreate` | How to handle existing tables: `drop_recreate` (drop and recreate) or `truncate` (keep structure, delete data) |
| `data_dir` | No | `~/.mssql-pg-migrate` | Directory for state database and temporary files |

**Schema Object Creation:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `create_indexes` | No | `true` | Create non-primary key indexes after data transfer |
| `create_foreign_keys` | No | `true` | Create foreign key constraints after data transfer |
| `create_check_constraints` | No | `true` | Create CHECK constraints after data transfer |

**Consistency Settings:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `strict_consistency` | No | `false` | Use table locks instead of NOLOCK hints (slower but consistent) |

**Validation Settings:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `sample_validation` | No | `false` | Enable random row sampling to verify data integrity |
| `sample_size` | No | 100 | Number of random rows per table to verify |

**Performance Tuning:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `read_ahead_buffers` | No | Auto-scaled (4-32) | Number of chunks to buffer ahead of writers |
| `write_ahead_writers` | No | 2 | Parallel writers per job. Use 8 for PG→MSSQL |
| `parallel_readers` | No | 2 | Parallel readers per job. Use 1 for local databases |
| `mssql_rows_per_batch` | No | Same as `chunk_size` | SQL Server bulk copy batch size hint |

### Slack Notification Settings

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `enabled` | No | `false` | Enable Slack notifications |
| `webhook_url` | Yes (if enabled) | - | Slack incoming webhook URL |
| `channel` | No | Webhook default | Channel to post to (e.g., `#data-engineering`) |
| `username` | No | `mssql-pg-migrate` | Bot username for messages |

## Kerberos Authentication

For enterprise environments, Kerberos authentication eliminates the need to store database passwords. Both SQL Server and PostgreSQL support Kerberos.

### SQL Server with Kerberos

```yaml
source:
  type: mssql
  host: sqlserver.example.com
  database: MyDatabase
  auth: kerberos
  user: svc_migrate@EXAMPLE.COM   # Kerberos principal (optional)
  # spn: MSSQLSvc/sqlserver.example.com:1433  # Auto-detected if not specified
  encrypt: "true"
```

**Requirements:**
- Linux: Install `krb5-user`, configure `/etc/krb5.conf`, run `kinit` or use a keytab
- Windows: Domain-joined machine with logged-in domain user
- macOS: Configure Kerberos in System Preferences

**Using a keytab (for service accounts):**
```yaml
source:
  type: mssql
  host: sqlserver.example.com
  database: MyDatabase
  auth: kerberos
  user: svc_migrate@EXAMPLE.COM
  keytab: /etc/krb5.keytab
  realm: EXAMPLE.COM
```

### PostgreSQL with Kerberos (GSSAPI)

```yaml
target:
  type: postgres
  host: postgres.example.com
  database: mydb
  auth: kerberos
  user: svc_migrate@EXAMPLE.COM
  gssencmode: require   # disable, prefer (default), require
  ssl_mode: disable     # SSL not needed when using GSSAPI encryption
```

### Kerberos Setup (Linux)

```bash
# Install Kerberos client
sudo apt install krb5-user   # Debian/Ubuntu
sudo yum install krb5-workstation  # RHEL/CentOS

# Configure /etc/krb5.conf with your realm
# Then authenticate:
kinit svc_migrate@EXAMPLE.COM

# Verify ticket
klist

# Run migration (no password needed)
./mssql-pg-migrate -c config.yaml run
```

## Example Configurations

Ready-to-use example configuration files are available in the [`examples/`](examples/) directory:

| File | Description |
|------|-------------|
| `config-mssql-to-pg.yaml` | SQL Server → PostgreSQL with password auth |
| `config-mssql-to-pg-kerberos.yaml` | SQL Server → PostgreSQL with Kerberos |
| `config-pg-to-mssql.yaml` | PostgreSQL → SQL Server with password auth |
| `config-pg-to-mssql-kerberos.yaml` | PostgreSQL → SQL Server with Kerberos |
| `config-local.yaml` | Minimal config for local Docker development |
| `config-production.yaml` | Full production config with all options |

### Example 1: SQL Server to PostgreSQL (Password Authentication)

Basic migration from SQL Server to PostgreSQL using username/password:

```yaml
# config-mssql-to-pg.yaml
source:
  type: mssql
  host: sqlserver.example.com
  port: 1433
  database: SourceDatabase
  user: sa
  password: ${MSSQL_PASSWORD}        # Set via: export MSSQL_PASSWORD="your-password"
  schema: dbo
  encrypt: "true"                    # Enable encryption (recommended)
  trust_server_cert: false           # Validate server certificate

target:
  type: postgres
  host: postgres.example.com
  port: 5432
  database: target_db
  user: postgres
  password: ${PG_PASSWORD}           # Set via: export PG_PASSWORD="your-password"
  schema: public
  ssl_mode: require                  # Enable SSL (recommended)

migration:
  workers: 8                         # Parallel workers
  chunk_size: 200000                 # Rows per chunk
  create_indexes: true               # Recreate indexes
  create_foreign_keys: true          # Recreate foreign keys
  target_mode: drop_recreate         # Drop and recreate tables
```

### Example 2: SQL Server to PostgreSQL (Kerberos Authentication)

Enterprise migration using Kerberos - no passwords in config file:

```yaml
# config-mssql-to-pg-kerberos.yaml
source:
  type: mssql
  host: sqlserver.corp.example.com
  port: 1433
  database: SourceDatabase
  schema: dbo
  auth: kerberos                     # Use Kerberos instead of password
  user: svc_migrate@CORP.EXAMPLE.COM # Kerberos principal
  keytab: /etc/mssql-migrate.keytab  # Service account keytab
  realm: CORP.EXAMPLE.COM            # Kerberos realm
  encrypt: "true"

target:
  type: postgres
  host: postgres.corp.example.com
  port: 5432
  database: target_db
  schema: public
  auth: kerberos                     # Use Kerberos/GSSAPI
  user: svc_migrate@CORP.EXAMPLE.COM
  gssencmode: require                # Require GSSAPI encryption
  ssl_mode: disable                  # SSL not needed with GSSAPI

migration:
  workers: 8
  chunk_size: 200000
  create_indexes: true
  create_foreign_keys: true
```

### Example 3: PostgreSQL to SQL Server (Password Authentication)

Reverse migration from PostgreSQL to SQL Server:

```yaml
# config-pg-to-mssql.yaml
source:
  type: postgres
  host: postgres.example.com
  port: 5432
  database: source_db
  user: postgres
  password: ${PG_PASSWORD}
  schema: public
  ssl_mode: require

target:
  type: mssql
  host: sqlserver.example.com
  port: 1433
  database: TargetDatabase
  user: sa
  password: ${MSSQL_PASSWORD}
  schema: dbo
  encrypt: "true"
  trust_server_cert: false

migration:
  workers: 8
  chunk_size: 200000
  write_ahead_writers: 8             # Use 8 writers for PG→MSSQL (faster)
  parallel_readers: 1                # Single reader per job
  create_indexes: true
  create_foreign_keys: true
```

### Example 4: PostgreSQL to SQL Server (Kerberos Authentication)

```yaml
# config-pg-to-mssql-kerberos.yaml
source:
  type: postgres
  host: postgres.corp.example.com
  port: 5432
  database: source_db
  schema: public
  auth: kerberos
  user: svc_migrate@CORP.EXAMPLE.COM
  gssencmode: require
  ssl_mode: disable

target:
  type: mssql
  host: sqlserver.corp.example.com
  port: 1433
  database: TargetDatabase
  schema: dbo
  auth: kerberos
  user: svc_migrate@CORP.EXAMPLE.COM
  keytab: /etc/mssql-migrate.keytab
  realm: CORP.EXAMPLE.COM
  encrypt: "true"

migration:
  workers: 8
  chunk_size: 200000
  write_ahead_writers: 8
  parallel_readers: 1
```

### Example 5: Minimal Configuration (Local Development)

Simplest config for local Docker databases:

```yaml
# config-local.yaml
source:
  host: localhost
  port: 1433
  database: MyDatabase
  user: sa
  password: ${MSSQL_PASSWORD}
  encrypt: "false"                   # Disable encryption for local dev
  trust_server_cert: true            # Trust self-signed certs

target:
  host: localhost
  port: 5432
  database: mydb
  user: postgres
  password: ${PG_PASSWORD}
  ssl_mode: disable                  # Disable SSL for local dev
```

### Example 6: Production Configuration with All Options

Full production configuration with Slack notifications and validation:

```yaml
# config-production.yaml
source:
  type: mssql
  host: sqlserver-prod.example.com
  port: 1433
  database: ProductionDB
  user: migrate_user
  password: ${MSSQL_PASSWORD}
  schema: dbo
  encrypt: "true"
  trust_server_cert: false

target:
  type: postgres
  host: postgres-prod.example.com
  port: 5432
  database: production_db
  user: migrate_user
  password: ${PG_PASSWORD}
  schema: public
  ssl_mode: verify-full              # Full certificate verification

migration:
  # Connection pools
  max_mssql_connections: 20
  max_pg_connections: 40

  # Parallelism
  workers: 16
  chunk_size: 250000
  max_partitions: 16
  large_table_threshold: 10000000

  # Table filtering
  exclude_tables:
    - temp_*
    - staging_*
    - __*
    - audit_log

  # Schema objects
  create_indexes: true
  create_foreign_keys: true
  create_check_constraints: true

  # Consistency
  strict_consistency: true           # Use locks for consistent reads

  # Validation
  sample_validation: true            # Verify random samples
  sample_size: 500                   # Check 500 rows per table

  # State persistence
  data_dir: /var/lib/mssql-pg-migrate

slack:
  enabled: true
  webhook_url: ${SLACK_WEBHOOK_URL}
  channel: "#data-migrations"
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

# View details for a specific run (shows error if failed)
./mssql-pg-migrate -c config.yaml history --run <run-id>
```

### Headless Mode (Airflow/Kubernetes)

For headless environments, use `--state-file` to store state in a YAML file instead of SQLite:

```bash
# Run with state file
./mssql-pg-migrate -c config.yaml --state-file state.yaml run

# Resume with state file
./mssql-pg-migrate -c config.yaml --state-file state.yaml resume

# All commands support --state-file
./mssql-pg-migrate -c config.yaml --state-file state.yaml status
./mssql-pg-migrate -c config.yaml --state-file state.yaml history
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

The tool saves progress to enable efficient resume after failures. By default, state is stored in SQLite (`~/.mssql-pg-migrate/migrate.db`). For headless environments, use `--state-file` for a portable YAML state file.

### Table-level resume
- Completed tables are skipped entirely on resume
- Verified by comparing row counts between source and target

### Chunk-level resume
- Progress saved every 10 chunks during transfer
- On resume, continues from the exact last successful chunk
- Partial data from interrupted chunks is cleaned up automatically

### Stale progress detection
- If target table has fewer rows than saved progress, the tool detects data loss
- Automatically clears stale progress and restarts the table transfer
- Prevents resuming with incorrect last_pk values

```bash
# Resume shows what's being skipped/continued
./mssql-pg-migrate -c config.yaml resume

# Or with state file for Airflow/Kubernetes
./mssql-pg-migrate -c config.yaml --state-file state.yaml resume

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

Tested on StackOverflow database dumps:
- **Environment**: Windows laptop with WSL2 (24GB RAM allocated), 16 cores
- **Databases**: SQL Server and PostgreSQL running in Docker containers (same host)
- **Dataset**: StackOverflow 2010 (19.3M rows) and 2013 (106.5M rows)

### MSSQL → PostgreSQL
| Dataset | Rows | Duration | Throughput |
|---------|------|----------|------------|
| SO2010 | 19.3M | 34s | **575,000 rows/sec** |
| SO2013 | 106.5M | ~3m | ~590,000 rows/sec |

### PostgreSQL → MSSQL
| Dataset | Rows | Duration | Throughput | Config |
|---------|------|----------|------------|--------|
| SO2010 | 19.3M | 83s | 234,000 rows/sec | 2 writers |
| SO2010 | 19.3M | 54s | 355,000 rows/sec | 4 writers |
| SO2010 | 19.3M | 46s | **419,000 rows/sec** | 8 writers |

PostgreSQL → MSSQL is ~1.4x slower with optimized settings (8 writers) due to TDS bulk copy protocol overhead vs PostgreSQL COPY.

Performance varies based on:
- Network latency between source and target
- Table structure (wide tables are slower)
- Data types (LOBs are slower)
- Available CPU cores and memory (auto-tuned)

## Comparison with Airflow DAG

| Feature | mssql-pg-migrate (Go) | Airflow DAG (Python) |
|---------|----------------------|---------------------|
| Throughput | 575k rows/sec | ~50-80k rows/sec |
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

## Development

### Running Tests

The project includes comprehensive unit tests for CLI parsing, orchestrator logic, and profile encryption.

```bash
# Run all tests
make test

# Run tests with short flag (faster, skips slow tests)
make test-short

# Generate coverage report
make test-coverage
# Open coverage.html in browser to view results
```

### Pre-Commit Hooks

To ensure tests pass before committing:

```bash
# Set up git hooks (one-time setup)
make setup-hooks

# This configures git to use .githooks/pre-commit which:
# - Checks code formatting
# - Runs all tests
# - Blocks commit if any check fails
```

### Building

```bash
# Build binary
make build

# Build for all platforms
make build-all

# Run all checks (format + tests)
make check
```

### Test Databases (Docker)

For integration testing:

```bash
# Start local SQL Server and PostgreSQL
make test-dbs-up

# Stop and remove test databases
make test-dbs-down
```

## License

MIT
