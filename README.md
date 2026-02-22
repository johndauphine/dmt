# dmt

[![CI](https://github.com/johndauphine/dmt/actions/workflows/ci.yml/badge.svg)](https://github.com/johndauphine/dmt/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/johndauphine/dmt)](https://github.com/johndauphine/dmt/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/johndauphine/dmt)](https://go.dev/)
[![License](https://img.shields.io/github/license/johndauphine/dmt)](LICENSE)

High-performance CLI tool for database migrations between SQL Server, PostgreSQL, and MySQL.

## Interactive Mode

Launch the tool without arguments to enter the **Interactive Shell**, a modern TUI designed for ease of use.

```bash
./dmt
```

### Features
*   **Slash Commands**: Type `/` to see all available commands (e.g., `/run`, `/wizard`, `/status`, `/analyze`).
*   **Resume**: Use `/resume` to continue interrupted migrations.
*   **Auto-Completion**:
    *   **Commands**: Tab-complete commands like `/validate` or `/history`.
    *   **Files**: Type `@` to browse and select configuration files from your current directory (e.g., `/run @conf<TAB>`).
*   **Configuration Wizard**: Type `/wizard` to interactively create or edit your `config.yaml`. It guides you through connection details, SSL settings, and performance tuning.
*   **Live Monitoring**: Watch migration progress with real-time logs and visual status indicators.
*   **Git Integration**: View your current branch and repository status directly in the status bar.

## Security

- **Credential redaction** - passwords are never stored in the state database or logs
- **DSN injection protection** - connection strings URL-encode credentials to prevent injection
- **SQL injection protection** - internal SQLite queries use whitelist validation for table names
- **Secret templates** - use `${env:VAR}` or `${file:/path}` instead of plaintext passwords in config
- **Secure permissions** - state files (0600) and data directories (0700) enforced automatically

## Incremental Sync

For large databases with frequent updates, use **date-based incremental loading** to dramatically reduce sync times. Instead of transferring all rows every time, only rows modified since the last sync are transferred.

### Recommended Workflow

```bash
# Step 1: Initial load (fast bulk copy, creates tables)
./dmt -c config.yaml run   # target_mode: drop_recreate

# Step 2: Incremental syncs (uses highwater marks)
./dmt -c config.yaml run   # target_mode: upsert
```

### Performance Impact

| Scenario | Time |
|----------|------|
| Full sync (19M rows) | 1m 47s |
| Incremental (no changes) | **12 seconds** |

### Configuration

```yaml
migration:
  target_mode: upsert     # Required for incremental sync

  # Date columns for incremental loading (tries each in order)
  # Supported types: datetime, datetime2, timestamp, timestamptz, date
  date_updated_columns:
    - UpdatedAt           # Common convention
    - ModifiedDate        # SQL Server convention
    - LastModified
    - CreationDate        # Fallback for append-only tables
```

### How It Works

1. **First run**: Full load of all rows, records sync timestamp per table
2. **Subsequent runs**: Only fetches rows where `date_column > last_sync_timestamp`
3. **Highwater marks**: Stored automatically in the state database (`~/.dmt/migrate.db`)
4. **Upsert logic**: INSERTs new rows, UPDATEs changed rows, preserves target-only rows

### Important Notes

- **Upsert requires existing tables** - Run `drop_recreate` first for initial load
- **Primary keys required** - Both source and target tables must have PKs
- **Tables without date columns** - Fall back to full table comparison (slower)

## AI-Driven Real-Time Parameter Adjustment

Continuously monitor migration performance and automatically adjust parameters in real-time. The tuner receives live system resource data (CPU cores, available RAM, connection limits) and makes informed decisions without hard-coded safety guards — an effectiveness tracker measures each adjustment's impact and pauses tuning if consecutive changes hurt performance.

### Quick Start

Enable in `config.yaml`:

```yaml
migration:
  ai_adjust: true
  ai_adjust_interval: 30s

ai:
  api_key: ${ANTHROPIC_API_KEY}
  provider: claude
```

### How It Works

1. **Continuous Monitoring**: Every 30 seconds, collects performance metrics:
   - Windowed throughput (rows/sec), memory usage, CPU utilization
   - Query latency, write latency, queue depth

2. **Resource-Aware Prompting**: The AI receives live system state:
   - CPU cores, available/used RAM, max database connections
   - Current parameter values and adjustment history
   - Effectiveness of previous adjustments

3. **AI Decision Making**: Analyzes trends and recommends adjustments:
   - **Scale up**: Increase workers or chunk_size if resources available
   - **Scale down**: Reduce workers to minimize lock contention
   - **Reduce chunk**: Decrease batch size if memory pressure detected
   - **Continue**: Maintain current parameters if performance optimal

4. **Safety Mechanisms**:
   - **Post-adjustment cooldown** (90s) — waits for metrics to stabilize before next adjustment
   - **Effectiveness tracking** — measures throughput change after each adjustment
   - **Consecutive-negative breaker** — pauses tuning after 3 adjustments that hurt performance
   - **Completion skip** — no adjustments when transfer is >90% complete
   - **API circuit breaker** — disables after 3 consecutive API/parse failures
   - Updates applied at chunk boundaries, never mid-transfer

### Performance

Tested on Stack Overflow 2013 dataset (106.5M rows, MSSQL to PostgreSQL):

```
Configuration                Duration    Throughput
──────────────────────────────────────────────────────────
AI-tuned (Haiku)             5m 14s      339,393 r/s
AI-tuned (Sonnet)            5m 14s      339,780 r/s
```

Haiku and Sonnet produce identical results — Haiku recommended for lower cost.

### Configuration

```yaml
migration:
  ai_adjust: true                  # Enable/disable (default: true when AI configured)
  ai_adjust_interval: 30s          # Evaluation interval (default: 30s)

  # Initial parameters (AI adjusts from here)
  chunk_size: 10000
  workers: 4
  read_ahead_buffers: 8
  write_ahead_writers: 2
  parallel_readers: 2

ai:
  api_key: ${ANTHROPIC_API_KEY}
  provider: claude                 # Also: openai, gemini, ollama, lmstudio
  model: claude-haiku-4-5-20251001 # Recommended (cheapest, same results as Sonnet)
  timeout_seconds: 30
```

### Cost

- **API Calls**: ~1-2 per minute (60s cache + 90s cooldown between adjustments)
- **Cost**: ~$0.005-0.01 per hour with Haiku
- **Fallback**: Heuristic rules apply if AI unavailable

### Troubleshooting

**AI adjustment not happening:**
- Check logs for "AI monitoring started" message
- Verify `ai_adjust: true` in config
- Verify API key: `echo $ANTHROPIC_API_KEY`
- Tuner skips adjustments when >90% complete or during 90s post-adjustment cooldown

**Performance degradation:**
- The effectiveness tracker will automatically pause after 3 negative adjustments
- Check `--verbosity debug` logs for "AI adjustment effect" measurements
- Disable with `ai_adjust: false` to use fixed parameters

## Encrypted Profiles (SQLite)

You can store full configuration profiles (including secrets) encrypted at rest inside the same SQLite database used for run history.

**Master key**
- Set `DMT_MASTER_KEY` to a **base64-encoded 32-byte key**.
- Example key generation (POSIX):
  ```bash
  openssl rand -base64 32
  ```
- Without this key, profile operations will fail, but YAML-based workflows continue to work.

**CLI workflow**
```bash
# Save a profile from YAML (encrypts and stores in SQLite)
DMT_MASTER_KEY=... ./dmt profile save --name prod --config config.yaml

# List profiles
DMT_MASTER_KEY=... ./dmt profile list

# Run using a profile
DMT_MASTER_KEY=... ./dmt run --profile prod

# Export a profile back to YAML
DMT_MASTER_KEY=... ./dmt profile export --name prod --out config.yaml
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
- Profiles are stored as encrypted blobs in the same SQLite DB (`~/.dmt/migrate.db` by default).
- In Airflow, you can set `DMT_MASTER_KEY` via your secrets backend and run `profile save` at deploy time, or stick with YAML + env vars for CI/CD.
- You can relocate the SQLite DB by setting `migration.data_dir` in your config (e.g., to a shared volume).
- On first run, the default data directory (`~/.dmt`) is created automatically if it does not exist.

## AI Features

The tool includes AI-powered features to help with complex migrations. All AI features share common configuration under the `ai` section.

### Quick Start

Simply add your API key to enable AI features:

```yaml
ai:
  api_key: ${ANTHROPIC_API_KEY}   # or OPENAI_API_KEY or GEMINI_API_KEY
```

This auto-enables AI type mapping. Provider defaults to Claude (Anthropic).

### Supported Providers

| Provider | Config Value | Default Model | API Key Variable |
|----------|--------------|---------------|------------------|
| **Claude** (default) | `claude` | `claude-sonnet-4-6` | `ANTHROPIC_API_KEY` |
| **OpenAI** | `openai` | `gpt-4o` | `OPENAI_API_KEY` |
| **Google Gemini** | `gemini` | `gemini-2.0-flash` | `GEMINI_API_KEY` |
| **Ollama** (local) | `ollama` | - | - |
| **LM Studio** (local) | `lmstudio` | - | - |

### Full Configuration

```yaml
ai:
  api_key: ${ANTHROPIC_API_KEY}  # Required - your API key
  provider: claude               # Optional - claude (default), openai, gemini, ollama, lmstudio
  model: claude-sonnet-4-6       # Optional - uses provider default if not set

  type_mapping:
    enabled: true                # Auto-enabled when api_key is set
    cache_file: ~/.dmt/type-cache.json
```

### AI Type Mapping

Automatically infers the best target type for unknown or complex source types.

**How it works:**
1. **Data Sampling**: Samples up to 5 rows from each table for context
2. **Intelligent Inference**: AI analyzes column metadata plus sample values
3. **Cross-Engine Awareness**: Understands encoding differences (e.g., PG varchar → MSSQL nvarchar)
4. **Caching**: Mappings cached to minimize API calls

**When to use:**
- Custom domains, user-defined types, or database-specific types
- Cross-engine migrations with different type systems
- Unicode handling (AI correctly infers `nvarchar` for UTF-8 text)

### Smart Config Detection

Analyze your source database and get optimal configuration suggestions:

```bash
./dmt -c config.yaml analyze
```

**Output example:**
```yaml
# AI-detected configuration suggestions

migration:
  date_updated_columns:
    - UpdatedAt
    - ModifiedDate
    - LastModified

  exclude_tables:
    - temp_imports
    - audit_log
    - __EFMigrationsHistory

  chunk_size: 150000
```

**What it detects:**
- **Date columns**: Columns suitable for incremental sync (UpdatedAt, ModifiedDate, etc.)
- **Exclude tables**: Tables that should probably be excluded (temp, log, archive, etc.)
- **Chunk size**: Optimal chunk size based on average row sizes

### AI Error Diagnosis

When a table transfer fails, AI automatically analyzes the error and provides actionable suggestions for resolution.

**Example output:**
```
Table Orders failed: pq: invalid input syntax for type integer: "abc"

  AI Diagnosis:
    Cause: Data type mismatch - column contains non-numeric values being inserted into integer column
    Suggestions:
      - Check source data for non-numeric values in numeric columns
      - Use TEXT type instead of INTEGER for this column
      - Add data transformation to filter/convert invalid values
    Confidence: high
```

**Features:**
- **Automatic**: Runs automatically when AI is configured and a transfer fails
- **Context-aware**: Includes table schema, column types, and source/target DB info in analysis
- **Cached**: Same errors are diagnosed once to minimize API calls
- **Categorized**: Errors classified as type_mismatch, constraint, permission, connection, or data_quality

**Common diagnoses:**
| Error Type | AI Diagnosis |
|------------|--------------|
| Type mismatch | Identifies incompatible column types and suggests mappings |
| NULL constraint | Detects NULL values in NOT NULL columns |
| Foreign key | Identifies missing parent records or ordering issues |
| Permission | Suggests required grants or role assignments |
| Connection | Diagnoses timeout, authentication, or network issues |

**Requirements:**
- AI must be configured (`ai.api_key` set)
- Works with all supported providers (Claude, OpenAI, Gemini)

### Cost Considerations

- Each unique type mapping requires one API call (cached for future runs)
- Typical migration: 20-50 API calls on first run, zero on subsequent runs
- Smart config analysis: 0 API calls (uses pattern matching, not AI)
- Error diagnosis: 1 API call per unique error (cached to avoid duplicates)

## State File Backend (Airflow/Kubernetes)

For headless environments like Airflow or Kubernetes where SQLite may be impractical, you can use a YAML-based state file instead.

```bash
# Use a YAML state file instead of SQLite
./dmt -c config.yaml --state-file /tmp/migration-state.yaml run

# Resume using the same state file
./dmt -c config.yaml --state-file /tmp/migration-state.yaml resume

# Check status
./dmt -c config.yaml --state-file /tmp/migration-state.yaml status

# View history
./dmt -c config.yaml --state-file /tmp/migration-state.yaml history
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
            /opt/dmt \
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
    name='dmt',
    image='your-registry/dmt:latest',
    cmds=['/dmt'],
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
./dmt --state-file state.yaml -c config.yaml resume --force-resume
```

### Airflow Sensor Pattern

Poll migration status from a separate task:

```python
from airflow.sensors.python import PythonSensor
import subprocess
import json

def check_migration_status(**context):
    result = subprocess.run([
        '/opt/dmt',
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

- **222K-717K rows/sec** depending on direction and row width
- **MSSQL → PG**: 717K rows/sec with AI startup tuning (106.5M rows in 2m29s)
- **PG → MSSQL**: 645K rows/sec (PG streaming + TDS bulk copy)
- **PG → PG**: 563K rows/sec (COPY protocol both ends)
- **MSSQL → MSSQL**: 222K rows/sec (TDS both ends)
- **Auto-tuning** based on CPU cores, available RAM, and AI-driven adjustments
- **Single binary** - no runtime dependencies, no CGO

## Supported Databases

| Database | As Source | As Target | Write Method | Auth |
|----------|-----------|-----------|--------------|------|
| PostgreSQL | ✓ | ✓ | COPY protocol (fastest) | Password, Kerberos |
| SQL Server | ✓ | ✓ | TDS bulk copy | Password, Kerberos |
| MySQL | ✓ | ✓ | Multi-row INSERT | Password |

All combinations are supported, including same-engine migrations (PG→PG, MSSQL→MSSQL, MySQL→MySQL).

### Same-Engine Migrations

Use cases: database cloning, environment sync (dev → staging → prod), disaster recovery, data center migrations.

```yaml
source:
  type: postgres
  host: source-pg.example.com
target:
  type: postgres
  host: target-pg.example.com
migration:
  target_mode: upsert  # or drop_recreate
```

### PostGIS Spatial Data Support

Cross-engine migrations (PG→MSSQL) preserve spatial reference systems:

- **SRID preservation** - reads SRID from PostGIS `geometry_columns`/`geography_columns` metadata
- **Automatic conversion** - WKT text converted to SQL Server geography/geometry with correct SRID
- **Default fallback** - uses SRID 4326 (WGS84) when source SRID is 0 or unset

## Features

### Database Support
- **PostgreSQL, SQL Server, MySQL** - migrate between any combination
- **Bulk copy protocols** - PostgreSQL COPY, TDS bulk copy, MySQL LOAD DATA for maximum throughput
- **Kerberos authentication** - SPNEGO/keytab support for SQL Server and PostgreSQL
- **SSL/TLS encryption** - configurable per connection

### Transfer Engine
- **Pipelined I/O** - read-ahead buffering with parallel writers (222K-717K rows/sec)
- **Keyset pagination** - efficient partitioning for integer PKs (no OFFSET degradation)
- **ROW_NUMBER pagination** - automatic fallback for composite/varchar PKs
- **Parallel partitioning** - large tables split via NTILE for concurrent transfer
- **Auto-tuning** - workers, connection pools, and buffers sized from CPU/RAM
- **Memory-bounded** - configurable memory cap (default: 70% available RAM)

### AI Integration
- **Multi-provider** - Claude, OpenAI, Gemini, Ollama, LM Studio
- **Type mapping** - LLM-powered cross-database type inference with caching
- **Smart config analysis** - analyze source database and recommend optimal parameters (`analyze` command)
- **Real-time parameter tuning** - monitor performance and auto-adjust workers, chunk size, buffers mid-migration
- **Error diagnosis** - AI-powered root cause analysis with remediation suggestions

### Checkpoint & Resume
- **Chunk-level resume** - progress saved every N chunks, resume from exact position
- **Table-level resume** - skip already-completed tables on restart
- **Idempotent retries** - partition cleanup on retry prevents duplicates
- **State backends** - SQLite (default) or YAML file for Airflow/headless environments
- **Config validation** - prevents resume with mismatched configuration

### Incremental Sync
- **Upsert mode** - INSERT new rows, UPDATE changed rows, preserve target-only data
- **Date-based highwater marks** - only transfer rows modified since last sync
- **Configurable date columns** - tries multiple column names in order

### Interactive Mode (TUI)
- **Slash commands** - `/run`, `/resume`, `/analyze`, `/wizard`, `/status`, `/history`, `/validate`, and more
- **Auto-completion** - tab-complete commands and `@` file browser for config selection
- **Configuration wizard** - interactive setup for source, target, and tuning parameters
- **Live monitoring** - real-time migration progress with log capture

### CLI & Automation
- **Commands** - `run`, `resume`, `status`, `validate`, `history`, `health-check`, `analyze`, `init`, `profile`
- **Dry-run mode** - preview migration plan without execution
- **JSON output** - structured results to stdout or file for orchestration tools
- **Exit codes** - semantic codes for retry logic (success, transient, config error, cancelled)
- **Airflow integration** - YAML state files, explicit run IDs, BashOperator/KubernetesPodOperator support
- **Graceful shutdown** - SIGINT/SIGTERM handling with configurable timeout and checkpoint save

### Security
- **Encrypted profiles** - AES-encrypted configs stored in SQLite with master key
- **Secret templates** - `${env:VAR}`, `${file:/path}` for credentials (no plaintext in config)
- **Credential redaction** - passwords never stored in state database or logs
- **Secure permissions** - 0600 files, 0700 directories enforced automatically

### Schema Management
- **Full schema transfer** - tables, primary keys, indexes, foreign keys, check constraints
- **Identity/sequence reset** - preserves auto-increment values after transfer
- **Table filtering** - include/exclude tables with glob patterns
- **Strict consistency mode** - table locks instead of NOLOCK for consistent reads

### Observability
- **Progress bar** - real-time throughput stats with ETA
- **Slack notifications** - start, completion, and failure alerts
- **JSON progress** - streaming updates to stderr for monitoring dashboards
- **Verbosity levels** - debug, info, warn, error with text or JSON log format
- **Row count validation** - automatic post-transfer verification
- **Sample data validation** - random row verification with composite PK support

### Deployment
- **Single binary** - no runtime dependencies, no CGO
- **Cross-platform** - Linux, macOS (Intel + Apple Silicon), Windows
- **YAML configuration** - with environment variable expansion

## Installation

### Download pre-built binaries

Download from [GitHub Releases](https://github.com/johndauphine/dmt/releases/latest):

```bash
# Linux x64
curl -LO https://github.com/johndauphine/dmt/releases/download/v3.53.0/dmt-v3.53.0-linux-amd64.tar.gz
tar -xzf dmt-v3.53.0-linux-amd64.tar.gz
chmod +x dmt-linux-amd64
./dmt-linux-amd64 --version

# macOS Apple Silicon
curl -LO https://github.com/johndauphine/dmt/releases/download/v3.53.0/dmt-v3.53.0-darwin-arm64.tar.gz
tar -xzf dmt-v3.53.0-darwin-arm64.tar.gz

# macOS Intel
curl -LO https://github.com/johndauphine/dmt/releases/download/v3.53.0/dmt-v3.53.0-darwin-amd64.tar.gz
tar -xzf dmt-v3.53.0-darwin-amd64.tar.gz

# Windows (PowerShell)
Invoke-WebRequest -Uri https://github.com/johndauphine/dmt/releases/download/v3.53.0/dmt-v3.53.0-windows-amd64.tar.gz -OutFile dmt.tar.gz
tar -xzf dmt.tar.gz
```

### Build from source

Requires Go 1.24+

```bash
git clone https://github.com/johndauphine/dmt.git
cd dmt
CGO_ENABLED=0 go build -o dmt ./cmd/migrate
```

### Go install

```bash
go install github.com/johndauphine/dmt/cmd/migrate@latest
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
./dmt -c config.yaml run
```

3. If interrupted, resume:

```bash
./dmt -c config.yaml resume
```

## Configuration Reference

The configuration file uses YAML format. Environment variables can be used with `${VAR_NAME}` syntax.

### Source Database Settings

The `source` section configures the database to migrate FROM.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `type` | No | `mssql` | Database type: `mssql`, `postgres`, or `mysql` |
| `host` | **Yes** | - | Database server hostname or IP address |
| `port` | No | Auto | Database server port (1433/5432/3306) |
| `database` | **Yes** | - | Database name |
| `user` | Yes* | - | Username for authentication (*not required for Kerberos) |
| `password` | Yes* | - | Password for authentication (*not required for Kerberos). Supports `${ENV_VAR}` syntax |
| `schema` | No | Auto | Schema containing tables to migrate |

**SSL/TLS Settings (source):**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `ssl_mode` | No | `require` | PostgreSQL SSL mode: `disable`, `require`, `verify-ca`, `verify-full` |
| `encrypt` | No | `true` | SQL Server encryption: `true` or `false` |
| `trust_server_cert` | No | `false` | SQL Server: Skip certificate validation (use only for testing) |
| `packet_size` | No | `32767` | SQL Server TDS packet size in bytes (max: 32767). Larger packets improve throughput. |

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
| `type` | No | `postgres` | Database type: `mssql`, `postgres`, or `mysql` |
| `host` | **Yes** | - | Database server hostname or IP address |
| `port` | No | Auto | Database server port (5432/1433/3306) |
| `database` | **Yes** | - | Database name |
| `user` | Yes* | - | Username for authentication |
| `password` | Yes* | - | Password for authentication |
| `schema` | No | Auto | Target schema for migrated tables |

The same SSL/TLS and Kerberos settings are available for `target`.

### Migration Settings

The `migration` section controls how data is transferred.

**Connection Pool Settings:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `max_source_connections` | No | Auto-sized | Maximum source database connection pool size |
| `max_target_connections` | No | Auto-sized | Maximum target database connection pool size |

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
| `target_mode` | No | `drop_recreate` | How to handle existing tables: `drop_recreate` (drop and recreate) or `upsert` (incremental sync). **Note:** `upsert` requires target tables to already exist - run `drop_recreate` first for initial load. |
| `date_updated_columns` | No | None | List of column names to check for last-modified date (e.g., `UpdatedAt`, `ModifiedDate`). Enables incremental sync - only rows modified since last sync are transferred. |
| `data_dir` | No | `~/.dmt` | Directory for state database and temporary files |

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
| `source.chunk_size` | No | Same as `migration.chunk_size` | Batch size for reading from source database |
| `target.chunk_size` | No | Same as `migration.chunk_size` | Batch size for writing to target database |

### AI Settings

The `ai` section configures AI-powered features.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `ai.api_key` | Yes (if using AI) | - | API key for the AI provider |
| `ai.provider` | No | `claude` | AI provider: `claude`, `openai`, `gemini`, `ollama`, or `lmstudio` |
| `ai.model` | No | Provider default | Model to use (e.g., `claude-sonnet-4-6`, `gpt-4o`, `gemini-2.0-flash`) |
| `ai.timeout_seconds` | No | `30` | API request timeout |
| `ai.type_mapping.enabled` | No | Auto | Enable AI type mapping (auto-enabled when api_key is set) |
| `ai.type_mapping.cache_file` | No | `~/.dmt/type-cache.json` | Path to cache AI type mappings |
| `ai.smart_config.enabled` | No | `false` | Enable smart config detection |
| `ai.smart_config.detect_date_columns` | No | `true` | Detect date_updated_columns candidates |
| `ai.smart_config.detect_exclude_tables` | No | `true` | Detect tables to exclude |
| `ai.smart_config.suggest_chunk_size` | No | `true` | Suggest optimal chunk size |

### Slack Notification Settings

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `enabled` | No | `false` | Enable Slack notifications |
| `webhook_url` | Yes (if enabled) | - | Slack incoming webhook URL |
| `channel` | No | Webhook default | Channel to post to (e.g., `#data-engineering`) |
| `username` | No | `dmt` | Bot username for messages |

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
./dmt -c config.yaml run
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
  max_source_connections: 20
  max_target_connections: 40

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
  data_dir: /var/lib/dmt

slack:
  enabled: true
  webhook_url: ${SLACK_WEBHOOK_URL}
  channel: "#data-migrations"
  username: dmt

```

## Usage

### Commands

```bash
# Run a new migration
./dmt -c config.yaml run

# Dry-run (preview plan without executing)
./dmt -c config.yaml run --dry-run

# Resume an interrupted migration (continues from last checkpoint)
./dmt -c config.yaml resume

# Check status of current/last run
./dmt -c config.yaml status

# Validate row counts between source and target
./dmt -c config.yaml validate

# View migration history
./dmt -c config.yaml history

# View details for a specific run
./dmt -c config.yaml history --run <run-id>

# Test database connections
./dmt -c config.yaml health-check

# Analyze source database and get AI configuration suggestions
./dmt -c config.yaml analyze

# Create a new config file interactively
./dmt init

# Create a secrets file template
./dmt init-secrets
```

### Headless Mode (Airflow/Kubernetes)

For headless environments, use `--state-file` to store state in a YAML file instead of SQLite:

```bash
# Run with state file
./dmt -c config.yaml --state-file state.yaml run

# Resume with state file
./dmt -c config.yaml --state-file state.yaml resume

# All commands support --state-file
./dmt -c config.yaml --state-file state.yaml status
./dmt -c config.yaml --state-file state.yaml history
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
Transferring 100% |███████████| (106534570/106534570, 717K rows/s)
Transferred 106534570 rows in 2m29s (716909 rows/sec)

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

1. **Extract schema** - Reads table structure, PKs, indexes, FKs, and check constraints from source
2. **Create tables** - Generates target DDL with proper type mapping and identity columns
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

The tool saves progress to enable efficient resume after failures. By default, state is stored in SQLite (`~/.dmt/migrate.db`). For headless environments, use `--state-file` for a portable YAML state file.

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
./dmt -c config.yaml resume

# Or with state file for Airflow/Kubernetes
./dmt -c config.yaml --state-file state.yaml resume

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

### Unknown Types

For types not in the built-in mappings (custom domains, user-defined types, etc.), enable [AI-assisted type mapping](#ai-assisted-type-mapping-new-in-v2240) to automatically infer the best target type.

## Benchmarks

### Small Dataset (WideWorldImporters, 701K rows)

- **Hardware**: WSL2 on Windows, 32GB RAM, 16 cores
- **Databases**: PostgreSQL 15 and SQL Server 2022 (Docker)

| Direction | Transfer | Overall |
|-----------|----------|---------|
| **PG → MSSQL** | 688K rows/sec | **645K rows/sec** |
| **PG → PG** | 605K rows/sec | **563K rows/sec** |
| **MSSQL → PG** | 302K rows/sec | 248K rows/sec |
| **MSSQL → MSSQL** | 280K rows/sec | 222K rows/sec |

### Large Dataset (Stack Overflow 2013, 106.5M rows)

- **Hardware**: macOS, Apple Silicon, 36GB RAM
- **Databases**: SQL Server 2022 and PostgreSQL 17 (Docker, 16GB limit)

| Configuration | Transfer | Overall | Throughput |
|---------------|----------|---------|------------|
| **AI startup + runtime tuning** | 2m 29s | 3m 05s | **717K rows/sec** |
| **Runtime tuning only** | 5m 14s | 5m 50s | 339K rows/sec |

AI startup tuning analyzes source schema and system resources to set optimal initial parameters, delivering a 2x speedup over runtime-only tuning.

Performance varies based on network latency, table width, data types, and available CPU/memory.

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

## Known Limitations

### Tables Without Primary Keys

All tables must have primary keys for migration. Tables without primary keys are automatically skipped with a warning. This is required for:
- Chunked pagination during data transfer
- Change detection in upsert mode

## License

MIT
