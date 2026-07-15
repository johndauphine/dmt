# Restartability Guide

This document describes the checkpoint and resume functionality in dmt, including implementation details, limitations, and testing procedures.

## Overview

dmt supports resumable migrations through a checkpoint system that tracks progress at multiple levels:

1. **Run level**: Tracks outcome (`running`, `success`, `partial`, `failed`)
   separately from whether its checkpoints remain resumable
2. **Table level**: Tracks which tables have been successfully transferred
3. **Chunk level**: Tracks progress within a table transfer (lastPK or rowNum)

## Architecture

### State Backends

Two state backends are available:

| Backend | Storage | Use Case | Config Hash | Chunk Progress |
|---------|---------|----------|-------------|----------------|
| SQLite (default) | `~/.dmt/migrate.db` | Desktop/interactive | Yes | Yes |
| File-based | User-specified YAML file | Airflow/headless | Yes | Yes |

Both backends are required to support the restartability contract: run
lifecycle, task lifecycle, transfer progress, partition progress, sync
timestamps, delete reconciliation state, schema snapshots, and fallback
event counters. This contract is exposed in code through
`StateBackend.Capabilities()` and covered by the checkpoint conformance
tests.

Both backends also enforce an exclusive migration lease keyed by the canonical
target driver, host, port, database, and schema. SQLite acquires and takes over
the lease with one compare-and-swap statement. File state holds an advisory
`<state-file>.lock` across each YAML read/compare/write cycle, so separate
processes cannot both win acquisition. The lock is only the atomicity mechanism;
the durable YAML lease record remains authoritative after a process exits.

### Migration-scoped strict snapshots

Strict consistency uses the strongest parallel stable-view mechanism exposed by
each source engine:

| Source and scope | Mechanism | Parallelism | Source-write blocking | Prerequisites |
|---|---|---|---|---|
| PostgreSQL, table or migration | Exported MVCC snapshot | Parallel readers; migration epochs also permit partitions | None | None beyond normal reads |
| MySQL/MariaDB, table | Repeatable-read sessions opened during a `LOCK TABLES` window | Parallel readers | Writes to that table pause for the few milliseconds needed to start reader sessions | InnoDB, `SELECT`, and `LOCK TABLES` |
| SQL Server, table | Shared table lock | Parallel readers | Writes to that table wait for the full table transfer | None beyond normal reads |
| SQL Server, migration | Database snapshot | Parallel readers and partitions | None; changed pages consume copy-on-write disk until cleanup | SQL Server 2016 SP1+, not Azure SQL Database, plus a documented create-database permission path |
| SQLite, table | Serializable read transaction | One reader by design | SQLite/file-lock dependent | No WAL snapshot API is exposed by the supported Go drivers |

With PostgreSQL or SQL Server `migration.strict_consistency: true` and
`migration.strict_consistency_scope: migration`, dmt opens one source epoch
for every table and partition reader. PostgreSQL imports one exported MVCC
snapshot. SQL Server creates a `dmt_strict_<runid>` database snapshot and
routes reads through a second pool, leaving live-source writers unblocked.

A resumed PostgreSQL run opens a new snapshot epoch. Resume and replay remain
correct per table, but its cross-table point-in-time guarantee applies only to
the original process. A SQL Server snapshot survives a crash: resume reuses it
and fails closed if it was removed, because a replacement would change the
promised source instant. In-process success, failure, and cancellation close
the snapshot pool and drop the snapshot database.
Schema extraction also occurs before the epoch opens, so DDL changes in that
small interval are intentionally outside the data-snapshot guarantee.

Optional history features are explicit capabilities:

| Capability | SQLite | File-based |
|------------|--------|------------|
| Full run history | Yes | No, only the current YAML run is available |
| Post-AI run config snapshots | Yes | No |
| Encrypted profiles | Yes | No |
| AI adjustment history | Yes | No, save/list methods are no-op/empty |
| AI tuning history | Yes | No, save/list methods are no-op/empty |

The file backend is intended for Airflow/headless restartability rather
than long-lived local history. Use SQLite when operators need `history`,
encrypted `profile` storage, or AI tuning history across many runs.

### Transfer task identity and upgrades

Transfer tasks use structured identity fields: task type, source schema,
source table, and an optional partition number. The human-facing `task_key`
is a collision-free encoded value and is not parsed or prefix-matched for
checkpoint correctness. This keeps quoted identifiers containing dots,
colons, percent signs, underscores, or backslashes distinct from partition
tasks in both SQLite and file state.

SQLite adds the structured columns and unique indexes automatically. Existing
completed history remains readable. An incomplete run containing the older,
delimiter-encoded transfer keys cannot be resumed safely because a key such as
`transfer:dbo.orders:p1` is ambiguous: it may mean partition 1 of `orders` or
the unpartitioned quoted table `orders:p1`. DMT fails that resume with an
actionable legacy-identity error; finish or abandon the old checkpoint and
start a fresh run. File-state YAML follows the same rule.

Before upgrading while a run is incomplete, back up `~/.dmt/migrate.db` or
the configured YAML state file. Do not roll an active structured-identity run
back to a pre-upgrade binary; restore the backup for rollback, or start a fresh
run after applying the target-mode recovery procedure.

### Required checkpoint writes

Checkpoint state is part of the transfer correctness protocol, not optional
telemetry. DMT creates every durable table/partition task before it creates,
drops, or truncates target objects. Once rows are moving, an unresolved
periodic checkpoint error, a final progress-save error, a task-status error,
or a run-completion error stops the success path. These failures use the state
error exit code (6); DMT does not silently continue with task ID zero or report
success beside pending state. SQLite and file state also reject progress or
status writes for unknown task IDs. Aggregate table completion and an optional
incremental sync watermark commit together (one SQLite transaction or one
atomic YAML save), so resume never sees one without the other.

A required-write error can happen after target rows committed, so do not assume
the target is untouched and do not start a competing run. Repair the state
storage first (free disk space, restore write permissions, or restore the
checkpoint path). If the run remains incomplete, use `dmt resume`; its
target-mode replay/cleanup rules handle already committed chunks. Back up the
checkpoint and inspect run/task status before choosing a fresh run or manual
target recovery.

### Run outcome and resumability

Run outcome and recoverability are separate durable fields in both backends.
A transfer attempt that finishes with failed tables records
`status: partial`, `resumable: true`, and a resumability reason. `dmt resume`
selects the newest `resumable: true` run for the configured canonical target,
then schedules only tables whose successful aggregate checkpoint and target
row count do not already agree. The resumed attempt transitions back to
`status: running`; its next outcome replaces the prior partial outcome.

`migration.allow_partial: true` explicitly accepts the partial outcome: the
command exits zero and records `resumable: false`, so a later `dmt resume`
does not silently retry work the operator chose to tolerate. To stop retrying
an ordinary partial or interrupted run without deleting checkpoint history,
use:

```bash
dmt --config production.yaml resume --abandon \
  --abandon-reason "restoring target backup and starting a fresh run"
```

Abandonment acquires the target lease. It cannot race a live owner. A partial
run keeps its truthful `partial` outcome; an interrupted `running` run becomes
`failed`. Both retain `resumable: false` and the operator reason in status
history. `dmt status --json`, migration result JSON, `dmt history`, and the
WebUI history API expose `status`, `resumable`, and `resumability_reason`.

SQLite upgrades add the two run columns automatically and conservatively mark
legacy `running` and `partial` rows resumable. Legacy `success`/`failed` rows
remain terminal. File-state YAML applies the same inference when the field is
absent and persists it on the next mutation. Back up checkpoint storage before
upgrading. A pre-upgrade binary does not understand this split; for rollback,
restore the pre-upgrade checkpoint backup rather than opening upgraded state
with the old binary.

### Exclusive target ownership and fencing

`dmt run` and `dmt resume` acquire the target lease before preflight or any
target mutation. Every owner has a random token and a monotonically increasing
generation. The owner renews the lease with the run heartbeat and releases it
on normal teardown. A second process targeting the same canonical database and
schema receives state exit code 6 while the lease is live; `--force-resume`
does not override a live owner. Different canonical targets use independent
lease rows.

After expiry, acquisition is an atomic stale takeover and increments the
generation. The new generation is bound to the run. Run, task, completion, and
progress mutations verify that binding in the same SQLite transaction or YAML
lock/save cycle, so a former owner receives `LeaseLostError` instead of
changing checkpoint state. Losing renewal also cancels the migration context;
the command cannot report success after losing ownership.

Do not delete or hand-edit `migration_leases`, the run's `lease_*` columns, or
the YAML `migration_leases` map to bypass a conflict. First identify and stop
the owning process. For a crashed process, wait for the configured heartbeat
TTL (15 minutes by default), verify the process is gone, and resume with
`--force-resume` when the stale-heartbeat guard requests it. Back up checkpoint
storage before manual recovery. A run created by a pre-lease binary with a
fresh heartbeat is rejected even with `--force-resume`, because that old
process cannot honor a fencing generation.

### Key Files

```
internal/checkpoint/
├── state.go        # SQLite backend implementation
├── filestate.go    # File-based backend implementation
├── backend.go      # StateBackend interface definition
└── profiles.go     # Encrypted profile storage (SQLite only)

internal/transfer/
└── transfer.go     # Chunk-level checkpoint saves during transfer

internal/orchestrator/
└── orchestrator.go # Run/table level coordination, retry logic

internal/config/
└── config.go       # Restartability config options
```

## Configuration Options

```yaml
migration:
  checkpoint_frequency: 10    # Save progress every N chunks (default: 10)
  max_retries: 3              # Retry failed tables N times after the first attempt (default: 3)
  history_retention_days: 30  # Keep run history for N days (default: 30)
```

### checkpoint_frequency

Controls how often chunk-level progress is saved during transfer.

- **Load-time default**: 10 chunks
- **Automatic pre-transfer tuning**: May replace an unpinned generated value;
  its restored baseline recommendation is 20 chunks
- **Trade-off**: Lower values = more frequent saves = less data loss on crash, but more I/O overhead
- **Location**: `internal/transfer/runner.go` supplies the configured frequency to the keyset and ROW_NUMBER checkpoint coordinators

```go
// Config loading supplies 10; automatic pre-transfer tuning may later
// replace an unpinned generated value with its 20-chunk baseline.
checkpointFreq := cfg.Migration.CheckpointFrequency
if checkpointFreq <= 0 {
    checkpointFreq = 10 // Defensive transfer fallback if config defaulting was bypassed
}
if job.Saver != nil && job.TaskID > 0 && chunkCount%checkpointFreq == 0 && lastPK != nil {
    // Save progress...
}
```

### max_retries

Controls automatic retry for transient errors.

- **Default**: 3 retries (total attempts = 4)
- **Backoff**: Exponential (1s, 2s, 4s, 8s...)
- **Retryable errors**: Connection reset, deadlock, timeout, broken pipe, etc.
- **Location**: `internal/orchestrator/orchestrator.go` lines 86-113 (isRetryableError) and 1025-1053 (retry loop)

```go
// Retryable error patterns
retryablePatterns := []string{
    "connection reset",
    "connection refused",
    "connection timed out",
    "deadlock",
    "lock timeout",
    "too many connections",
    "server is shutting down",
    "broken pipe",
    "unexpected eof",
    "i/o timeout",
    "context deadline exceeded",
}
```

### history_retention_days

Controls automatic cleanup of old completed/failed runs.

- **Default**: 30 days
- **Location**: `internal/checkpoint/state.go` lines 824-881 (CleanupOldRuns)
- **Called**: On orchestrator initialization (`internal/orchestrator/orchestrator.go` lines 244-253)

## How Checkpointing Works

### 1. Run Creation

When a migration starts, a run record is created:

```go
// internal/checkpoint/state.go CreateRun()
INSERT INTO runs (id, started_at, status, source_schema, target_schema, config, config_hash, ...)
```

The `config_hash` is computed from the sanitized config JSON (SHA256, first 8 bytes hex-encoded).

### 2. Task Creation

For each table, a task is created:

```go
// internal/checkpoint/state.go CreateTask()
INSERT INTO tasks (run_id, task_type, task_key, status)
VALUES (?, 'transfer', 'transfer:schema.table', 'pending')
```

### 3. Chunk-Level Progress

During transfer, progress is saved periodically:

```go
// internal/checkpoint/state.go SaveTransferProgress()
INSERT INTO transfer_progress (task_id, table_name, partition_id, last_pk, rows_done, rows_total, updated_at)
VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
ON CONFLICT(task_id) DO UPDATE SET
    last_pk = excluded.last_pk,
    rows_done = excluded.rows_done,
    updated_at = excluded.updated_at
```

**Key fields**:
- `last_pk`: JSON-encoded primary key value (for keyset pagination) or row number (for ROW_NUMBER pagination)
- `rows_done`: Approximate rows transferred
- `partition_id`: For partitioned tables, which partition this progress belongs to

Checkpoint saves occur after a chunk write completes. With parallel readers, the checkpoint uses a conservative safe point (the lowest fully written range) to avoid skipping data after a crash.

### 4. Resume Flow

On resume (`/resume` or `dmt resume`):

1. **Find resumable run**: select the most recent target-scoped run with `resumable=true` (including `partial` outcomes)
2. **Acquire and bind target lease**: reject a live owner or atomically take over an expired generation
3. **Validate config hash**: Compare stored hash with current config hash (skip if `--force-resume`)
4. **Reactivate outcome**: transition the selected run to `status=running` while preserving resume eligibility
5. **Get completed tables**: `GetCompletedTables()` returns tables marked as `success`
6. **Load progress**: For incomplete tables, `GetTransferProgress()` returns saved checkpoint
7. **Cleanup partial data**: Delete rows beyond saved lastPK (handles partially written chunks)
8. **Resume transfer**: Start from saved lastPK/rowNum

```go
// internal/transfer/transfer.go Execute()
if job.Saver != nil && job.TaskID > 0 {
    resumeLastPK, resumeRowsDone, err = job.Saver.GetProgress(job.TaskID)
    if resumeLastPK != nil {
        logging.Info("Resuming %s at row %d (checkpoint: %v)", job.Table.Name, resumeRowsDone, resumeLastPK)
    }
}
```

## Pagination Strategies

### Keyset Pagination (Preferred)

Used when table has a single-column integer primary key.

- **Query**: `SELECT ... WHERE pk > @lastPK ORDER BY pk LIMIT @chunkSize`
- **Progress tracking**: Stores actual PK value as `last_pk`
- **Resume**: Query continues from `WHERE pk > savedLastPK`
- **Cleanup on resume**: `DELETE FROM table WHERE pk > savedLastPK AND pk <= maxPK`

**Location**: `internal/transfer/transfer.go` `executeKeysetPagination()` lines 496-802

### Tuple Keyset Pagination

Tuple keyset is used for composite and otherwise tuple-safe primary keys. It
keeps the source engine's tuple ordering semantics, including text collation:

- **Query**: `WHERE (a,b,...) > (last_a,last_b,...) ORDER BY a,b,...`
- **Parallel eligibility**: when the leading component is int64-safe and
  `parallel_readers > 1`, DMT splits that component into work-stealing ranges.
  Relaxed reads always qualify; strict reads additionally require an engine
  strategy with tuple-safe worker sessions (PostgreSQL exported snapshots,
  MySQL lock-window sessions, or SQL Server shared locks/database snapshots).
  Each reader still advances with the complete tuple inside its own
  `min <= a <= max` range.
- **Progress tracking**: parallel tuple readers save a versioned
  `range_state` envelope containing each range's bounds, completion bit, and
  typed tuple watermark. A periodic checkpoint's legacy `last_pk` is a
  range-ordered safe frontier, never an arbitrary faster reader's watermark.
- **Resume**: current binaries restore every range verbatim and use
  duplicate-safe target writes. No target-side tuple range deletion is used,
  because it could apply target collation semantics to a source-order
  watermark.

Checkpoint compatibility:

- An older, single-tuple checkpoint resumes on a new binary through the
  established single-reader tuple path; it is not range-split from the table
  beginning.
- Do **not** downgrade an interrupted task that has a #667 range envelope.
  Pre-#667 binaries ignore the envelope and decode legacy JSON `last_pk`
  numbers through `float64`; that can round BIGINT tuple values above `2^53`
  and skip or replay the wrong suffix. Resume it with this or a newer binary,
  or begin a fresh migration after the normal target recovery procedure.

Nonnumeric leading keys, PK value converters, engines without a vetted range
template, and strict strategies without tuple-safe worker sessions keep the
prior single-reader tuple path.

### ROW_NUMBER Pagination (Fallback)

Used for tables without a tuple-safe primary key or with a converter-touched
primary key (tables without primary keys are rejected).

- **Query**: `WITH numbered AS (SELECT ..., ROW_NUMBER() OVER (ORDER BY pk) as __rn) SELECT ... WHERE __rn > @rowNum AND __rn <= @rowNumEnd`
- **Progress tracking**: Stores row number as `last_pk`
- **Resume**: Query continues from saved row number
- **Limitation**: No cleanup possible - must re-transfer from saved row number

**Location**: `internal/transfer/transfer.go` `executeRowNumberPagination()` lines 804-1112

## Database Schema (SQLite)

```sql
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',  -- running, success, failed
    phase TEXT NOT NULL DEFAULT 'initializing',  -- initializing, transferring, finalizing, validating, complete
    source_schema TEXT NOT NULL,
    target_schema TEXT NOT NULL,
    config TEXT,
    config_hash TEXT,  -- SHA256 hash for change detection
    profile_name TEXT,
    config_path TEXT,
    error TEXT
);

CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES runs(id),
    task_type TEXT NOT NULL,  -- transfer, create_pks, create_indexes, etc.
    task_key TEXT NOT NULL,   -- e.g., "transfer:dbo.Users"
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, running, success, failed
    started_at TEXT,
    completed_at TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    error_message TEXT,
    UNIQUE(run_id, task_key)
);

CREATE TABLE transfer_progress (
    task_id INTEGER PRIMARY KEY REFERENCES tasks(id),
    table_name TEXT NOT NULL,
    partition_id INTEGER,
    last_pk TEXT,  -- JSON-encoded PK value or row number
    rows_done INTEGER DEFAULT 0,
    rows_total INTEGER,
    updated_at TEXT
);
```

## Limitations

### 1. Checkpoint Granularity

**Issue**: Progress is saved every N chunks, not after every row.

**Impact**: On crash, up to `checkpoint_frequency * chunk_size` rows may need to be re-transferred.

**Example**: The load-time default is `checkpoint_frequency=10`; automatic pre-transfer tuning may replace an unpinned value with 20. The generated `chunk_size` is RAM-shaped and may later be replaced by pre-transfer tuning or a global representative-width/protocol clamp. An applied runtime writer-count transition can also ratchet later chunks downward. At 50,000 effective rows per chunk, the replay window is up to 500,000 rows at frequency 10 or 1,000,000 rows at frequency 20.

**Mitigation**: Reduce `checkpoint_frequency` for critical migrations (at cost of more I/O).

### 2. ROW_NUMBER Pagination — Resume Safety (fixed in #227)

**Status:** Safe to resume. Tables paginated via `ROW_NUMBER()` (composite or varchar primary keys) used to risk silent duplicates or skipped rows on resume; both holes are closed.

**What went wrong before the fix**

ROW_NUMBER-paged tables saved chunk progress at the partition level (`transfer:schema.table:p<N>`), not the table level. Two distinct bugs combined to cause data loss on resume:

1. **Resume preflight truncated the wrong tables.** The preflight check decided "is this table partitioned?" by `IsLarge && SupportsKeysetPagination()`, which is false for composite/varchar PKs even when the table *is* partitioned via ROW_NUMBER. The result: large ROW_NUMBER tables were truncated on resume because their table-level checkpoint was always nil — but the partition-level checkpoints survived. Each partition then resumed from its saved `rowNum`, the source's `ORDER BY ROW_NUMBER()` started at 1, and rows 1..lastRowNum on each partition were skipped silently.
2. **Per-table checkpoints lag acked rows.** The checkpoint coordinator only saves progress every `checkpoint_freq` acked chunks. Between flushes, up to `checkpoint_freq - 1` chunks of target-side data sit in a window where the state DB says less data has been written than is actually committed. On crash + resume, those chunks get replayed → duplicate PK errors (or silent overwrites for tables without unique constraints).

**What changed (#227)**

- **Preflight is partition-aware.** The truncation decision now uses `IsLarge && HasPK()`, which matches the actual partitioning decision in `job_builder.go`. Large ROW_NUMBER tables are no longer truncated based on a missing table-level checkpoint. When preflight *does* truncate (e.g. small ROW_NUMBER tables, or when the target row count fell below saved progress), it also clears any stale partition-level checkpoints so partitions don't resume from a non-zero `rowNum` against a freshly-cleared target.
- **Idempotent INSERT on resume.** When the orchestrator dispatches a job as part of a Resume() call for a ROW_NUMBER-paged table in non-upsert mode, the writer routes through driver-specific idempotent paths so replayed already-committed rows are silent no-ops. The gate is the resume flag on the dispatched job — not the per-partition checkpoint state — so partitions that crashed AFTER committing rows but BEFORE the first checkpoint flush are still protected:
  - **PostgreSQL**: temp staging table + COPY + `INSERT ... SELECT ... ON CONFLICT (pk) DO NOTHING`.
  - **MySQL**: `INSERT ... ON DUPLICATE KEY UPDATE pk_col = pk_col` (no-op PK self-assignment — NOT `INSERT IGNORE`, which would also mask data-conversion errors).
  - **MSSQL**: per-writer/per-partition staging table + MERGE with `WHEN NOT MATCHED THEN INSERT` only (no `WHEN MATCHED ... UPDATE` branch — replayed rows must not overwrite the target with potentially-changed source values).

**Non-regression**

Clean first-time runs and the upsert target mode are untouched. The idempotent path activates only on resume of ROW_NUMBER-paged tables in non-upsert mode, so first-time `drop_recreate` migrations still use the fast plain-INSERT/COPY/BCP path.

**Operational notes**

- Tables paginated via ROW_NUMBER must have a primary key; transfer fails fast with a clear error if not. Tables without a PK cannot be made resume-safe by this fix because there's nothing to conflict on.
- The PostgreSQL idempotent path uses a `TEMP TABLE ... ON COMMIT DELETE ROWS`, so it adds one CREATE-TEMP-TABLE per chunk on resume. The MSSQL path uses a local `#temp` table. MySQL adds no extra DDL.

### 3. Upsert Mode Considerations

**Issue**: Upsert mode is idempotent, so checkpointing is less critical but still beneficial.

**Impact**: Re-running the same data just updates existing rows (no duplicates).

**Note**: Progress tracking still works, reducing unnecessary work on resume.

### 4. Partitioned Table Progress

**Issue**: Each partition has separate progress, but all partitions must complete for table to be marked success.

**Impact**: If one partition fails, entire table may need re-evaluation on resume.

**Location**: Partition progress is tracked via `partition_id` in `transfer_progress` table.

### 5. Schema Changes Between Runs

**Issue**: Config hash validation only checks config, not source schema.

**Impact**: If source schema changes between run start and resume, data may be inconsistent.

**Mitigation**: Use `--force-resume` only when you understand the implications.

### 6. No Automatic Cleanup of Incomplete Runs

**Issue**: Incomplete runs remain in SQLite until manually cleaned or retention expires.

**Impact**: `GetLastIncompleteRun()` may return stale runs.

**Mitigation**: Use `HasSuccessfulRunAfter()` check to detect superseded runs.

## Testing Procedures

### Unit Tests

```bash
# Run all tests
go test ./...

# Run checkpoint-specific tests
go test ./internal/checkpoint/... -v
```

### Manual Testing: Checkpoint Frequency

1. **Setup**: Create a large table (1M+ rows) in source database

2. **Configure low checkpoint frequency**:
   ```yaml
   migration:
     checkpoint_frequency: 2  # Save every 2 chunks
     chunk_size: 10000
   ```

3. **Start migration**:
   ```bash
   ./dmt -c config.yaml run
   ```

4. **Kill process mid-transfer**: Press Ctrl+C or `kill -9 <pid>` during transfer phase

5. **Check SQLite state**:
   ```bash
   sqlite3 ~/.dmt/migrate.db "SELECT * FROM transfer_progress"
   sqlite3 ~/.dmt/migrate.db "SELECT id,status,resumable,resumability_reason FROM runs WHERE resumable=1"
   ```

6. **Resume**:
   ```bash
   ./dmt -c config.yaml resume
   ```

7. **Verify**:
   - Log should show "Resuming <table> at row X"
   - Transfer should continue from checkpoint, not start over
   - Final row count should match source

### Manual Testing: Config Hash Validation

1. **Start migration and kill mid-transfer** (as above)

2. **Modify config** (e.g., change `chunk_size`)

3. **Attempt resume**:
   ```bash
   ./dmt -c config.yaml resume
   ```

4. **Expected**: Error message about config hash mismatch

5. **Force resume** (if needed):
   ```bash
   ./dmt -c config.yaml resume --force-resume
   ```

### Manual Testing: Retry Logic

1. **Setup**: Configure target database to reject connections intermittently (e.g., firewall rule)

2. **Configure retries**:
   ```yaml
   migration:
     max_retries: 3
   ```

3. **Start migration** and trigger connection failure

4. **Expected**: Log shows retry attempts with backoff:
   ```
   WARN Retry 1/3 for Users after 1s (error: connection reset)
   WARN Retry 2/3 for Users after 2s (error: connection reset)
   ```

### Manual Testing: History Cleanup

1. **Create old test runs**:
   ```bash
   sqlite3 ~/.dmt/migrate.db "INSERT INTO runs (id, started_at, completed_at, status, source_schema, target_schema) VALUES ('old-run-1', datetime('now', '-60 days'), datetime('now', '-60 days'), 'success', 'dbo', 'public')"
   ```

2. **Configure retention**:
   ```yaml
   migration:
     history_retention_days: 30
   ```

3. **Start any migration** (cleanup runs on init)

4. **Verify cleanup**:
   ```bash
   sqlite3 ~/.dmt/migrate.db "SELECT id FROM runs WHERE id='old-run-1'"
   # Should return no rows
   ```

### Integration Test Script

```bash
#!/bin/bash
# test_restartability.sh

set -e

CONFIG="examples/config.yaml"
DB="$HOME/.dmt/migrate.db"

echo "=== Testing Restartability ==="

# 1. Clean state
rm -f "$DB"

# 2. Start migration in background
./dmt -c "$CONFIG" run &
PID=$!
sleep 10  # Let it run for a bit

# 3. Kill it
kill -9 $PID 2>/dev/null || true
sleep 2

# 4. Check state
echo "Checking state after kill..."
sqlite3 "$DB" "SELECT id, status, phase FROM runs"
sqlite3 "$DB" "SELECT task_key, status FROM tasks LIMIT 5"
sqlite3 "$DB" "SELECT table_name, rows_done, rows_total FROM transfer_progress LIMIT 5"

# 5. Resume
echo "Resuming..."
./dmt -c "$CONFIG" resume

# 6. Verify
echo "Verifying..."
sqlite3 "$DB" "SELECT id, status FROM runs ORDER BY started_at DESC LIMIT 1"

echo "=== Test Complete ==="
```

## Debugging

### View Current State

```bash
# All runs
sqlite3 ~/.dmt/migrate.db "SELECT id, status, phase, started_at FROM runs ORDER BY started_at DESC"

# Tasks for a run
sqlite3 ~/.dmt/migrate.db "SELECT task_key, status, retry_count FROM tasks WHERE run_id='<run-id>'"

# Progress for incomplete transfers
sqlite3 ~/.dmt/migrate.db "SELECT tp.table_name, tp.rows_done, tp.rows_total, tp.last_pk FROM transfer_progress tp JOIN tasks t ON tp.task_id = t.id WHERE t.status != 'success'"
```

### Enable Debug Logging

```bash
export LOG_LEVEL=debug
./dmt -c config.yaml run
```

Debug logs show:
- Checkpoint saves: "Checkpoint save failed for X" (on error only)
- Resume points: "Resuming X at row Y (checkpoint: Z)"
- Retry attempts: "Retry N/M for X after Ys"
- Pipeline stats: "Pipeline X: N chunks, overlap=..."

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "config changed since run started" | Config modified between run and resume | Use `--force-resume` or revert config |
| "incomplete run is obsolete" | A successful run completed after the incomplete one | Start fresh with `run` instead of `resume` |
| Duplicate rows after resume | ROW_NUMBER pagination + crash mid-chunk | Use `target_mode: drop_recreate` and re-run |
| Resume starts from beginning | No checkpoint saved (small table or early crash) | Expected behavior - checkpoint_frequency not reached |

## Future Improvements

1. **Per-row checkpointing**: Save progress after each successful batch write (more overhead, less data loss)
2. **Transactional checkpoints**: Wrap checkpoint save in same transaction as data write
3. **Checkpoint compression**: Compress large lastPK values (e.g., composite keys)
4. **Parallel partition resume**: Resume multiple partitions in parallel
5. **Schema change detection**: Hash source schema and validate on resume
