# Restartability Guide

This document describes the checkpoint and resume functionality in data-transfer-tool, including implementation details, limitations, and testing procedures.

## Overview

data-transfer-tool supports resumable migrations through a checkpoint system that tracks progress at multiple levels:

1. **Run level**: Tracks overall migration status (running, success, failed)
2. **Table level**: Tracks which tables have been successfully transferred
3. **Chunk level**: Tracks progress within a table transfer (lastPK or rowNum)

## Architecture

### State Backends

Two state backends are available:

| Backend | Storage | Use Case | Config Hash | Chunk Progress |
|---------|---------|----------|-------------|----------------|
| SQLite (default) | `~/.data-transfer-tool/migrate.db` | Desktop/interactive | ✅ Yes | ✅ Yes |
| File-based | User-specified YAML file | Airflow/headless | ✅ Yes | ✅ Yes |

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

- **Default**: 10 chunks
- **Trade-off**: Lower values = more frequent saves = less data loss on crash, but more I/O overhead
- **Location**: `internal/transfer/transfer.go` lines 758-773 (keyset) and 1060-1079 (row_number)

```go
// Chunk-level checkpointing: save progress every N chunks
checkpointFreq := cfg.Migration.CheckpointFrequency
if checkpointFreq <= 0 {
    checkpointFreq = 10 // Default fallback
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

On resume (`/resume` command or `--resume` flag):

1. **Find incomplete run**: `GetLastIncompleteRun()` returns the most recent run with status='running'
2. **Validate config hash**: Compare stored hash with current config hash (skip if `--force-resume`)
3. **Get completed tables**: `GetCompletedTables()` returns tables marked as 'success'
4. **Load progress**: For incomplete tables, `GetTransferProgress()` returns saved checkpoint
5. **Cleanup partial data**: Delete rows beyond saved lastPK (handles partially written chunks)
6. **Resume transfer**: Start from saved lastPK/rowNum

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

### ROW_NUMBER Pagination (Fallback)

Used for composite PKs, varchar PKs, or tables without PKs (rejected).

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

**Example**: With default settings (checkpoint_frequency=10, chunk_size=100000), up to 1M rows could be lost on crash.

**Mitigation**: Reduce `checkpoint_frequency` for critical migrations (at cost of more I/O).

### 2. ROW_NUMBER Pagination Limitations

**Issue**: ROW_NUMBER pagination cannot cleanup partial data on resume.

**Impact**: If a crash occurs mid-chunk, duplicate rows may be inserted on resume.

**Affected tables**: Tables with composite PKs or varchar PKs.

**Mitigation**: Use `target_mode: drop_recreate` for affected tables, or ensure tables have single-column integer PKs.

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
   ./data-transfer-tool -c config.yaml run
   ```

4. **Kill process mid-transfer**: Press Ctrl+C or `kill -9 <pid>` during transfer phase

5. **Check SQLite state**:
   ```bash
   sqlite3 ~/.data-transfer-tool/migrate.db "SELECT * FROM transfer_progress"
   sqlite3 ~/.data-transfer-tool/migrate.db "SELECT * FROM runs WHERE status='running'"
   ```

6. **Resume**:
   ```bash
   ./data-transfer-tool -c config.yaml resume
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
   ./data-transfer-tool -c config.yaml resume
   ```

4. **Expected**: Error message about config hash mismatch

5. **Force resume** (if needed):
   ```bash
   ./data-transfer-tool -c config.yaml resume --force-resume
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
   sqlite3 ~/.data-transfer-tool/migrate.db "INSERT INTO runs (id, started_at, completed_at, status, source_schema, target_schema) VALUES ('old-run-1', datetime('now', '-60 days'), datetime('now', '-60 days'), 'success', 'dbo', 'public')"
   ```

2. **Configure retention**:
   ```yaml
   migration:
     history_retention_days: 30
   ```

3. **Start any migration** (cleanup runs on init)

4. **Verify cleanup**:
   ```bash
   sqlite3 ~/.data-transfer-tool/migrate.db "SELECT id FROM runs WHERE id='old-run-1'"
   # Should return no rows
   ```

### Integration Test Script

```bash
#!/bin/bash
# test_restartability.sh

set -e

CONFIG="examples/config.yaml"
DB="$HOME/.data-transfer-tool/migrate.db"

echo "=== Testing Restartability ==="

# 1. Clean state
rm -f "$DB"

# 2. Start migration in background
./data-transfer-tool -c "$CONFIG" run &
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
./data-transfer-tool -c "$CONFIG" resume

# 6. Verify
echo "Verifying..."
sqlite3 "$DB" "SELECT id, status FROM runs ORDER BY started_at DESC LIMIT 1"

echo "=== Test Complete ==="
```

## Debugging

### View Current State

```bash
# All runs
sqlite3 ~/.data-transfer-tool/migrate.db "SELECT id, status, phase, started_at FROM runs ORDER BY started_at DESC"

# Tasks for a run
sqlite3 ~/.data-transfer-tool/migrate.db "SELECT task_key, status, retry_count FROM tasks WHERE run_id='<run-id>'"

# Progress for incomplete transfers
sqlite3 ~/.data-transfer-tool/migrate.db "SELECT tp.table_name, tp.rows_done, tp.rows_total, tp.last_pk FROM transfer_progress tp JOIN tasks t ON tp.task_id = t.id WHERE t.status != 'success'"
```

### Enable Debug Logging

```bash
export LOG_LEVEL=debug
./data-transfer-tool -c config.yaml run
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
