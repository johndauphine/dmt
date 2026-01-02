# Upsert Performance Improvement Plan

## Overview

This plan addresses the ~40% performance gap between the Go and Rust implementations for upsert operations. The improvements target both migration directions:
- **PostgreSQL → MSSQL** (upsert to MSSQL target)
- **MSSQL → PostgreSQL** (upsert to PostgreSQL target)

## Current State Analysis

### PostgreSQL Target (MSSQL → PostgreSQL)

**Current Go Implementation** (`target/pool.go:389-527`):
```go
// Batched multi-row INSERT...VALUES with parameters
INSERT INTO table (cols) VALUES ($1, $2, ...), ($N+1, $N+2, ...), ...
ON CONFLICT (pk) DO UPDATE SET col1 = EXCLUDED.col1, ...
WHERE (table.col1, ...) IS DISTINCT FROM (EXCLUDED.col1, ...)
```

**Problems**:
1. Parameter binding overhead — PostgreSQL parses up to 65,000 parameters per batch
2. Multiple batched statements — each batch requires separate round-trip
3. No binary protocol — text parameter serialization

**Rust Implementation** (`target/mod.rs:1297-1483`):
```rust
// 1. Create/reuse temp staging table per writer
CREATE TEMP TABLE IF NOT EXISTS _staging_schema_table_w0 (LIKE target)
// 2. TRUNCATE (faster than DROP/CREATE)
TRUNCATE TABLE _staging_schema_table_w0
// 3. Binary COPY into staging
COPY _staging_schema_table_w0 FROM STDIN WITH (FORMAT binary)
// 4. Single merge statement
INSERT INTO target SELECT * FROM staging ON CONFLICT (pk) DO UPDATE SET ...
```

**Performance**: Go ~75K rows/s vs Rust ~106K rows/s (40% gap)

### MSSQL Target (PostgreSQL → MSSQL)

**Current Go Implementation** (`target/mssql_pool.go:394-585`):
- Uses shared staging table across all writers
- All chunks bulk-inserted to staging first
- Single UPDATE + INSERT at the END (not per-chunk)
- No deadlock retry logic
- No per-writer isolation

**Rust Implementation** (`target/mssql.rs:1391-1503`):
- Per-writer staging tables (`_staging_table_w0`, `_staging_table_w1`, ...)
- Per-chunk MERGE (streaming, incremental)
- MERGE with `WITH (TABLOCK)` to prevent deadlocks
- Deadlock retry with exponential backoff (5 retries)
- Bounded memory (staging holds only 1 chunk)

---

## Implementation Plan

### Phase 1: Interface Updates

**File: `internal/pool/interfaces.go`**

Add new method signature to support writer isolation:

```go
type TargetPool interface {
    // Existing methods...

    // UpsertChunk - existing signature (for backward compatibility)
    UpsertChunk(ctx context.Context, schema, table string, cols []string,
                pkCols []string, rows [][]any) error

    // NEW: UpsertChunkWithWriter - adds writer isolation parameters
    UpsertChunkWithWriter(ctx context.Context, schema, table string, cols []string,
                          pkCols []string, rows [][]any, writerID int,
                          partitionID *int) error
}
```

### Phase 2: PostgreSQL Target Improvements

**File: `internal/target/pool.go`**

Implement staging table approach:

1. **Add `UpsertChunkWithWriter` function**:
   - Create per-writer UNLOGGED temp staging table (reused across chunks)
   - Use `pgx.CopyFrom()` for binary bulk load into staging
   - Single `INSERT...SELECT...ON CONFLICT` to merge
   - **KEEP `IS DISTINCT FROM` clause** to prevent unnecessary row versions/WAL writes
   - TRUNCATE staging between chunks (not DROP/CREATE)

2. **Helper functions**:
   - `ensureStagingTable(schema, table, writerID)` - CREATE TEMP TABLE IF NOT EXISTS
   - `buildStagingMergeSQL(target, staging, cols, pkCols)` - generate merge SQL with IS DISTINCT FROM
   - `safeStagingName(schema, table, writerID)` - handle long table names with hash fallback

3. **Safe Naming Strategy** (per Gemini review):
   ```go
   func safeStagingName(schema, table string, writerID int) string {
       suffix := fmt.Sprintf("_w%d", writerID)
       base := fmt.Sprintf("_stg_%s_%s", schema, table)
       maxLen := 63 // PostgreSQL identifier limit

       if len(base)+len(suffix) > maxLen {
           // Use hash for long names
           hash := sha256.Sum256([]byte(schema + table))
           base = fmt.Sprintf("_stg_%x", hash[:8]) // 16 hex chars
       }
       return base + suffix
   }
   ```

**Example implementation**:
```go
func (p *Pool) UpsertChunkWithWriter(ctx context.Context, schema, table string,
    cols []string, pkCols []string, rows [][]any, writerID int, partitionID *int) error {

    conn, err := p.pool.Acquire(ctx)
    if err != nil {
        return err
    }
    defer conn.Release()

    // 1. Generate safe staging table name (handles long names)
    stagingTable := safeStagingName(schema, table, writerID)

    // 2. Create UNLOGGED temp staging table if not exists
    // UNLOGGED reduces WAL I/O - we don't need crash safety for staging
    createSQL := fmt.Sprintf(
        `CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s.%s INCLUDING DEFAULTS)`,
        quotePGIdent(stagingTable), quotePGIdent(schema), quotePGIdent(table))
    if _, err := conn.Exec(ctx, createSQL); err != nil {
        return err
    }

    // 3. Truncate staging (reuse table, faster than DROP/CREATE)
    if _, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", quotePGIdent(stagingTable))); err != nil {
        return err
    }

    // 4. Binary COPY into staging (uses pgx.CopyFrom - already available!)
    _, err = conn.Conn().CopyFrom(ctx, pgx.Identifier{stagingTable}, cols, pgx.CopyFromRows(rows))
    if err != nil {
        return err
    }

    // 5. Merge staging into target WITH IS DISTINCT FROM to prevent empty updates
    // This is critical to avoid unnecessary WAL writes and table bloat
    mergeSQL := buildPGStagingMergeSQL(schema, table, stagingTable, cols, pkCols)
    _, err = conn.Exec(ctx, mergeSQL)
    return err
}

func buildPGStagingMergeSQL(schema, table, stagingTable string, cols, pkCols []string) string {
    // ... build column lists ...

    // CRITICAL: Keep IS DISTINCT FROM to prevent unnecessary row versions
    // PostgreSQL MVCC writes new tuples even for identical data without this
    var changeConditions []string
    for _, col := range cols {
        if !isPKColumn(col, pkCols) {
            changeConditions = append(changeConditions,
                fmt.Sprintf("target.%s IS DISTINCT FROM excluded.%s",
                    quotePGIdent(col), quotePGIdent(col)))
        }
    }
    whereClause := ""
    if len(changeConditions) > 0 {
        whereClause = fmt.Sprintf(" WHERE %s", strings.Join(changeConditions, " OR "))
    }

    return fmt.Sprintf(
        `INSERT INTO %s.%s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s%s`,
        quotePGIdent(schema), quotePGIdent(table), colStr, colStr,
        quotePGIdent(stagingTable), pkStr, setClauses, whereClause)
}
```

### Phase 3: MSSQL Target Improvements

**File: `internal/target/mssql_pool.go`**

Switch from batch-all-then-merge to per-chunk merge using session-scoped temp tables:

1. **Use `#TempTable` syntax** (per Gemini review):
   - Session-scoped temp tables are automatically dropped when connection closes
   - No cleanup logic needed - handles crashes gracefully
   - `tempdb` is optimized for this use case
   - Avoids schema pollution in user namespace

2. **Add `UpsertChunkWithWriter` function**:
   - Create per-writer session temp table `#staging_table_w0`
   - Bulk insert chunk using existing TDS bulk copy
   - Execute MERGE with `WITH (TABLOCK)` immediately
   - TRUNCATE temp table for reuse within session
   - Add deadlock retry with linear backoff

3. **Safe Naming Strategy**:
   ```go
   func safeMSSQLStagingName(table string, writerID int, partitionID *int) string {
       suffix := fmt.Sprintf("_w%d", writerID)
       if partitionID != nil {
           suffix = fmt.Sprintf("_p%d%s", *partitionID, suffix)
       }
       base := fmt.Sprintf("#stg_%s", table)
       maxLen := 116 // MSSQL temp table limit (128 - system suffix)

       if len(base)+len(suffix) > maxLen {
           hash := sha256.Sum256([]byte(table))
           base = fmt.Sprintf("#stg_%x", hash[:8])
       }
       return base + suffix
   }
   ```

**Example implementation**:
```go
func (p *MSSQLPool) UpsertChunkWithWriter(ctx context.Context, schema, table string,
    cols []string, pkCols []string, rows [][]any, writerID int, partitionID *int) error {

    conn, err := p.db.Conn(ctx)
    if err != nil {
        return err
    }
    defer conn.Close()

    // 1. Generate session-scoped temp table name (#table auto-drops on disconnect)
    stagingTable := safeMSSQLStagingName(table, writerID, partitionID)

    // 2. Create temp table from target structure (if not exists)
    // Using SELECT INTO for structure, then truncate for reuse
    createSQL := fmt.Sprintf(`
        IF OBJECT_ID('tempdb..%s') IS NULL
            SELECT TOP 0 * INTO %s FROM [%s].[%s]
        ELSE
            TRUNCATE TABLE %s`,
        stagingTable, stagingTable, schema, table, stagingTable)
    if _, err := conn.ExecContext(ctx, createSQL); err != nil {
        return err
    }

    // 3. Bulk insert into temp staging table
    if err := p.bulkInsertToTempTable(ctx, conn, stagingTable, cols, rows); err != nil {
        return err
    }

    // 4. MERGE with deadlock retry and TABLOCK
    targetTable := fmt.Sprintf("[%s].[%s]", schema, table)
    mergeSQL := buildMSSQLMergeSQL(targetTable, stagingTable, cols, pkCols)
    if err := p.executeMergeWithRetry(ctx, conn, schema, table, mergeSQL, 5); err != nil {
        return err
    }

    return nil
}

func buildMSSQLMergeSQL(targetTable, stagingTable string, cols, pkCols []string) string {
    // Build join condition
    var joinConds []string
    for _, pk := range pkCols {
        joinConds = append(joinConds, fmt.Sprintf("target.[%s] = source.[%s]", pk, pk))
    }

    // Build update SET clause (exclude PKs)
    var setClauses []string
    for _, col := range cols {
        if !isPKColumn(col, pkCols) {
            setClauses = append(setClauses, fmt.Sprintf("[%s] = source.[%s]", col, col))
        }
    }

    // Build change detection (NULL-safe for MSSQL)
    var changeDetection []string
    for _, col := range cols {
        if !isPKColumn(col, pkCols) {
            changeDetection = append(changeDetection, fmt.Sprintf(
                "(target.[%s] <> source.[%s] OR "+
                "(target.[%s] IS NULL AND source.[%s] IS NOT NULL) OR "+
                "(target.[%s] IS NOT NULL AND source.[%s] IS NULL))",
                col, col, col, col, col, col))
        }
    }

    // WITH (TABLOCK) prevents S->X lock conversion deadlocks
    return fmt.Sprintf(`
        MERGE INTO %s WITH (TABLOCK) AS target
        USING %s AS source
        ON %s
        WHEN MATCHED AND (%s) THEN UPDATE SET %s
        WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
        targetTable, stagingTable,
        strings.Join(joinConds, " AND "),
        strings.Join(changeDetection, " OR "),
        strings.Join(setClauses, ", "),
        colList, sourceColList)
}

func (p *MSSQLPool) executeMergeWithRetry(ctx context.Context, conn *sql.Conn,
    schema, table, mergeSQL string, maxRetries int) error {

    const baseDelayMs = 200

    for attempt := 1; attempt <= maxRetries; attempt++ {
        _, err := conn.ExecContext(ctx, mergeSQL)
        if err == nil {
            return nil
        }

        if !isDeadlockError(err) || attempt == maxRetries {
            return err
        }

        logging.Warn("Deadlock on %s.%s, retry %d/%d", schema, table, attempt, maxRetries)
        time.Sleep(time.Duration(baseDelayMs*attempt) * time.Millisecond)
    }

    return fmt.Errorf("merge failed after %d retries", maxRetries)
}

func isDeadlockError(err error) bool {
    // MSSQL deadlock error code is 1205
    if mssqlErr, ok := err.(interface{ SQLErrorNumber() int32 }); ok {
        return mssqlErr.SQLErrorNumber() == 1205
    }
    return strings.Contains(err.Error(), "deadlock") ||
           strings.Contains(err.Error(), "1205")
}
```

### Phase 4: Transfer Layer Updates

**File: `internal/transfer/transfer.go`**

Update writer goroutines to pass writer ID:

```go
// Around line 645-677, in the writer goroutine setup
for i := 0; i < numWriters; i++ {
    writerID := i  // Capture for closure
    writerWg.Add(1)
    go func() {
        defer writerWg.Done()
        for rows := range writeJobChan {
            if useUpsert {
                var partID *int
                if job.Partition != nil {
                    partID = &job.Partition.PartitionID
                }
                // Use new method with writer isolation
                err = writeChunkUpsertWithWriter(writerCtx, tgtPool, cfg.Target.Schema,
                    job.Table.Name, cols, pkCols, rows, writerID, partID)
            } else {
                err = writeChunkGeneric(writerCtx, tgtPool, cfg.Target.Schema,
                    job.Table.Name, cols, rows)
            }
            // ... error handling ...
        }
    }()
}
```

**Add new helper function**:
```go
func writeChunkUpsertWithWriter(ctx context.Context, tgtPool pool.TargetPool,
    schema, table string, cols []string, pkCols []string, rows [][]any,
    writerID int, partitionID *int) error {

    // Use interface method that supports writer isolation
    if wp, ok := tgtPool.(interface {
        UpsertChunkWithWriter(context.Context, string, string, []string, []string,
                              [][]any, int, *int) error
    }); ok {
        return wp.UpsertChunkWithWriter(ctx, schema, table, cols, pkCols, rows,
                                        writerID, partitionID)
    }

    // Fallback to existing method (no writer isolation)
    return tgtPool.UpsertChunk(ctx, schema, table, cols, pkCols, rows)
}
```

### Phase 5: Cleanup (Simplified)

**Per Gemini review**: With session-scoped temp tables (#table for MSSQL, TEMP for PostgreSQL), cleanup is automatic:
- **PostgreSQL**: Temp tables are session-scoped and auto-dropped on disconnect
- **MSSQL**: `#` tables are session-scoped and auto-dropped on disconnect

No explicit cleanup phase needed. If app crashes, temp tables are automatically cleaned up by the database.

---

## Testing Plan

### Unit Tests

1. **`target/pool_test.go`**:
   - `TestBuildPGStagingMergeSQL` - verify SQL generation includes IS DISTINCT FROM
   - `TestSafeStagingName` - verify naming with long names, hash fallback
   - `TestUpsertChunkWithWriter_Integration` (requires test DB)

2. **`target/mssql_pool_test.go`**:
   - `TestBuildMSSQLMergeSQL` - verify MERGE SQL generation with TABLOCK
   - `TestSafeMSSQLStagingName` - verify #table naming
   - `TestIsDeadlockError` - verify error detection for code 1205
   - `TestExecuteMergeWithRetry_Integration` (requires test DB)

### Integration Tests

1. **PostgreSQL target upsert benchmark**:
   ```bash
   # Before changes
   go test -bench=BenchmarkUpsertPG -benchtime=10s

   # After changes
   go test -bench=BenchmarkUpsertPG -benchtime=10s
   ```

2. **MSSQL target upsert benchmark**:
   ```bash
   go test -bench=BenchmarkUpsertMSSQL -benchtime=10s
   ```

3. **End-to-end migration test**:
   - StackOverflow dataset (19M rows)
   - Compare throughput before/after changes
   - Target: Match Rust performance (~106K rows/s for PG, ~70K rows/s for MSSQL)

---

## Rollout Plan

1. **Feature flag** (optional):
   - Add config option `use_staging_upsert: true|false`
   - Default to `true` for new installations
   - Allow fallback to old method if issues arise

2. **Backward compatibility**:
   - Keep existing `UpsertChunk` method
   - New `UpsertChunkWithWriter` used when writer ID available
   - Graceful fallback if interface not supported

3. **Documentation**:
   - Update README with performance comparison
   - Add troubleshooting section for staging table issues

---

## Expected Outcomes

| Direction | Current | After Changes | Rust Reference |
|-----------|---------|---------------|----------------|
| MSSQL → PostgreSQL | ~75K rows/s | ~100-110K rows/s | ~106K rows/s |
| PostgreSQL → MSSQL | ~50K rows/s (est) | ~70K rows/s | ~70K rows/s |

### Memory Improvements
- **Before**: Staging table holds ALL rows for MSSQL target
- **After**: Staging table holds only 1 chunk (~10K rows)

### Reliability Improvements
- Deadlock retry logic for MSSQL
- Per-writer isolation prevents contention
- Incremental progress enables better resume
- Session-scoped temp tables auto-cleanup on crash

---

## Files to Modify

| File | Changes |
|------|---------|
| `internal/pool/interfaces.go` | Add `UpsertChunkWithWriter` interface method |
| `internal/target/pool.go` | Implement PostgreSQL staging table upsert with IS DISTINCT FROM |
| `internal/target/mssql_pool.go` | Implement per-chunk MERGE with #temp tables and deadlock retry |
| `internal/transfer/transfer.go` | Pass writerID to upsert, add helper function |
| `internal/target/pool_test.go` | Add unit tests for new functions |
| `internal/target/mssql_pool_test.go` | Add unit tests for MERGE SQL generation |

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| ~~Staging table accumulation~~ | Session-scoped temp tables auto-cleanup |
| MERGE deadlocks | WITH (TABLOCK) + retry logic with backoff |
| Temp table permission issues | Standard permissions, no special grants needed |
| Regression in existing functionality | Keep old methods, add new ones |
| Performance regression edge cases | Benchmark with various data sizes |
| Long table name overflow | Hash-based naming fallback |
| Unnecessary row versions (PG) | Keep IS DISTINCT FROM clause |

---

## Implementation Priority (per Gemini review)

1. **Phase 1 (Interface)**: Essential plumbing, quick to implement
2. **Phase 2 (PostgreSQL)**: Highest ROI - `pgx.CopyFrom` provides biggest speedup
   - Remember to keep IS DISTINCT FROM for update suppression
3. **Phase 3 (MSSQL)**: Higher complexity, but important for bidirectional parity
4. **Phase 4 (Transfer)**: Wire up the new methods
5. ~~Phase 5 (Cleanup)~~: Not needed with session-scoped temp tables

---

## Gemini Review Feedback (Incorporated)

1. **CRITICAL: Keep IS DISTINCT FROM for PostgreSQL** - Without it, PostgreSQL writes new tuples even for identical data, causing table bloat and excessive WAL writes

2. **Use session-scoped temp tables for MSSQL (#table)** - Auto-drops on connection close, handles crashes gracefully, no cleanup needed

3. **Safe naming with hash fallback** - Handles long table names by falling back to hash-based names

4. **UNLOGGED temp tables for PostgreSQL** - Reduces WAL I/O for staging data (temp tables are inherently unlogged)

5. **Verified: Go MSSQL driver supports #temp tables** - Standard functionality, no compatibility concerns
