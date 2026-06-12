package generic

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// PostgreSQL COPY strategies (#509), extracted from the hand-written
// postgres writer. The generic engine holds one database/sql pool over
// the pgx stdlib driver; these strategies reach the native *pgx.Conn
// through sql.Conn.Raw for the COPY protocol. Adaptive sub-batch
// sizing (SO_SNDBUF probe, p90 row-size estimation) moves with them —
// pgx sends COPY data straight to the socket without its bgReader
// deadlock prevention, so per-call data must stay within safe limits.

const (
	pgFallbackCopyBytes = 3 << 20 // 3 MB floor — throughput vs TCP deadlock safety
	pgMinCopyBatchRows  = 100     // floor to avoid degenerate single-row COPY calls
	pgMaxCopyBatchRows  = 50_000  // cap to prevent oversized batches
)

// pgBulkState caches the per-writer COPY byte budget; probed once from
// the first acquired connection.
type pgBulkState struct {
	once           sync.Once
	copyBatchBytes int
}

func init() {
	bulkStrategies["copy_from"] = pgCopyFromWrite
	upsertStrategies["pg_copy_staging"] = pgCopyStagingUpsert
	sequenceStrategies["pg_setval"] = pgSetvalReset
}

// pgSetvalReset mirrors the hand-written writer: setval each identity
// column's conventional <table>_<col>_seq to MAX(col). Failures are
// debug-logged, not fatal (the sequence may not exist for non-serial
// identities).
func pgSetvalReset(ctx context.Context, db *sql.DB, dialect *Dialect, schema string, t *driver.Table) error {
	sanitizedTable := sanitizePGTableName(t.Name)
	for _, col := range t.Columns {
		if !col.IsIdentity {
			continue
		}
		sanitizedCol := sanitizePGIdentifier(col.Name)
		seqName := fmt.Sprintf("%s_%s_seq", sanitizedTable, sanitizedCol)
		query := fmt.Sprintf("SELECT setval('%s.%s', COALESCE((SELECT MAX(%s) FROM %s), 1))",
			schema, seqName, dialect.QuoteIdentifier(sanitizedCol), dialect.QualifyTable(schema, sanitizedTable))
		if _, err := db.ExecContext(ctx, query); err != nil {
			logging.Debug("Failed to reset sequence %s: %v", seqName, err)
		}
	}
	return nil
}

// withPgxConn runs fn on the native pgx connection underneath the
// database/sql pool (pgx stdlib driver).
func withPgxConn(ctx context.Context, db *sql.DB, fn func(*pgx.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Close()
	return conn.Raw(func(dc any) error {
		stdConn, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("copy_from requires the pgx stdlib backend (got %T)", dc)
		}
		return fn(stdConn.Conn())
	})
}

// pgCopyBudget returns the cached per-CopyFrom byte budget, probing the
// connection's TCP send buffer on first use (mirrors the hand-written
// writer's construction-time probe).
func pgCopyBudget(state *pgBulkState, conn *pgx.Conn) int {
	if state == nil {
		return pgFallbackCopyBytes
	}
	state.once.Do(func() {
		state.copyBatchBytes = pgFallbackCopyBytes
		netConn := conn.PgConn().Conn()
		if sndbuf, err := tcpSendBufSize(netConn); err == nil && sndbuf > 0 {
			// Half the send buffer per CopyFrom call — the same safety
			// margin the hand-written writer derived.
			budget := sndbuf / 2
			if budget < pgFallbackCopyBytes {
				budget = pgFallbackCopyBytes
			}
			state.copyBatchBytes = budget
			logging.Debug("COPY batch probe: SO_SNDBUF=%d bytes, using %d bytes per CopyFrom", sndbuf, budget)
		}
	})
	return state.copyBatchBytes
}

// pgEstimateRowBytes samples up to sampleSize rows and returns the p90
// row size for COPY batch sizing — average underestimates on
// outlier-heavy tables (posts bodies), max is too pessimistic.
func pgEstimateRowBytes(rows [][]any, sampleSize int) int {
	if len(rows) == 0 || sampleSize <= 0 {
		return 64
	}
	n := sampleSize
	if n > len(rows) {
		n = len(rows)
	}
	sizes := make([]int, n)
	for i := 0; i < n; i++ {
		idx := i * (len(rows) - 1) / max(n-1, 1)
		rowSize := 0
		for _, v := range rows[idx] {
			switch val := v.(type) {
			case string:
				rowSize += len(val)
			case []byte:
				rowSize += len(val)
			default:
				rowSize += 8
			}
		}
		sizes[i] = rowSize
	}
	sort.Ints(sizes)
	p90Idx := n * 9 / 10
	if p90Idx >= n {
		p90Idx = n - 1
	}
	if sizes[p90Idx] < 64 {
		return 64
	}
	return sizes[p90Idx]
}

func pgCopyBatchSize(rows [][]any, targetBytes int) int {
	n := targetBytes / pgEstimateRowBytes(rows, 100)
	if n < pgMinCopyBatchRows {
		return pgMinCopyBatchRows
	}
	if n > pgMaxCopyBatchRows {
		return pgMaxCopyBatchRows
	}
	return n
}

// pgCopyTimeout assumes ≥1 MB/s write throughput with a 30s floor, so
// outlier-heavy batches fail fast instead of stalling to a fixed cap.
func pgCopyTimeout(batch [][]any) time.Duration {
	const mb = 1024 * 1024
	batchBytes := pgEstimateRowBytes(batch, 100) * len(batch)
	secs := (batchBytes + mb - 1) / mb
	if secs < 30 {
		secs = 30
	}
	return time.Duration(secs) * time.Second
}

// pgCopyFromWrite is the copy_from bulk strategy: COPY protocol inside
// one transaction (a mid-batch failure or timeout rolls back cleanly,
// preventing duplicates on chunk retry). IdempotentOnDup routes to the
// staging variant.
func pgCopyFromWrite(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if opts.IdempotentOnDup {
		return pgWriteBatchIdempotent(ctx, env, opts)
	}

	return withPgxConn(ctx, env.db, func(conn *pgx.Conn) error {
		// Identifiers were lowercased at create time — match them.
		sanitizedTable := sanitizePGTableName(opts.Table)
		sanitizedCols := make([]string, len(opts.Columns))
		for i, col := range opts.Columns {
			sanitizedCols[i] = sanitizePGIdentifier(col)
		}
		ident := pgx.Identifier{opts.Schema, sanitizedTable}

		batchSize := pgCopyBatchSize(opts.Rows, pgCopyBudget(env.pgState, conn))

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		defer tx.Rollback(context.Background()) //nolint:errcheck

		for start := 0; start < len(opts.Rows); start += batchSize {
			end := start + batchSize
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]
			copyCtx, cancel := context.WithTimeout(ctx, pgCopyTimeout(subBatch))
			_, err = tx.CopyFrom(copyCtx, ident, sanitizedCols, pgx.CopyFromRows(subBatch))
			cancel()
			if err != nil {
				return fmt.Errorf("copy batch [%d:%d]: %w", start, end, err)
			}
		}
		return tx.Commit(ctx)
	})
}

// pgWriteBatchIdempotent: per-writer/per-partition temp staging + COPY
// + INSERT ... SELECT ... ON CONFLICT DO NOTHING (#227 resume replay —
// deliberately no DO UPDATE: replayed committed rows must not absorb
// mid-migration source changes). CREATE TEMP / COPY / INSERT-SELECT
// share one transaction: ON COMMIT DELETE ROWS would otherwise wipe
// staging between autocommitted statements and silently insert zero
// rows.
func pgWriteBatchIdempotent(ctx context.Context, env bulkEnv, opts driver.WriteBatchOptions) error {
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
	}

	return withPgxConn(ctx, env.db, func(conn *pgx.Conn) error {
		sanitizedTable := sanitizePGTableName(opts.Table)
		sanitizedCols := make([]string, len(opts.Columns))
		for i, col := range opts.Columns {
			sanitizedCols[i] = sanitizePGIdentifier(col)
		}
		sanitizedPK := make([]string, len(opts.PKColumns))
		for i, pk := range opts.PKColumns {
			sanitizedPK[i] = sanitizePGIdentifier(pk)
		}

		stagingKey := fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)
		if opts.PartitionID != nil {
			stagingKey = fmt.Sprintf("%s.p%d", stagingKey, *opts.PartitionID)
		}
		hash := sha256.Sum256([]byte(stagingKey))
		stagingTable := fmt.Sprintf("_stg_idem_%x", hash[:8])

		target := env.dialect.QualifyTable(opts.Schema, sanitizedTable)
		quotedStaging := env.dialect.QuoteIdentifier(stagingTable)
		colList := env.dialect.ColumnList(sanitizedCols)
		pkList := env.dialect.ColumnList(sanitizedPK)

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
			target, colList, colList, quotedStaging, pkList,
		)

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin idempotent transaction: %w", err)
		}
		defer tx.Rollback(context.Background()) //nolint:errcheck

		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DELETE ROWS",
			quotedStaging, target)); err != nil {
			return fmt.Errorf("creating idempotent staging table: %w", err)
		}
		// Defensive: a prior failed attempt on this pooled session may
		// have left rows.
		if _, err := tx.Exec(ctx, "TRUNCATE "+quotedStaging); err != nil {
			return fmt.Errorf("truncating idempotent staging table: %w", err)
		}

		batchSize := pgCopyBatchSize(opts.Rows, pgCopyBudget(env.pgState, conn))
		for start := 0; start < len(opts.Rows); start += batchSize {
			end := start + batchSize
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]
			copyCtx, cancel := context.WithTimeout(ctx, pgCopyTimeout(subBatch))
			_, err = tx.CopyFrom(copyCtx, pgx.Identifier{stagingTable}, sanitizedCols, pgx.CopyFromRows(subBatch))
			cancel()
			if err != nil {
				return fmt.Errorf("idempotent staging copy [%d:%d]: %w", start, end, err)
			}
		}

		if _, err = tx.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("idempotent insert from staging: %w", err)
		}
		return tx.Commit(ctx)
	})
}

// pgCopyStagingUpsert: temp staging + COPY + INSERT ... ON CONFLICT
// (pk) DO UPDATE — the hand-written UpsertBatch.
func pgCopyStagingUpsert(ctx context.Context, env bulkEnv, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	return withPgxConn(ctx, env.db, func(conn *pgx.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		defer tx.Rollback(context.Background()) //nolint:errcheck

		hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)))
		stagingTable := fmt.Sprintf("_stg_%x", hash[:8])

		if _, err = tx.Exec(ctx, fmt.Sprintf(
			"CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL) ON COMMIT DELETE ROWS",
			env.dialect.QuoteIdentifier(stagingTable),
			env.dialect.QualifyTable(opts.Schema, opts.Table))); err != nil {
			return fmt.Errorf("creating staging table: %w", err)
		}

		batchSize := pgCopyBatchSize(opts.Rows, pgCopyBudget(env.pgState, conn))
		for start := 0; start < len(opts.Rows); start += batchSize {
			end := start + batchSize
			if end > len(opts.Rows) {
				end = len(opts.Rows)
			}
			subBatch := opts.Rows[start:end]
			copyCtx, cancel := context.WithTimeout(ctx, pgCopyTimeout(subBatch))
			_, err = tx.CopyFrom(copyCtx, pgx.Identifier{stagingTable}, opts.Columns, pgx.CopyFromRows(subBatch))
			cancel()
			if err != nil {
				return fmt.Errorf("copying to staging [%d:%d]: %w", start, end, err)
			}
		}

		if _, err = tx.Exec(ctx, pgBuildUpsertSQL(env.dialect, opts, stagingTable)); err != nil {
			return fmt.Errorf("upserting: %w", err)
		}
		return tx.Commit(ctx)
	})
}

func pgBuildUpsertSQL(dialect *Dialect, opts driver.UpsertBatchOptions, stagingTable string) string {
	colList := dialect.ColumnList(opts.Columns)

	pkSet := make(map[string]bool)
	quotedPK := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		pkSet[strings.ToLower(pk)] = true
		quotedPK[i] = dialect.QuoteIdentifier(pk)
	}

	var updateClauses []string
	for _, col := range opts.Columns {
		if pkSet[strings.ToLower(col)] {
			continue
		}
		qCol := dialect.QuoteIdentifier(col)
		updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", qCol, qCol))
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s)",
		dialect.QualifyTable(opts.Schema, opts.Table), colList, colList,
		dialect.QuoteIdentifier(stagingTable), strings.Join(quotedPK, ", "))
	if len(updateClauses) > 0 {
		fmt.Fprintf(&sb, " DO UPDATE SET %s", strings.Join(updateClauses, ", "))
	} else {
		sb.WriteString(" DO NOTHING")
	}
	return sb.String()
}
