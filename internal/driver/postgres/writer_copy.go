package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// Adaptive COPY sub-batch sizing. Each CopyFrom call is capped at
// copyBatchBytes so that pgx CopyFrom never saturates the TCP buffers and
// deadlocks. pgx sends COPY data directly to the socket without its bgReader
// deadlock-prevention mechanism, so we must keep per-call data within safe
// limits.
//
// Narrow-row tables (e.g. Votes at ~6 bytes/row) get large batches while
// wide-row tables (e.g. Posts at ~10KB/row) get small ones.
const (
	fallbackCopyBytes = 3 << 20 // 3 MB floor - balances throughput vs TCP deadlock safety
	minCopyBatchRows  = 100     // floor to avoid degenerate single-row COPY calls
	maxCopyBatchRows  = 50_000  // cap to prevent oversized batches
)

// estimateRowBytes samples up to sampleSize rows and returns a conservative
// estimate of the row size in bytes for COPY batch sizing. For tables with
// high variance (e.g., posts with Body ranging from 0 to 53KB), using the
// average underestimates batch sizes and causes timeouts. Instead, we use
// the p90 row size from the sample to handle outlier-heavy distributions
// while avoiding worst-case degenerate sizing from a single max row.
// Fixed-width types (numbers, bools) count as 8 bytes; strings and byte
// slices use their actual length. Returns at least 64.
func estimateRowBytes(rows [][]any, sampleSize int) int {
	if len(rows) == 0 || sampleSize <= 0 {
		return 64
	}
	n := sampleSize
	if n > len(rows) {
		n = len(rows)
	}

	// Spread samples proportionally across the batch to avoid sampling bias
	// from clustered large/small rows at the start or tail.
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

	// Use p90: sort and pick the 90th percentile value.
	// This handles outlier-heavy distributions (posts, comments) without
	// being as pessimistic as max (which could be a single 53KB row).
	sort.Ints(sizes)
	p90Idx := n * 9 / 10
	if p90Idx >= n {
		p90Idx = n - 1
	}
	estimate := sizes[p90Idx]
	if estimate < 64 {
		return 64
	}
	return estimate
}

// probeCopyBatchBytes acquires a connection, reads the TCP send buffer size
// from the underlying socket, and returns a safe per-CopyFrom byte limit.
// Falls back to fallbackCopyBytes on error.
func probeCopyBatchBytes(pool *pgxpool.Pool) int {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		logging.Debug("COPY batch probe: acquire failed: %v, using fallback %d bytes", err, fallbackCopyBytes)
		return fallbackCopyBytes
	}
	defer conn.Release()

	netConn := conn.Conn().PgConn().Conn()
	sndbuf, err := tcpSendBufSize(netConn)
	if err != nil || sndbuf <= 0 {
		logging.Debug("COPY batch probe: could not read SO_SNDBUF: %v, using fallback %d bytes", err, fallbackCopyBytes)
		return fallbackCopyBytes
	}

	// Scale batch size relative to TCP buffer. The actual TCP window (with
	// autotuning) is larger than SO_SNDBUF. Use 4x as a safe multiplier,
	// with a 3MB floor to maintain throughput on systems with small buffers
	// (macOS SO_SNDBUF ~146KB -> 4x = 584KB would be too small).
	batchBytes := sndbuf * 4
	if batchBytes < fallbackCopyBytes {
		batchBytes = fallbackCopyBytes
	}

	logging.Debug("COPY batch probe: SO_SNDBUF=%d bytes, using %d bytes per CopyFrom", sndbuf, batchBytes)
	return batchBytes
}

// copyBatchSize returns the number of rows to send in a single CopyFrom call,
// targeting targetBytes per operation and clamped to [minCopyBatchRows, maxCopyBatchRows].
func copyBatchSize(rows [][]any, targetBytes int) int {
	rowBytes := estimateRowBytes(rows, 100)
	n := targetBytes / rowBytes
	if n < minCopyBatchRows {
		return minCopyBatchRows
	}
	if n > maxCopyBatchRows {
		return maxCopyBatchRows
	}
	return n
}

// WriteBatch writes a batch of rows using COPY protocol.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	// Resume-safe path for ROW_NUMBER-paged tables: skip rows whose PK already
	// exists in the target instead of failing on duplicate (#227).
	if opts.IdempotentOnDup {
		return w.writeBatchIdempotent(ctx, opts)
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Sanitize table and column names to match how they were created (lowercase)
	sanitizedTable := sanitizePGTableName(opts.Table)
	sanitizedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		sanitizedCols[i] = sanitizePGIdentifier(col)
	}

	ident := pgx.Identifier{opts.Schema, sanitizedTable}

	// All CopyFrom calls run inside a transaction so that a timeout or
	// mid-batch failure rolls back cleanly. This prevents duplicate rows
	// when the caller retries the same chunk after a context deadline.
	// Adaptive sub-batching caps each CopyFrom at copyBatchBytes (derived
	// from TCP send buffer) to prevent pgx TCP buffer deadlocks.
	batchSize := copyBatchSize(opts.Rows, w.copyBatchBytes)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}

		subBatch := opts.Rows[start:end]
		batchBytes := estimateRowBytes(subBatch, 100) * len(subBatch)
		// Timeout: assume minimum 1 MB/s write throughput, with a 30s floor.
		// A 3MB batch gets 30s; a 60MB batch gets 60s. Prevents 5-minute
		// silent stalls from outlier-heavy batches that complete just under
		// a fixed timeout.
		const mb = 1024 * 1024
		timeoutSecs := (batchBytes + mb - 1) / mb
		if timeoutSecs < 30 {
			timeoutSecs = 30
		}
		copyCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		_, err = tx.CopyFrom(
			copyCtx,
			ident,
			sanitizedCols,
			pgx.CopyFromRows(subBatch),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("copy batch [%d:%d]: %w", start, end, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// writeBatchIdempotent implements the IdempotentOnDup path for WriteBatch
// (#227). Rows are COPY'd into a per-writer temp staging table and then moved
// to the target via INSERT ... SELECT ... ON CONFLICT (<pk>) DO NOTHING.
// Already-present rows are a silent no-op; this preserves resume safety for
// ROW_NUMBER-paged tables without overwriting (no DO UPDATE branch - that
// would be wrong on replay if the source value changed mid-migration).
//
// The CREATE TEMP / COPY / INSERT-SELECT must run in a single transaction.
// Without it, autocommit at the end of each CopyFrom would fire the temp
// table's ON COMMIT DELETE ROWS clause, wiping the staging rows before the
// INSERT-SELECT runs - the writer would silently insert zero rows, advance
// the checkpoint, and corrupt the resume (codex review).
func (w *Writer) writeBatchIdempotent(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	// Sanitize identifiers consistently with the create-table path.
	sanitizedTable := sanitizePGTableName(opts.Table)
	sanitizedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		sanitizedCols[i] = sanitizePGIdentifier(col)
	}
	sanitizedPK := make([]string, len(opts.PKColumns))
	for i, pk := range opts.PKColumns {
		sanitizedPK[i] = sanitizePGIdentifier(pk)
	}

	// Per-writer + per-partition staging name so concurrent writers /
	// partitions on the same connection pool don't collide. Hash keeps
	// the identifier under PostgreSQL's 63-char limit even for long
	// schema.table names.
	stagingKey := fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)
	if opts.PartitionID != nil {
		stagingKey = fmt.Sprintf("%s.p%d", stagingKey, *opts.PartitionID)
	}
	hash := sha256.Sum256([]byte(stagingKey))
	stagingTable := fmt.Sprintf("_stg_idem_%x", hash[:8])

	target := w.dialect.QualifyTable(opts.Schema, sanitizedTable)
	quotedStaging := w.dialect.QuoteIdentifier(stagingTable)

	quotedCols := make([]string, len(sanitizedCols))
	for i, c := range sanitizedCols {
		quotedCols[i] = w.dialect.QuoteIdentifier(c)
	}
	colList := strings.Join(quotedCols, ", ")

	quotedPK := make([]string, len(sanitizedPK))
	for i, p := range sanitizedPK {
		quotedPK[i] = w.dialect.QuoteIdentifier(p)
	}
	pkList := strings.Join(quotedPK, ", ")

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
		target, colList, colList, quotedStaging, pkList,
	)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin idempotent transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// CREATE the temp table inside our transaction. ON COMMIT DELETE ROWS
	// fires at THIS transaction's commit (after the INSERT-SELECT below).
	// IF NOT EXISTS is idempotent if a prior call left the table around
	// from a previous resumed-batch call on the same pooled connection.
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DELETE ROWS",
		quotedStaging, target)); err != nil {
		return fmt.Errorf("creating idempotent staging table: %w", err)
	}

	// TRUNCATE handles the case where a prior IF-NOT-EXISTS hit didn't
	// trigger create and the connection's session still has rows from a
	// failed prior attempt (rollback would normally clear them, but be
	// defensive - staging must be empty before COPY).
	if _, err := tx.Exec(ctx, "TRUNCATE "+quotedStaging); err != nil {
		return fmt.Errorf("truncating idempotent staging table: %w", err)
	}

	// Adaptive sub-batching mirrors WriteBatch/UpsertBatch so we don't blow
	// past TCP send buffers on wide rows.
	batchSize := copyBatchSize(opts.Rows, w.copyBatchBytes)
	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		subBatch := opts.Rows[start:end]

		const mb = 1024 * 1024
		batchBytes := estimateRowBytes(subBatch, 100) * len(subBatch)
		timeoutSecs := (batchBytes + mb - 1) / mb
		if timeoutSecs < 30 {
			timeoutSecs = 30
		}
		copyCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		_, err = tx.CopyFrom(
			copyCtx,
			pgx.Identifier{stagingTable},
			sanitizedCols,
			pgx.CopyFromRows(subBatch),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("idempotent staging copy [%d:%d]: %w", start, end, err)
		}
	}

	// INSERT ... SELECT ... ON CONFLICT DO NOTHING. The conflict target lists
	// the PK columns; PG matches any unique constraint that covers exactly
	// those columns, so this works whether the table has a real PRIMARY KEY
	// or just a unique index (CreatePrimaryKey is idempotent - see orchestrator
	// resume preflight - so the PK should exist by this point).
	if _, err = tx.Exec(ctx, insertSQL); err != nil {
		return fmt.Errorf("idempotent insert from staging: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit idempotent transaction: %w", err)
	}
	return nil
}
