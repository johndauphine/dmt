package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmt/internal/driver"
)

// UpsertBatch performs an upsert using staging table + INSERT ON CONFLICT.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// Create staging table name (unique per writer)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s.%s.%d", opts.Schema, opts.Table, opts.WriterID)))
	stagingTable := fmt.Sprintf("_stg_%x", hash[:8])

	// Create temp table. COPY and INSERT must run in the same transaction
	// because the staging table uses ON COMMIT DELETE ROWS.
	_, err = tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL) ON COMMIT DELETE ROWS",
		w.dialect.QuoteIdentifier(stagingTable),
		w.dialect.QualifyTable(opts.Schema, opts.Table)))
	if err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	// Adaptive sub-batching for staging COPY
	batchSize := copyBatchSize(opts.Rows, w.copyBatchBytes)
	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		subBatch := opts.Rows[start:end]
		const upsertMB = 1024 * 1024
		upsertBatchBytes := estimateRowBytes(subBatch, 100) * len(subBatch)
		upsertTimeoutSecs := (upsertBatchBytes + upsertMB - 1) / upsertMB
		if upsertTimeoutSecs < 30 {
			upsertTimeoutSecs = 30
		}
		copyCtx, cancel := context.WithTimeout(ctx, time.Duration(upsertTimeoutSecs)*time.Second)
		_, err = tx.CopyFrom(
			copyCtx,
			pgx.Identifier{stagingTable},
			opts.Columns,
			pgx.CopyFromRows(subBatch),
		)
		cancel()
		if err != nil {
			return fmt.Errorf("copying to staging [%d:%d]: %w", start, end, err)
		}
	}

	// Build INSERT ... ON CONFLICT
	upsertSQL := w.buildUpsertSQL(opts, stagingTable)

	_, err = tx.Exec(ctx, upsertSQL)
	if err != nil {
		return fmt.Errorf("upserting: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (w *Writer) buildUpsertSQL(opts driver.UpsertBatchOptions, stagingTable string) string {
	var sb strings.Builder

	// Column lists
	quotedCols := make([]string, len(opts.Columns))
	for i, c := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(c)
	}
	colList := strings.Join(quotedCols, ", ")

	// PK columns for conflict
	quotedPK := make([]string, len(opts.PKColumns))
	for i, c := range opts.PKColumns {
		quotedPK[i] = w.dialect.QuoteIdentifier(c)
	}
	pkList := strings.Join(quotedPK, ", ")

	// Build UPDATE SET clause with IS DISTINCT FROM change detection
	var setClauses []string
	var distinctClauses []string
	for i, col := range opts.Columns {
		isPK := false
		for _, pk := range opts.PKColumns {
			if col == pk {
				isPK = true
				break
			}
		}
		if !isPK {
			qCol := w.dialect.QuoteIdentifier(col)
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", qCol, qCol))

			// Skip spatial columns from change detection if needed
			colType := ""
			if i < len(opts.ColumnTypes) {
				colType = strings.ToLower(opts.ColumnTypes[i])
			}
			if colType != "geography" && colType != "geometry" {
				distinctClauses = append(distinctClauses, fmt.Sprintf("%s.%s", opts.Table, qCol))
			}
		}
	}

	sb.WriteString("INSERT INTO ")
	sb.WriteString(w.dialect.QualifyTable(opts.Schema, opts.Table))
	sb.WriteString(" (")
	sb.WriteString(colList)
	sb.WriteString(") SELECT ")
	sb.WriteString(colList)
	sb.WriteString(" FROM ")
	sb.WriteString(w.dialect.QuoteIdentifier(stagingTable))
	sb.WriteString(" ON CONFLICT (")
	sb.WriteString(pkList)
	sb.WriteString(") DO UPDATE SET ")
	sb.WriteString(strings.Join(setClauses, ", "))

	// Add IS DISTINCT FROM clause for change detection
	if len(distinctClauses) > 0 {
		sb.WriteString(" WHERE (")
		sb.WriteString(strings.Join(distinctClauses, ", "))
		sb.WriteString(") IS DISTINCT FROM (")

		excludedClauses := make([]string, len(distinctClauses))
		for i, dc := range distinctClauses {
			// Replace table prefix with EXCLUDED
			excludedClauses[i] = strings.Replace(dc, opts.Table+".", "EXCLUDED.", 1)
		}
		sb.WriteString(strings.Join(excludedClauses, ", "))
		sb.WriteString(")")
	}

	return sb.String()
}
