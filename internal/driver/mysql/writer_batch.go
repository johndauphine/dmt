package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// WriteBatch writes a batch of rows using multi-row INSERT.
func (w *Writer) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	// Build column list
	quotedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	fullTableName := w.dialect.QualifyTable(opts.Schema, opts.Table)

	// Resume-safe path for ROW_NUMBER-paged tables: append
	// ON DUPLICATE KEY UPDATE <pk> = <pk> so replayed rows that already
	// exist become no-ops (#227). Deliberately NOT INSERT IGNORE — that
	// also silently masks data-conversion errors, which we don't want.
	var dupSuffix string
	if opts.IdempotentOnDup {
		if len(opts.PKColumns) == 0 {
			return fmt.Errorf("IdempotentOnDup requires PKColumns to be set")
		}
		quotedPK := w.dialect.QuoteIdentifier(opts.PKColumns[0])
		dupSuffix = " ON DUPLICATE KEY UPDATE " + quotedPK + " = " + quotedPK
	}

	// Process in batches to avoid max_allowed_packet limits and placeholder limits.
	// Per-call BatchSize (from AI tuner) takes priority over the writer's default.
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // Fallback default
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.insertBatch(ctx, fullTableName, colList, opts.Columns, batch, dupSuffix); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) insertBatch(ctx context.Context, tableName, colList string, columns []string, rows [][]any, dupSuffix string) error {
	if len(rows) == 0 {
		return nil
	}

	query := buildInsertSQL(tableName, colList, len(columns), len(rows), dupSuffix)

	// Flatten all values
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, convertRowValues(row)...)
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

// buildInsertSQL returns a multi-row INSERT statement, optionally followed by
// dupSuffix (e.g. " ON DUPLICATE KEY UPDATE id = id" for the #227
// IdempotentOnDup path). Extracted so the resume-safe shape is unit-testable
// without a real MySQL connection.
func buildInsertSQL(tableName, colList string, numCols, numRows int, dupSuffix string) string {
	placeholders := make([]string, numCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	rowPlaceholders := make([]string, numRows)
	for i := range rowPlaceholders {
		rowPlaceholders[i] = rowPlaceholder
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s%s",
		tableName, colList, strings.Join(rowPlaceholders, ", "), dupSuffix)
}

// UpsertBatch performs upsert using INSERT ... ON DUPLICATE KEY UPDATE.
func (w *Writer) UpsertBatch(ctx context.Context, opts driver.UpsertBatchOptions) error {
	if len(opts.Rows) == 0 {
		return nil
	}

	if len(opts.PKColumns) == 0 {
		return fmt.Errorf("upsert requires primary key columns")
	}

	// Build column list
	quotedCols := make([]string, len(opts.Columns))
	for i, col := range opts.Columns {
		quotedCols[i] = w.dialect.QuoteIdentifier(col)
	}
	colList := strings.Join(quotedCols, ", ")

	fullTableName := w.dialect.QualifyTable(opts.Schema, opts.Table)

	// Build UPDATE clause for non-PK columns
	pkSet := make(map[string]bool)
	for _, pk := range opts.PKColumns {
		pkSet[strings.ToLower(pk)] = true
	}

	var updateClauses []string
	for _, col := range opts.Columns {
		if !pkSet[strings.ToLower(col)] {
			qCol := w.dialect.QuoteIdentifier(col)
			// Use new.col syntax (MySQL 8.0.19+) instead of deprecated VALUES(col)
			updateClauses = append(updateClauses, fmt.Sprintf("%s = new.%s", qCol, qCol))
		}
	}

	updateClause := ""
	if len(updateClauses) > 0 {
		updateClause = " ON DUPLICATE KEY UPDATE " + strings.Join(updateClauses, ", ")
	}

	// Process in batches.
	// Per-call BatchSize (from AI tuner) takes priority over the writer's default.
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = w.defaultBatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	for start := 0; start < len(opts.Rows); start += batchSize {
		end := start + batchSize
		if end > len(opts.Rows) {
			end = len(opts.Rows)
		}
		batch := opts.Rows[start:end]

		if err := w.upsertBatch(ctx, fullTableName, colList, opts.Columns, batch, updateClause); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) upsertBatch(ctx context.Context, tableName, colList string, columns []string, rows [][]any, updateClause string) error {
	if len(rows) == 0 {
		return nil
	}

	// Build placeholder row
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}
	rowPlaceholder := "(" + strings.Join(placeholders, ", ") + ")"

	// Build all row placeholders
	rowPlaceholders := make([]string, len(rows))
	for i := range rows {
		rowPlaceholders[i] = rowPlaceholder
	}

	// Use AS new alias (MySQL 8.0.19+) for the new row reference in ON DUPLICATE KEY UPDATE
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new%s",
		tableName, colList, strings.Join(rowPlaceholders, ", "), updateClause)

	// Flatten all values
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, convertRowValues(row)...)
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}
