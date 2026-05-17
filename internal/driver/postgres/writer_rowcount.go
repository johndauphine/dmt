package postgres

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// HasPrimaryKey checks if a table has a primary key.
func (w *Writer) HasPrimaryKey(ctx context.Context, schema, table string) (bool, error) {
	sanitizedTable := sanitizePGTableName(table)
	var exists bool
	err := w.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
		)
	`, schema, sanitizedTable).Scan(&exists)
	return exists, err
}

// GetRowCount returns the row count for a table.
// It first tries a fast statistics-based count, then falls back to COUNT(*) if needed.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := w.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	sanitizedTable := sanitizePGTableName(table)
	err = w.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	err := w.pool.QueryRow(ctx,
		`SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`,
		schema, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// Postgres has no NOLOCK equivalent (uses MVCC); strictConsistency
// is accepted for interface symmetry and ignored here.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	sanitizedTable := sanitizePGTableName(table)
	var count int64
	err := w.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", w.dialect.QualifyTable(schema, sanitizedTable))).Scan(&count)
	return count, err
}

// ResetSequence resets the sequence for an identity column.
func (w *Writer) ResetSequence(ctx context.Context, schema string, t *driver.Table) error {
	sanitizedTable := sanitizePGTableName(t.Name)
	for _, col := range t.Columns {
		if col.IsIdentity {
			// Find the sequence name (uses sanitized names)
			sanitizedCol := sanitizePGIdentifier(col.Name)
			seqName := fmt.Sprintf("%s_%s_seq", sanitizedTable, sanitizedCol)
			query := fmt.Sprintf("SELECT setval('%s.%s', COALESCE((SELECT MAX(%s) FROM %s), 1))",
				schema, seqName, w.dialect.QuoteIdentifier(sanitizedCol), w.dialect.QualifyTable(schema, sanitizedTable))
			if _, err := w.pool.Exec(ctx, query); err != nil {
				logging.Debug("Failed to reset sequence %s: %v", seqName, err)
			}
		}
	}
	return nil
}
