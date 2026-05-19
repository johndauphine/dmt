package mysql

import (
	"context"

	"github.com/johndauphine/dmt/internal/driver/shared"
)

// GetRowCount returns the row count for a table.
func (w *Writer) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	return shared.RowCountWithFallback(
		func() (int64, error) { return w.GetRowCountFast(ctx, schema, table) },
		func() (int64, error) { return w.GetRowCountExact(ctx, schema, table, false) },
	)
}

// GetRowCountFast returns an approximate row count using system statistics.
func (w *Writer) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	dbName := schema
	if dbName == "" {
		dbName = w.config.Database
	}

	var count int64
	err := w.db.QueryRowContext(ctx, `
		SELECT TABLE_ROWS FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, dbName, table).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// MySQL has no NOLOCK equivalent (uses MVCC); strictConsistency is
// accepted for interface symmetry and ignored here.
func (w *Writer) GetRowCountExact(ctx context.Context, schema, table string, _ bool) (int64, error) {
	return shared.ExactRowCount(ctx, w.db, w.dialect, schema, table)
}
