package mssql

import (
	"context"

	"github.com/johndauphine/dmt/internal/driver/shared"
)

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	return shared.ExecRaw(ctx, w.db, query, args...)
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use sql.Named parameters for SQL Server.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return shared.QueryRowRaw(ctx, w.db, query, dest, args...)
}
