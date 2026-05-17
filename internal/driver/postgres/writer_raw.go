package postgres

import "context"

// ExecRaw executes a raw SQL query and returns the number of rows affected.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) ExecRaw(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := w.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// QueryRowRaw executes a raw SQL query that returns a single row.
// The query should use $1, $2, etc. for parameter placeholders.
func (w *Writer) QueryRowRaw(ctx context.Context, query string, dest any, args ...any) error {
	return w.pool.QueryRow(ctx, query, args...).Scan(dest)
}
