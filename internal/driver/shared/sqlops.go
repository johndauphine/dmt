package shared

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLDB is the small database/sql surface shared writer and count helpers need.
type SQLDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// RowCountFunc returns a table row count using one strategy.
type RowCountFunc func() (int64, error)

// ExecRaw executes a raw SQL statement and returns its affected row count.
func ExecRaw(ctx context.Context, db SQLDB, query string, args ...any) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("exec raw: db is nil")
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// QueryRowRaw executes a raw single-row SQL query and scans into dest.
func QueryRowRaw(ctx context.Context, db SQLDB, query string, dest any, args ...any) error {
	if db == nil {
		return fmt.Errorf("query row raw: db is nil")
	}
	return db.QueryRowContext(ctx, query, args...).Scan(dest)
}

// ExactRowCountSQL returns the exact COUNT(*) query for an already-qualified
// table reference. tableSuffix is for table hints such as SQL Server NOLOCK.
func ExactRowCountSQL(qualifiedTable, tableSuffix string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", qualifiedTable, tableSuffix)
}

// ExactRowCountQuery returns the exact COUNT(*) query for a dialect-qualified
// table reference.
func ExactRowCountQuery(qualifier TableQualifier, schema, table string) (string, error) {
	return ExactRowCountQueryWithSuffix(qualifier, schema, table, "")
}

// ExactRowCountQueryWithSuffix returns the exact COUNT(*) query with an
// optional table suffix, such as SQL Server's WITH (NOLOCK) hint.
func ExactRowCountQueryWithSuffix(qualifier TableQualifier, schema, table, tableSuffix string) (string, error) {
	if qualifier == nil {
		return "", fmt.Errorf("exact row count query: qualifier is nil")
	}
	if table == "" {
		return "", fmt.Errorf("exact row count query: table is required")
	}
	return ExactRowCountSQL(qualifier.QualifyTable(schema, table), tableSuffix), nil
}

// ExactRowCount executes a COUNT(*) query using a dialect-qualified table name.
func ExactRowCount(ctx context.Context, db SQLDB, qualifier TableQualifier, schema, table string) (int64, error) {
	query, err := ExactRowCountQuery(qualifier, schema, table)
	if err != nil {
		return 0, err
	}
	return QueryExactRowCount(ctx, db, query)
}

// QueryExactRowCount executes a prepared COUNT(*) query string.
func QueryExactRowCount(ctx context.Context, db SQLDB, query string, args ...any) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("query exact row count: db is nil")
	}

	var count int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

// RowCountWithFallback runs fastCount first and falls back to exactCount when
// the fast strategy errors or returns zero.
func RowCountWithFallback(fastCount, exactCount RowCountFunc) (int64, error) {
	if exactCount == nil {
		return 0, fmt.Errorf("row count fallback: exact count is nil")
	}
	if fastCount == nil {
		return exactCount()
	}

	count, err := fastCount()
	if err == nil && count > 0 {
		return count, nil
	}
	return exactCount()
}
