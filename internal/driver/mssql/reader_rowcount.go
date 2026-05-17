package mssql

import (
	"context"
	"database/sql"
	"fmt"
)

func (r *Reader) GetRowCount(ctx context.Context, schema, table string) (int64, error) {
	// Try fast stats-based count first
	count, err := r.GetRowCountFast(ctx, schema, table)
	if err == nil && count > 0 {
		return count, nil
	}

	// Fall back to COUNT(*)
	err = r.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", r.dialect.QualifyTable(schema, table))).Scan(&count)
	return count, err
}

// GetRowCountFast returns an approximate row count using system statistics.
// This is much faster than COUNT(*) for large tables.
func (r *Reader) GetRowCountFast(ctx context.Context, schema, table string) (int64, error) {
	var count int64
	query := `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)
	`
	err := r.db.QueryRowContext(ctx, query,
		sql.Named("schema", schema),
		sql.Named("table", table)).Scan(&count)
	return count, err
}

// GetRowCountExact returns the exact row count using COUNT(*).
// This may be slow on large tables. When strictConsistency is true
// the `WITH (NOLOCK)` table hint is dropped so the count is
// read-committed rather than dirty (#253). The pre-#253 behavior
// (always NOLOCK) was a silent override of the operator's
// strict_consistency setting.
func (r *Reader) GetRowCountExact(ctx context.Context, schema, table string, strictConsistency bool) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, buildExactRowCountQuery(r.dialect.QualifyTable(schema, table), strictConsistency)).Scan(&count)
	return count, err
}

// buildExactRowCountQuery is the testable string-building half of
// GetRowCountExact — split out so the NOLOCK / strict_consistency
// contract can be unit-tested without a live DB (#253).
func buildExactRowCountQuery(qualifiedTable string, strictConsistency bool) string {
	hint := " WITH (NOLOCK)"
	if strictConsistency {
		hint = ""
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", qualifiedTable, hint)
}

// GetPartitionBoundaries calculates partition boundaries using MIN/MAX.
// This uses index lookups for MIN/MAX (very fast) and divides the PK range evenly.
