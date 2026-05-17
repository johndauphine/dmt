package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// GetDateColumnInfo returns information about a date column for incremental sync.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt string
		err := r.db.QueryRowContext(ctx,
			`SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
			schema, table, col).Scan(&dt)

		if err == nil {
			validTypes := r.dialect.ValidDateTypes()
			if validTypes[strings.ToLower(dt)] {
				return col, dt, true
			}
		}
	}
	return "", "", false
}

// SampleColumnValues retrieves sample values from a column for AI type mapping context.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}

	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT CAST(%s AS CHAR) AS sample_val
		FROM %s
		WHERE %s IS NOT NULL
		LIMIT ?
	`, r.dialect.QuoteIdentifier(column), r.dialect.QualifyTable(schema, table), r.dialect.QuoteIdentifier(column))

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling column %s: %w", column, err)
	}
	defer rows.Close()

	var samples []string
	for rows.Next() {
		var val sql.NullString
		if err := rows.Scan(&val); err != nil {
			return nil, fmt.Errorf("scanning sample value: %w", err)
		}
		if val.Valid {
			samples = append(samples, val.String)
		}
	}

	return samples, rows.Err()
}

// SampleRows retrieves sample rows from a table for AI type mapping context.
func (r *Reader) SampleRows(ctx context.Context, schema, table string, columns []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 5
	}

	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	var quotedCols []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quotedCols = append(quotedCols, fmt.Sprintf("CAST(%s AS CHAR)", r.dialect.QuoteIdentifier(col)))
	}

	query := fmt.Sprintf(`SELECT %s FROM %s LIMIT ?`,
		strings.Join(quotedCols, ", "),
		r.dialect.QualifyTable(schema, table))

	result, err := driver.SampleRowsHelper(ctx, r.db, query, columns, limit, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling rows from %s: %w", table, err)
	}
	return result, nil
}
