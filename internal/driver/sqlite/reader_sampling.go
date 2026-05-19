package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// GetDateColumnInfo finds a date-like column among candidates.
func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	for _, col := range candidates {
		var dt sql.NullString
		err := r.db.QueryRowContext(ctx,
			`SELECT type FROM pragma_table_info(?) WHERE name = ?`,
			table, col).Scan(&dt)
		if err != nil || !dt.Valid {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(dt.String))
		if r.dialect.ValidDateTypes()[t] {
			return col, t, true
		}
	}
	return "", "", false
}

// GetMaxDateColumnValue returns the current source-side high watermark for a
// date column. A nil value means the table has no non-NULL values in that
// column yet.
func (r *Reader) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
	)

	var raw any
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}

// SampleColumnValues retrieves sample values from a column.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	query := fmt.Sprintf(
		`SELECT DISTINCT CAST(%s AS TEXT) FROM %s WHERE %s IS NOT NULL LIMIT ?`,
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
		r.dialect.QuoteIdentifier(column),
	)

	samples, err := shared.QuerySampleColumnValues(ctx, r.db, query, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling column %s: %w", column, err)
	}
	return samples, nil
}

// SampleRows retrieves sample rows from a table.
func (r *Reader) SampleRows(ctx context.Context, schema, table string, columns []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 5
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	var quoted []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quoted = append(quoted, fmt.Sprintf("CAST(%s AS TEXT)", r.dialect.QuoteIdentifier(col)))
	}
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT ?",
		strings.Join(quoted, ", "),
		r.dialect.QualifyTable(schema, table))
	return shared.QuerySampleRows(ctx, r.db, query, columns, limit, limit)
}
