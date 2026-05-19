package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// SampleColumnValues retrieves sample values from a column for AI type mapping context.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}

	// Validate identifiers to prevent SQL injection
	// These come from information_schema but we validate anyway for defense in depth
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	// Query distinct non-null values with LIMIT
	query := fmt.Sprintf(`
		SELECT DISTINCT %s::text AS sample_val
		FROM %s
		WHERE %s IS NOT NULL
		LIMIT $1
	`, r.dialect.QuoteIdentifier(column), r.dialect.QualifyTable(schema, table), r.dialect.QuoteIdentifier(column))

	samples, err := shared.QuerySampleColumnValues(ctx, r.sqlDB, query, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling column %s: %w", column, err)
	}
	return samples, nil
}

// SampleRows retrieves sample rows from a table for AI type mapping context.
// Returns a map of column name -> sample values (one query for all columns).
func (r *Reader) SampleRows(ctx context.Context, schema, table string, columns []string, limit int) (map[string][]string, error) {
	if limit <= 0 {
		limit = 5
	}

	// Validate identifiers
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	// Build column list with PostgreSQL text cast
	// Use COALESCE to handle NULL and potential cast failures gracefully
	// The ::text cast works for most types including geometry/geography (returns WKT)
	var quotedCols []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quotedCols = append(quotedCols, fmt.Sprintf("(%s)::text", r.dialect.QuoteIdentifier(col)))
	}

	// Query N rows with all columns
	query := fmt.Sprintf(`SELECT %s FROM %s LIMIT $1`,
		strings.Join(quotedCols, ", "),
		r.dialect.QualifyTable(schema, table))

	result, err := shared.QuerySampleRows(ctx, r.sqlDB, query, columns, limit, limit)
	if err != nil {
		return nil, fmt.Errorf("sampling rows from %s: %w", table, err)
	}
	return result, nil
}
