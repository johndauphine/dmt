package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

func (r *Reader) GetDateColumnInfo(ctx context.Context, schema, table string, candidates []string) (columnName, dataType string, found bool) {
	validTypes := r.dialect.ValidDateTypes()

	for _, candidate := range candidates {
		var colType string
		err := r.db.QueryRowContext(ctx, r.dialect.DateColumnQuery(),
			sql.Named("schema", schema),
			sql.Named("table", table),
			sql.Named("column", candidate)).Scan(&colType)

		if err == nil && validTypes[colType] {
			return candidate, colType, true
		}
	}

	return "", "", false
}

// GetMaxDateColumnValue returns the current source-side high watermark for a
// date column. A nil value means the table has no non-NULL values in that
// column yet.
func (r *Reader) GetMaxDateColumnValue(ctx context.Context, schema, table, column string) (*time.Time, error) {
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	// SQL Server datetime stores values on 3.33ms ticks. go-mssqldb scans
	// MAX(datetime) rounded to milliseconds, which can make an equal source
	// row compare greater than the persisted watermark on the next run. Cast
	// to datetime2(7) so the scanned high watermark keeps SQL Server's full
	// comparable precision.
	query := fmt.Sprintf(
		"SELECT CONVERT(datetime2(7), MAX(%s)) FROM %s",
		r.dialect.QuoteIdentifier(column),
		r.dialect.QualifyTable(schema, table),
	)

	var raw any
	if err := r.db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("querying max %s: %w", column, err)
	}
	return driver.ParseDateValue(raw)
}

// SampleColumnValues retrieves sample values from a column for AI type mapping context.
func (r *Reader) SampleColumnValues(ctx context.Context, schema, table, column string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}

	// Validate identifiers to prevent SQL injection
	// These come from INFORMATION_SCHEMA but we validate anyway for defense in depth
	if err := driver.ValidateIdentifier(schema); err != nil {
		return nil, fmt.Errorf("invalid schema name: %w", err)
	}
	if err := driver.ValidateIdentifier(table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := driver.ValidateIdentifier(column); err != nil {
		return nil, fmt.Errorf("invalid column name: %w", err)
	}

	// Query distinct non-null values with TOP
	query := fmt.Sprintf(`
		SELECT DISTINCT TOP (@limit) CAST(%s AS NVARCHAR(MAX)) AS sample_val
		FROM %s
		WHERE %s IS NOT NULL
	`, r.dialect.QuoteIdentifier(column), r.dialect.QualifyTable(schema, table), r.dialect.QuoteIdentifier(column))

	samples, err := shared.QuerySampleColumnValues(ctx, r.db, query, sql.Named("limit", limit))
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

	// Build column list with MSSQL text conversion
	// Use TRY_CONVERT which returns NULL instead of failing for unconvertible types
	// This handles geography/geometry columns gracefully (returns NULL, query doesn't fail)
	var quotedCols []string
	for _, col := range columns {
		if err := driver.ValidateIdentifier(col); err != nil {
			return nil, fmt.Errorf("invalid column name %s: %w", col, err)
		}
		quotedCol := r.dialect.QuoteIdentifier(col)
		quotedCols = append(quotedCols, fmt.Sprintf("TRY_CONVERT(NVARCHAR(MAX), %s)", quotedCol))
	}

	// Query TOP N rows with all columns
	query := fmt.Sprintf(`SELECT TOP (@limit) %s FROM %s`,
		strings.Join(quotedCols, ", "),
		r.dialect.QualifyTable(schema, table))

	result, err := shared.QuerySampleRows(ctx, r.db, query, columns, limit, sql.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("sampling rows from %s: %w", table, err)
	}
	return result, nil
}
