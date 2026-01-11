// Package dialect provides database-specific SQL generation using the Adapter pattern.
// This consolidates all identifier quoting, DSN building, and query syntax into a single
// source of truth, eliminating duplication across source, target, and transfer packages.
package dialect

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Dialect defines the interface for database-specific SQL generation (Adapter Pattern).
// Implementations adapt database-specific syntax to a common interface.
type Dialect interface {
	// DBType returns the database type identifier ("postgres" or "mssql")
	DBType() string

	// Identifier quoting
	QuoteIdentifier(name string) string
	QualifyTable(schema, table string) string

	// DSN building
	BuildDSN(host string, port int, database, user, password string, opts map[string]any) string

	// Query syntax
	ParameterPlaceholder(index int) string
	ColumnList(cols []string) string
	TableHint(strictConsistency bool) string

	// Pagination queries
	BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *DateFilter) string
	BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *DateFilter) []any
	BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string
	BuildRowNumberArgs(rowNum int64, limit int) []any

	// Spatial column handling for cross-engine migrations
	ColumnListForSelect(cols, colTypes []string, targetDBType string) string

	// Schema queries (return query string for schema introspection)
	PartitionBoundariesQuery(pkCol string, numPartitions int) string
	RowCountQuery(useStats bool) string
	DateColumnQuery() string
	ValidDateTypes() map[string]bool
}

// DateFilter specifies an incremental sync filter
type DateFilter struct {
	Column    string
	Timestamp time.Time
}

// GetDialect returns the appropriate dialect for the given database type (Factory Method).
func GetDialect(dbType string) Dialect {
	switch strings.ToLower(dbType) {
	case "postgres", "postgresql":
		return &PostgresDialect{}
	default:
		return &MSSQLDialect{}
	}
}

// ---------------------------------------------------------------------------
// PostgreSQL Dialect
// ---------------------------------------------------------------------------

// PostgresDialect implements Dialect for PostgreSQL databases.
type PostgresDialect struct{}

func (d *PostgresDialect) DBType() string { return "postgres" }

func (d *PostgresDialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *PostgresDialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *PostgresDialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// URL-encode credentials for safety
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)
	encodedDatabase := url.QueryEscape(database)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		encodedUser, encodedPassword, host, port, encodedDatabase)

	// Add query parameters
	params := url.Values{}
	if sslMode, ok := opts["sslmode"].(string); ok && sslMode != "" {
		params.Set("sslmode", sslMode)
	} else {
		params.Set("sslmode", "prefer")
	}

	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}
	return dsn
}

func (d *PostgresDialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

func (d *PostgresDialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *PostgresDialect) TableHint(_ bool) string {
	return "" // PostgreSQL doesn't use table hints
}

func (d *PostgresDialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "postgres"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// Convert spatial types for cross-engine migrations
		if isCrossEngine && (colType == "geography" || colType == "geometry") {
			// PostGIS → WKT for SQL Server
			quoted[i] = fmt.Sprintf("ST_AsText(%s) AS %s", d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *PostgresDialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, dateFilter *DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		paramNum := 3
		if hasMaxPK {
			paramNum = 4
		}
		dateClause = fmt.Sprintf(" AND (%s > $%d OR %s IS NULL)",
			d.QuoteIdentifier(dateFilter.Column), paramNum, d.QuoteIdentifier(dateFilter.Column))
	}

	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s > $1 AND %s <= $2%s
			ORDER BY %s
			LIMIT $3
		`, cols, d.QualifyTable(schema, table),
			d.QuoteIdentifier(pkCol), d.QuoteIdentifier(pkCol), dateClause, d.QuoteIdentifier(pkCol))
	}
	return fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE %s > $1%s
		ORDER BY %s
		LIMIT $2
	`, cols, d.QualifyTable(schema, table),
		d.QuoteIdentifier(pkCol), dateClause, d.QuoteIdentifier(pkCol))
}

func (d *PostgresDialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *DateFilter) []any {
	if hasMaxPK {
		if dateFilter != nil {
			return []any{lastPK, maxPK, limit, dateFilter.Timestamp}
		}
		return []any{lastPK, maxPK, limit}
	}
	if dateFilter != nil {
		return []any{lastPK, limit, dateFilter.Timestamp}
	}
	return []any{lastPK, limit}
}

func (d *PostgresDialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string) string {
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s
		)
		SELECT %s FROM numbered
		WHERE __rn > $1 AND __rn <= $2
		ORDER BY __rn
	`, cols, orderBy, d.QualifyTable(schema, table), cols)
}

func (d *PostgresDialect) BuildRowNumberArgs(rowNum int64, limit int) []any {
	return []any{rowNum, rowNum + int64(limit)}
}

func (d *PostgresDialect) PartitionBoundariesQuery(pkCol string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id FROM %%s
		)
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM numbered
		GROUP BY partition_id ORDER BY partition_id
	`, qPK, numPartitions, qPK, qPK, qPK)
}

func (d *PostgresDialect) RowCountQuery(useStats bool) string {
	if useStats {
		return `SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`
	}
	return `SELECT COUNT(*) FROM %s` // Caller must format with table name
}

func (d *PostgresDialect) DateColumnQuery() string {
	return `SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`
}

func (d *PostgresDialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"timestamp without time zone": true,
		"timestamp with time zone":    true,
		"timestamptz":                 true,
		"timestamp":                   true,
		"date":                        true,
	}
}

// ---------------------------------------------------------------------------
// SQL Server Dialect
// ---------------------------------------------------------------------------

// MSSQLDialect implements Dialect for Microsoft SQL Server databases.
type MSSQLDialect struct{}

func (d *MSSQLDialect) DBType() string { return "mssql" }

func (d *MSSQLDialect) QuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (d *MSSQLDialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *MSSQLDialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// URL-encode credentials for safety
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)
	encodedDatabase := url.QueryEscape(database)

	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
		encodedUser, encodedPassword, host, port, encodedDatabase)

	// Add optional parameters
	if encrypt, ok := opts["encrypt"].(bool); ok {
		if encrypt {
			dsn += "&encrypt=true"
		} else {
			dsn += "&encrypt=false"
		}
	}
	if trustCert, ok := opts["trustServerCertificate"].(bool); ok && trustCert {
		dsn += "&TrustServerCertificate=true"
	}
	if packetSize, ok := opts["packetSize"].(int); ok && packetSize > 0 {
		dsn += fmt.Sprintf("&packet+size=%d", packetSize)
	}

	return dsn
}

func (d *MSSQLDialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

func (d *MSSQLDialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *MSSQLDialect) TableHint(strictConsistency bool) string {
	if strictConsistency {
		return ""
	}
	return "WITH (NOLOCK)"
}

func (d *MSSQLDialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "mssql"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// Convert spatial types for cross-engine migrations
		if isCrossEngine && (colType == "geography" || colType == "geometry") {
			// SQL Server geography/geometry → WKT for PostgreSQL
			quoted[i] = fmt.Sprintf("%s.STAsText() AS %s", d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *MSSQLDialect) BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		dateClause = fmt.Sprintf(" AND ([%s] > @lastSyncDate OR [%s] IS NULL)", dateFilter.Column, dateFilter.Column)
	}

	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT TOP (@limit) %s
			FROM %s %s
			WHERE [%s] > @lastPK AND [%s] <= @maxPK%s
			ORDER BY [%s]
		`, cols, d.QualifyTable(schema, table), tableHint, pkCol, pkCol, dateClause, pkCol)
	}
	return fmt.Sprintf(`
		SELECT TOP (@limit) %s
		FROM %s %s
		WHERE [%s] > @lastPK%s
		ORDER BY [%s]
	`, cols, d.QualifyTable(schema, table), tableHint, pkCol, dateClause, pkCol)
}

func (d *MSSQLDialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *DateFilter) []any {
	if hasMaxPK {
		args := []any{
			sql.Named("limit", limit),
			sql.Named("lastPK", lastPK),
			sql.Named("maxPK", maxPK),
		}
		if dateFilter != nil {
			args = append(args, sql.Named("lastSyncDate", dateFilter.Timestamp))
		}
		return args
	}
	args := []any{
		sql.Named("limit", limit),
		sql.Named("lastPK", lastPK),
	}
	if dateFilter != nil {
		args = append(args, sql.Named("lastSyncDate", dateFilter.Timestamp))
	}
	return args
}

func (d *MSSQLDialect) BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string {
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s %s
		)
		SELECT %s FROM numbered
		WHERE __rn > @rowNum AND __rn <= @rowNumEnd
		ORDER BY __rn
	`, cols, orderBy, d.QualifyTable(schema, table), tableHint, cols)
}

func (d *MSSQLDialect) BuildRowNumberArgs(rowNum int64, limit int) []any {
	return []any{
		sql.Named("rowNum", rowNum),
		sql.Named("rowNumEnd", rowNum+int64(limit)),
	}
}

func (d *MSSQLDialect) PartitionBoundariesQuery(pkCol string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id FROM %%s
		)
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM numbered
		GROUP BY partition_id ORDER BY partition_id
	`, qPK, numPartitions, qPK, qPK, qPK)
}

func (d *MSSQLDialect) RowCountQuery(useStats bool) string {
	if useStats {
		return `SELECT SUM(p.rows) FROM sys.partitions p INNER JOIN sys.tables t ON p.object_id = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)`
	}
	return `SELECT COUNT(*) FROM %s` // Caller must format with table name
}

func (d *MSSQLDialect) DateColumnQuery() string {
	return `SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column`
}

func (d *MSSQLDialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"datetime":       true,
		"datetime2":      true,
		"smalldatetime":  true,
		"date":           true,
		"datetimeoffset": true,
	}
}
