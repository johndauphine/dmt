package mssql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// Dialect implements driver.Dialect for SQL Server.
type Dialect struct{}

func (d *Dialect) DBType() string { return "mssql" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (d *Dialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
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
		// Note: "packet size" is the go-mssqldb parameter name; + is URL encoding for space
		dsn += fmt.Sprintf("&packet%%20size=%d", packetSize)
	}

	return dsn
}

func (d *Dialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) TableHint(strictConsistency bool) string {
	if strictConsistency {
		return ""
	}
	return "WITH (NOLOCK)"
}

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
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

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, tableHint string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
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

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, dateFilter *driver.DateFilter) []any {
	if maxPK != nil {
		args := []any{
			sql.Named("lastPK", lastPK),
			sql.Named("maxPK", maxPK),
		}
		if dateFilter != nil {
			args = append(args, sql.Named("lastSyncDate", dateFilter.Timestamp))
		}
		return args
	}
	args := []any{
		sql.Named("lastPK", lastPK),
	}
	if dateFilter != nil {
		args = append(args, sql.Named("lastSyncDate", dateFilter.Timestamp))
	}
	return args
}

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, tableHint string) string {
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

func (d *Dialect) BuildRowNumberArgs(startRow, endRow int64) []any {
	return []any{
		sql.Named("rowNum", startRow),
		sql.Named("rowNumEnd", endRow),
	}
}

func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	qualifiedTable := d.QualifyTable(schema, table)
	return fmt.Sprintf(`
		WITH numbered AS (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id FROM %s
		)
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM numbered
		GROUP BY partition_id ORDER BY partition_id
	`, qPK, numPartitions, qPK, qualifiedTable, qPK, qPK)
}

func (d *Dialect) RowCountQuery(useStats bool) string {
	if useStats {
		return `SELECT SUM(p.rows) FROM sys.partitions p INNER JOIN sys.tables t ON p.object_id = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0, 1)`
	}
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	return `SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"datetime":       true,
		"datetime2":      true,
		"smalldatetime":  true,
		"date":           true,
		"datetimeoffset": true,
	}
}
