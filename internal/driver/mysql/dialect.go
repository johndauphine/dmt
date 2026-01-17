package mysql

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// Dialect implements driver.Dialect for MySQL/MariaDB.
type Dialect struct{}

func (d *Dialect) DBType() string { return "mysql" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (d *Dialect) QualifyTable(schema, table string) string {
	// MySQL uses database.table, but schema is often empty (database is in DSN)
	if schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// MySQL DSN format: user:password@tcp(host:port)/database?params
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)

	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("multiStatements", "true")
	params.Set("interpolateParams", "true")

	// Handle SSL/TLS mode
	if sslMode, ok := opts["ssl_mode"].(string); ok && sslMode != "" {
		switch strings.ToLower(sslMode) {
		case "disable", "disabled", "false":
			params.Set("tls", "false")
		case "require", "required", "true":
			params.Set("tls", "true")
		case "verify-ca", "verify_ca":
			params.Set("tls", "skip-verify")
		case "verify-full", "verify_full", "verify-identity", "verify_identity":
			params.Set("tls", "true")
		default:
			params.Set("tls", "preferred")
		}
	} else {
		params.Set("tls", "preferred")
	}

	// Handle charset
	if charset, ok := opts["charset"].(string); ok && charset != "" {
		params.Set("charset", charset)
	} else {
		params.Set("charset", "utf8mb4")
	}

	// Handle collation
	if collation, ok := opts["collation"].(string); ok && collation != "" {
		params.Set("collation", collation)
	}

	// Handle timezone
	if loc, ok := opts["loc"].(string); ok && loc != "" {
		params.Set("loc", loc)
	} else {
		params.Set("loc", "UTC")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		encodedUser, encodedPassword, host, port, database, params.Encode())

	return dsn
}

func (d *Dialect) ParameterPlaceholder(_ int) string {
	return "?"
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
		return "" // No hint needed for consistent reads (use FOR SHARE if needed)
	}
	return "" // MySQL doesn't have NOLOCK equivalent; uses MVCC
}

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "mysql"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// Convert spatial types for cross-engine migrations
		if isCrossEngine && (colType == "geometry" || colType == "point" ||
			colType == "linestring" || colType == "polygon" ||
			colType == "multipoint" || colType == "multilinestring" ||
			colType == "multipolygon" || colType == "geometrycollection") {
			quoted[i] = fmt.Sprintf("ST_AsText(%s) AS %s", d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		dateClause = fmt.Sprintf(" AND (%s > ? OR %s IS NULL)",
			d.QuoteIdentifier(dateFilter.Column), d.QuoteIdentifier(dateFilter.Column))
	}

	qualifiedTable := d.QualifyTable(schema, table)
	qPK := d.QuoteIdentifier(pkCol)

	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s > ? AND %s <= ?%s
			ORDER BY %s
			LIMIT ?
		`, cols, qualifiedTable, qPK, qPK, dateClause, qPK)
	}
	return fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE %s > ?%s
		ORDER BY %s
		LIMIT ?
	`, cols, qualifiedTable, qPK, dateClause, qPK)
}

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *driver.DateFilter) []any {
	if hasMaxPK {
		if dateFilter != nil {
			return []any{lastPK, maxPK, dateFilter.Timestamp, limit}
		}
		return []any{lastPK, maxPK, limit}
	}
	if dateFilter != nil {
		return []any{lastPK, dateFilter.Timestamp, limit}
	}
	return []any{lastPK, limit}
}

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string) string {
	// MySQL 8.0+ supports window functions
	outerCols := extractColumnAliases(cols)
	qualifiedTable := d.QualifyTable(schema, table)
	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s
		) AS numbered
		WHERE __rn > ? AND __rn <= ?
		ORDER BY __rn
	`, outerCols, cols, orderBy, qualifiedTable)
}

// extractColumnAliases extracts just the column aliases from a column expression list.
func extractColumnAliases(cols string) string {
	parts := strings.Split(cols, ",")
	aliases := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		upperPart := strings.ToUpper(part)
		if idx := strings.LastIndex(upperPart, " AS "); idx != -1 {
			aliases[i] = strings.TrimSpace(part[idx+4:])
		} else {
			aliases[i] = part
		}
	}
	return strings.Join(aliases, ", ")
}

func (d *Dialect) BuildRowNumberArgs(rowNum int64, limit int) []any {
	return []any{rowNum, rowNum + int64(limit)}
}

func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	qualifiedTable := d.QualifyTable(schema, table)
	return fmt.Sprintf(`
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) as partition_id FROM %s
		) AS numbered
		GROUP BY partition_id ORDER BY partition_id
	`, qPK, qPK, qPK, numPartitions, qPK, qualifiedTable)
}

func (d *Dialect) RowCountQuery(useStats bool) string {
	if useStats {
		// Use information_schema for fast approximate count
		return `SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	}
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	return `SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"datetime":  true,
		"timestamp": true,
		"date":      true,
	}
}

// AIPromptAugmentation returns MySQL-specific instructions for AI DDL generation.
func (d *Dialect) AIPromptAugmentation() string {
	return "" // No special requirements for MySQL
}
