package postgres

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/driver"
)

// Dialect implements driver.Dialect for PostgreSQL.
type Dialect struct{}

func (d *Dialect) DBType() string { return "postgres" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *Dialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)
	encodedDatabase := url.QueryEscape(database)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		encodedUser, encodedPassword, host, port, encodedDatabase)

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

func (d *Dialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) TableHint(_ bool) string {
	return "" // PostgreSQL doesn't use table hints
}

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "postgres"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToLower(colTypes[i])
		}

		// Convert spatial types for cross-engine migrations (PostGIS → WKT for SQL Server)
		if isCrossEngine && (colType == "geography" || colType == "geometry") {
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

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, dateFilter *driver.DateFilter) []any {
	// Note: limit is now part of the query, not args
	if maxPK != nil {
		if dateFilter != nil {
			return []any{lastPK, maxPK, dateFilter.Timestamp}
		}
		return []any{lastPK, maxPK}
	}
	if dateFilter != nil {
		return []any{lastPK, dateFilter.Timestamp}
	}
	return []any{lastPK}
}

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string) string {
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

func (d *Dialect) BuildRowNumberArgs(startRow, endRow int64) []any {
	return []any{startRow, endRow}
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
		return `SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2`
	}
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	return `SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"timestamp without time zone": true,
		"timestamp with time zone":    true,
		"timestamptz":                 true,
		"timestamp":                   true,
		"date":                        true,
	}
}
