package oracle

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// Dialect implements driver.Dialect for Oracle Database.
type Dialect struct{}

func (d *Dialect) DBType() string { return "oracle" }

func (d *Dialect) QuoteIdentifier(name string) string {
	// Oracle uses double quotes, escape embedded quotes by doubling
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *Dialect) QualifyTable(schema, table string) string {
	if schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) ParameterPlaceholder(index int) string {
	// Oracle uses :1, :2, :3 format
	return fmt.Sprintf(":%d", index)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	// Oracle DSN format: user/password@host:port/service_name
	// or with godror: user/password@(DESCRIPTION=...)

	encodedUser := url.QueryEscape(user)
	encodedPassword := url.QueryEscape(password)

	// Check for explicit service_name in opts
	serviceName := database
	if sn, ok := opts["service_name"].(string); ok && sn != "" {
		serviceName = sn
	}

	// Check for TNS connect string
	if tnsConnect, ok := opts["tns_connect"].(string); ok && tnsConnect != "" {
		return fmt.Sprintf("%s/%s@%s", encodedUser, encodedPassword, tnsConnect)
	}

	// Build parameters for godror
	params := []string{}

	// Connection pooling
	if poolMin, ok := opts["pool_min"].(int); ok && poolMin > 0 {
		params = append(params, fmt.Sprintf("poolMinSessions=%d", poolMin))
	}
	if poolMax, ok := opts["pool_max"].(int); ok && poolMax > 0 {
		params = append(params, fmt.Sprintf("poolMaxSessions=%d", poolMax))
	}

	// Timezone
	if tz, ok := opts["timezone"].(string); ok && tz != "" {
		params = append(params, fmt.Sprintf("timezone=%s", tz))
	}

	// Performance tuning - larger prefetch for bulk operations
	params = append(params, "prefetchCount=5000")
	params = append(params, "fetchArraySize=5000")

	// Build Easy Connect format
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s",
		encodedUser, encodedPassword, host, port, serviceName)

	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}

	return dsn
}

func (d *Dialect) TableHint(strictConsistency bool) string {
	// Oracle doesn't need hints like MSSQL's NOLOCK - uses MVCC
	return ""
}

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	quoted := make([]string, len(cols))
	isCrossEngine := targetDBType != "oracle"

	for i, c := range cols {
		colType := ""
		if i < len(colTypes) {
			colType = strings.ToUpper(colTypes[i])
		}

		// Convert spatial types (SDO_GEOMETRY) to WKT for cross-engine
		if isCrossEngine && colType == "SDO_GEOMETRY" {
			quoted[i] = fmt.Sprintf("SDO_UTIL.TO_WKTGEOMETRY(%s) AS %s",
				d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}

		// Convert CLOBs to VARCHAR2 for cross-engine (limited to 4000 chars)
		if isCrossEngine && (colType == "CLOB" || colType == "NCLOB") {
			quoted[i] = fmt.Sprintf("DBMS_LOB.SUBSTR(%s, 4000, 1) AS %s",
				d.QuoteIdentifier(c), d.QuoteIdentifier(c))
			continue
		}

		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		paramIdx := 3
		if hasMaxPK {
			paramIdx = 4
		}
		dateClause = fmt.Sprintf(" AND (%s > :%d OR %s IS NULL)",
			d.QuoteIdentifier(dateFilter.Column), paramIdx,
			d.QuoteIdentifier(dateFilter.Column))
	}

	qualifiedTable := d.QualifyTable(schema, table)
	qPK := d.QuoteIdentifier(pkCol)

	// Oracle 12c+ supports FETCH FIRST n ROWS ONLY
	if hasMaxPK {
		return fmt.Sprintf(`
			SELECT %s FROM %s
			WHERE %s > :1 AND %s <= :2%s
			ORDER BY %s
			FETCH FIRST :3 ROWS ONLY
		`, cols, qualifiedTable, qPK, qPK, dateClause, qPK)
	}
	return fmt.Sprintf(`
		SELECT %s FROM %s
		WHERE %s > :1%s
		ORDER BY %s
		FETCH FIRST :2 ROWS ONLY
	`, cols, qualifiedTable, qPK, dateClause, qPK)
}

func (d *Dialect) BuildKeysetArgs(lastPK, maxPK any, limit int, hasMaxPK bool, dateFilter *driver.DateFilter) []any {
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

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string) string {
	outerCols := extractColumnAliases(cols)
	qualifiedTable := d.QualifyTable(schema, table)

	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) AS rn
			FROM %s
		)
		WHERE rn > :1 AND rn <= :2
		ORDER BY rn
	`, outerCols, cols, orderBy, qualifiedTable)
}

func (d *Dialect) BuildRowNumberArgs(rowNum int64, limit int) []any {
	return []any{rowNum, rowNum + int64(limit)}
}

func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	qualifiedTable := d.QualifyTable(schema, table)

	return fmt.Sprintf(`
		SELECT partition_id, MIN(%s), MAX(%s), COUNT(*) FROM (
			SELECT %s, NTILE(%d) OVER (ORDER BY %s) AS partition_id FROM %s
		)
		GROUP BY partition_id
		ORDER BY partition_id
	`, qPK, qPK, qPK, numPartitions, qPK, qualifiedTable)
}

func (d *Dialect) RowCountQuery(useStats bool) string {
	if useStats {
		return `SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`
	}
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	return `SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 AND COLUMN_NAME = :3`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"DATE":                              true,
		"TIMESTAMP":                         true,
		"TIMESTAMP WITH TIME ZONE":          true,
		"TIMESTAMP WITH LOCAL TIME ZONE":    true,
		"TIMESTAMP(6)":                      true,
		"TIMESTAMP(6) WITH TIME ZONE":       true,
		"TIMESTAMP(6) WITH LOCAL TIME ZONE": true,
	}
}

// extractColumnAliases extracts column aliases from a SELECT list
func extractColumnAliases(cols string) string {
	parts := strings.Split(cols, ",")
	aliases := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		upperPart := strings.ToUpper(part)
		if idx := strings.LastIndex(upperPart, " AS "); idx != -1 {
			aliases[i] = strings.TrimSpace(part[idx+4:])
		} else {
			// Extract just the column name (after any . for qualified names)
			if dotIdx := strings.LastIndex(part, "."); dotIdx != -1 {
				aliases[i] = strings.TrimSpace(part[dotIdx+1:])
			} else {
				aliases[i] = part
			}
		}
	}
	return strings.Join(aliases, ", ")
}
