//go:build cgo

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

// QuoteIdentifier returns the Oracle-safe identifier.
// Oracle folds unquoted identifiers to uppercase, so this function:
// 1. Always converts to uppercase for consistency
// 2. Quotes if the name contains special characters, starts with a digit, or is a reserved word
//
// Note: Oracle quoted identifiers CAN preserve case, but we intentionally use uppercase
// for consistency between quoted and unquoted identifiers. This ensures that:
// - "FOO" and FOO refer to the same object
// - AI-generated DDL works correctly (AI typically generates uppercase identifiers)
// - Migrations remain predictable regardless of source case
func (d *Dialect) QuoteIdentifier(name string) string {
	upper := strings.ToUpper(name)

	// Check if name needs quoting (contains non-alphanumeric chars other than underscore)
	needsQuote := false
	for i, r := range name {
		if i == 0 && r >= '0' && r <= '9' {
			needsQuote = true
			break
		}
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			needsQuote = true
			break
		}
	}

	// Check if it's a reserved word that must be quoted
	if !needsQuote && isOracleReservedWord(upper) {
		needsQuote = true
	}

	if needsQuote {
		// Quote with uppercase (Oracle convention for quoted identifiers)
		return `"` + strings.ReplaceAll(upper, `"`, `""`) + `"`
	}

	// Return uppercase unquoted identifier
	return upper
}

// isOracleReservedWord checks if the identifier is an Oracle reserved word.
// These words cannot be used as unquoted identifiers.
func isOracleReservedWord(name string) bool {
	// Oracle reserved words that commonly appear as column names
	reservedWords := map[string]bool{
		// SQL reserved words
		"ACCESS": true, "ADD": true, "ALL": true, "ALTER": true, "AND": true,
		"ANY": true, "AS": true, "ASC": true, "AUDIT": true, "BETWEEN": true,
		"BY": true, "CHAR": true, "CHECK": true, "CLUSTER": true, "COLUMN": true,
		"COMMENT": true, "COMPRESS": true, "CONNECT": true, "CREATE": true, "CURRENT": true,
		"DATE": true, "DECIMAL": true, "DEFAULT": true, "DELETE": true, "DESC": true,
		"DISTINCT": true, "DROP": true, "ELSE": true, "EXCLUSIVE": true, "EXISTS": true,
		"FILE": true, "FLOAT": true, "FOR": true, "FROM": true, "GRANT": true,
		"GROUP": true, "HAVING": true, "IDENTIFIED": true, "IMMEDIATE": true, "IN": true,
		"INCREMENT": true, "INDEX": true, "INITIAL": true, "INSERT": true, "INTEGER": true,
		"INTERSECT": true, "INTO": true, "IS": true, "LEVEL": true, "LIKE": true,
		"LOCK": true, "LONG": true, "MAXEXTENTS": true, "MINUS": true, "MLSLABEL": true,
		"MODE": true, "MODIFY": true, "NOAUDIT": true, "NOCOMPRESS": true, "NOT": true,
		"NOWAIT": true, "NULL": true, "NUMBER": true, "OF": true, "OFFLINE": true,
		"ON": true, "ONLINE": true, "OPTION": true, "OR": true, "ORDER": true,
		"PCTFREE": true, "PRIOR": true, "PUBLIC": true, "RAW": true, "RENAME": true,
		"RESOURCE": true, "REVOKE": true, "ROW": true, "ROWID": true, "ROWNUM": true,
		"ROWS": true, "SELECT": true, "SESSION": true, "SET": true, "SHARE": true,
		"SIZE": true, "SMALLINT": true, "START": true, "SUCCESSFUL": true, "SYNONYM": true,
		"SYSDATE": true, "TABLE": true, "THEN": true, "TO": true, "TRIGGER": true,
		"UID": true, "UNION": true, "UNIQUE": true, "UPDATE": true, "USER": true,
		"VALIDATE": true, "VALUES": true, "VARCHAR": true, "VARCHAR2": true, "VIEW": true,
		"WHENEVER": true, "WHERE": true, "WITH": true,
		// Common problematic column names
		"NAME": true, "TYPE": true, "VALUE": true, "KEY": true, "TIME": true,
		"TIMESTAMP": true, "YEAR": true, "MONTH": true, "DAY": true, "HOUR": true,
		"MINUTE": true, "SECOND": true, "ZONE": true, "DATA": true, "RESULT": true,
	}
	return reservedWords[name]
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

		// CLOBs: godror driver handles CLOB->string conversion properly.
		// Target databases (MySQL TEXT, PostgreSQL TEXT) can handle large values.
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

// AIPromptAugmentation returns Oracle-specific instructions for AI DDL generation.
func (d *Dialect) AIPromptAugmentation() string {
	return `
CRITICAL Oracle rules - YOU MUST FOLLOW THESE EXACTLY:

1. COLUMN NAMES - CONVERT TO UPPERCASE WITHOUT ANY MODIFICATIONS:
   - Simply uppercase each character from the source name
   - NEVER abbreviate, remove, or change any characters
   - NEVER add spaces between parts of the name

   CORRECT mappings (source -> target):
   - LastEditorDisplayName -> LASTEDITORDISPLAYNAME
   - LastEditorUserId -> LASTEDITORUSERID
   - OwnerUserId -> OWNERUSERID
   - CommunityOwnedDate -> COMMUNITYOWNEDDATE
   - AcceptedAnswerId -> ACCEPTEDANSWERID

   WRONG mappings (these are ERRORS):
   - LastEditorDisplayName -> LASTEDITDISPLAYNAME (missing "OR")
   - LastEditorUserId -> LASTEDITUSERID (missing "OR")
   - OwnerUserId -> OWNERUSEID (missing "R" from User)

2. IDENTITY COLUMNS - use this EXACT syntax:
   COLUMN_NAME NUMBER GENERATED BY DEFAULT AS IDENTITY
   - NEVER use GENERATED ALWAYS AS IDENTITY (prevents explicit inserts during migration)
   - NEVER put NOT NULL before GENERATED (causes ORA-03076 syntax error)
   - Identity columns are implicitly NOT NULL, so omit NOT NULL

3. IDENTIFIERS:
   - Reserved words (TYPE, NAME, VALUE, DATE, etc.) must be quoted: "TYPE"
   - NUMBER is preferred over INTEGER for numeric columns
`
}

// extractColumnAliases extracts column aliases from a SELECT list
func extractColumnAliases(cols string) string {
	parts := splitColumnsRespectingParens(cols)
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

// splitColumnsRespectingParens splits a column list on commas, but ignores
// commas inside parentheses (e.g., function calls like DBMS_LOB.SUBSTR(col, 4000, 1))
func splitColumnsRespectingParens(cols string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for _, ch := range cols {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	// Don't forget the last part
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}
