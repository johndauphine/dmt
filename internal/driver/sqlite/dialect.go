package sqlite

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// Dialect implements driver.Dialect for SQLite.
type Dialect struct{}

func (d *Dialect) DBType() string { return "sqlite" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QualifyTable returns the table reference. SQLite has no schema concept
// distinct from attached databases; the implicit "main" database covers
// every table the migration touches.
func (d *Dialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(table)
}

// BuildDSN returns a SQLite DSN for the modernc.org/sqlite driver. The
// path is taken from the database field — host/port/user/password are
// ignored. PRAGMAs are baked into the DSN: WAL journal for concurrent
// readers, busy timeout to avoid spurious "database is locked" errors
// under contention, foreign-key enforcement enabled by default. The
// caller can append additional pragmas via opts["pragmas"] (slice of
// strings, each like "synchronous(NORMAL)").
func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	if database == "" {
		// In-memory DB — useful for tests. modernc.org/sqlite supports
		// ":memory:" verbatim and also the file::memory:?cache=shared
		// shape for cross-connection sharing.
		database = ":memory:"
	}

	params := url.Values{}
	params.Add("_pragma", "journal_mode(WAL)")
	params.Add("_pragma", "busy_timeout(30000)")
	params.Add("_pragma", "foreign_keys(ON)")
	params.Add("_pragma", "synchronous(NORMAL)")

	if extra, ok := opts["pragmas"].([]string); ok {
		for _, p := range extra {
			params.Add("_pragma", p)
		}
	}

	// :memory: needs the file: URI form for the driver to honor query
	// parameters consistently across connections.
	prefix := ""
	if database == ":memory:" || strings.HasPrefix(database, "file:") {
		if !strings.HasPrefix(database, "file:") {
			prefix = "file:"
		}
	}

	return fmt.Sprintf("%s%s?%s", prefix, database, params.Encode())
}

func (d *Dialect) ParameterPlaceholder(_ int) string { return "?" }

func (d *Dialect) ColumnList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.QuoteIdentifier(c)
	}
	return strings.Join(quoted, ", ")
}

func (d *Dialect) TableHint(strictConsistency bool) string { return "" }

func (d *Dialect) ColumnListForSelect(cols, colTypes []string, targetDBType string) string {
	// SQLite has no spatial types — plain quoted column list.
	return d.ColumnList(cols)
}

func (d *Dialect) BuildKeysetQuery(cols, pkCol, schema, table, _ string, hasMaxPK bool, dateFilter *driver.DateFilter) string {
	dateClause := ""
	if dateFilter != nil {
		dateClause = fmt.Sprintf(" AND %s >= ?", d.QuoteIdentifier(dateFilter.Column))
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

func (d *Dialect) BuildRowNumberQuery(cols, orderBy, schema, table, _ string, dateFilter *driver.DateFilter) string {
	// SQLite 3.25+ supports window functions including ROW_NUMBER().
	outerCols := extractColumnAliases(cols)
	qualifiedTable := d.QualifyTable(schema, table)

	whereClause := ""
	if dateFilter != nil {
		whereClause = fmt.Sprintf(" WHERE %s >= ?", d.QuoteIdentifier(dateFilter.Column))
	}

	return fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, ROW_NUMBER() OVER (ORDER BY %s) as __rn
			FROM %s%s
		)
		WHERE __rn > ? AND __rn <= ?
		ORDER BY __rn
	`, outerCols, cols, orderBy, qualifiedTable, whereClause)
}

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

func (d *Dialect) BuildRowNumberArgs(rowNum int64, limit int, dateFilter *driver.DateFilter) []any {
	if dateFilter != nil {
		return []any{dateFilter.Timestamp, rowNum, rowNum + int64(limit)}
	}
	return []any{rowNum, rowNum + int64(limit)}
}

// PartitionBoundariesQuery returns a partition-boundary query that always
// produces a single partition spanning the whole PK range. SQLite has no
// parallelism benefit from splitting a table — single writer, one shared
// file lock. The reader's GetPartitionBoundaries returns a single
// partition regardless of numPartitions.
func (d *Dialect) PartitionBoundariesQuery(pkCol, schema, table string, numPartitions int) string {
	qPK := d.QuoteIdentifier(pkCol)
	qualifiedTable := d.QualifyTable(schema, table)
	return fmt.Sprintf(
		"SELECT 1 AS partition_id, MIN(%s), MAX(%s), COUNT(*) FROM %s",
		qPK, qPK, qualifiedTable,
	)
}

// RowCountQuery returns the query for getting a row count. SQLite has no
// stats catalog comparable to information_schema.TABLES — always
// COUNT(*).
func (d *Dialect) RowCountQuery(useStats bool) string {
	return `SELECT COUNT(*) FROM %s`
}

func (d *Dialect) DateColumnQuery() string {
	// SQLite stores dates as TEXT/INTEGER/REAL based on convention. The
	// declared type is what we have to go on. Query is structured for
	// (schema, table, column) — schema is ignored in SQLite, the reader
	// passes empty.
	return `SELECT type FROM pragma_table_info(?) WHERE name = ?`
}

func (d *Dialect) ValidDateTypes() map[string]bool {
	return map[string]bool{
		"datetime":  true,
		"timestamp": true,
		"date":      true,
		"text":      true,
	}
}

// AIPromptAugmentation returns SQLite-specific instructions for AI DDL
// generation. SQLite uses type affinity — declared types are hints.
func (d *Dialect) AIPromptAugmentation() string {
	return `
SQLite-specific requirements:
- Use type affinity: INTEGER, REAL, TEXT, BLOB, NUMERIC, BOOLEAN, DATE, DATETIME.
- Auto-increment columns must be declared as "INTEGER PRIMARY KEY AUTOINCREMENT" inline.
- SQLite has no schema concept distinct from attached databases; do not qualify with schema.
- Foreign keys are only enforced when PRAGMA foreign_keys = ON (dmt sets this).
- ALTER TABLE in SQLite is limited; FK and CHECK constraints must be declared inline.
`
}

func (d *Dialect) AIDropTablePromptAugmentation() string {
	return `
SQLite-specific requirements:
- Use DROP TABLE IF EXISTS to handle non-existent tables idempotently.
- SQLite does not support CASCADE for DROP TABLE; foreign-key enforcement
  must be temporarily disabled if dependent tables exist:
    PRAGMA foreign_keys = OFF;
    DROP TABLE IF EXISTS "<table>";
    PRAGMA foreign_keys = ON;
`
}
