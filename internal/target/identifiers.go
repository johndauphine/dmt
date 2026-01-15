package target

import (
	"strings"
	"unicode"

	"github.com/johndauphine/dmt/internal/driver"
	// Import driver packages to register dialects
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
)

// Package-level dialect instances for identifier quoting
var (
	pgDialect    = driver.GetDialect("postgres")
	mssqlDialect = driver.GetDialect("mssql")
)

// quotePGIdent safely quotes a PostgreSQL identifier using the dialect package.
func quotePGIdent(ident string) string {
	return pgDialect.QuoteIdentifier(ident)
}

// quoteMSSQLIdent safely quotes a SQL Server identifier using the dialect package.
func quoteMSSQLIdent(ident string) string {
	return mssqlDialect.QuoteIdentifier(ident)
}

// qualifyPGTable returns a fully qualified PostgreSQL table name.
func qualifyPGTable(schema, table string) string {
	return pgDialect.QualifyTable(schema, table)
}

// qualifyMSSQLTable returns a fully qualified SQL Server table name.
func qualifyMSSQLTable(schema, table string) string {
	return mssqlDialect.QualifyTable(schema, table)
}

// SanitizePGIdentifier converts a SQL Server identifier to a PostgreSQL-friendly format.
// Rules:
// 1. Convert to lowercase
// 2. Replace non-alphanumeric characters with underscores
// 3. If it starts with a digit, prefix with "col_"
// 4. If empty (unlikely), fallback to "col_"
func SanitizePGIdentifier(ident string) string {
	if ident == "" {
		return "col_"
	}

	// 1. Convert to lowercase
	s := strings.ToLower(ident)

	// 2. Replace non-alphanumeric characters with underscores
	// We iterate through the string and build a new one
	var sb strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s = sb.String()

	// 3. If it starts with a digit, prefix with "col_"
	if len(s) > 0 && unicode.IsDigit(rune(s[0])) {
		s = "col_" + s
	}

	// Double check for empty string after sanitization
	if s == "" {
		return "col_"
	}

	return s
}

// IdentifierChange represents a single identifier name change
type IdentifierChange struct {
	Original  string
	Sanitized string
}

// TableIdentifierChanges represents all identifier changes for a table
type TableIdentifierChanges struct {
	TableName       IdentifierChange
	ColumnChanges   []IdentifierChange
	HasTableChange  bool
	HasColumnChange bool
}

// IdentifierChangeReport contains all identifier changes for a migration
type IdentifierChangeReport struct {
	Tables             []TableIdentifierChanges
	TotalTableChanges  int
	TotalColumnChanges int
	TablesWithChanges  int
	TablesUnchanged    int
}

// TableInfo is a minimal interface for table metadata needed for identifier change detection
type TableInfo interface {
	GetName() string
	GetColumnNames() []string
}

// CollectPGIdentifierChanges analyzes tables and collects all identifier changes
// that will be applied when migrating to PostgreSQL
func CollectPGIdentifierChanges(tables []TableInfo) *IdentifierChangeReport {
	report := &IdentifierChangeReport{}

	for _, t := range tables {
		tableName := t.GetName()
		sanitizedTableName := SanitizePGIdentifier(tableName)

		// Always populate TableName so logging can display the correct table name
		tableChanges := TableIdentifierChanges{
			TableName: IdentifierChange{
				Original:  tableName,
				Sanitized: sanitizedTableName,
			},
		}

		// Check table name change
		if tableName != sanitizedTableName {
			tableChanges.HasTableChange = true
			report.TotalTableChanges++
		}

		// Check column name changes
		for _, colName := range t.GetColumnNames() {
			sanitizedColName := SanitizePGIdentifier(colName)
			if colName != sanitizedColName {
				tableChanges.ColumnChanges = append(tableChanges.ColumnChanges, IdentifierChange{
					Original:  colName,
					Sanitized: sanitizedColName,
				})
				tableChanges.HasColumnChange = true
				report.TotalColumnChanges++
			}
		}

		if tableChanges.HasTableChange || tableChanges.HasColumnChange {
			report.Tables = append(report.Tables, tableChanges)
			report.TablesWithChanges++
		} else {
			report.TablesUnchanged++
		}
	}

	return report
}

// HasChanges returns true if any identifier changes were detected
func (r *IdentifierChangeReport) HasChanges() bool {
	return r.TotalTableChanges > 0 || r.TotalColumnChanges > 0
}
