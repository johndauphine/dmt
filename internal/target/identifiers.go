package target

import (
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/ident"
	// Import driver packages to register dialects. postgres and mysql
	// register via the catalog-driven generic engine (#509).
	_ "github.com/johndauphine/dmt/v5/internal/driver/generic"
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

// SanitizePGIdentifier converts an identifier to PostgreSQL-friendly lowercase format.
// Delegates to ident.SanitizePG for the shared implementation.
func SanitizePGIdentifier(identStr string) string {
	return ident.SanitizePG(identStr)
}

// SanitizePGTableName is an alias for SanitizePGIdentifier for table names.
func SanitizePGTableName(ident string) string {
	return SanitizePGIdentifier(ident)
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
		sanitizedTableName := SanitizePGTableName(tableName)

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
