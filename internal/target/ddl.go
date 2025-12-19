package target

import (
	"fmt"
	"strings"

	"github.com/johndauphine/mssql-pg-migrate/internal/source"
	"github.com/johndauphine/mssql-pg-migrate/internal/typemap"
)

// GenerateDDL generates CREATE TABLE statement for PostgreSQL
func GenerateDDL(t *source.Table, targetSchema string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE UNLOGGED TABLE IF NOT EXISTS %s.%q (\n",
		targetSchema, t.Name))

	for i, col := range t.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}

		pgType := typemap.MSSQLToPostgres(col.DataType, col.MaxLength, col.Precision, col.Scale)

		sb.WriteString(fmt.Sprintf("    %q %s", col.Name, pgType))

		if !col.IsNullable {
			sb.WriteString(" NOT NULL")
		}
	}

	sb.WriteString("\n)")

	return sb.String()
}

// GenerateCreatePK generates ALTER TABLE ADD PRIMARY KEY statement
func GenerateCreatePK(t *source.Table, targetSchema string) string {
	if len(t.PrimaryKey) == 0 {
		return ""
	}

	pkCols := make([]string, len(t.PrimaryKey))
	for i, col := range t.PrimaryKey {
		pkCols[i] = fmt.Sprintf("%q", col)
	}

	return fmt.Sprintf("ALTER TABLE %s.%q ADD PRIMARY KEY (%s)",
		targetSchema, t.Name, strings.Join(pkCols, ", "))
}
