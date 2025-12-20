package target

import "strings"

// quotePGIdent safely quotes a PostgreSQL identifier, escaping embedded quotes.
func quotePGIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// quoteMSSQLIdent safely quotes a SQL Server identifier, escaping embedded ].
func quoteMSSQLIdent(ident string) string {
	return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
}

func qualifyPGTable(schema, table string) string {
	return quotePGIdent(schema) + "." + quotePGIdent(table)
}

func qualifyMSSQLTable(schema, table string) string {
	return quoteMSSQLIdent(schema) + "." + quoteMSSQLIdent(table)
}
