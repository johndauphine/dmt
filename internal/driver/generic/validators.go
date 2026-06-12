package generic

import (
	"database/sql"
	"fmt"
)

// connectionValidators are post-connect gates selectable via
// connection.validate_strategy. Entries live in this literal for the
// same init-ordering reason as preflightStrategies.
var connectionValidators = map[string]func(db *sql.DB, database string) error{
	"mssql_compat": mssqlCompatValidate,
}

// mssqlCompatValidate requires compatibility level 140+ (SQL Server
// 2017) — the introspection queries use STRING_AGG. Moved verbatim
// from the hand-written reader's connect path.
func mssqlCompatValidate(db *sql.DB, database string) error {
	var level int
	if err := db.QueryRow("SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()").Scan(&level); err != nil {
		return fmt.Errorf("checking database compatibility level: %w", err)
	}
	if level < 140 {
		return fmt.Errorf("database compatibility level 140+ required (found %d). Run: ALTER DATABASE [%s] SET COMPATIBILITY_LEVEL = 160", level, database)
	}
	return nil
}
