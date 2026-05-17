package mssql

import (
	"database/sql"
	"fmt"
)

func getCompatibilityLevel(db *sql.DB) (int, error) {
	var level int
	err := db.QueryRow("SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()").Scan(&level)
	if err != nil {
		return 0, fmt.Errorf("querying compatibility level: %w", err)
	}
	return level, nil
}
