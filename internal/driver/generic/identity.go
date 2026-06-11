package generic

import (
	"context"
	"database/sql"
	"strings"
)

// identityFunc reports which of a table's PK columns are identity /
// auto-increment columns. Imperative per engine (audit:
// strategy-selected): sqlite requires sniffing the original CREATE
// TABLE text, mssql reads sys.identity_columns, and so on.
type identityFunc func(ctx context.Context, db *sql.DB, table string, pkCols []string) (map[string]bool, error)

// identityStrategies is the named registry catalogs select via
// introspection.identity_strategy.
var identityStrategies = map[string]identityFunc{
	"sqlite_autoincrement": sqliteAutoincrement,
}

// sqliteAutoincrement mirrors the hand-written sqlite reader:
// AUTOINCREMENT applies only to a single-column INTEGER PRIMARY KEY,
// and the only record of it is the verbatim CREATE TABLE statement in
// sqlite_master.
func sqliteAutoincrement(ctx context.Context, db *sql.DB, table string, pkCols []string) (map[string]bool, error) {
	if len(pkCols) != 1 {
		return nil, nil
	}
	var createSQL sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
		table).Scan(&createSQL)
	if err != nil || !createSQL.Valid {
		// Mirrors the hand-written reader: detection is best-effort;
		// a miss degrades to no identity flag, not a failed extract.
		return nil, nil //nolint:nilerr
	}
	if strings.Contains(strings.ToLower(createSQL.String), "autoincrement") {
		return map[string]bool{pkCols[0]: true}, nil
	}
	return nil, nil
}
