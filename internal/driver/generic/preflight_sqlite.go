package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// preflightFunc is the engine-specific preflight battery (imperative;
// strategy-selected). Catalogs select via preflight_strategy.
type preflightFunc func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding

// Entries live in this literal (not per-file init funcs): the driver
// registration init() validates catalogs against this map, and Go runs
// var initializers before any init(), keeping ordering safe.
var preflightStrategies = map[string]preflightFunc{
	"sqlite":   sqlitePreFlight,
	"mysql":    mysqlPreFlight,
	"postgres": postgresPreFlight,
	"mssql":    mssqlPreFlight,
}

// sqlitePreFlight runs SQLite preflight checks (moved verbatim from
// the hand-written driver). Returns findings in stable order. Failures
// of individual probes surface as findings rather than propagating.
func sqlitePreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if db == nil {
		return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{
			NilDatabaseRemedy: "internal error — please report",
		})
	}

	var findings []driver.PreFlightFinding
	if f := sqliteCheckConnection(ctx, db, req.Side); f != nil {
		findings = append(findings, *f)
		return findings // can't run further checks without connectivity
	}
	findings = append(findings, sqliteCheckIntegrity(ctx, db, req.Side)...)
	findings = append(findings, sqliteCheckBackupAck(ctx, db, req)...)
	return findings
}

func sqliteCheckConnection(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	return shared.CheckConnection(ctx, db, side, shared.ConnectionCheckConfig{
		Check:         "connection.alive",
		MessagePrefix: "could not query database",
		Remedy:        "verify the database file path is correct and the process has read/write permissions",
	})
}

// checkIntegrity runs PRAGMA integrity_check. It returns "ok" on a
// healthy DB; any other value indicates corruption.
func sqliteCheckIntegrity(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check(1)")
	if err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "integrity.check",
			Side:     side,
			Message:  fmt.Sprintf("integrity_check probe failed: %v", err),
			Remedy:   "if the file is a valid SQLite database, this is likely a permissions or driver issue",
		}}
	}
	defer rows.Close()

	if !rows.Next() {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "integrity.check",
			Side:     side,
			Message:  "integrity_check returned no rows",
			Remedy:   "verify the SQLite file is not corrupt",
		}}
	}
	var status string
	if err := rows.Scan(&status); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "integrity.check",
			Side:     side,
			Message:  fmt.Sprintf("scanning integrity_check result: %v", err),
		}}
	}
	if strings.TrimSpace(strings.ToLower(status)) != "ok" {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "integrity.check",
			Side:     side,
			Message:  fmt.Sprintf("SQLite integrity_check reported: %s", status),
			Remedy:   "the database file is corrupt — restore from backup or recreate",
		}}
	}
	return nil
}

// checkBackupAck fires when target_mode is drop_recreate and the
// operator hasn't acknowledged the backup (ConfirmBackup=false), and any
// existing table holds rows.
func sqliteCheckBackupAck(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if !shared.BackupAcknowledgmentRequired(req) {
		return nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT name FROM sqlite_master
		WHERE type='table' AND name NOT LIKE 'sqlite_%'
	`)
	if err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "backup.acknowledgment",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not list tables for backup-ack probe: %v", err),
			Remedy:   "fix the connection issue, or re-run with --confirm-backup",
		}}
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables = append(tables, name)
	}

	for _, name := range tables {
		var n int64
		// Identifier comes from sqlite_master so it's trusted to be a
		// well-formed table name; quoting still prevents reserved-word
		// collisions.
		q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, strings.ReplaceAll(name, `"`, `""`))
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			continue
		}
		if n > 0 {
			return []driver.PreFlightFinding{{
				Severity: driver.SeverityError,
				Check:    "backup.acknowledgment",
				Side:     req.Side,
				Message:  fmt.Sprintf("target SQLite file has non-empty table %q (%d rows); drop_recreate will destroy this data", name, n),
				Remedy:   "back up the .db file, then re-run with --confirm-backup (or switch to target_mode: upsert)",
			}}
		}
	}
	return nil
}
