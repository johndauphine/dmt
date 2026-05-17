package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// preFlight runs SQL Server preflight checks (#228) and returns findings in
// stable order. Individual probe failures become findings rather than
// propagating, so a single broken query doesn't mask the rest.
func preFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if db == nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "connection.handle",
			Side:     req.Side,
			Message:  "no database handle supplied to preflight",
			Remedy:   "internal error — please report",
		}}
	}

	var findings []driver.PreFlightFinding
	findings = appendIfNonNilMSSQL(findings, checkConnectionMSSQL(ctx, db, req.Side))
	findings = append(findings, checkVersionMSSQL(ctx, db, req.Side)...)
	findings = append(findings, checkCollationMSSQL(ctx, db, req.Side)...)
	findings = append(findings, checkPoolHeadroomMSSQL(ctx, db, req)...)
	findings = append(findings, checkPrivilegesMSSQL(ctx, db, req)...)
	findings = append(findings, checkBackupAcknowledgmentMSSQL(ctx, db, req)...)
	return findings
}

// checkBackupAcknowledgmentMSSQL fires when target_mode is drop_recreate
// and ConfirmBackup wasn't set. Looks for any user table with rows in the
// target schema; one hit blocks the migration. Reads sys.partitions for
// row count (cheap; no scan needed) and joins to sys.tables/schemas for
// the schema filter. Requires no special permissions beyond ordinary
// database access — sys.partitions is readable by db_datareader.
func checkBackupAcknowledgmentMSSQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side != driver.PreFlightSideTarget {
		return nil
	}
	if strings.ToLower(strings.TrimSpace(req.TargetMode)) != "drop_recreate" {
		return nil
	}
	if req.ConfirmBackup {
		return nil
	}
	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		schema = "dbo"
	}
	var name string
	err := db.QueryRowContext(ctx, `
		SELECT TOP 1 t.name
		FROM sys.tables t
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
		WHERE s.name = @p1 AND p.rows > 0`, schema).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		// Clean schema — guard satisfied.
		return nil
	case err != nil:
		// Probe failure must NOT silently pass; same data-safety rule
		// as the PG/MySQL backup-ack guards (Copilot review).
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "backup.acknowledgment",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not verify target schema [%s] is empty: %v", schema, err),
			Remedy:   "grant the dmt login SELECT on sys.tables/sys.schemas/sys.partitions, fix the connection issue, or re-run with --confirm-backup to acknowledge that drop_recreate may destroy data",
		}}
	}
	return []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "backup.acknowledgment",
		Side:     req.Side,
		Message:  fmt.Sprintf("target schema [%s] has non-empty table [%s]; drop_recreate will destroy this data", schema, name),
		Remedy:   "back up the target, then re-run with --confirm-backup (or switch to target_mode: upsert)",
	}}
}

func checkConnectionMSSQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return &driver.PreFlightFinding{
			Severity: driver.SeverityError,
			Check:    "connection.ping",
			Side:     side,
			Message:  fmt.Sprintf("connection ping failed: %v", err),
			Remedy:   "verify host/port/credentials and that the SQL Server instance is reachable; check TLS and trust_server_cert settings",
		}
	}
	return nil
}

// checkVersionMSSQL enforces SQL Server 2016 (major 13) as the floor.
// 2016 introduced JSON and is the oldest version still receiving extended
// support; older versions (2014 and earlier) lack JSON support and have
// edge cases dmt's TVP and bulk-copy paths don't accommodate.
func checkVersionMSSQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var major int
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)").Scan(&major); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not read ProductMajorVersion: %v", err),
		}}
	}
	if major < 13 {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("SQL Server major version %d is below the supported floor (2016 = 13)", major),
			Remedy:   "upgrade SQL Server to 2016 (13) or newer",
		}}
	}
	return nil
}

// checkCollationMSSQL warns if the database collation is case-sensitive or
// non-UTF8. dmt sanitizes identifiers assuming case-insensitive collation
// (the SQL Server default); a CS collation can cause silent target-table
// resolution failures.
func checkCollationMSSQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var coll string
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS nvarchar(200))").Scan(&coll); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "encoding.collation",
			Side:     side,
			Message:  fmt.Sprintf("could not read database collation: %v", err),
		}}
	}
	upper := strings.ToUpper(coll)
	if strings.Contains(upper, "_CS_") {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "encoding.collation",
			Side:     side,
			Message:  fmt.Sprintf("database collation %q is case-sensitive; identifier resolution may produce unexpected results", coll),
			Remedy:   "if cross-engine migration is intended, verify table/column names match exactly in case",
		}}
	}
	return nil
}

// checkPoolHeadroomMSSQL compares @@MAX_CONNECTIONS against the current
// session count, leaving Workers + 5 headroom. Reading sys.dm_exec_sessions
// requires VIEW SERVER STATE; if denied, emit an info-only finding rather
// than aborting — the migration will surface connection errors at runtime
// if there really isn't headroom.
func checkPoolHeadroomMSSQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	var maxConns int
	if err := db.QueryRowContext(ctx, "SELECT @@MAX_CONNECTIONS").Scan(&maxConns); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read @@MAX_CONNECTIONS: %v", err),
		}}
	}
	var current int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id <> @@SPID").Scan(&current); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read sys.dm_exec_sessions: %v", err),
			Remedy:   "grant VIEW SERVER STATE to the dmt login for accurate pool-headroom checks",
		}}
	}
	needed := req.Workers + 5
	if needed <= 0 {
		return nil
	}
	available := maxConns - current
	if available < needed {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message: fmt.Sprintf("only %d of %d connections free; need %d (workers=%d + 5 margin)",
				available, maxConns, needed, req.Workers),
			Remedy: "lower migration.workers or wait until existing connections drain (SQL Server's default 32767 limit is usually high enough; check sp_configure 'user connections')",
		}}
	}
	return nil
}

// checkPrivilegesMSSQL uses HAS_PERMS_BY_NAME to verify the connected login
// has the database-level permissions downstream phases need. PERMISSIONS()
// is the older catalog function but HAS_PERMS_BY_NAME is the standard since
// SQL 2005 and handles role inheritance correctly.
func checkPrivilegesMSSQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side == driver.PreFlightSideSource {
		// Source needs SELECT on objects in the configured schema.
		// HAS_PERMS_BY_NAME at database scope with 'SELECT' checks the
		// blanket SELECT (typical for read-only logins) — granular per-
		// table grants will surface at extract time if missing.
		return mssqlCheckDBPerm(ctx, db, "SELECT", "read source schema", req.Side)
	}

	mode := strings.ToLower(strings.TrimSpace(req.TargetMode))
	if mode == "" {
		mode = "drop_recreate"
	}
	var findings []driver.PreFlightFinding
	switch mode {
	case "drop_recreate":
		findings = append(findings, mssqlCheckDBPerm(ctx, db, "CREATE TABLE", "create target tables", req.Side)...)
		findings = append(findings, mssqlCheckDBPerm(ctx, db, "INSERT", "bulk-load target tables", req.Side)...)
	case "upsert":
		findings = append(findings, mssqlCheckDBPerm(ctx, db, "INSERT", "merge new rows", req.Side)...)
		findings = append(findings, mssqlCheckDBPerm(ctx, db, "UPDATE", "merge existing rows", req.Side)...)
		// Upsert also creates temp staging tables; in MSSQL temp objects
		// live in tempdb and don't require schema CREATE TABLE perms, so
		// we don't probe for it here.
	}
	return findings
}

func mssqlCheckDBPerm(ctx context.Context, db *sql.DB, perm, intent string, side driver.PreFlightSide) []driver.PreFlightFinding {
	var ok int
	if err := db.QueryRowContext(ctx,
		"SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', @p1)", perm).Scan(&ok); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "privileges." + strings.ToLower(strings.ReplaceAll(perm, " ", "_")),
			Side:     side,
			Message:  fmt.Sprintf("could not check %s permission: %v", perm, err),
			Remedy:   fmt.Sprintf("verify the dmt login exists and can read its own permissions; needed to %s", intent),
		}}
	}
	if ok != 1 {
		// Look up the actual database user name so the remedy is
		// copy-pasteable. SUSER_SNAME() / USER_NAME() are always
		// available; falling back to a placeholder keeps the remedy
		// useful even if the lookup fails (Copilot review:
		// "[current_user]" is not valid T-SQL).
		principal := lookupDBPrincipal(ctx, db)
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "privileges." + strings.ToLower(strings.ReplaceAll(perm, " ", "_")),
			Side:     side,
			Message:  fmt.Sprintf("connected login lacks %s permission on this database (needed to %s)", perm, intent),
			Remedy:   fmt.Sprintf("GRANT %s TO %s;", perm, principal),
		}}
	}
	return nil
}

// lookupDBPrincipal returns the quoted database user name suitable for
// pasting into a GRANT statement. Falls back to <user> when USER_NAME()
// can't be resolved — better an obvious placeholder than an invalid
// T-SQL identifier.
func lookupDBPrincipal(ctx context.Context, db *sql.DB) string {
	var name string
	if err := db.QueryRowContext(ctx, "SELECT USER_NAME()").Scan(&name); err != nil || strings.TrimSpace(name) == "" {
		return "<user>"
	}
	return "[" + strings.ReplaceAll(strings.TrimSpace(name), "]", "]]") + "]"
}

func appendIfNonNilMSSQL(s []driver.PreFlightFinding, f *driver.PreFlightFinding) []driver.PreFlightFinding {
	if f == nil {
		return s
	}
	return append(s, *f)
}
