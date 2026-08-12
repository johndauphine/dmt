package generic

// SQL Server preflight battery, moved verbatim from the hand-written
// driver (#509). The oracle keeps its own copy until the registration
// flip removes it.

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/driver/shared"
)

// preFlight runs SQL Server preflight checks (#228) and returns findings in
// stable order. Individual probe failures become findings rather than
// propagating, so a single broken query doesn't mask the rest.
func mssqlPreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{
		NilDatabaseRemedy: "internal error — please report",
	},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return shared.SingleFinding(mssqlPFConnection(ctx, db, req.Side))
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return mssqlPFVersion(ctx, db, req.Side)
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return mssqlPFCompatLevel(ctx, db, req.Side)
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return mssqlPFCollation(ctx, db, req.Side)
		},
		mssqlPFPoolHeadroom,
		mssqlPFPrivileges,
		mssqlPFDatabaseSnapshot,
		mssqlPFParallelBCPIndexRisk,
		mssqlPFBackupAck,
	)
}

func mssqlPFDatabaseSnapshot(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side != driver.PreFlightSideSource || !req.StrictConsistency || req.StrictConsistencyScope != "migration" {
		return nil
	}
	var edition, createAny, createDatabase, alterAnyDatabase, dbCreator int
	var version string
	if err := db.QueryRowContext(ctx, `SELECT CAST(SERVERPROPERTY('EngineEdition') AS int), CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), COALESCE(HAS_PERMS_BY_NAME(NULL, NULL, 'CREATE ANY DATABASE'), 0), COALESCE(HAS_PERMS_BY_NAME('master', 'DATABASE', 'CREATE DATABASE'), 0), COALESCE(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY DATABASE'), 0), COALESCE(IS_SRVROLEMEMBER('dbcreator'), 0)`).Scan(&edition, &version, &createAny, &createDatabase, &alterAnyDatabase, &dbCreator); err != nil {
		return []driver.PreFlightFinding{{Severity: driver.SeverityError, Check: "compat.database_snapshot", Side: req.Side, Message: fmt.Sprintf("could not verify SQL Server database-snapshot support: %v", err), Remedy: "grant permission to read SERVERPROPERTY and server permissions, or use strict_consistency_scope: table"}}
	}
	findings := mssqlPFSnapshotCapabilityFindings(req, edition, version, createAny, createDatabase, alterAnyDatabase, dbCreator)
	database := strings.TrimSpace(req.Database)
	if database == "" {
		_ = db.QueryRowContext(ctx, "SELECT DB_NAME()").Scan(&database)
	}
	rows, err := db.QueryContext(ctx, `SELECT name FROM sys.databases WHERE source_database_id = DB_ID(@p1) AND name LIKE 'dmt_strict[_]%' ORDER BY name`, database)
	if err != nil {
		return append(findings, driver.PreFlightFinding{Severity: driver.SeverityWarn, Check: "strict.snapshot_orphans", Side: req.Side, Message: fmt.Sprintf("could not inspect SQL Server snapshot orphans: %v", err), Remedy: "inspect sys.databases for dmt_strict_* snapshots before starting"})
	}
	defer rows.Close()
	var orphans []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return append(findings, driver.PreFlightFinding{Severity: driver.SeverityWarn, Check: "strict.snapshot_orphans", Side: req.Side, Message: fmt.Sprintf("could not read SQL Server snapshot orphan: %v", err)})
		}
		orphans = append(orphans, name)
	}
	if err := rows.Err(); err != nil {
		return append(findings, driver.PreFlightFinding{Severity: driver.SeverityWarn, Check: "strict.snapshot_orphans", Side: req.Side, Message: fmt.Sprintf("could not finish reading SQL Server snapshot orphans: %v", err)})
	}
	if len(orphans) > 0 {
		quoted := make([]string, len(orphans))
		for i, name := range orphans {
			quoted[i] = "[" + strings.ReplaceAll(name, "]", "]]") + "]"
		}
		findings = append(findings, driver.PreFlightFinding{Severity: driver.SeverityWarn, Check: "strict.snapshot_orphans", Side: req.Side, Message: "leftover dmt SQL Server database snapshots: " + strings.Join(quoted, ", "), Remedy: "after confirming no matching run will resume, drop each orphan with DROP DATABASE <snapshot_name>"})
	}
	return findings
}

func mssqlPFSnapshotCapabilityFindings(req driver.PreFlightRequest, edition int, version string, createAny, createDatabase, alterAnyDatabase, dbCreator int) []driver.PreFlightFinding {
	var findings []driver.PreFlightFinding
	if edition == 5 {
		findings = append(findings, driver.PreFlightFinding{Severity: driver.SeverityError, Check: "compat.database_snapshot", Side: req.Side, Message: "Azure SQL Database does not support CREATE DATABASE AS SNAPSHOT OF", Remedy: "use migration.strict_consistency_scope: table on Azure SQL Database"})
	} else if !mssqlSnapshotVersionSupported(version, edition) {
		findings = append(findings, driver.PreFlightFinding{Severity: driver.SeverityError, Check: "compat.database_snapshot", Side: req.Side, Message: fmt.Sprintf("SQL Server %s does not support database snapshots on this edition; 2016 SP1 (13.0.4001) or newer is required", version), Remedy: "upgrade SQL Server or use migration.strict_consistency_scope: table"})
	}
	if createAny != 1 && createDatabase != 1 && alterAnyDatabase != 1 && dbCreator != 1 {
		findings = append(findings, driver.PreFlightFinding{Severity: driver.SeverityError, Check: "privileges.database_snapshot", Side: req.Side, Message: "connected login cannot create the requested SQL Server database snapshot", Remedy: "grant CREATE ANY DATABASE or ALTER ANY DATABASE at server scope, grant CREATE DATABASE in master, add the login to dbcreator, or use migration.strict_consistency_scope: table"})
	}
	return findings
}

func mssqlSnapshotVersionSupported(version string, engineEdition int) bool {
	if engineEdition == 3 {
		return true
	}
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return false
	}
	major, errMajor := strconv.Atoi(parts[0])
	build, errBuild := strconv.Atoi(parts[2])
	if errMajor != nil || errBuild != nil {
		return false
	}
	return major > 13 || (major == 13 && build >= 4001)
}

// mssqlPFCompatLevel mirrors the connection gate
// (connection.validate_strategy mssql_compat): introspection uses
// STRING_AGG, which needs database compatibility level 140+. Preflight
// must not pass a database the reader will then reject (codex on #509).
func mssqlPFCompatLevel(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	// Only readers need the gate: STRING_AGG introspection runs on the
	// source. NewWriter has no compat requirement — a SQL Server 2016
	// target (compat 130) is valid and must not fail preflight (codex).
	if side != driver.PreFlightSideSource {
		return nil
	}
	var level int
	if err := db.QueryRowContext(ctx,
		"SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME()").Scan(&level); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.compatibility_level",
			Side:     side,
			Message:  fmt.Sprintf("could not read database compatibility level: %v", err),
		}}
	}
	if level < 140 {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "version.compatibility_level",
			Side:     side,
			Message:  fmt.Sprintf("database compatibility level %d is below the required 140 (SQL Server 2017)", level),
			Remedy:   "run: ALTER DATABASE [<db>] SET COMPATIBILITY_LEVEL = 160",
		}}
	}
	return nil
}

// mssqlPFBackupAck fires when target_mode is drop_recreate
// and ConfirmBackup wasn't set. Looks for any user table with rows in the
// target schema; one hit blocks the migration. Reads sys.partitions for
// row count (cheap; no scan needed) and joins to sys.tables/schemas for
// the schema filter. Requires no special permissions beyond ordinary
// database access — sys.partitions is readable by db_datareader.
func mssqlPFBackupAck(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if !shared.BackupAcknowledgmentRequired(req) {
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
		Remedy:   "back up the target, then re-run with --confirm-backup (on 'dmt resume' use --skip-preflight backup instead), or switch to target_mode: upsert",
	}}
}

func mssqlPFConnection(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	return shared.CheckConnection(ctx, db, side, shared.ConnectionCheckConfig{
		Remedy: "verify host/port/credentials and that the SQL Server instance is reachable; check TLS and trust_server_cert settings",
	})
}

// mssqlPFVersion enforces SQL Server 2016 (major 13) as the floor.
// 2016 introduced JSON and is the oldest version still receiving extended
// support; older versions (2014 and earlier) lack JSON support and have
// edge cases dmt's TVP and bulk-copy paths don't accommodate.
func mssqlPFVersion(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
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

// mssqlPFCollation warns if the database collation is case-sensitive or
// non-UTF8. dmt sanitizes identifiers assuming case-insensitive collation
// (the SQL Server default); a CS collation can cause silent target-table
// resolution failures.
func mssqlPFCollation(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
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

// mssqlPFPoolHeadroom compares @@MAX_CONNECTIONS against the current
// session count, leaving Workers + 5 headroom. Reading sys.dm_exec_sessions
// requires VIEW SERVER STATE; if denied, emit an info-only finding rather
// than aborting — the migration will surface connection errors at runtime
// if there really isn't headroom.
func mssqlPFPoolHeadroom(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
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
	return shared.PoolHeadroomFinding(
		req,
		int64(maxConns),
		int64(current),
		"lower migration.workers or wait until existing connections drain (SQL Server's default 32767 limit is usually high enough; check sp_configure 'user connections')",
	)
}

// mssqlPFPrivileges uses HAS_PERMS_BY_NAME to verify the connected login
// has the database-level permissions downstream phases need. PERMISSIONS()
// is the older catalog function but HAS_PERMS_BY_NAME is the standard since
// SQL 2005 and handles role inheritance correctly.
func mssqlPFPrivileges(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side == driver.PreFlightSideSource {
		// Source needs SELECT on objects in the configured schema.
		// HAS_PERMS_BY_NAME at database scope with 'SELECT' checks the
		// blanket SELECT (typical for read-only logins) — granular per-
		// table grants will surface at extract time if missing.
		return mssqlPFCheckDBPerm(ctx, db, "SELECT", "read source schema", req.Side)
	}

	mode := shared.TargetModeOrDefault(req.TargetMode, "drop_recreate")
	var findings []driver.PreFlightFinding
	switch mode {
	case "drop_recreate":
		findings = append(findings, mssqlPFCheckDBPerm(ctx, db, "CREATE TABLE", "create target tables", req.Side)...)
		findings = append(findings, mssqlPFCheckDBPerm(ctx, db, "INSERT", "bulk-load target tables", req.Side)...)
	case "upsert":
		findings = append(findings, mssqlPFCheckDBPerm(ctx, db, "INSERT", "merge new rows", req.Side)...)
		findings = append(findings, mssqlPFCheckDBPerm(ctx, db, "UPDATE", "merge existing rows", req.Side)...)
		// Upsert also creates temp staging tables; in MSSQL temp objects
		// live in tempdb and don't require schema CREATE TABLE perms, so
		// we don't probe for it here.
	}
	return findings
}

func mssqlPFCheckDBPerm(ctx context.Context, db *sql.DB, perm, intent string, side driver.PreFlightSide) []driver.PreFlightFinding {
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
		principal := mssqlPFLookupDBPrincipal(ctx, db)
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

// mssqlPFParallelBCPIndexRisk warns when the target schema already has
// enabled nonclustered indexes. dmt's MSSQL target writer currently favors
// parallel BCP without TABLOCK; that is the right default for fresh
// drop_recreate loads where secondary indexes are built after transfer, but
// it can amplify logging and lock contention when loading into existing
// indexed tables. PreFlightRequest does not currently expose the final
// write_ahead_writers value, so this advisory assumes the MSSQL parallel
// default and points at write_ahead_writers=1 as the operator mitigation.
func mssqlPFParallelBCPIndexRisk(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side != driver.PreFlightSideTarget {
		return nil
	}

	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		schema = "dbo"
	}

	var indexCount, tableCount int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(DISTINCT t.object_id)
		FROM sys.indexes i
		JOIN sys.tables t ON t.object_id = i.object_id
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		WHERE s.name = @p1
		  AND t.is_ms_shipped = 0
		  AND UPPER(i.type_desc) LIKE 'NONCLUSTERED%'
		  AND i.is_primary_key = 0
		  AND i.is_disabled = 0
		  AND i.is_hypothetical = 0`, sql.Named("p1", schema)).Scan(&indexCount, &tableCount)
	if err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "bulk.parallel_bcp_indexes",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not inspect nonclustered indexes in target schema [%s] for MSSQL parallel BCP risk: %v", schema, err),
			Remedy:   "grant metadata visibility on target tables if you want this advisory check to be precise",
		}}
	}
	if indexCount == 0 {
		return nil
	}

	return []driver.PreFlightFinding{{
		Severity: driver.SeverityWarn,
		Check:    "bulk.parallel_bcp_indexes",
		Side:     req.Side,
		Message: fmt.Sprintf(
			"target schema [%s] has %d enabled nonclustered %s across %d %s; MSSQL loads use parallel BCP without TABLOCK by default, which can increase logging and lock contention when preserving indexed target tables",
			schema,
			indexCount,
			mssqlPFPlural(indexCount, "index", "indexes"),
			tableCount,
			mssqlPFPlural(tableCount, "table", "tables"),
		),
		Remedy: "for upsert or other loads into existing tables, consider dropping/rebuilding secondary indexes around the migration or set migration.write_ahead_writers: 1; in drop_recreate, dmt creates non-PK indexes after transfer",
	}}
}

func mssqlPFPlural(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}

// mssqlPFLookupDBPrincipal returns the quoted database user name suitable for
// pasting into a GRANT statement. Falls back to <user> when USER_NAME()
// can't be resolved — better an obvious placeholder than an invalid
// T-SQL identifier.
func mssqlPFLookupDBPrincipal(ctx context.Context, db *sql.DB) string {
	var name string
	if err := db.QueryRowContext(ctx, "SELECT USER_NAME()").Scan(&name); err != nil || strings.TrimSpace(name) == "" {
		return "<user>"
	}
	return "[" + strings.ReplaceAll(strings.TrimSpace(name), "]", "]]") + "]"
}
