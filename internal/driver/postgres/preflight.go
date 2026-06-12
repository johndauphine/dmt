package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// preFlight runs the PostgreSQL preflight checks (#228) and returns findings
// in stable order. All queries use the passed-in db handle and ctx; errors
// from individual checks become SeverityError findings rather than propagating,
// so a single broken probe doesn't mask the other findings.
func preFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{
		NilDatabaseRemedy: "this is an internal error — please report it with the dmt version",
	},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return shared.SingleFinding(checkConnectionPG(ctx, db, req.Side))
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return checkVersionPG(ctx, db, req.Side)
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return checkEncodingPG(ctx, db, req.Side)
		},
		checkPoolHeadroomPG,
		checkPrivilegesPG,
		checkDiskSpacePG,
		checkBackupAcknowledgmentPG,
	)
}

// checkBackupAcknowledgmentPG fires only on the target side when
// target_mode is drop_recreate AND the operator hasn't set ConfirmBackup.
// It looks for any non-empty user table in the target schema; a single hit
// is enough to fail the check. Empty schemas (fresh DBs) pass silently.
// Uses pg_class.reltuples (planner estimate) — accurate enough for
// "is this table empty or not" without paying for COUNT(*) on large tables.
func checkBackupAcknowledgmentPG(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if !shared.BackupAcknowledgmentRequired(req) {
		return nil
	}
	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		schema = "public"
	}
	var name string
	err := db.QueryRowContext(ctx, `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relkind = 'r'
		  AND c.reltuples > 0
		LIMIT 1`, schema).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		// Clean schema — nothing to destroy, guard is satisfied.
		return nil
	case err != nil:
		// Probe failure must NOT silently pass: we can't prove the
		// schema is empty, so we must require explicit acknowledgment
		// rather than risk dropping populated tables (Copilot review).
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "backup.acknowledgment",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not verify target schema %q is empty: %v", schema, err),
			Remedy:   "grant the dmt role read access to pg_class/pg_namespace, fix the connection issue, or re-run with --confirm-backup to acknowledge that drop_recreate may destroy data",
		}}
	}
	return []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "backup.acknowledgment",
		Side:     req.Side,
		Message:  fmt.Sprintf("target schema %q has non-empty table %q; drop_recreate will destroy this data", schema, name),
		Remedy:   "back up the target, then re-run with --confirm-backup (or switch to target_mode: upsert)",
	}}
}

// checkConnectionPG runs `SELECT 1`. The other checks all assume the DB is
// reachable; if this fails everything downstream will too, so emit a single
// connection-level error and let callers short-circuit (orchestrator may
// choose to skip remaining checks on a hard connection failure).
func checkConnectionPG(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	return shared.CheckConnection(ctx, db, side, shared.ConnectionCheckConfig{
		Remedy: "verify the host/port/credentials in your config and that the PostgreSQL server is reachable from this host",
	})
}

// checkVersionPG verifies the server is on a supported major version. PG 12
// is the floor because earlier versions lack features dmt's COPY+ON CONFLICT
// path assumes (filterable indexes, INSERT...ON CONFLICT mature behavior).
// PG 11 hit EOL in 2023; anything older is unsupported by the upstream
// project too.
func checkVersionPG(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var serverVersionNum string
	if err := db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&serverVersionNum); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not read server_version_num: %v", err),
			Remedy:   "no remedy required if connection itself works — this check is informational",
		}}
	}
	n, err := strconv.Atoi(strings.TrimSpace(serverVersionNum))
	if err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("server_version_num %q is not a number", serverVersionNum),
		}}
	}
	// PG packs versions as e.g. 120006 → 12.6, 160001 → 16.1.
	major := n / 10000
	if major < 12 {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("PostgreSQL %d is below the supported floor (PG 12); end-of-life", major),
			Remedy:   "upgrade the PostgreSQL server to a supported major version (12 or newer)",
		}}
	}
	return nil
}

// checkEncodingPG verifies the server's client_encoding is UTF8. dmt assumes
// UTF-8 on both sides for cross-engine identifier sanitization and text
// transport; LATIN1/WIN1252 targets risk silent mojibake.
func checkEncodingPG(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var encoding string
	if err := db.QueryRowContext(ctx, "SHOW server_encoding").Scan(&encoding); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "encoding.server",
			Side:     side,
			Message:  fmt.Sprintf("could not read server_encoding: %v", err),
		}}
	}
	if strings.ToUpper(encoding) != "UTF8" {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "encoding.server",
			Side:     side,
			Message:  fmt.Sprintf("server_encoding is %q; dmt assumes UTF8 and may mis-transcode non-ASCII text", encoding),
			Remedy:   "consider migrating to a UTF8-encoded database; otherwise verify your data is ASCII-clean",
		}}
	}
	return nil
}

// checkPoolHeadroomPG ensures the server has enough free connection slots
// for the planned migration. Compares max_connections against the current
// session count, leaving a margin of 5 over migration.workers — enough for
// the writer pool plus dmt's metadata queries plus a few reserved slots for
// the operator's own tools.
func checkPoolHeadroomPG(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	var maxConns, current int
	if err := db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConns); err != nil {
		// Reading max_connections via SHOW requires a normal role; if it
		// fails, surface as info — the migration can still proceed and
		// connection errors at runtime will be obvious.
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read max_connections: %v", err),
		}}
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid()").Scan(&current); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read pg_stat_activity: %v", err),
			Remedy:   "grant pg_read_all_stats to the dmt role for accurate pool-headroom checks",
		}}
	}
	return shared.PoolHeadroomFinding(
		req,
		int64(maxConns),
		int64(current),
		"increase max_connections, reduce migration.workers, or wait until existing connections drain",
	)
}

// checkPrivilegesPG verifies the connected role has the privileges
// downstream phases will need. Uses has_*_privilege() built-ins, which
// honor role inheritance correctly (unlike querying information_schema
// directly). Each check returns either a discrete finding or nil.
func checkPrivilegesPG(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	var findings []driver.PreFlightFinding
	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		schema = "public"
	}

	if req.Side == driver.PreFlightSideSource {
		// Source needs SELECT on the schema's tables. has_schema_privilege
		// with USAGE covers the ability to access objects in the schema at
		// all; per-table SELECT is granular but USAGE is the cheap precondition.
		findings = shared.AppendNonNilFinding(findings, pgHasSchemaPriv(ctx, db, schema, "USAGE", req.Side))
		return findings
	}

	// Both target modes need USAGE + CREATE on the schema (for either
	// CREATE TABLE in drop_recreate or temp staging in upsert).
	findings = shared.AppendNonNilFinding(findings, pgHasSchemaPriv(ctx, db, schema, "USAGE", req.Side))
	findings = shared.AppendNonNilFinding(findings, pgHasSchemaPriv(ctx, db, schema, "CREATE", req.Side))
	return findings
}

func pgHasSchemaPriv(ctx context.Context, db *sql.DB, schema, priv string, side driver.PreFlightSide) *driver.PreFlightFinding {
	var ok bool
	if err := db.QueryRowContext(ctx,
		"SELECT has_schema_privilege(current_user, $1, $2)",
		schema, priv).Scan(&ok); err != nil {
		return &driver.PreFlightFinding{
			Severity: driver.SeverityError,
			Check:    "privileges.schema_" + strings.ToLower(priv),
			Side:     side,
			Message:  fmt.Sprintf("could not check %s privilege on schema %q: %v", priv, schema, err),
			Remedy:   fmt.Sprintf("verify schema %q exists and the connected role can resolve it", schema),
		}
	}
	if !ok {
		return &driver.PreFlightFinding{
			Severity: driver.SeverityError,
			Check:    "privileges.schema_" + strings.ToLower(priv),
			Side:     side,
			Message:  fmt.Sprintf("connected role lacks %s on schema %q", priv, schema),
			Remedy:   fmt.Sprintf("GRANT %s ON SCHEMA %q TO current_user;", priv, schema),
		}
	}
	return nil
}

// checkDiskSpacePG compares the current database's size + the migration's
// estimated bytes × 1.5 overhead against the tablespace's reported size on
// disk. The overhead factor accounts for WAL, indexes built after data
// load, and free-list churn. Only runs on the target side and when an
// estimate was supplied.
func checkDiskSpacePG(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if req.Side != driver.PreFlightSideTarget || req.EstimatedBytes <= 0 {
		return nil
	}
	// PG can't report free disk via a portable SQL query without filesystem
	// access. We emit an info-only check that shows the estimate vs the
	// current database size so the operator can eyeball the gap. Real
	// free-space monitoring belongs to the SRE stack (#229 covers that
	// surface), not to a one-shot preflight probe.
	var dbSize int64
	if err := db.QueryRowContext(ctx, "SELECT pg_database_size(current_database())").Scan(&dbSize); err != nil {
		return nil
	}
	return []driver.PreFlightFinding{{
		Severity: driver.SeverityInfo,
		Check:    "disk.estimate",
		Side:     req.Side,
		Message: fmt.Sprintf("target db is %.1f GiB now; migration plans to add ~%.1f GiB (×1.5 overhead = %.1f GiB)",
			gib(dbSize), gib(req.EstimatedBytes), gib(req.EstimatedBytes*3/2)),
		Remedy: "verify the target volume has enough free space before starting; preflight cannot inspect the filesystem from SQL",
	}}
}

func gib(b int64) float64 { return float64(b) / (1 << 30) }
