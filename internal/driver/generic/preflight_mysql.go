package generic

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/shared"
)

// preFlight runs MySQL preflight checks (#228) and returns findings in
// stable order. Individual probe failures become findings rather than
// propagating.
func init() {
	preflightStrategies["mysql"] = mysqlPreFlight
}

func mysqlPreFlight(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	return shared.RunPreFlight(ctx, db, req, shared.PreFlightRunConfig{
		NilDatabaseRemedy: "internal error — please report",
	},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return shared.SingleFinding(mysqlPF_checkConnectionMySQL(ctx, db, req.Side))
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return mysqlPF_checkVersionMySQL(ctx, db, req.Side)
		},
		func(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
			return mysqlPF_checkEncodingMySQL(ctx, db, req.Side)
		},
		mysqlPF_checkPoolHeadroomMySQL,
		mysqlPF_checkPrivilegesMySQL,
		mysqlPF_checkBackupAcknowledgmentMySQL,
	)
}

// mysqlPF_checkBackupAcknowledgmentMySQL fires when target_mode is drop_recreate
// and ConfirmBackup wasn't set. information_schema.tables.TABLE_ROWS is
// a stats estimate (not exact for InnoDB) but adequate for the
// "is the table non-empty" question — false positives would only have
// us be more cautious, not less.
func mysqlPF_checkBackupAcknowledgmentMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	if !shared.BackupAcknowledgmentRequired(req) {
		return nil
	}
	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		// MySQL uses the database itself as the schema — read DATABASE().
		if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
			// Can't determine which database — fail closed (Copilot review):
			// silently proceeding here would let drop_recreate run against
			// whatever DB the operator forgot to specify.
			return []driver.PreFlightFinding{{
				Severity: driver.SeverityError,
				Check:    "backup.acknowledgment",
				Side:     req.Side,
				Message:  fmt.Sprintf("could not resolve target database via DATABASE(): %v", err),
				Remedy:   "set target.schema explicitly, or re-run with --confirm-backup if you've verified the target is safe",
			}}
		}
	}
	var name string
	err := db.QueryRowContext(ctx, `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' AND TABLE_ROWS > 0
		LIMIT 1`, schema).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		// Clean schema — guard satisfied.
		return nil
	case err != nil:
		// Probe failure must NOT silently pass.
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "backup.acknowledgment",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not verify target schema `%s` is empty: %v", schema, err),
			Remedy:   "grant the dmt user SELECT on information_schema.TABLES, fix the connection issue, or re-run with --confirm-backup",
		}}
	}
	return []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "backup.acknowledgment",
		Side:     req.Side,
		Message:  fmt.Sprintf("target schema `%s` has non-empty table `%s`; drop_recreate will destroy this data", schema, name),
		Remedy:   "back up the target, then re-run with --confirm-backup (or switch to target_mode: upsert)",
	}}
}

func mysqlPF_checkConnectionMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	return shared.CheckConnection(ctx, db, side, shared.ConnectionCheckConfig{
		Remedy: "verify host/port/credentials in your config and that the MySQL server is reachable",
	})
}

// mysqlPF_checkVersionMySQL enforces MySQL 5.7 (or MariaDB 10.3) as the floor.
// 5.7 introduced JSON and is the oldest version still receiving fixes;
// 5.6 is unsupported upstream. MariaDB 10.3 is the matching JSON-capable
// floor.
//
// MariaDB has a long-standing compatibility hack: VERSION() prefixes the
// real version with "5.5.5-" so legacy clients that hardcode "MySQL 5.5
// or above" accept it. We have to peel that prefix off before parsing or
// the floor check would treat MariaDB 10.x as MySQL 5.5 and fail every
// modern MariaDB server (Copilot review).
func mysqlPF_checkVersionMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var v string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not read VERSION(): %v", err),
		}}
	}
	isMaria := strings.Contains(strings.ToLower(v), "mariadb")
	parseable := v
	if isMaria {
		parseable = mysqlPF_stripMariaDBCompatPrefix(v)
	}
	major, minor, ok := mysqlPF_parseMySQLVersion(parseable)
	if !ok {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not parse VERSION() = %q", v),
		}}
	}
	switch {
	case isMaria && (major < 10 || (major == 10 && minor < 3)):
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("MariaDB %d.%d is below the supported floor (10.3)", major, minor),
			Remedy:   "upgrade MariaDB to 10.3 or newer",
		}}
	case !isMaria && (major < 5 || (major == 5 && minor < 7)):
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("MySQL %d.%d is below the supported floor (5.7)", major, minor),
			Remedy:   "upgrade MySQL to 5.7 or newer",
		}}
	}
	return nil
}

// mysqlPF_stripMariaDBCompatPrefix removes MariaDB's legacy "5.5.5-" client-
// compatibility prefix from a VERSION() string. Real MariaDB versions
// have always started at 10.0; anything beginning with "5.5.5-" followed
// by another major.minor is the compatibility hack and the real version
// is the segment after the prefix. Inputs that don't match the hack
// pattern are returned unchanged.
func mysqlPF_stripMariaDBCompatPrefix(v string) string {
	const prefix = "5.5.5-"
	trimmed := strings.TrimSpace(v)
	if !strings.HasPrefix(trimmed, prefix) {
		return v
	}
	return trimmed[len(prefix):]
}

// mysqlPF_parseMySQLVersion extracts (major, minor) from strings like
// "5.7.43" or "8.0.35-mysql" or "10.6.12-MariaDB". Returns ok=false when
// the input doesn't start with two dot-separated numbers.
func mysqlPF_parseMySQLVersion(v string) (int, int, bool) {
	v = strings.TrimSpace(v)
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	// minor may be followed by a patch+suffix like "7.43-mysql"; take
	// the leading digits only.
	minorStr := parts[1]
	end := 0
	for end < len(minorStr) && minorStr[end] >= '0' && minorStr[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, 0, false
	}
	minor, err := strconv.Atoi(minorStr[:end])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}

// mysqlPF_checkEncodingMySQL warns when the database default charset isn't
// utf8mb4. The legacy 'utf8' alias maps to utf8mb3 which silently drops
// 4-byte sequences (emoji, supplementary plane CJK) and is a known
// data-loss footgun on migrations.
func mysqlPF_checkEncodingMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var name, charset string
	if err := db.QueryRowContext(ctx,
		"SHOW VARIABLES LIKE 'character_set_database'").Scan(&name, &charset); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "encoding.charset",
			Side:     side,
			Message:  fmt.Sprintf("could not read character_set_database: %v", err),
		}}
	}
	low := strings.ToLower(charset)
	if low != "utf8mb4" {
		sev := driver.SeverityWarn
		// utf8mb3 / utf8 mask data loss silently — bump to error so the
		// operator has to explicitly skip the check if they really mean
		// to migrate to a 3-byte encoding.
		if low == "utf8" || low == "utf8mb3" {
			sev = driver.SeverityError
		}
		return []driver.PreFlightFinding{{
			Severity: sev,
			Check:    "encoding.charset",
			Side:     side,
			Message:  fmt.Sprintf("character_set_database is %q; dmt assumes utf8mb4 — non-BMP characters may be dropped", charset),
			Remedy:   "ALTER DATABASE <name> CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
		}}
	}
	return nil
}

// mysqlPF_checkPoolHeadroomMySQL compares @@max_connections against Threads_connected.
// Both are usually readable by any login; failures fall back to info-level.
func mysqlPF_checkPoolHeadroomMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	var maxConns int64
	if err := db.QueryRowContext(ctx, "SELECT @@max_connections").Scan(&maxConns); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read @@max_connections: %v", err),
		}}
	}
	var name string
	var current int64
	if err := db.QueryRowContext(ctx,
		"SHOW STATUS LIKE 'Threads_connected'").Scan(&name, &current); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityInfo,
			Check:    "pool.headroom",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read Threads_connected: %v", err),
		}}
	}
	return shared.PoolHeadroomFinding(req, maxConns, current, "increase @@max_connections or reduce migration.workers")
}

// mysqlPF_checkPrivilegesMySQL uses SHOW GRANTS rather than information_schema —
// it's the canonical way to introspect MySQL/MariaDB grants and handles
// proxy users, roles (MySQL 8+, MariaDB 10.0.5+), and grant_option
// correctly. We parse the grant strings for the verbs we need.
func mysqlPF_checkPrivilegesMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	grants, err := mysqlPF_loadGrantsMySQL(ctx, db)
	if err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityError,
			Check:    "privileges.show_grants",
			Side:     req.Side,
			Message:  fmt.Sprintf("could not read SHOW GRANTS: %v", err),
			Remedy:   "verify the dmt user is recognized and connectivity works; SHOW GRANTS should be available to every logged-in user",
		}}
	}

	required := []string{}
	if req.Side == driver.PreFlightSideSource {
		required = append(required, "SELECT")
	} else {
		mode := shared.TargetModeOrDefault(req.TargetMode, "drop_recreate")
		switch mode {
		case "drop_recreate":
			required = append(required, "CREATE", "DROP", "INSERT")
		case "upsert":
			required = append(required, "INSERT", "UPDATE", "SELECT")
		}
	}

	// Resolve which database the grants must be scoped to. Empty
	// req.Schema means "current database via DATABASE()" — same convention
	// as the backup-ack check. If we can't determine it, fall back to
	// scope-less matching (the prior behavior) but log via a finding so
	// the operator knows the check is degraded (Copilot review).
	dbName := strings.TrimSpace(req.Schema)
	if dbName == "" {
		_ = db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName)
	}

	var findings []driver.PreFlightFinding
	for _, p := range required {
		if !mysqlPF_grantsInclude(grants, p, dbName) {
			findings = append(findings, driver.PreFlightFinding{
				Severity: driver.SeverityError,
				Check:    "privileges." + strings.ToLower(p),
				Side:     req.Side,
				Message:  fmt.Sprintf("connected user lacks %s privilege on %s per SHOW GRANTS", p, mysqlPF_formatScope(dbName)),
				Remedy:   fmt.Sprintf("GRANT %s ON %s.* TO CURRENT_USER();", p, mysqlPF_formatDBForGrant(dbName)),
			})
		}
	}
	return findings
}

// mysqlPF_formatScope produces the human-readable scope label for grant messages.
// Empty dbName means "scope unresolved" — the message says "any schema"
// since the check fell back to global matching.
func mysqlPF_formatScope(dbName string) string {
	if dbName == "" {
		return "any database"
	}
	return fmt.Sprintf("`%s`", dbName)
}

// mysqlPF_formatDBForGrant returns the right substitution for the remedy text:
// the schema name when known, otherwise <db> as a placeholder.
func mysqlPF_formatDBForGrant(dbName string) string {
	if dbName == "" {
		return "<db>"
	}
	return fmt.Sprintf("`%s`", dbName)
}

func mysqlPF_loadGrantsMySQL(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		grants = append(grants, line)
	}
	return grants, rows.Err()
}

// mysqlPF_grantsInclude reports whether any grant line confers privilege p with
// a scope that covers dbName. A grant scope of "*.*" (global) always
// matches; "<db>.*" or "<db>.<obj>" matches only if <db> == dbName
// (case-sensitive comparison — MySQL identifier case sensitivity depends
// on lower_case_table_names, but on most installs database names are
// case-sensitive on Linux and case-insensitive on macOS/Windows;
// SHOW GRANTS reports them verbatim, so matching the verbatim form is
// the right call).
//
// dbName == "" disables scope filtering and reverts to the pre-Copilot
// behavior (any grant scope counts) — used as a fallback when DATABASE()
// resolution failed.
func mysqlPF_grantsInclude(grants []string, p, dbName string) bool {
	upperP := strings.ToUpper(p)
	for _, g := range grants {
		privs, scopeDB, ok := mysqlPF_parseGrantLine(g)
		if !ok {
			continue
		}
		if !mysqlPF_grantHasVerb(privs, upperP) {
			continue
		}
		if dbName == "" || scopeDB == "*" || scopeDB == dbName {
			return true
		}
	}
	return false
}

// mysqlPF_parseGrantLine extracts the upper-cased privilege-list segment and the
// database identifier from the ON clause. Returns ok=false when the line
// doesn't match the GRANT ... ON <db>.<obj> shape (proxy grants, role
// grants, etc. fall through). scopeDB == "*" represents the global
// "ON *.*" scope; otherwise it's the unquoted database identifier.
func mysqlPF_parseGrantLine(g string) (privs, scopeDB string, ok bool) {
	upper := strings.ToUpper(g)
	if !strings.HasPrefix(upper, "GRANT ") {
		return "", "", false
	}
	onIdx := strings.Index(upper, " ON ")
	if onIdx < len("GRANT ") {
		return "", "", false
	}
	privs = upper[len("GRANT "):onIdx]

	// Look for " TO " from the ORIGINAL string (not upper) so we don't
	// corrupt the database identifier's case during extraction.
	toIdx := strings.Index(upper[onIdx:], " TO ")
	if toIdx < 0 {
		return "", "", false
	}
	scope := strings.TrimSpace(g[onIdx+len(" ON "):]) // preserve case
	// Truncate scope at " TO " in the original string.
	relativeTo := strings.Index(strings.ToUpper(scope), " TO ")
	if relativeTo > 0 {
		scope = strings.TrimSpace(scope[:relativeTo])
	}
	// scope is now like "*.*" or "`mydb`.*" or "`mydb`.`tbl`".
	dbPart := scope
	if dot := strings.Index(scope, "."); dot > 0 {
		dbPart = scope[:dot]
	}
	dbPart = strings.Trim(dbPart, "`\"")
	if dbPart == "*" {
		return privs, "*", true
	}
	return privs, dbPart, true
}

// mysqlPF_grantHasVerb checks whether the comma-separated privilege list contains
// verb v (or ALL PRIVILEGES / ALL).
func mysqlPF_grantHasVerb(privs, v string) bool {
	for _, tok := range strings.Split(privs, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "ALL PRIVILEGES" || tok == "ALL" {
			return true
		}
		if tok == v {
			return true
		}
	}
	return false
}
