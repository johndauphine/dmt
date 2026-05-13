package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
)

// preFlight runs MySQL preflight checks (#228) and returns findings in
// stable order. Individual probe failures become findings rather than
// propagating.
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
	findings = appendIfNonNilMySQL(findings, checkConnectionMySQL(ctx, db, req.Side))
	findings = append(findings, checkVersionMySQL(ctx, db, req.Side)...)
	findings = append(findings, checkEncodingMySQL(ctx, db, req.Side)...)
	findings = append(findings, checkPoolHeadroomMySQL(ctx, db, req)...)
	findings = append(findings, checkPrivilegesMySQL(ctx, db, req)...)
	findings = append(findings, checkBackupAcknowledgmentMySQL(ctx, db, req)...)
	return findings
}

// checkBackupAcknowledgmentMySQL fires when target_mode is drop_recreate
// and ConfirmBackup wasn't set. information_schema.tables.TABLE_ROWS is
// a stats estimate (not exact for InnoDB) but adequate for the
// "is the table non-empty" question — false positives would only have
// us be more cautious, not less.
func checkBackupAcknowledgmentMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
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
		// MySQL uses the database itself as the schema — read DATABASE().
		if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
			return nil
		}
	}
	var name string
	err := db.QueryRowContext(ctx, `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' AND TABLE_ROWS > 0
		LIMIT 1`, schema).Scan(&name)
	if err != nil {
		return nil
	}
	return []driver.PreFlightFinding{{
		Severity: driver.SeverityError,
		Check:    "backup.acknowledgment",
		Side:     req.Side,
		Message:  fmt.Sprintf("target schema `%s` has non-empty table `%s`; drop_recreate will destroy this data", schema, name),
		Remedy:   "back up the target, then re-run with --confirm-backup (or switch to target_mode: upsert)",
	}}
}

func checkConnectionMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) *driver.PreFlightFinding {
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return &driver.PreFlightFinding{
			Severity: driver.SeverityError,
			Check:    "connection.ping",
			Side:     side,
			Message:  fmt.Sprintf("connection ping failed: %v", err),
			Remedy:   "verify host/port/credentials in your config and that the MySQL server is reachable",
		}
	}
	return nil
}

// checkVersionMySQL enforces MySQL 5.7 (or MariaDB 10.3) as the floor.
// 5.7 introduced JSON and is the oldest version still receiving fixes;
// 5.6 is unsupported upstream. MariaDB 10.3 is the matching JSON-capable
// floor.
func checkVersionMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
	var v string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not read VERSION(): %v", err),
		}}
	}
	major, minor, ok := parseMySQLVersion(v)
	if !ok {
		return []driver.PreFlightFinding{{
			Severity: driver.SeverityWarn,
			Check:    "version.server",
			Side:     side,
			Message:  fmt.Sprintf("could not parse VERSION() = %q", v),
		}}
	}
	isMaria := strings.Contains(strings.ToLower(v), "mariadb")
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

// parseMySQLVersion extracts (major, minor) from strings like
// "5.7.43" or "8.0.35-mysql" or "10.6.12-MariaDB". Returns ok=false when
// the input doesn't start with two dot-separated numbers.
func parseMySQLVersion(v string) (int, int, bool) {
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

// checkEncodingMySQL warns when the database default charset isn't
// utf8mb4. The legacy 'utf8' alias maps to utf8mb3 which silently drops
// 4-byte sequences (emoji, supplementary plane CJK) and is a known
// data-loss footgun on migrations.
func checkEncodingMySQL(ctx context.Context, db *sql.DB, side driver.PreFlightSide) []driver.PreFlightFinding {
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

// checkPoolHeadroomMySQL compares @@max_connections against Threads_connected.
// Both are usually readable by any login; failures fall back to info-level.
func checkPoolHeadroomMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
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
	needed := int64(req.Workers + 5)
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
			Remedy: "increase @@max_connections or reduce migration.workers",
		}}
	}
	return nil
}

// checkPrivilegesMySQL uses SHOW GRANTS rather than information_schema —
// it's the canonical way to introspect MySQL/MariaDB grants and handles
// proxy users, roles (MySQL 8+, MariaDB 10.0.5+), and grant_option
// correctly. We parse the grant strings for the verbs we need.
func checkPrivilegesMySQL(ctx context.Context, db *sql.DB, req driver.PreFlightRequest) []driver.PreFlightFinding {
	grants, err := loadGrantsMySQL(ctx, db)
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
		mode := strings.ToLower(strings.TrimSpace(req.TargetMode))
		if mode == "" {
			mode = "drop_recreate"
		}
		switch mode {
		case "drop_recreate":
			required = append(required, "CREATE", "DROP", "INSERT")
		case "upsert":
			required = append(required, "INSERT", "UPDATE", "SELECT")
		}
	}

	var findings []driver.PreFlightFinding
	for _, p := range required {
		if !grantsInclude(grants, p) {
			findings = append(findings, driver.PreFlightFinding{
				Severity: driver.SeverityError,
				Check:    "privileges." + strings.ToLower(p),
				Side:     req.Side,
				Message:  fmt.Sprintf("connected user lacks %s privilege per SHOW GRANTS", p),
				Remedy:   fmt.Sprintf("GRANT %s ON <db>.* TO CURRENT_USER();", p),
			})
		}
	}
	return findings
}

func loadGrantsMySQL(ctx context.Context, db *sql.DB) ([]string, error) {
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

// grantsInclude reports whether any grant line confers privilege p. ALL
// PRIVILEGES is treated as covering anything; partial-grant strings like
// "GRANT SELECT, INSERT ON ..." are matched on the verb list inside the
// GRANT prefix.
func grantsInclude(grants []string, p string) bool {
	upperP := strings.ToUpper(p)
	for _, g := range grants {
		ug := strings.ToUpper(g)
		// "GRANT ALL PRIVILEGES ON ..." or "GRANT ALL ON ..."
		if strings.Contains(ug, "GRANT ALL PRIVILEGES") || strings.HasPrefix(ug, "GRANT ALL ON") {
			return true
		}
		// "GRANT SELECT, INSERT, UPDATE ON ..." — extract the privilege
		// list between GRANT and ON.
		if idx := strings.Index(ug, " ON "); idx > len("GRANT ") {
			privList := ug[len("GRANT "):idx]
			for _, tok := range strings.Split(privList, ",") {
				if strings.TrimSpace(tok) == upperP {
					return true
				}
			}
		}
	}
	return false
}

func appendIfNonNilMySQL(s []driver.PreFlightFinding, f *driver.PreFlightFinding) []driver.PreFlightFinding {
	if f == nil {
		return s
	}
	return append(s, *f)
}
