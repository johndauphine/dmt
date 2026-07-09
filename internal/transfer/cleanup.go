package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/target"
	"strconv"
	"strings"
)

// cleanupPartitionData removes any existing data for a partition's PK range (idempotent retry) - PostgreSQL version
func cleanupPartitionData(ctx context.Context, pgPool *pgxpool.Pool, schema string, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	pkCol := target.SanitizePGIdentifier(job.Table.PrimaryKey[0])
	tableName := target.SanitizePGIdentifier(job.Table.Name)

	query := fmt.Sprintf(
		`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
		schema, tableName, pkCol, pkCol,
	)

	_, err := pgPool.Exec(ctx, query, job.Partition.MinPK, job.Partition.MaxPK)
	return err
}

// cleanupPartitionDataGeneric removes partition data using the appropriate pool interface
func cleanupPartitionDataGeneric(ctx context.Context, tgtPool pool.TargetPool, schema string, job *Job) error {
	if job.Partition == nil || job.Partition.MinPK == nil {
		return nil
	}

	pkCol := job.Table.PrimaryKey[0]

	// Build query and args based on target type
	var query string
	var args []any

	switch tgtPool.DBType() {
	case "postgres":
		// PostgreSQL target - sanitize identifiers and use $N parameters
		sanitizedPK := target.SanitizePGIdentifier(pkCol)
		sanitizedTable := target.SanitizePGIdentifier(job.Table.Name)
		query = fmt.Sprintf(
			`DELETE FROM %s.%q WHERE %q >= $1 AND %q <= $2`,
			schema, sanitizedTable, sanitizedPK, sanitizedPK,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	case "mysql":
		// MySQL target - use backtick identifiers and ? positional parameters
		query = fmt.Sprintf(
			"DELETE FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` <= ?",
			schema, job.Table.Name, pkCol, pkCol,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	case "sqlite":
		// SQLite target - double-quoted identifiers, ? placeholders, no
		// schema qualification (SQLite has no schemas distinct from
		// attached databases).
		query = fmt.Sprintf(
			`DELETE FROM "%s" WHERE "%s" >= ? AND "%s" <= ?`,
			job.Table.Name, pkCol, pkCol,
		)
		args = []any{job.Partition.MinPK, job.Partition.MaxPK}
	case "mssql":
		// SQL Server target - use bracket identifiers and @p parameters
		query = fmt.Sprintf(
			`DELETE FROM [%s].[%s] WHERE [%s] >= @p1 AND [%s] <= @p2`,
			schema, job.Table.Name, pkCol, pkCol,
		)
		args = []any{sql.Named("p1", job.Partition.MinPK), sql.Named("p2", job.Partition.MaxPK)}
	default:
		return unsupportedKeysetCleanupError(tgtPool.DBType())
	}

	_, err := tgtPool.ExecRaw(ctx, query, args...)
	return err
}

// cleanupPartialData removes rows beyond the saved lastPK for chunk-level resume
func cleanupPartialData(ctx context.Context, tgtPool pool.TargetPool, schema, tableName, pkCol string, lastPK any, maxPK any) error {
	var deleteQuery string
	var args []any

	switch tgtPool.DBType() {
	case "postgres":
		// PostgreSQL target - sanitize identifiers
		sanitizedPK := target.SanitizePGIdentifier(pkCol)
		sanitizedTable := target.SanitizePGIdentifier(tableName)

		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM %s.%q WHERE %q > $1 AND %q <= $2`,
				schema, sanitizedTable, sanitizedPK, sanitizedPK)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM %s.%q WHERE %q > $1`,
				schema, sanitizedTable, sanitizedPK)
			args = []any{lastPK}
		}
	case "mysql":
		// MySQL target - use backtick identifiers and ? positional parameters
		if maxPK != nil {
			deleteQuery = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` > ? AND `%s` <= ?",
				schema, tableName, pkCol, pkCol)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` > ?",
				schema, tableName, pkCol)
			args = []any{lastPK}
		}
	case "sqlite":
		// SQLite target - double-quoted identifiers, ? placeholders, no
		// schema qualification.
		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" > ? AND "%s" <= ?`,
				tableName, pkCol, pkCol)
			args = []any{lastPK, maxPK}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" > ?`,
				tableName, pkCol)
			args = []any{lastPK}
		}
	case "mssql":
		// SQL Server target
		if maxPK != nil {
			deleteQuery = fmt.Sprintf(`DELETE FROM [%s].[%s] WHERE [%s] > @p1 AND [%s] <= @p2`,
				schema, tableName, pkCol, pkCol)
			args = []any{sql.Named("p1", lastPK), sql.Named("p2", maxPK)}
		} else {
			deleteQuery = fmt.Sprintf(`DELETE FROM [%s].[%s] WHERE [%s] > @p1`,
				schema, tableName, pkCol)
			args = []any{sql.Named("p1", lastPK)}
		}
	default:
		return unsupportedKeysetCleanupError(tgtPool.DBType())
	}

	rowsAffected, err := tgtPool.ExecRaw(ctx, deleteQuery, args...)
	if err != nil {
		return err
	}
	if rowsAffected > 0 {
		logging.Debug("Removed %d stale rows from %s beyond pk=%v", rowsAffected, tableName, lastPK)
	}
	return nil
}

func unsupportedKeysetCleanupError(dbType string) error {
	return fmt.Errorf("target %q does not support synchronous keyset cleanup; start a fresh run after truncating or recreating the target table", dbType)
}

func parseResumeRowNum(lastPK any) (int64, bool) {
	if lastPK == nil {
		return 0, false
	}
	switch v := lastPK.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func parseNumericPK(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// isTableNotFoundError reports whether err is a "table/relation does not
// exist" error from any supported engine. The pre-transfer truncate
// (transfer.go) treats this as benign — on a fresh drop_recreate run the
// table is created before transfer, but a defensive not-found must not be
// confused with a real failure (permission denied, lock timeout), which
// would otherwise be silently swallowed and resurface later as a confusing
// duplicate-key error against un-truncated stale rows (#619).
func isTableNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// Guard the ambiguous case first (codex review on #619): SQL Server
	// reports a permission-denied TRUNCATE as `Cannot find the object "x"
	// because it does not exist or you do not have permissions` — the same
	// wording it uses for a truly-absent table. Anything mentioning a
	// permission problem is NOT benign; surface it. Over-warning on a
	// genuinely-absent table with this exact wording is a harmless cosmetic
	// cost; swallowing a permission failure is the bug we're fixing.
	if strings.Contains(s, "permission") || strings.Contains(s, "denied") {
		return false
	}
	// postgres: relation "x" does not exist; mysql: table 'x' doesn't exist;
	// sqlite: no such table: x; mssql: invalid object name 'x'.
	switch {
	case strings.Contains(s, "does not exist"),
		strings.Contains(s, "doesn't exist"),
		strings.Contains(s, "no such table"),
		strings.Contains(s, "invalid object name"),
		strings.Contains(s, "cannot find the object"),
		strings.Contains(s, "unknown table"):
		return true
	}
	return false
}
