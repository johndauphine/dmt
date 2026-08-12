package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/pool"
)

// StrictSnapshotEpochOptions supplies run identity and connection settings for
// engines whose migration-wide view is a separate database.
type StrictSnapshotEpochOptions struct {
	RunID          string
	SourceConfig   config.SourceConfig
	MaxConnections int
	Resume         bool
}

// BeginStrictSnapshotEpochForRun creates or reconnects the engine-native
// migration-wide view. PostgreSQL retains its existing exported snapshot.
func BeginStrictSnapshotEpochForRun(ctx context.Context, srcPool pool.SourcePool, opts StrictSnapshotEpochOptions) (*StrictSnapshotEpoch, error) {
	if srcPool == nil || srcPool.DB() == nil {
		return nil, fmt.Errorf("strict_consistency_scope: migration requires an open source database connection")
	}
	switch strings.ToLower(srcPool.DBType()) {
	case "postgres", "postgresql", "pg":
		return BeginStrictSnapshotEpoch(ctx, srcPool)
	case "mssql", "sqlserver", "sql-server":
		return beginMSSQLSnapshotEpoch(ctx, srcPool, opts)
	default:
		return nil, fmt.Errorf("strict_consistency_scope: migration requires a PostgreSQL or SQL Server source; got %q", srcPool.DBType())
	}
}

type mssqlDataFile struct {
	logical  string
	physical string
}

func beginMSSQLSnapshotEpoch(ctx context.Context, live pool.SourcePool, opts StrictSnapshotEpochOptions) (*StrictSnapshotEpoch, error) {
	name, err := mssqlSnapshotName(opts.RunID)
	if err != nil {
		return nil, err
	}
	exists, err := mssqlDatabaseExists(ctx, live.DB(), name)
	if err != nil {
		return nil, fmt.Errorf("checking SQL Server strict snapshot %s: %w", name, err)
	}
	if opts.Resume {
		if !exists {
			return nil, fmt.Errorf("strict_consistency resume requires surviving SQL Server snapshot [%s]; restart the migration because the original source instant is unavailable", name)
		}
	} else {
		if exists {
			return nil, fmt.Errorf("creating SQL Server strict snapshot: database [%s] already exists; drop the orphan or resume its owning run", name)
		}
		if err := createMSSQLDatabaseSnapshot(ctx, live.DB(), opts.SourceConfig.Database, name); err != nil {
			return nil, err
		}
	}

	snapshotCfg := opts.SourceConfig
	snapshotCfg.Database = name
	snapshotPool, err := pool.NewSourcePool(&snapshotCfg, opts.MaxConnections)
	if err != nil {
		if !opts.Resume {
			dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			dropErr := dropMSSQLDatabaseSnapshot(dropCtx, live.DB(), name)
			cancel()
			if dropErr != nil {
				logging.Warn("SQL Server strict snapshot [%s] could not be dropped after snapshot-pool setup failed and remains as an orphan: %v", name, dropErr)
			}
		}
		return nil, fmt.Errorf("opening SQL Server strict snapshot [%s]: %w", name, err)
	}
	epoch := &StrictSnapshotEpoch{
		sourcePool:   snapshotPool,
		dbType:       "mssql",
		snapshotName: name,
		startedAt:    time.Now(),
	}
	epoch.closeSource = func() {
		_ = snapshotPool.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := dropMSSQLDatabaseSnapshot(dropCtx, live.DB(), name); err != nil {
			logging.Warn("SQL Server strict snapshot [%s] could not be dropped and remains as an orphan: %v", name, err)
		}
	}
	return epoch, nil
}

func mssqlSnapshotName(runID string) (string, error) {
	var b strings.Builder
	for _, r := range strings.ToLower(runID) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
		if b.Len() >= 8 {
			break
		}
	}
	if b.Len() == 0 {
		return "", fmt.Errorf("creating SQL Server strict snapshot requires a non-empty run ID")
	}
	return "dmt_strict_" + b.String(), nil
}

func mssqlDatabaseExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, "SELECT CASE WHEN DB_ID(@p1) IS NULL THEN 0 ELSE 1 END", name).Scan(&exists)
	return exists == 1, err
}

func createMSSQLDatabaseSnapshot(ctx context.Context, db *sql.DB, sourceDB, snapshotName string) error {
	rows, err := db.QueryContext(ctx, `SELECT name, physical_name FROM sys.master_files WHERE database_id = DB_ID(@p1) AND type = 0 ORDER BY file_id`, sourceDB)
	if err != nil {
		return fmt.Errorf("enumerating SQL Server data files for snapshot: %w", err)
	}
	defer rows.Close()
	var files []mssqlDataFile
	for rows.Next() {
		var file mssqlDataFile
		if err := rows.Scan(&file.logical, &file.physical); err != nil {
			return fmt.Errorf("reading SQL Server data file for snapshot: %w", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("enumerating SQL Server data files for snapshot: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("creating SQL Server strict snapshot: source database %q has no data files", sourceDB)
	}
	clauses := make([]string, len(files))
	for i, file := range files {
		pos := strings.LastIndexAny(file.physical, `/\\`)
		dir := ""
		if pos >= 0 {
			dir = file.physical[:pos+1]
		}
		physical := fmt.Sprintf("%s%s_%d.ss", dir, snapshotName, i)
		clauses[i] = fmt.Sprintf("(NAME = %s, FILENAME = %s)", mssqlStringLiteral(file.logical), mssqlStringLiteral(physical))
	}
	query := fmt.Sprintf("CREATE DATABASE %s ON %s AS SNAPSHOT OF %s", mssqlQuoteIdentifier(snapshotName), strings.Join(clauses, ", "), mssqlQuoteIdentifier(sourceDB))
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating SQL Server strict snapshot [%s]: %w", snapshotName, err)
	}
	return nil
}

func dropMSSQLDatabaseSnapshot(ctx context.Context, db *sql.DB, name string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := db.ExecContext(ctx, "DROP DATABASE "+mssqlQuoteIdentifier(name)); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return lastErr
}

func mssqlQuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func mssqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}
