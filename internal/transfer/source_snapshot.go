package transfer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/source"
)

// sourceQueryer is the common query surface shared by sql.DB and sql.Tx.
// Pagination receives one through context so strict-consistency reads stay on
// a pinned transaction while ordinary transfers keep using the source pool.
type sourceQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type sourceQueryerContextKey struct{}

// sourceQueryerFactory gives every concurrent keyset worker a queryer of its
// own. Strict reader strategies populate it when sharing the coordinator's
// queryer would serialize workers back onto one connection.
type sourceQueryerFactory func(context.Context, int) (sourceQueryer, func(), error)

type sourceQueryerFactoryContextKey struct{}

// sharedSnapshot keeps PostgreSQL's exporting transaction open while reader
// transactions import its MVCC snapshot. PostgreSQL invalidates an exported
// snapshot as soon as the exporting transaction ends, so the lead must outlive
// every joined reader.
type sharedSnapshot struct {
	lead *sql.Tx
	id   string
	db   *sql.DB
}

func (s *sharedSnapshot) release() { _ = s.lead.Rollback() }

func (s *sharedSnapshot) joinReader(ctx context.Context) (*sql.Tx, func(), error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("starting PostgreSQL strict_consistency reader transaction: %w", err)
	}

	// PostgreSQL requires SET TRANSACTION SNAPSHOT to be the first statement
	// after BEGIN. The snapshot ID comes from pg_export_snapshot(), but quote it
	// as a SQL literal nevertheless so this helper keeps that invariant local.
	statement := "SET TRANSACTION SNAPSHOT '" + strings.ReplaceAll(s.id, "'", "''") + "'"
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		_ = tx.Rollback()
		return nil, nil, fmt.Errorf("importing PostgreSQL strict_consistency snapshot: %w", err)
	}

	return tx, func() { _ = tx.Rollback() }, nil
}

// StrictSnapshotEpoch owns one migration-wide source view. PostgreSQL anchors
// an exported MVCC snapshot; SQL Server points reads at a database snapshot.
type StrictSnapshotEpoch struct {
	snapshot     *sharedSnapshot
	sourcePool   pool.SourcePool
	closeSource  func()
	dbType       string
	snapshotName string
	startedAt    time.Time
	closeOnce    sync.Once
}

// BeginStrictSnapshotEpoch opens a PostgreSQL-only migration-wide source view.
// It fails before target mutation for unsupported sources, rather than falling
// back to an unrelated per-table snapshot.
func BeginStrictSnapshotEpoch(ctx context.Context, srcPool pool.SourcePool) (*StrictSnapshotEpoch, error) {
	if srcPool == nil || srcPool.DB() == nil {
		return nil, fmt.Errorf("strict_consistency_scope: migration requires an open PostgreSQL source database connection")
	}
	strategyName, _, err := resolveStrictReaderStrategyForDBType(srcPool.DBType())
	if err != nil {
		return nil, err
	}
	if strategyName != strictParallelExportedSnapshot {
		return nil, fmt.Errorf("strict_consistency_scope: migration requires a PostgreSQL source; got %q", srcPool.DBType())
	}
	snapshot, err := beginPostgresExportedSnapshot(ctx, srcPool.DB())
	if err != nil {
		return nil, err
	}
	return &StrictSnapshotEpoch{snapshot: snapshot, dbType: "postgres", startedAt: time.Now()}, nil
}

// Close releases the lead transaction that anchors the exported view. sql.Tx
// rollback is intentionally safe to call after a cancellation or failed job.
func (e *StrictSnapshotEpoch) Close() {
	if e == nil {
		return
	}
	e.closeOnce.Do(func() {
		if e.snapshot != nil {
			e.snapshot.release()
		}
		if e.closeSource != nil {
			e.closeSource()
		}
	})
}

// Age reports how long the migration-wide snapshot has held PostgreSQL's
// vacuum horizon. It is used only for operational logging.
func (e *StrictSnapshotEpoch) Age() time.Duration {
	if e == nil {
		return 0
	}
	return time.Since(e.startedAt)
}

func (e *StrictSnapshotEpoch) queryer() sourceQueryer {
	if e == nil {
		return nil
	}
	if e.snapshot != nil {
		return e.snapshot.lead
	}
	if e.sourcePool != nil {
		return e.sourcePool.DB()
	}
	return nil
}

func (e *StrictSnapshotEpoch) queryerForWorker(ctx context.Context, workerID int) (sourceQueryer, func(), error) {
	if e == nil {
		return nil, nil, fmt.Errorf("strict snapshot epoch is unavailable for reader %d", workerID)
	}
	if e.snapshot != nil {
		return e.snapshot.joinReader(ctx)
	}
	if e.sourcePool != nil && e.sourcePool.DB() != nil {
		return e.sourcePool.DB(), func() {}, nil
	}
	return nil, nil, fmt.Errorf("strict snapshot epoch is unavailable for reader %d", workerID)
}

// DBType identifies the epoch mechanism for operational logging.
func (e *StrictSnapshotEpoch) DBType() string {
	if e == nil {
		return ""
	}
	return e.dbType
}

// SnapshotName is non-empty for SQL Server database-snapshot epochs.
func (e *StrictSnapshotEpoch) SnapshotName() string {
	if e == nil {
		return ""
	}
	return e.snapshotName
}

// SourcePool returns the dedicated pool that reads an external snapshot
// database. It is nil for PostgreSQL, whose epoch is imported transaction by
// transaction instead of exposed as a separate database.
func (e *StrictSnapshotEpoch) SourcePool() pool.SourcePool {
	if e == nil {
		return nil
	}
	return e.sourcePool
}

func beginPostgresExportedSnapshot(ctx context.Context, db *sql.DB) (*sharedSnapshot, error) {
	options, err := strictSnapshotTxOptions("postgres")
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("starting PostgreSQL strict_consistency snapshot: %w", err)
	}
	var snapshotID string
	if err := tx.QueryRowContext(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("exporting PostgreSQL strict_consistency snapshot: %w", err)
	}
	if snapshotID == "" {
		_ = tx.Rollback()
		return nil, fmt.Errorf("exporting PostgreSQL strict_consistency snapshot: received an empty snapshot ID")
	}
	return &sharedSnapshot{lead: tx, id: snapshotID, db: db}, nil
}

// beginStrictSourceSnapshot starts the table-scoped source transaction before
// target preparation changes anything. Ordinary strict pagination receives
// that transaction through the context; PostgreSQL keyset workers instead
// import its exported snapshot into their own transactions (#640, #662).
//
// PostgreSQL and MySQL use repeatable-read MVCC snapshots. SQL Server uses
// serializable range locking, which is a stable (but potentially blocking)
// view without requiring database-wide snapshot-isolation configuration. SQLite
// uses a serializable read transaction. Engines without a suitable transaction
// contract fail before the transfer can truncate or write a target table.
func beginStrictSourceSnapshot(ctx context.Context, srcPool pool.SourcePool, table source.Table) (context.Context, func(), error) {
	return beginStrictSourceSnapshotWithOptions(ctx, srcPool, table, strictSnapshotBeginOptions{workerSessions: 1})
}

type strictSnapshotBeginOptions struct {
	workerSessions int
	auditEvent     func(string, map[string]any)
}

func beginStrictSourceSnapshotForJob(ctx context.Context, srcPool pool.SourcePool, job Job, workerSessions int) (context.Context, func(), error) {
	return beginStrictSourceSnapshotWithOptions(ctx, srcPool, job.Table, strictSnapshotBeginOptions{
		workerSessions: workerSessions,
		auditEvent:     job.AuditEvent,
	})
}

func beginStrictSourceSnapshotWithOptions(ctx context.Context, srcPool pool.SourcePool, table source.Table, opts strictSnapshotBeginOptions) (context.Context, func(), error) {
	if srcPool == nil || srcPool.DB() == nil {
		return nil, nil, fmt.Errorf("strict_consistency requires an open source database connection")
	}
	if err := validateStrictSnapshotTable(ctx, srcPool, table); err != nil {
		return nil, nil, err
	}
	strategyName, strategy, err := resolveStrictReaderStrategyForDBType(srcPool.DBType())
	if err != nil {
		return nil, nil, err
	}
	if (strategyName == strictParallelLockWindow || strategyName == strictParallelTableSharedLock) && opts.workerSessions < 1 {
		return beginSingleReaderStrictSnapshot(ctx, srcPool)
	}
	if strategy != nil {
		view, err := strategy.begin(ctx, srcPool, table, opts.workerSessions)
		if err != nil {
			var degrade *strictParallelDegradeError
			if !errors.As(err, &degrade) {
				return nil, nil, err
			}
			fields := map[string]any{
				"table":      table.FullName(),
				"strategy":   degrade.strategy,
				"reason":     degrade.reason,
				"error_code": degrade.code,
			}
			logging.WarnEvent("strict_parallel_degraded",
				"table", table.FullName(),
				"strategy", degrade.strategy,
				"reason", degrade.reason,
				"error_code", degrade.code,
			)
			if opts.auditEvent != nil {
				opts.auditEvent("strict_parallel_degraded", fields)
			}
			return beginSingleReaderStrictSnapshot(ctx, srcPool)
		}
		if view.queryer == nil || view.workerFactory == nil || view.release == nil {
			if view.release != nil {
				view.release()
			}
			return nil, nil, fmt.Errorf("strict parallel strategy returned an incomplete source view")
		}
		if strategyName == strictParallelTableSharedLock {
			fields := map[string]any{
				"table":    table.FullName(),
				"strategy": strategyName,
				"blocking": "concurrent_writes_wait_until_table_complete",
			}
			logging.InfoEvent("strict_shared_table_lock_acquired",
				"table", table.FullName(),
				"strategy", strategyName,
				"blocking", fields["blocking"],
			)
			if opts.auditEvent != nil {
				opts.auditEvent("strict_shared_table_lock_acquired", fields)
			}
		}
		return strictReaderContext(ctx, view), view.release, nil
	}

	return beginSingleReaderStrictSnapshot(ctx, srcPool)
}

func beginSingleReaderStrictSnapshot(ctx context.Context, srcPool pool.SourcePool) (context.Context, func(), error) {
	options, err := strictSnapshotTxOptions(srcPool.DBType())
	if err != nil {
		return nil, nil, err
	}
	tx, err := srcPool.DB().BeginTx(ctx, options)
	if err != nil {
		return nil, nil, fmt.Errorf("starting strict_consistency source snapshot for %s: %w", srcPool.DBType(), err)
	}
	cleanup := func() {
		// This is a read transaction. Rollback releases its snapshot/locks on
		// both success and failure; Commit would express an unintended write
		// lifecycle and is not needed for a stable read view.
		_ = tx.Rollback()
	}
	strictCtx := context.WithValue(ctx, sourceQueryerContextKey{}, sourceQueryer(tx))
	return strictCtx, cleanup, nil
}

// validateStrictSnapshotTable handles engine-specific preconditions that a
// transaction isolation level alone cannot prove. MyISAM and similar MySQL
// engines ignore InnoDB's MVCC snapshot semantics, so reject them before any
// target preparation instead of offering a false strict-consistency guarantee.
func validateStrictSnapshotTable(ctx context.Context, srcPool pool.SourcePool, table source.Table) error {
	if !isMySQLSource(srcPool.DBType()) {
		return nil
	}

	var (
		engine string
		err    error
	)
	if table.Schema == "" {
		err = srcPool.DB().QueryRowContext(ctx, `
			SELECT ENGINE FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		`, table.Name).Scan(&engine)
	} else {
		err = srcPool.DB().QueryRowContext(ctx, `
			SELECT ENGINE FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		`, table.Schema, table.Name).Scan(&engine)
	}
	if err != nil {
		return fmt.Errorf("checking MySQL storage engine for strict_consistency table %s: %w", table.FullName(), err)
	}
	if !strings.EqualFold(engine, "innodb") {
		return fmt.Errorf("strict_consistency requires MySQL table %s to use InnoDB, found %q", table.FullName(), engine)
	}
	return nil
}

func isMySQLSource(dbType string) bool {
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "mysql", "mariadb", "maria":
		return true
	default:
		return false
	}
}

func strictSnapshotTxOptions(dbType string) (*sql.TxOptions, error) {
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "postgres", "postgresql", "pg", "mysql", "mariadb", "maria":
		return &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, nil
	case "mssql", "sqlserver":
		// go-mssqldb rejects TxOptions.ReadOnly. The transfer only performs
		// source queries on this transaction; serializable range locking is the
		// invariant that supplies its stable view.
		return &sql.TxOptions{Isolation: sql.LevelSerializable}, nil
	case "sqlite":
		return &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}, nil
	default:
		return nil, fmt.Errorf(
			"strict_consistency cannot provide a stable source snapshot for %q; use a supported source (postgres, mysql, mssql, sqlite), disable strict_consistency, or migrate from a quiesced snapshot/replica",
			dbType,
		)
	}
}

func sourceQueryerFor(ctx context.Context, fallback *sql.DB) sourceQueryer {
	if queryer, ok := ctx.Value(sourceQueryerContextKey{}).(sourceQueryer); ok && queryer != nil {
		return queryer
	}
	return fallback
}

func sourceQueryerFactoryFor(ctx context.Context) sourceQueryerFactory {
	factory, _ := ctx.Value(sourceQueryerFactoryContextKey{}).(sourceQueryerFactory)
	return factory
}

func sourceQueryerForJob(ctx context.Context, fallback *sql.DB, job Job) sourceQueryer {
	if queryer := job.StrictSnapshotEpoch.queryer(); queryer != nil {
		return queryer
	}
	return sourceQueryerFor(ctx, fallback)
}

func sourceQueryerFactoryForJob(ctx context.Context, job Job) sourceQueryerFactory {
	if job.StrictSnapshotEpoch != nil {
		return job.StrictSnapshotEpoch.queryerForWorker
	}
	return sourceQueryerFactoryFor(ctx)
}

// strictSnapshotRowCount reads the full-table count through the same pinned
// transaction used for pagination. Besides providing strict-validation
// evidence, this first table read establishes engines' lazy MVCC snapshot
// before target preparation can take time (#664).
func strictSnapshotRowCount(ctx context.Context, srcPool pool.SourcePool, table source.Table) (int64, error) {
	return strictSnapshotRowCountWithQueryer(ctx, srcPool, table, sourceQueryerFor(ctx, srcPool.DB()))
}

func strictSnapshotRowCountWithQueryer(ctx context.Context, srcPool pool.SourcePool, table source.Table, queryer sourceQueryer) (int64, error) {
	dialect := driver.GetDialect(srcPool.DBType())
	if dialect == nil {
		return 0, fmt.Errorf("strict_consistency row count: no dialect registered for source DB type %q", srcPool.DBType())
	}
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", dialect.QualifyTable(table.Schema, table.Name))
	if err := queryer.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("counting strict_consistency snapshot for %s: %w", table.FullName(), err)
	}
	return count, nil
}
