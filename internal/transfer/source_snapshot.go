package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

// sourceQueryer is the common query surface shared by sql.DB and sql.Tx.
// Pagination receives one through context so strict-consistency reads stay on
// a pinned transaction while ordinary transfers keep using the source pool.
type sourceQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type sourceQueryerContextKey struct{}

// beginStrictSourceSnapshot starts the table-scoped source transaction before
// target preparation changes anything. Every pagination query receives this
// transaction through the context, which prevents a pooled connection from
// observing a newer source state between pages (#640).
//
// PostgreSQL and MySQL use repeatable-read MVCC snapshots. SQL Server uses
// serializable range locking, which is a stable (but potentially blocking)
// view without requiring database-wide snapshot-isolation configuration. SQLite
// uses a serializable read transaction. Engines without a suitable transaction
// contract fail before the transfer can truncate or write a target table.
func beginStrictSourceSnapshot(ctx context.Context, srcPool pool.SourcePool, table source.Table) (context.Context, func(), error) {
	if srcPool == nil || srcPool.DB() == nil {
		return nil, nil, fmt.Errorf("strict_consistency requires an open source database connection")
	}
	if err := validateStrictSnapshotTable(ctx, srcPool, table); err != nil {
		return nil, nil, err
	}

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
	return context.WithValue(ctx, sourceQueryerContextKey{}, sourceQueryer(tx)), cleanup, nil
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
	case "postgres", "postgresql", "mysql", "mariadb", "maria":
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
