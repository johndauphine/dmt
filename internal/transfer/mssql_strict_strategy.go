package transfer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

const mssqlSharedLockTimeoutMS = 30000

type mssqlTableSharedLockStrategy struct {
	lockTimeoutMS int
	afterLock     func()
}

func (mssqlTableSharedLockStrategy) joinBudget() int                  { return 1 }
func (mssqlTableSharedLockStrategy) perJobParallel() bool             { return true }
func (mssqlTableSharedLockStrategy) sharedViewAcrossJobs(string) bool { return false }

func (s mssqlTableSharedLockStrategy) begin(ctx context.Context, srcPool pool.SourcePool, table source.Table, _ int) (strictReaderView, error) {
	conn, err := srcPool.DB().Conn(ctx)
	if err != nil {
		return strictReaderView{}, fmt.Errorf("opening SQL Server strict_consistency lock coordinator: %w", err)
	}
	fail := func(err error) (strictReaderView, error) {
		_ = conn.Close()
		return strictReaderView{}, err
	}
	timeout := s.lockTimeoutMS
	if timeout <= 0 {
		timeout = mssqlSharedLockTimeoutMS
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET LOCK_TIMEOUT %d", timeout)); err != nil {
		return fail(fmt.Errorf("setting SQL Server strict_consistency lock timeout: %w", err))
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fail(fmt.Errorf("starting SQL Server strict_consistency lock transaction: %w", err))
	}
	dialect := driver.GetDialect(srcPool.DBType())
	if dialect == nil {
		_ = tx.Rollback()
		return fail(fmt.Errorf("SQL Server strict_consistency shared lock: no dialect registered for %q", srcPool.DBType()))
	}
	query := "SELECT TOP (0) 1 FROM " + dialect.QualifyTable(table.Schema, table.Name) + " WITH (TABLOCK, HOLDLOCK)"
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		if code, ok := mssqlLockTimeoutCode(err); ok {
			return strictReaderView{}, &strictParallelDegradeError{strategy: strictParallelTableSharedLock, reason: "lock_wait_timeout", code: code, err: err}
		}
		return strictReaderView{}, fmt.Errorf("acquiring SQL Server strict_consistency shared lock for %s: %w", table.FullName(), err)
	}
	_ = rows.Close()
	if s.afterLock != nil {
		s.afterLock()
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			_ = tx.Rollback()
			_ = conn.Close()
		})
	}
	return strictReaderView{
		queryer: srcPool.DB(),
		workerFactory: func(context.Context, int) (sourceQueryer, func(), error) {
			return srcPool.DB(), func() {}, nil
		},
		release: release,
	}, nil
}

func mssqlLockTimeoutCode(err error) (uint16, bool) {
	var sqlErr interface{ SQLErrorNumber() int32 }
	if !errors.As(err, &sqlErr) || sqlErr.SQLErrorNumber() != 1222 {
		return 0, false
	}
	return 1222, true
}
