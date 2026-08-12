package transfer

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/source"
)

const mysqlLockWaitSeconds = 30

type mysqlLockWindowStrategy struct {
	lockWaitSeconds int
	afterLock       func()
}

func (mysqlLockWindowStrategy) joinBudget() int                  { return 1 }
func (mysqlLockWindowStrategy) perJobParallel() bool             { return true }
func (mysqlLockWindowStrategy) sharedViewAcrossJobs(string) bool { return false }

func (s mysqlLockWindowStrategy) begin(ctx context.Context, srcPool pool.SourcePool, table source.Table, workerSessions int) (strictReaderView, error) {
	if workerSessions < 1 {
		session, err := startMySQLConsistentSession(ctx, srcPool.DB())
		if err != nil {
			return strictReaderView{}, err
		}
		return mysqlStrictReaderView([]*mysqlPinnedSession{session}), nil
	}

	coordinator, err := srcPool.DB().Conn(ctx)
	if err != nil {
		return strictReaderView{}, fmt.Errorf("opening MySQL strict_consistency lock coordinator: %w", err)
	}
	coordinatorClosed := false
	closeCoordinator := func() {
		if !coordinatorClosed {
			coordinatorClosed = true
			_ = coordinator.Close()
		}
	}
	defer closeCoordinator()

	lockWaitSeconds := s.lockWaitSeconds
	if lockWaitSeconds <= 0 {
		lockWaitSeconds = mysqlLockWaitSeconds
	}
	if _, err := coordinator.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", lockWaitSeconds)); err != nil {
		return strictReaderView{}, fmt.Errorf("setting MySQL strict_consistency lock timeout: %w", err)
	}
	dialect := driver.GetDialect(srcPool.DBType())
	if dialect == nil {
		return strictReaderView{}, fmt.Errorf("MySQL strict_consistency lock window: no dialect registered for %q", srcPool.DBType())
	}
	lockSQL := "LOCK TABLES " + dialect.QualifyTable(table.Schema, table.Name) + " READ"
	if _, err := coordinator.ExecContext(ctx, lockSQL); err != nil {
		if reason, code, ok := mysqlStrictFallbackReason(err); ok {
			return strictReaderView{}, &strictParallelDegradeError{
				strategy: strictParallelLockWindow,
				reason:   reason,
				code:     code,
				err:      err,
			}
		}
		return strictReaderView{}, fmt.Errorf("acquiring MySQL strict_consistency read lock for %s: %w", table.FullName(), err)
	}
	locked := true
	defer func() {
		if locked {
			if err := unlockMySQLTables(coordinator); err != nil {
				discardSQLConn(coordinator)
				coordinatorClosed = true
			}
		}
	}()
	if s.afterLock != nil {
		s.afterLock()
	}

	sessions := make([]*mysqlPinnedSession, 0, workerSessions)
	cleanupSessions := func() {
		for _, session := range sessions {
			session.release()
		}
	}
	for range workerSessions {
		session, err := startMySQLConsistentSession(ctx, srcPool.DB())
		if err != nil {
			cleanupSessions()
			return strictReaderView{}, err
		}
		sessions = append(sessions, session)
	}
	if _, err := coordinator.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		cleanupSessions()
		discardSQLConn(coordinator)
		coordinatorClosed = true
		locked = false // physical discard releases the server-side table lock
		return strictReaderView{}, fmt.Errorf("releasing MySQL strict_consistency read lock for %s: %w", table.FullName(), err)
	}
	locked = false
	closeCoordinator()
	return mysqlStrictReaderView(sessions), nil
}

type mysqlPinnedSession struct {
	conn *sql.Conn
	once sync.Once
}

func startMySQLConsistentSession(ctx context.Context, db *sql.DB) (*mysqlPinnedSession, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening MySQL strict_consistency reader session: %w", err)
	}
	fail := func(err error) (*mysqlPinnedSession, error) {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fail(fmt.Errorf("setting MySQL strict_consistency reader isolation: %w", err))
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		return fail(fmt.Errorf("starting MySQL strict_consistency reader snapshot: %w", err))
	}
	return &mysqlPinnedSession{conn: conn}, nil
}

func (s *mysqlPinnedSession) release() {
	if s == nil || s.conn == nil {
		return
	}
	s.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.conn.ExecContext(ctx, "ROLLBACK")
		_ = s.conn.Close()
	})
}

func unlockMySQLTables(conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, "UNLOCK TABLES")
	return err
}

// discardSQLConn prevents session state from escaping into database/sql's
// idle pool. In particular, go-sql-driver/mysql does not UNLOCK TABLES from
// ResetSession, so an unlock failure must physically kill the server session.
func discardSQLConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Raw(func(raw any) error {
		if closer, ok := raw.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return sqldriver.ErrBadConn
	})
	_ = conn.Close()
}

func mysqlStrictReaderView(sessions []*mysqlPinnedSession) strictReaderView {
	releaseAll := func() {
		for _, session := range sessions {
			session.release()
		}
	}
	return strictReaderView{
		queryer: sessions[0].conn,
		workerFactory: func(_ context.Context, workerID int) (sourceQueryer, func(), error) {
			if workerID < 0 || workerID >= len(sessions) {
				return nil, nil, fmt.Errorf("MySQL strict_consistency reader %d has no pinned session", workerID)
			}
			session := sessions[workerID]
			// A dead MySQL session cannot join the already-established view.
			// Its query error therefore fails the table normally; a job retry
			// creates an entirely new lock window and reader-session set.
			return session.conn, session.release, nil
		},
		release: releaseAll,
	}
}

type strictParallelDegradeError struct {
	strategy string
	reason   string
	code     uint16
	err      error
}

func (e *strictParallelDegradeError) Error() string { return e.err.Error() }
func (e *strictParallelDegradeError) Unwrap() error { return e.err }

func mysqlStrictFallbackReason(err error) (string, uint16, bool) {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return "", 0, false
	}
	switch mysqlErr.Number {
	case 1205:
		return "lock_wait_timeout", mysqlErr.Number, true
	case 1044, 1142:
		return "missing_lock_tables_privilege", mysqlErr.Number, true
	default:
		return "", mysqlErr.Number, false
	}
}
