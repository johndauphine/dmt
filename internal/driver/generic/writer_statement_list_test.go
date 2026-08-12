package generic

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/logging"
	"github.com/johndauphine/dmt/v5/internal/smtddl"
	"github.com/johndauphine/smt/schema"
)

var smtBatchDriverSeq atomic.Uint64

func TestExecuteSMTBatchPinsConnectionAndRunsIndependentCleanup(t *testing.T) {
	tests := []struct {
		name   string
		render func() (schema.Batch, error)
	}{
		{
			name: "mysql drop",
			render: func() (schema.Batch, error) {
				return smtddl.RenderDropTable(smtddl.Request{
					TargetDialect: "mysql",
					TargetSchema:  "crm",
					Table:         smtddl.Table{Name: "child"},
				}, false)
			},
		},
		{
			name: "mysql truncate",
			render: func() (schema.Batch, error) {
				return smtddl.RenderTruncateTable(smtddl.Request{
					TargetDialect: "mysql",
					TargetSchema:  "crm",
					Table:         smtddl.Table{Name: "child"},
				}, false)
			},
		},
		{
			name: "sqlite drop",
			render: func() (schema.Batch, error) {
				return smtddl.RenderDropTable(smtddl.Request{
					TargetDialect: "sqlite",
					Table:         smtddl.Table{Name: "child"},
				}, false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch, err := tt.render()
			if err != nil {
				t.Fatalf("render SMT batch: %v", err)
			}
			if !batch.RequiresSingleConnection || len(batch.Statements) != 3 || len(batch.Cleanup) != 1 {
				t.Fatalf("SMT affinity contract = %+v", batch)
			}

			coreSQL := batch.Statements[1].SQL
			db, recorder := openSMTBatchTestDB(t, map[string]error{
				coreSQL: errors.New("required DDL failure"),
			})
			ctx, cancel := context.WithCancel(context.Background())
			recorder.cancelOn = coreSQL
			recorder.cancel = cancel
			w := &Writer{db: db}

			err = w.executeSMTBatch(ctx, batch)
			if err == nil || !strings.Contains(err.Error(), "required DDL failure") {
				t.Fatalf("executeSMTBatch error = %v, want required failure", err)
			}

			execs := recorder.snapshot()
			if len(execs) != 3 {
				t.Fatalf("exec count = %d, want setup/core/failure-cleanup; execs=%+v", len(execs), execs)
			}
			wantSQL := []string{batch.Statements[0].SQL, coreSQL, batch.Cleanup[0].SQL}
			connID := execs[0].connID
			for index, exec := range execs {
				if exec.stmt != wantSQL[index] {
					t.Fatalf("statement %d changed:\n got: %q\nwant: %q", index, exec.stmt, wantSQL[index])
				}
				if exec.connID != connID {
					t.Fatalf("batch used multiple physical connections: %+v", execs)
				}
			}
			cleanup := execs[len(execs)-1]
			if cleanup.ctxErr != nil {
				t.Fatalf("cleanup inherited canceled operation context: %v", cleanup.ctxErr)
			}
			if !cleanup.hasDeadline {
				t.Fatal("cleanup context has no deadline")
			}

			successDB, successRecorder := openSMTBatchTestDB(t, nil)
			if err := (&Writer{db: successDB}).executeSMTBatch(context.Background(), batch); err != nil {
				t.Fatalf("successful executeSMTBatch: %v", err)
			}
			successExecs := successRecorder.snapshot()
			if len(successExecs) != len(batch.Statements) {
				t.Fatalf("successful execs = %+v, want every SMT statement", successExecs)
			}
			successConnID := successExecs[0].connID
			for index, statement := range batch.Statements {
				if successExecs[index].stmt != statement.SQL {
					t.Fatalf("successful statement %d changed:\n got: %q\nwant: %q", index, successExecs[index].stmt, statement.SQL)
				}
				if successExecs[index].connID != successConnID {
					t.Fatalf("successful batch used multiple physical connections: %+v", successExecs)
				}
			}
		})
	}
}

func TestExecuteSMTBatchSQLiteTruncateLogsAndContinuesBestEffortCleanup(t *testing.T) {
	batch, err := smtddl.RenderTruncateTable(smtddl.Request{
		TargetDialect: "sqlite",
		Table:         smtddl.Table{Name: "O'Reilly"},
	}, false)
	if err != nil {
		t.Fatalf("RenderTruncateTable: %v", err)
	}
	if len(batch.Statements) != 2 || !batch.IsBestEffort(1) || batch.IsBestEffort(0) {
		t.Fatalf("SQLite truncate batch = %+v, want required delete plus best-effort sequence cleanup", batch)
	}

	db, recorder := openSMTBatchTestDB(t, map[string]error{
		batch.Statements[1].SQL: errors.New("sqlite_sequence is absent"),
	})
	var logs bytes.Buffer
	oldLevel := logging.GetLevel()
	logging.SetLevel(logging.LevelWarn)
	logging.SetOutput(&logs)
	t.Cleanup(func() {
		logging.SetOutput(os.Stdout)
		logging.SetLevel(oldLevel)
	})

	w := &Writer{db: db}
	if err := w.executeSMTBatch(context.Background(), batch); err != nil {
		t.Fatalf("best-effort cleanup failed truncate: %v", err)
	}
	execs := recorder.snapshot()
	if len(execs) != len(batch.Statements) {
		t.Fatalf("execs = %+v, want every SMT statement", execs)
	}
	for index, statement := range batch.Statements {
		if execs[index].stmt != statement.SQL {
			t.Fatalf("statement %d changed:\n got: %q\nwant: %q", index, execs[index].stmt, statement.SQL)
		}
	}
	if !strings.Contains(logs.String(), "best-effort") || !strings.Contains(logs.String(), "statement_index=1") {
		t.Fatalf("best-effort failure was not logged with its index:\n%s", logs.String())
	}
}

func TestExecuteSMTBatchRequiredFailureStopsAndCleanupCannotMaskPrimary(t *testing.T) {
	primaryErr := errors.New("primary required failure")
	db, recorder := openSMTBatchTestDB(t, map[string]error{
		"BEST_EFFORT":     errors.New("advisory failure"),
		"REQUIRED_FAIL":   primaryErr,
		"FAILURE_CLEANUP": errors.New("cleanup failure"),
	})
	batch := schema.Batch{
		Statements: []schema.Statement{
			{Kind: schema.StatementAddColumn, SQL: "FIRST_REQUIRED"},
			{Kind: schema.StatementBestEffortCleanup, SQL: "BEST_EFFORT"},
			{Kind: schema.StatementAlterColumnType, SQL: "REQUIRED_FAIL"},
			{Kind: schema.StatementDropTable, SQL: "MUST_NOT_RUN"},
		},
		BestEffortStatementIndexes: []int{1},
		Cleanup: []schema.Statement{
			{Kind: schema.StatementSessionCleanup, SQL: "FAILURE_CLEANUP"},
		},
	}

	var logs bytes.Buffer
	oldLevel := logging.GetLevel()
	logging.SetLevel(logging.LevelWarn)
	logging.SetOutput(&logs)
	t.Cleanup(func() {
		logging.SetOutput(os.Stdout)
		logging.SetLevel(oldLevel)
	})

	err := (&Writer{db: db}).executeSMTBatch(context.Background(), batch)
	if !errors.Is(err, primaryErr) {
		t.Fatalf("executeSMTBatch error = %v, want primary required failure", err)
	}
	if strings.Contains(err.Error(), "advisory failure") || strings.Contains(err.Error(), "cleanup failure") {
		t.Fatalf("secondary error masked or contaminated primary error: %v", err)
	}

	execs := recorder.snapshot()
	want := []string{"FIRST_REQUIRED", "BEST_EFFORT", "REQUIRED_FAIL", "FAILURE_CLEANUP"}
	if len(execs) != len(want) {
		t.Fatalf("execs = %+v, want %v", execs, want)
	}
	for index := range want {
		if execs[index].stmt != want[index] {
			t.Fatalf("exec order = %+v, want %v", execs, want)
		}
	}
	if !strings.Contains(logs.String(), "best-effort") || !strings.Contains(logs.String(), "failure cleanup failed") {
		t.Fatalf("secondary failures were not logged:\n%s", logs.String())
	}
}

func openSMTBatchTestDB(t *testing.T, failures map[string]error) (*sql.DB, *smtBatchRecorder) {
	t.Helper()

	name := fmt.Sprintf("smt_batch_test_%d", smtBatchDriverSeq.Add(1))
	recorder := &smtBatchRecorder{failures: failures}
	sql.Register(name, &smtBatchDriver{recorder: recorder})

	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open fake driver: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, recorder
}

type smtBatchRecorder struct {
	mu         sync.Mutex
	nextConnID int
	failures   map[string]error
	cancelOn   string
	cancel     context.CancelFunc
	execs      []smtBatchExec
}

type smtBatchExec struct {
	connID      int
	stmt        string
	ctxErr      error
	hasDeadline bool
}

func (r *smtBatchRecorder) openConn() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextConnID++
	return r.nextConnID
}

func (r *smtBatchRecorder) record(ctx context.Context, connID int, stmt string) error {
	_, hasDeadline := ctx.Deadline()
	r.mu.Lock()
	r.execs = append(r.execs, smtBatchExec{
		connID:      connID,
		stmt:        stmt,
		ctxErr:      ctx.Err(),
		hasDeadline: hasDeadline,
	})
	cancel := r.cancelOn != "" && strings.Contains(stmt, r.cancelOn) && r.cancel != nil
	var result error
	for fragment, err := range r.failures {
		if strings.Contains(stmt, fragment) {
			result = err
			break
		}
	}
	r.mu.Unlock()
	if cancel {
		r.cancel()
	}
	return result
}

func (r *smtBatchRecorder) snapshot() []smtBatchExec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]smtBatchExec(nil), r.execs...)
}

type smtBatchDriver struct {
	recorder *smtBatchRecorder
}

func (d *smtBatchDriver) Open(string) (sqldriver.Conn, error) {
	return &smtBatchConn{recorder: d.recorder, id: d.recorder.openConn()}, nil
}

type smtBatchConn struct {
	recorder *smtBatchRecorder
	id       int
}

func (c *smtBatchConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *smtBatchConn) Close() error { return nil }

func (c *smtBatchConn) Begin() (sqldriver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}

func (c *smtBatchConn) ExecContext(ctx context.Context, query string, _ []sqldriver.NamedValue) (sqldriver.Result, error) {
	if err := c.recorder.record(ctx, c.id, query); err != nil {
		return nil, err
	}
	return sqldriver.RowsAffected(0), nil
}
