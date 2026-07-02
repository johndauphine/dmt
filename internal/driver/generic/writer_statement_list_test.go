package generic

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var ddlListDriverSeq atomic.Uint64

func TestExecDDLStatementListPinsConnectionAndRestoresMySQLFKChecks(t *testing.T) {
	db, recorder := openDDLListTestDB(t, "DROP TABLE")
	w := &Writer{
		db:  db,
		cat: &Catalog{Name: "mysql"},
	}

	err := w.execDDLStatementList(context.Background(), []string{
		"SET FOREIGN_KEY_CHECKS = 0",
		"DROP TABLE IF EXISTS {table}",
		"SET FOREIGN_KEY_CHECKS = 1",
	}, "`child`")
	if err == nil {
		t.Fatal("execDDLStatementList() error = nil, want injected drop failure")
	}

	execs := recorder.snapshot()
	if len(execs) != 3 {
		t.Fatalf("exec count = %d, want disable/drop/deferred-enable; execs=%+v", len(execs), execs)
	}
	connID := execs[0].connID
	for _, exec := range execs {
		if exec.connID != connID {
			t.Fatalf("statements ran on different connections: %+v", execs)
		}
	}
	if execs[0].stmt != "SET FOREIGN_KEY_CHECKS = 0" {
		t.Fatalf("first stmt = %q, want FK disable", execs[0].stmt)
	}
	if !strings.Contains(execs[1].stmt, "DROP TABLE") {
		t.Fatalf("second stmt = %q, want drop", execs[1].stmt)
	}
	if execs[2].stmt != "SET FOREIGN_KEY_CHECKS = 1" {
		t.Fatalf("third stmt = %q, want deferred FK enable", execs[2].stmt)
	}
}

func openDDLListTestDB(t *testing.T, failContains string) (*sql.DB, *ddlListRecorder) {
	t.Helper()

	name := fmt.Sprintf("ddl_list_test_%d", ddlListDriverSeq.Add(1))
	recorder := &ddlListRecorder{failContains: failContains}
	sql.Register(name, &ddlListDriver{recorder: recorder})

	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open fake driver: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, recorder
}

type ddlListRecorder struct {
	mu           sync.Mutex
	nextConnID   int
	failContains string
	execs        []ddlListExec
}

type ddlListExec struct {
	connID int
	stmt   string
}

func (r *ddlListRecorder) openConn() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextConnID++
	return r.nextConnID
}

func (r *ddlListRecorder) record(connID int, stmt string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.execs = append(r.execs, ddlListExec{connID: connID, stmt: stmt})
	if r.failContains != "" && strings.Contains(stmt, r.failContains) {
		return errors.New("injected DDL failure")
	}
	return nil
}

func (r *ddlListRecorder) snapshot() []ddlListExec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ddlListExec(nil), r.execs...)
}

type ddlListDriver struct {
	recorder *ddlListRecorder
}

func (d *ddlListDriver) Open(string) (sqldriver.Conn, error) {
	return &ddlListConn{recorder: d.recorder, id: d.recorder.openConn()}, nil
}

type ddlListConn struct {
	recorder *ddlListRecorder
	id       int
}

func (c *ddlListConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}

func (c *ddlListConn) Close() error { return nil }

func (c *ddlListConn) Begin() (sqldriver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}

func (c *ddlListConn) ExecContext(_ context.Context, query string, _ []sqldriver.NamedValue) (sqldriver.Result, error) {
	if err := c.recorder.record(c.id, query); err != nil {
		return nil, err
	}
	return sqldriver.RowsAffected(0), nil
}
