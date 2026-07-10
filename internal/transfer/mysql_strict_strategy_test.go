package transfer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/johndauphine/dmt/internal/source"
)

type mysqlSnapshotRecordingSource struct{ *keysetRuntimeSourcePool }

func (p *mysqlSnapshotRecordingSource) DBType() string { return "mysql" }

func TestMySQLLockWindowSessionOrdering(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	src := &mysqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	view, err := (mysqlLockWindowStrategy{}).begin(context.Background(), src, source.Table{Schema: "app", Name: "events"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer view.release()

	want := []string{
		"exec SET SESSION lock_wait_timeout = 30",
		"exec LOCK TABLES `app`.`events` READ",
		"exec SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		"exec START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
		"exec SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		"exec START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
		"exec UNLOCK TABLES",
	}
	if got := tracker.events(); !strictSnapshotEventPrefix(got, want) {
		t.Fatalf("MySQL lock-window events = %v, want prefix %v", got, want)
	}
	for worker := range 2 {
		queryer, release, err := view.workerFactory(context.Background(), worker)
		if err != nil || queryer == nil || release == nil {
			t.Fatalf("worker %d session = (%v, release=%t, err=%v)", worker, queryer, release != nil, err)
		}
	}
}

func TestMySQLStrictFallbackClassification(t *testing.T) {
	tests := []struct {
		code       uint16
		wantReason string
		wantOK     bool
	}{
		{code: 1205, wantReason: "lock_wait_timeout", wantOK: true},
		{code: 1044, wantReason: "missing_lock_tables_privilege", wantOK: true},
		{code: 1142, wantReason: "missing_lock_tables_privilege", wantOK: true},
		{code: 1213},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
			reason, code, ok := mysqlStrictFallbackReason(&mysql.MySQLError{Number: tc.code, Message: "injected"})
			if reason != tc.wantReason || code != tc.code || ok != tc.wantOK {
				t.Fatalf("classification = (%q, %d, %t), want (%q, %d, %t)", reason, code, ok, tc.wantReason, tc.code, tc.wantOK)
			}
		})
	}
}

func TestMySQLLockFailureAuditsAndFallsBackToSingleReader(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	lockSQL := "LOCK TABLES `app`.`events` READ"
	tracker.failExec(lockSQL, &mysql.MySQLError{Number: 1205, Message: "lock wait timeout"})
	src := &mysqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	table := source.Table{Schema: "app", Name: "events"}
	var auditType string
	var auditFields map[string]any
	ctx, release, err := beginStrictSourceSnapshotWithOptions(context.Background(), src, table, strictSnapshotBeginOptions{
		workerSessions: 2,
		auditEvent: func(typeName string, fields map[string]any) {
			auditType = typeName
			auditFields = fields
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if sourceQueryerFor(ctx, nil) == nil {
		t.Fatal("fallback did not install a single-reader queryer")
	}
	if sourceQueryerFactoryFor(ctx) != nil {
		t.Fatal("fallback unexpectedly retained the parallel worker factory")
	}
	if auditType != "strict_parallel_degraded" || auditFields["reason"] != "lock_wait_timeout" || auditFields["error_code"] != uint16(1205) {
		t.Fatalf("audit = %q %+v", auditType, auditFields)
	}
	if got := tracker.count("begin"); got != 1 {
		t.Fatalf("fallback single-reader transactions = %d, want 1", got)
	}
}

func TestMySQLSingleReaderDoesNotAcquireTableLock(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	src := &mysqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	ctx, release, err := beginStrictSourceSnapshotWithOptions(
		context.Background(), src, source.Table{Schema: "app", Name: "events"},
		strictSnapshotBeginOptions{workerSessions: 0},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if sourceQueryerFor(ctx, nil) == nil || sourceQueryerFactoryFor(ctx) != nil {
		t.Fatal("single-reader strict path did not retain the ordinary transaction")
	}
	for _, event := range tracker.events() {
		if event == "exec LOCK TABLES `app`.`events` READ" {
			t.Fatalf("single-reader strict path acquired a table lock: %v", tracker.events())
		}
	}
	if tracker.count("begin") != 1 {
		t.Fatalf("single-reader transactions = %d, want 1", tracker.count("begin"))
	}
}

func TestMySQLUnlockFailureDiscardsCoordinatorSession(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	tracker.failExec("UNLOCK TABLES", errors.New("injected unlock failure"))
	src := &mysqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	_, err := (mysqlLockWindowStrategy{}).begin(
		context.Background(), src, source.Table{Schema: "app", Name: "events"}, 2,
	)
	if err == nil {
		t.Fatal("unlock failure unexpectedly succeeded")
	}
	if tracker.count("close") == 0 {
		t.Fatalf("unlock failure did not physically discard the coordinator: %v", tracker.events())
	}
}
