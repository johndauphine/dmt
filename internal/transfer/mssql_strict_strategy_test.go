package transfer

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/source"
	mssql "github.com/microsoft/go-mssqldb"
)

type mssqlSnapshotRecordingSource struct{ *keysetRuntimeSourcePool }

func (p *mssqlSnapshotRecordingSource) DBType() string { return "mssql" }

func TestMSSQLTableSharedLockOrderingAndReleaseOnce(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	src := &mssqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	view, err := (mssqlTableSharedLockStrategy{}).begin(context.Background(), src, source.Table{Schema: "dbo", Name: "events"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"exec SET LOCK_TIMEOUT 30000",
		"begin",
		"query SELECT TOP (0) 1 FROM [dbo].[events] WITH (TABLOCK, HOLDLOCK)",
	}
	if got := tracker.events(); !strictSnapshotEventPrefix(got, want) {
		t.Fatalf("SQL Server shared-lock events = %v, want prefix %v", got, want)
	}
	for worker := range 2 {
		queryer, release, err := view.workerFactory(context.Background(), worker)
		if err != nil || queryer != db || release == nil {
			t.Fatalf("worker %d = (%v, release=%t, err=%v)", worker, queryer, release != nil, err)
		}
	}
	view.release()
	view.release()
	if got := tracker.count("rollback"); got != 1 {
		t.Fatalf("coordinator rollbacks = %d, want exactly 1", got)
	}
}

func TestMSSQLLockTimeoutClassificationAndFallback(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	lockSQL := "SELECT TOP (0) 1 FROM [dbo].[events] WITH (TABLOCK, HOLDLOCK)"
	tracker.failQuery(lockSQL, mssql.Error{Number: 1222, Message: "lock timeout"})
	src := &mssqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	var auditType string
	var auditFields map[string]any
	ctx, release, err := beginStrictSourceSnapshotWithOptions(context.Background(), src, source.Table{Schema: "dbo", Name: "events"}, strictSnapshotBeginOptions{
		workerSessions: 2,
		auditEvent: func(event string, fields map[string]any) {
			auditType, auditFields = event, fields
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if sourceQueryerFactoryFor(ctx) != nil {
		t.Fatal("timeout fallback retained shared-lock parallel factory")
	}
	if auditType != "strict_parallel_degraded" || auditFields["reason"] != "lock_wait_timeout" || auditFields["error_code"] != uint16(1222) {
		t.Fatalf("audit = %q %+v", auditType, auditFields)
	}
	if got := tracker.count("begin"); got != 2 {
		t.Fatalf("transactions = %d, want coordinator plus serializable fallback", got)
	}
}

func TestMSSQLSharedLockAcquisitionAuditsInfo(t *testing.T) {
	db, _ := openSnapshotRecordingDB(t)
	db.SetMaxOpenConns(3)
	src := &mssqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
	var auditType string
	var auditFields map[string]any
	_, release, err := beginStrictSourceSnapshotWithOptions(context.Background(), src, source.Table{Schema: "dbo", Name: "events"}, strictSnapshotBeginOptions{
		workerSessions: 2,
		auditEvent: func(event string, fields map[string]any) {
			auditType, auditFields = event, fields
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if auditType != "strict_shared_table_lock_acquired" || auditFields["table"] != "dbo.events" {
		t.Fatalf("audit = %q %+v", auditType, auditFields)
	}
}

func TestMSSQLSharedLockReleaseAfterCancelAndNoBudgetFallback(t *testing.T) {
	t.Run("cancel releases coordinator exactly once", func(t *testing.T) {
		db, tracker := openSnapshotRecordingDB(t)
		db.SetMaxOpenConns(3)
		src := &mssqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
		ctx, cancel := context.WithCancel(context.Background())
		view, err := (mssqlTableSharedLockStrategy{}).begin(ctx, src, source.Table{Schema: "dbo", Name: "events"}, 2)
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		view.release()
		view.release()
		if got := tracker.count("rollback"); got != 1 {
			t.Fatalf("coordinator rollbacks = %d, want 1", got)
		}
	})

	t.Run("no worker budget retains serializable single reader", func(t *testing.T) {
		db, tracker := openSnapshotRecordingDB(t)
		src := &mssqlSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}
		ctx, release, err := beginStrictSourceSnapshotWithOptions(context.Background(), src, source.Table{Schema: "dbo", Name: "events"}, strictSnapshotBeginOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if sourceQueryerFor(ctx, nil) == nil || sourceQueryerFactoryFor(ctx) != nil {
			t.Fatal("no-budget fallback did not retain ordinary single-reader transaction")
		}
		for _, event := range tracker.events() {
			if event == "exec SET LOCK_TIMEOUT 30000" {
				t.Fatalf("no-budget fallback acquired shared-lock coordinator: %v", tracker.events())
			}
		}
	})
}
