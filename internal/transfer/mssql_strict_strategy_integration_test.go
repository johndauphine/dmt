package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/progress"
	"github.com/johndauphine/dmt/internal/source"
	_ "github.com/microsoft/go-mssqldb"
)

type mssqlIntegrationSource struct{ keysetRuntimeSourcePool }

func (p *mssqlIntegrationSource) DBType() string { return "mssql" }

func TestMSSQLStrictSharedLockExecuteIntegration(t *testing.T) {
	admin, db, writer, table, dbName := openMSSQLStrictIntegration(t)
	firstWrite := make(chan struct{})
	writeGate := make(chan struct{})
	target := &mssqlStrictCaptureTarget{firstWrite: firstWrite, writeGate: writeGate, rows: make(map[int]int)}
	cfg := &config.Config{Migration: config.MigrationConfig{
		StrictConsistency: true, TargetMode: "drop_recreate", ChunkSize: 2,
		ParallelReaders: 4, MaxSourceConnections: 5, ReadAheadBuffers: 1, WriteAheadWriters: 1,
	}}
	var auditMu sync.Mutex
	var auditEvents []string
	job := Job{Table: table, AuditEvent: func(event string, _ map[string]any) {
		auditMu.Lock()
		auditEvents = append(auditEvents, event)
		auditMu.Unlock()
	}}
	type result struct {
		stats *TransferStats
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		stats, err := Execute(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, target, cfg, job, progress.New(), nil)
		resultCh <- result{stats: stats, err: err}
	}()
	select {
	case <-firstWrite:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute did not stream a target batch")
	}

	writerCh := make(chan error, 1)
	go func() {
		tx, err := writer.BeginTx(context.Background(), nil)
		if err == nil {
			_, err = tx.Exec(`UPDATE dbo.events SET val = 1001 WHERE id = 1; DELETE FROM dbo.events WHERE id = 2; INSERT INTO dbo.events VALUES (101, 101)`)
		}
		if err == nil {
			err = tx.Commit()
		} else if tx != nil {
			_ = tx.Rollback()
		}
		writerCh <- err
	}()
	select {
	case err := <-writerCh:
		t.Fatalf("writer completed while shared table lock was held: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(writeGate)
	var gotResult result
	select {
	case gotResult = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("strict Execute did not complete")
	}
	if gotResult.err != nil || gotResult.stats.Rows != 100 {
		t.Fatalf("Execute = (rows=%v, err=%v), want 100 rows", gotResult.stats, gotResult.err)
	}
	select {
	case err := <-writerCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer remained blocked after table transfer released its lock")
	}
	rows := target.snapshotRows()
	if len(rows) != 100 {
		t.Fatalf("copied rows = %d, want 100", len(rows))
	}
	for id := 1; id <= 100; id++ {
		if rows[id] != id {
			t.Fatalf("copied row %d = %d, want lock-instant value %d", id, rows[id], id)
		}
	}
	auditMu.Lock()
	if len(auditEvents) != 1 || auditEvents[0] != "strict_shared_table_lock_acquired" {
		t.Fatalf("audit events = %v", auditEvents)
	}
	auditMu.Unlock()
	waitMSSQLTransactions(t, admin, dbName, 0)
}

func TestMSSQLStrictSharedLockTimeoutFallbackIntegration(t *testing.T) {
	admin, db, writer, table, dbName := openMSSQLStrictIntegration(t)
	writerTx, err := writer.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writerTx.Exec(`UPDATE dbo.events SET val = 999 WHERE id = 1`); err != nil {
		_ = writerTx.Rollback()
		t.Fatal(err)
	}
	rollbackCh := make(chan error, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		rollbackCh <- writerTx.Rollback()
	}()

	oldStrategy := strictReaderStrategies[strictParallelTableSharedLock]
	strictReaderStrategies[strictParallelTableSharedLock] = mssqlTableSharedLockStrategy{lockTimeoutMS: 200}
	t.Cleanup(func() { strictReaderStrategies[strictParallelTableSharedLock] = oldStrategy })
	var auditEvents []string
	var auditFields map[string]any
	job := Job{Table: table, AuditEvent: func(event string, fields map[string]any) {
		auditEvents = append(auditEvents, event)
		auditFields = fields
	}}
	cfg := &config.Config{Migration: config.MigrationConfig{
		StrictConsistency: true, TargetMode: "drop_recreate", ChunkSize: 5,
		ParallelReaders: 4, MaxSourceConnections: 5, ReadAheadBuffers: 1, WriteAheadWriters: 1,
	}}
	target := &keysetRuntimeTargetPool{updated: true}
	stats, err := Execute(context.Background(), &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}, target, cfg, job, progress.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-rollbackCh; err != nil && err != sql.ErrTxDone {
		t.Fatal(err)
	}
	if stats.Rows != 100 {
		t.Fatalf("fallback rows = %d, want 100", stats.Rows)
	}
	if len(auditEvents) != 1 || auditEvents[0] != "strict_parallel_degraded" || auditFields["error_code"] != uint16(1222) {
		t.Fatalf("fallback audit = %v %+v", auditEvents, auditFields)
	}
	ids, _ := target.snapshot()
	if len(ids) != 100 {
		t.Fatalf("fallback copied %d rows, want 100", len(ids))
	}
	waitMSSQLTransactions(t, admin, dbName, 0)
}

func TestMSSQLStrictSharedLockReadThroughputIntegration(t *testing.T) {
	_, db, _, table, _ := openMSSQLStrictIntegration(t)
	table = createMSSQLThroughputFixture(t, db, table)
	src := &mssqlIntegrationSource{keysetRuntimeSourcePool{db: db}}
	view, err := (mssqlTableSharedLockStrategy{}).begin(context.Background(), src, table, 4)
	if err != nil {
		t.Fatal(err)
	}
	ranges := [][2]int{{1, 250_000}, {250_001, 500_000}, {500_001, 750_000}, {750_001, 1_000_000}}
	single := timeMSSQLRangeReads(t, []sourceQueryer{view.queryer}, ranges, false)
	strictReaders := make([]sourceQueryer, 4)
	for worker := range 4 {
		queryer, _, err := view.workerFactory(context.Background(), worker)
		if err != nil {
			t.Fatal(err)
		}
		strictReaders[worker] = queryer
	}
	strictParallel := timeMSSQLRangeReads(t, strictReaders, ranges, true)
	view.release()
	relaxedReaders := []sourceQueryer{db, db, db, db}
	relaxedParallel := timeMSSQLRangeReads(t, relaxedReaders, ranges, true)
	if strictParallel*2 > single {
		t.Fatalf("strict parallel reads %s are less than 2x faster than sequential strict %s", strictParallel, single)
	}
	delta := math.Abs(float64(strictParallel-relaxedParallel)) / float64(relaxedParallel)
	if delta > 0.15 {
		t.Fatalf("strict parallel reads %s differ from relaxed %s by %.1f%%, want <=15%%", strictParallel, relaxedParallel, delta*100)
	}
	t.Logf("sequential strict=%s parallel strict=%s relaxed parallel=%s", single, strictParallel, relaxedParallel)
}

func createMSSQLThroughputFixture(t *testing.T, db *sql.DB, table source.Table) source.Table {
	t.Helper()
	if _, err := db.Exec(`
CREATE TABLE dbo.throughput_events (id INT NOT NULL PRIMARY KEY, val INT NOT NULL);
WITH numbers AS (
    SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.throughput_events (id, val)
SELECT CONVERT(INT, id), CONVERT(INT, id) FROM numbers OPTION (MAXDOP 1);`); err != nil {
		t.Fatal(err)
	}
	table.Name = "throughput_events"
	table.RowCount = 1_000_000
	return table
}

func timeMSSQLRangeReads(t *testing.T, readers []sourceQueryer, ranges [][2]int, parallel bool) time.Duration {
	t.Helper()
	start := time.Now()
	read := func(queryer sourceQueryer, bounds [2]int) error {
		var sum int64
		return queryer.QueryRowContext(context.Background(), `SELECT COALESCE(SUM(CONVERT(BIGINT, val)), 0) FROM dbo.throughput_events WHERE id BETWEEN @p1 AND @p2 OPTION (MAXDOP 1)`, bounds[0], bounds[1]).Scan(&sum)
	}
	if !parallel {
		for _, bounds := range ranges {
			if err := read(readers[0], bounds); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start)
	}
	errCh := make(chan error, len(ranges))
	var wg sync.WaitGroup
	for i, bounds := range ranges {
		wg.Add(1)
		go func(queryer sourceQueryer, bounds [2]int) {
			defer wg.Done()
			errCh <- read(queryer, bounds)
		}(readers[i], bounds)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	return time.Since(start)
}

func openMSSQLStrictIntegration(t *testing.T) (*sql.DB, *sql.DB, *sql.DB, source.Table, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("sqlserver", "sqlserver://sa:TestPass2024@localhost:1433?database=master&encrypt=disable")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MSSQL_REQUIRED") == "1" {
			t.Fatalf("SQL Server required but unreachable: %v", err)
		}
		t.Skipf("SQL Server not reachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	dbName := "dmt_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := admin.Exec("CREATE DATABASE [" + dbName + "]"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + dbName + "]")
	})
	dsn := "sqlserver://sa:TestPass2024@localhost:1433?database=" + dbName + "&encrypt=disable"
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	writer, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := db.Exec(`CREATE TABLE dbo.events (id INT NOT NULL PRIMARY KEY, val INT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 100; id++ {
		if _, err := db.Exec(`INSERT INTO dbo.events VALUES (@p1, @p2)`, id, id); err != nil {
			t.Fatal(err)
		}
	}
	table := source.Table{Schema: "dbo", Name: "events", Columns: []source.Column{{Name: "id", DataType: "int"}, {Name: "val", DataType: "int"}}, PrimaryKey: []string{"id"}, RowCount: 100}
	table.PopulatePKColumns()
	return admin, db, writer, table, dbName
}

func waitMSSQLTransactions(t *testing.T, admin *sql.DB, dbName string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		if err := admin.QueryRow(`SELECT COUNT(*) FROM sys.dm_tran_session_transactions st JOIN sys.dm_exec_sessions s ON s.session_id = st.session_id WHERE s.database_id = DB_ID(@p1)`, dbName).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("SQL Server open transactions = %d, want %d", got, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

type mssqlStrictCaptureTarget struct {
	keysetRuntimeTargetPool
	mu         sync.Mutex
	firstOnce  sync.Once
	firstWrite chan struct{}
	writeGate  <-chan struct{}
	rows       map[int]int
}

func (p *mssqlStrictCaptureTarget) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
	p.firstOnce.Do(func() { close(p.firstWrite) })
	select {
	case <-p.writeGate:
	case <-ctx.Done():
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, row := range opts.Rows {
		id, idOK := keysetRuntimeInt(row[0])
		val, valOK := keysetRuntimeInt(row[1])
		if !idOK || !valOK {
			return fmt.Errorf("captured SQL Server row has unexpected values (%T, %T)", row[0], row[1])
		}
		p.rows[id] = val
	}
	return nil
}

func (p *mssqlStrictCaptureTarget) snapshotRows() map[int]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := make(map[int]int, len(p.rows))
	for id, val := range p.rows {
		rows[id] = val
	}
	return rows
}
