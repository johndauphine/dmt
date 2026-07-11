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
)

// These tests exercise the actual MySQL lock/session contract. They skip when
// MySQL is unavailable unless MYSQL_REQUIRED=1, matching the existing MySQL
// keyset integration suite.
func TestMySQLStrictParallelSharedViewIntegration(t *testing.T) {
	admin, db, writer, table := openMySQLStrictIntegration(t, "dmt_strict_shared_view")
	view, err := (mysqlLockWindowStrategy{}).begin(context.Background(), &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}, table, 4)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Exec(`UPDATE events SET val = 1001 WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`DELETE FROM events WHERE id = 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`INSERT INTO events VALUES (101, 101)`); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := view.queryer.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("snapshot count = %d, want lock-instant count 100", count)
	}

	got := make(map[int]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for worker := range 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			queryer, release, err := view.workerFactory(context.Background(), worker)
			if err != nil {
				errCh <- err
				return
			}
			defer release()
			rows, err := queryer.QueryContext(context.Background(), `SELECT id, val FROM events WHERE MOD(id, 4) = ? ORDER BY id`, worker)
			if err != nil {
				errCh <- err
				return
			}
			defer rows.Close()
			for rows.Next() {
				var id, val int
				if err := rows.Scan(&id, &val); err != nil {
					errCh <- err
					return
				}
				mu.Lock()
				got[id] = val
				mu.Unlock()
			}
			if err := rows.Err(); err != nil {
				errCh <- err
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	view.release()

	if len(got) != 100 {
		t.Fatalf("snapshot rows = %d, want 100", len(got))
	}
	for id := 1; id <= 100; id++ {
		if got[id] != id {
			t.Fatalf("snapshot row %d = %d, want pre-mutation value %d", id, got[id], id)
		}
	}
	waitMySQLStrictSessions(t, admin, table.Schema, 0)
}

func TestMySQLStrictParallelExecuteMutationIntegration(t *testing.T) {
	admin, db, writer, table := openMySQLStrictIntegration(t, "dmt_strict_execute")
	firstWrite := make(chan struct{})
	writeGate := make(chan struct{})
	target := &mysqlStrictCaptureTarget{firstWrite: firstWrite, writeGate: writeGate, rows: make(map[int]int)}
	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			StrictConsistency:    true,
			TargetMode:           "drop_recreate",
			ChunkSize:            2,
			ParallelReaders:      4,
			MaxSourceConnections: 5,
			ReadAheadBuffers:     1,
			WriteAheadWriters:    1,
		},
	}
	type executeResult struct {
		stats *TransferStats
		err   error
	}
	resultCh := make(chan executeResult, 1)
	go func() {
		stats, err := Execute(
			context.Background(),
			&mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}},
			target,
			cfg,
			Job{Table: table},
			progress.New(),
			nil,
		)
		resultCh <- executeResult{stats: stats, err: err}
	}()

	select {
	case <-firstWrite:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute did not begin streaming target batches")
	}
	// The first target batch proves Execute has finished the lock window and
	// started reading its pinned sessions. These commits occur while chunks are
	// still flowing but must remain invisible to every reader snapshot.
	for _, statement := range []string{
		`UPDATE events SET val = 1001 WHERE id = 1`,
		`DELETE FROM events WHERE id = 2`,
		`INSERT INTO events VALUES (101, 101)`,
	} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := writer.ExecContext(ctx, statement)
		cancel()
		if err != nil {
			t.Fatalf("post-window mutation %q: %v", statement, err)
		}
	}
	close(writeGate)
	var result executeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("strict Execute did not complete")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.stats.Rows != 100 {
		t.Fatalf("Execute rows = %d, want 100", result.stats.Rows)
	}
	got := target.snapshotRows()
	if len(got) != 100 {
		t.Fatalf("copied rows = %d, want lock-instant set of 100", len(got))
	}
	for id := 1; id <= 100; id++ {
		if got[id] != id {
			t.Fatalf("copied row %d = %d, want lock-instant value %d", id, got[id], id)
		}
	}
	if _, exists := got[101]; exists {
		t.Fatal("copied set included post-window insert 101")
	}
	waitMySQLStrictSessions(t, admin, table.Schema, 0)
}

func TestMySQLStrictLockWindowBoundsWriterBlockingIntegration(t *testing.T) {
	admin, db, writer, table := openMySQLStrictIntegration(t, "dmt_strict_lock_window")
	locked := make(chan struct{})
	continueReaders := make(chan struct{})
	type beginResult struct {
		view strictReaderView
		err  error
	}
	beginCh := make(chan beginResult, 1)
	go func() {
		view, err := (mysqlLockWindowStrategy{afterLock: func() {
			close(locked)
			<-continueReaders
		}}).begin(context.Background(), &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}, table, 2)
		beginCh <- beginResult{view: view, err: err}
	}()
	<-locked

	writerCh := make(chan error, 1)
	go func() {
		_, err := writer.Exec(`UPDATE events SET val = 777 WHERE id = 1`)
		writerCh <- err
	}()
	select {
	case err := <-writerCh:
		t.Fatalf("writer completed inside LOCK/UNLOCK window: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(continueReaders)

	var result beginResult
	select {
	case result = <-beginCh:
	case <-time.After(5 * time.Second):
		t.Fatal("reader sessions did not finish opening")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.view.release()
	select {
	case err := <-writerCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer remained blocked after UNLOCK TABLES")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := writer.ExecContext(ctx, `UPDATE events SET val = 778 WHERE id = 2`); err != nil {
		t.Fatalf("writer after lock window remained blocked while readers were open: %v", err)
	}
	var snapshotValue int
	if err := result.view.queryer.QueryRowContext(context.Background(), `SELECT val FROM events WHERE id = 1`).Scan(&snapshotValue); err != nil {
		t.Fatal(err)
	}
	if snapshotValue != 1 {
		t.Fatalf("reader snapshot saw post-window write %d, want pre-window value 1", snapshotValue)
	}
	result.view.release()
	waitMySQLStrictSessions(t, admin, table.Schema, 0)
}

func TestMySQLStrictTimeoutAuditsAndFallsBackIntegration(t *testing.T) {
	admin, db, writer, table := openMySQLStrictIntegration(t, "dmt_strict_fallback")
	tx, err := writer.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE events SET val = 900 WHERE id = 1`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	commitCh := make(chan error, 1)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		commitCh <- tx.Commit()
	}()

	oldStrategy := strictReaderStrategies[strictParallelLockWindow]
	strictReaderStrategies[strictParallelLockWindow] = mysqlLockWindowStrategy{lockWaitSeconds: 1}
	t.Cleanup(func() { strictReaderStrategies[strictParallelLockWindow] = oldStrategy })
	var auditCount int
	var auditFields map[string]any
	strictCtx, release, err := beginStrictSourceSnapshotWithOptions(
		context.Background(),
		&mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}},
		table,
		strictSnapshotBeginOptions{
			workerSessions: 4,
			auditEvent: func(event string, fields map[string]any) {
				if event == "strict_parallel_degraded" {
					auditCount++
					auditFields = fields
				}
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 || auditFields["reason"] != "lock_wait_timeout" || auditFields["error_code"] != uint16(1205) {
		t.Fatalf("fallback audit count=%d fields=%+v", auditCount, auditFields)
	}
	if sourceQueryerFactoryFor(strictCtx) != nil {
		t.Fatal("fallback retained a parallel reader factory")
	}
	var count int
	if err := sourceQueryerFor(strictCtx, db).QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("fallback snapshot count = %d, want 100", count)
	}
	release()
	if err := <-commitCh; err != nil {
		t.Fatal(err)
	}
	waitMySQLStrictSessions(t, admin, table.Schema, 0)
}

func TestMySQLStrictSessionsReleasedAfterFailureAndCancelIntegration(t *testing.T) {
	admin, db, _, table := openMySQLStrictIntegration(t, "dmt_strict_release")
	src := &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}
	missing := table
	missing.Name = "missing_events"
	if _, err := (mysqlLockWindowStrategy{}).begin(context.Background(), src, missing, 2); err == nil {
		t.Fatal("missing-table lock unexpectedly succeeded")
	}
	waitMySQLStrictSessions(t, admin, table.Schema, 0)

	locked := make(chan struct{})
	continueReaders := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := (mysqlLockWindowStrategy{afterLock: func() {
			close(locked)
			<-continueReaders
		}}).begin(ctx, src, table, 2)
		errCh <- err
	}()
	<-locked
	cancel()
	close(continueReaders)
	if err := <-errCh; err == nil {
		t.Fatal("canceled reader-session setup unexpectedly succeeded")
	}
	waitMySQLStrictSessions(t, admin, table.Schema, 0)
}

func TestMySQLStrictParallelReadThroughputIntegration(t *testing.T) {
	_, db, _, table := openMySQLStrictIntegration(t, "dmt_strict_throughput")
	table = createMySQLThroughputFixture(t, db, table)
	src := &mysqlTupleSourcePool{keysetRuntimeSourcePool{db: db}}
	singleView, err := (mysqlLockWindowStrategy{}).begin(context.Background(), src, table, 0)
	if err != nil {
		t.Fatal(err)
	}
	ranges := [][2]int{{1, 250_000}, {250_001, 500_000}, {500_001, 750_000}, {750_001, 1_000_000}}
	single := timeMySQLRangeReads(t, []sourceQueryer{singleView.queryer}, table, ranges, false)
	singleView.release()

	parallelView, err := (mysqlLockWindowStrategy{}).begin(context.Background(), src, table, 4)
	if err != nil {
		t.Fatal(err)
	}
	strictReaders := make([]sourceQueryer, 4)
	for worker := range 4 {
		queryer, _, err := parallelView.workerFactory(context.Background(), worker)
		if err != nil {
			t.Fatal(err)
		}
		strictReaders[worker] = queryer
	}
	strictParallel := timeMySQLRangeReads(t, strictReaders, table, ranges, true)
	parallelView.release()

	relaxedReaders := make([]sourceQueryer, 4)
	for i := range relaxedReaders {
		relaxedReaders[i] = db
	}
	relaxedParallel := timeMySQLRangeReads(t, relaxedReaders, table, ranges, true)
	if strictParallel*2 > single {
		t.Fatalf("strict parallel read %s is less than 2x faster than single strict %s", strictParallel, single)
	}
	delta := math.Abs(float64(strictParallel-relaxedParallel)) / float64(relaxedParallel)
	if delta > 0.15 {
		t.Fatalf("strict parallel read %s differs from relaxed %s by %.1f%%, want <=15%%", strictParallel, relaxedParallel, delta*100)
	}
	t.Logf("single strict=%s parallel strict=%s relaxed parallel=%s", single, strictParallel, relaxedParallel)
}

func createMySQLThroughputFixture(t *testing.T, db *sql.DB, table source.Table) source.Table {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE throughput_events (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO throughput_events VALUES (1, 1)`); err != nil {
		t.Fatal(err)
	}
	for rows := 1; rows < 1_000_000; {
		add := rows
		if rows+add > 1_000_000 {
			add = 1_000_000 - rows
		}
		if _, err := db.Exec(`INSERT INTO throughput_events SELECT id + ?, val + ? FROM throughput_events WHERE id <= ?`, rows, rows, add); err != nil {
			t.Fatal(err)
		}
		rows += add
	}
	table.Name = "throughput_events"
	table.RowCount = 1_000_000
	return table
}

func timeMySQLRangeReads(t *testing.T, readers []sourceQueryer, table source.Table, ranges [][2]int, parallel bool) time.Duration {
	t.Helper()
	start := time.Now()
	read := func(queryer sourceQueryer, bounds [2]int) error {
		query := `SELECT COALESCE(SUM(val), 0) FROM ` + mysqlTestIdentifier(table.Name) + ` WHERE id BETWEEN ? AND ?`
		var sum int64
		return queryer.QueryRowContext(context.Background(), query, bounds[0], bounds[1]).Scan(&sum)
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
	for worker, bounds := range ranges {
		wg.Add(1)
		go func(queryer sourceQueryer, bounds [2]int) {
			defer wg.Done()
			if err := read(queryer, bounds); err != nil {
				errCh <- fmt.Errorf("range read %s: %w", table.FullName(), err)
			}
		}(readers[worker], bounds)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	return time.Since(start)
}

func openMySQLStrictIntegration(t *testing.T, dbName string) (*sql.DB, *sql.DB, *sql.DB, source.Table) {
	t.Helper()
	dbName += "_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if testing.Short() {
		t.Skip("integration test; -short set")
	}
	admin, err := sql.Open("mysql", "root:TestPass2024@tcp(localhost:3306)/?multiStatements=true")
	if err == nil {
		err = admin.Ping()
	}
	if err != nil {
		if os.Getenv("MYSQL_REQUIRED") == "1" {
			t.Fatalf("mysql required but not reachable: %v", err)
		}
		t.Skipf("mysql not reachable: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if _, err := admin.Exec("DROP DATABASE IF EXISTS " + mysqlTestIdentifier(dbName)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("CREATE DATABASE " + mysqlTestIdentifier(dbName)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + mysqlTestIdentifier(dbName)); err != nil {
			t.Errorf("drop temporary MySQL database %s: %v", dbName, err)
		}
	})
	if _, err := admin.Exec("CREATE TABLE " + mysqlTestIdentifier(dbName) + ".events (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	for first := 1; first <= 100; first += 25 {
		query := "INSERT INTO " + mysqlTestIdentifier(dbName) + ".events VALUES "
		args := make([]any, 0, 50)
		for id := first; id < first+25; id++ {
			if id > first {
				query += ","
			}
			query += "(?,?)"
			args = append(args, id, id)
		}
		if _, err := admin.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	dsn := "root:TestPass2024@tcp(localhost:3306)/" + dbName + "?parseTime=true&charset=utf8mb4&loc=UTC"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { _ = db.Close() })
	writer, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	writer.SetMaxOpenConns(2)
	writer.SetMaxIdleConns(0)
	t.Cleanup(func() { _ = writer.Close() })
	return admin, db, writer, source.Table{
		Schema:     dbName,
		Name:       "events",
		Columns:    []source.Column{{Name: "id", DataType: "int"}, {Name: "val", DataType: "int"}},
		PrimaryKey: []string{"id"},
		RowCount:   100,
	}
}

func mysqlTestIdentifier(name string) string {
	return "`" + name + "`"
}

func waitMySQLStrictSessions(t *testing.T, admin *sql.DB, dbName string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var got int
		if err := admin.QueryRow(`SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = ?`, dbName).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL sessions for %s = %d, want %d", dbName, got, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

type mysqlStrictCaptureTarget struct {
	keysetRuntimeTargetPool
	mu         sync.Mutex
	firstOnce  sync.Once
	firstWrite chan struct{}
	writeGate  <-chan struct{}
	rows       map[int]int
}

func (p *mysqlStrictCaptureTarget) WriteBatch(ctx context.Context, opts driver.WriteBatchOptions) error {
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
			return fmt.Errorf("captured MySQL row has unexpected values (%T, %T)", row[0], row[1])
		}
		p.rows[id] = val
	}
	return nil
}

func (p *mysqlStrictCaptureTarget) snapshotRows() map[int]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := make(map[int]int, len(p.rows))
	for id, val := range p.rows {
		rows[id] = val
	}
	return rows
}
