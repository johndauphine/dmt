package transfer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
)

func TestStrictKeysetReaderPlan(t *testing.T) {
	tests := []struct {
		name                          string
		strict                        bool
		strategy                      string
		requested, maxSource          int
		wantReaders                   int
		wantJoinSnapshot, wantClamped bool
	}{
		{
			name:        "non strict preserves configured readers",
			strategy:    strictParallelExportedSnapshot,
			requested:   4,
			maxSource:   2,
			wantReaders: 4,
		},
		{
			name:             "postgres reserves lead connection",
			strict:           true,
			strategy:         strictParallelExportedSnapshot,
			requested:        4,
			maxSource:        3,
			wantReaders:      2,
			wantJoinSnapshot: true,
			wantClamped:      true,
		},
		{
			name:             "postgres unlimited pool preserves requested readers",
			strict:           true,
			strategy:         strictParallelExportedSnapshot,
			requested:        4,
			wantReaders:      4,
			wantJoinSnapshot: true,
		},
		{
			name:             "mysql reserves lock coordinator connection",
			strict:           true,
			strategy:         strictParallelLockWindow,
			requested:        4,
			maxSource:        4,
			wantReaders:      3,
			wantJoinSnapshot: true,
			wantClamped:      true,
		},
		{
			name:        "postgres uses lead when pool has one connection",
			strict:      true,
			strategy:    strictParallelExportedSnapshot,
			requested:   4,
			maxSource:   1,
			wantReaders: 1,
			wantClamped: true,
		},
		{
			name:        "non postgres remains one reader",
			strict:      true,
			requested:   4,
			maxSource:   12,
			wantReaders: 1,
			wantClamped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			strategy := strictReaderStrategies[tc.strategy]
			readers, joins, clamped := strictKeysetReaderPlan(tc.strict, strategy, tc.requested, tc.maxSource)
			if readers != tc.wantReaders || joins != tc.wantJoinSnapshot || clamped != tc.wantClamped {
				t.Fatalf("strictKeysetReaderPlan(%t, %q, %d, %d) = (%d, %t, %t), want (%d, %t, %t)", tc.strict, tc.strategy, tc.requested, tc.maxSource, readers, joins, clamped, tc.wantReaders, tc.wantJoinSnapshot, tc.wantClamped)
			}
		})
	}
}

type multiConnectionStrictStrategy struct{ budget int }

func (s multiConnectionStrictStrategy) begin(context.Context, pool.SourcePool, source.Table, int) (strictReaderView, error) {
	return strictReaderView{}, nil
}

func (s multiConnectionStrictStrategy) joinBudget() int { return s.budget }

func TestStrictKeysetReaderPlanHonorsMultiConnectionJoinBudget(t *testing.T) {
	strategy := multiConnectionStrictStrategy{budget: 2}
	readers, joins, clamped := strictKeysetReaderPlan(true, strategy, 4, 5)
	if readers != 3 || !joins || !clamped {
		t.Fatalf("five-connection plan = (%d, %t, %t), want (3, true, true)", readers, joins, clamped)
	}
	readers, joins, clamped = strictKeysetReaderPlan(true, strategy, 4, 2)
	if readers != 1 || joins || !clamped {
		t.Fatalf("reservation-only plan = (%d, %t, %t), want (1, false, true)", readers, joins, clamped)
	}
}

func TestStrictMigrationEpochReaderPlanUsesFullPoolBudget(t *testing.T) {
	readers, joins, clamped := strictKeysetReaderPlan(true, migrationEpochReaderStrategy{}, 4, 4)
	if readers != 4 || !joins || clamped {
		t.Fatalf("migration epoch plan = (%d, %t, %t), want (4, true, false)", readers, joins, clamped)
	}
	readers, joins, clamped = strictKeysetReaderPlan(true, migrationEpochReaderStrategy{}, 4, 3)
	if readers != 3 || !joins || !clamped {
		t.Fatalf("clamped migration epoch plan = (%d, %t, %t), want (3, true, true)", readers, joins, clamped)
	}
}

func TestStrictStrategyWorkerCountForTableUsesClampedPlan(t *testing.T) {
	table := source.Table{
		Columns:    []source.Column{{Name: "id", DataType: "bigint"}},
		PrimaryKey: []string{"id"},
	}
	table.PopulatePKColumns()

	workers, err := strictStrategyWorkerCountForTable("postgres", table, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 2 {
		t.Fatalf("worker sessions = %d, want two after reserving the lead connection", workers)
	}
	workers, err = strictStrategyWorkerCountForTable("postgres", table, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 0 {
		t.Fatalf("worker sessions with lead-only budget = %d, want zero", workers)
	}
	workers, err = strictStrategyWorkerCountForTable("mysql", table, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 3 {
		t.Fatalf("MySQL worker sessions = %d, want three after reserving the lock coordinator", workers)
	}
	workers, err = strictStrategyWorkerCountForTable("mysql", table, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 0 {
		t.Fatalf("single-reader MySQL worker sessions = %d, want zero", workers)
	}
	workers, err = strictStrategyWorkerCountForTable("mssql", table, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 3 {
		t.Fatalf("SQL Server worker sessions = %d, want three after reserving the lock coordinator", workers)
	}
	workers, err = strictStrategyWorkerCountForTable("mssql", table, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if workers != 0 {
		t.Fatalf("SQL Server workers with coordinator-only budget = %d, want zero", workers)
	}
}

func TestStrictReaderStrategyResolution(t *testing.T) {
	tests := []struct {
		dbType       string
		wantName     string
		wantStrategy bool
	}{
		{dbType: "postgres", wantName: strictParallelExportedSnapshot, wantStrategy: true},
		{dbType: "postgresql", wantName: strictParallelExportedSnapshot, wantStrategy: true},
		{dbType: "mysql", wantName: strictParallelLockWindow, wantStrategy: true},
		{dbType: "mariadb", wantName: strictParallelLockWindow, wantStrategy: true},
		{dbType: "maria", wantName: strictParallelLockWindow, wantStrategy: true},
		{dbType: "mssql", wantName: strictParallelTableSharedLock, wantStrategy: true},
		{dbType: "sqlite", wantName: strictParallelNone},
		{dbType: "clickhouse", wantName: strictParallelNone},
	}
	for _, tc := range tests {
		t.Run(tc.dbType, func(t *testing.T) {
			name, strategy, err := resolveStrictReaderStrategyForDBType(tc.dbType)
			if err != nil {
				t.Fatal(err)
			}
			if name != tc.wantName || (strategy != nil) != tc.wantStrategy {
				t.Fatalf("resolveStrictReaderStrategyForDBType(%q) = (%q, strategy=%t), want (%q, strategy=%t)", tc.dbType, name, strategy != nil, tc.wantName, tc.wantStrategy)
			}
		})
	}
}

func TestPostgresSnapshotReaderImportsBeforeFirstQuery(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	ctx, releaseLead, err := beginStrictSourceSnapshot(context.Background(), &postgresSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}, source.Table{Name: "events"})
	if err != nil {
		t.Fatalf("beginStrictSourceSnapshot: %v", err)
	}
	defer releaseLead()

	factory := sourceQueryerFactoryFor(ctx)
	if factory == nil {
		t.Fatal("PostgreSQL strict snapshot did not install a worker queryer factory")
	}
	queryer, releaseReader, err := factory(ctx, 0)
	if err != nil {
		t.Fatalf("join strict snapshot reader: %v", err)
	}
	defer releaseReader()
	rows, err := queryer.QueryContext(ctx, "SELECT worker_page")
	if err != nil {
		t.Fatalf("reader query: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close reader rows: %v", err)
	}

	want := []string{
		"begin",
		"query SELECT pg_export_snapshot()",
		"begin",
		"exec SET TRANSACTION SNAPSHOT '00000001-1'",
		"query SELECT worker_page",
	}
	if got := tracker.events(); !strictSnapshotEventPrefix(got, want) {
		t.Fatalf("snapshot events = %v, want prefix %v", got, want)
	}
}

func TestPostgresSnapshotJoinFailureRollsBackReader(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	tracker.importErr = errors.New("snapshot import rejected")
	ctx, releaseLead, err := beginStrictSourceSnapshot(context.Background(), &postgresSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}}, source.Table{Name: "events"})
	if err != nil {
		t.Fatalf("beginStrictSourceSnapshot: %v", err)
	}
	factory := sourceQueryerFactoryFor(ctx)
	if factory == nil {
		t.Fatal("PostgreSQL strict snapshot did not install a worker queryer factory")
	}
	if _, _, err := factory(ctx, 0); err == nil || !strings.Contains(err.Error(), "snapshot import rejected") {
		t.Fatalf("join strict snapshot reader error = %v, want import failure", err)
	}
	if got := tracker.count("rollback"); got != 1 {
		t.Fatalf("rollbacks after failed join = %d, want reader rollback", got)
	}
	releaseLead()
	if got := tracker.count("rollback"); got != 2 {
		t.Fatalf("rollbacks after lead release = %d, want reader and lead rollback", got)
	}
}

func TestMigrationSnapshotEpochExportsOnceAndReleasesLead(t *testing.T) {
	db, tracker := openSnapshotRecordingDB(t)
	epoch, err := BeginStrictSnapshotEpoch(context.Background(), &postgresSnapshotRecordingSource{keysetRuntimeSourcePool: &keysetRuntimeSourcePool{db: db}})
	if err != nil {
		t.Fatalf("BeginStrictSnapshotEpoch: %v", err)
	}
	ctx := context.Background()
	factory := sourceQueryerFactoryForJob(ctx, Job{StrictSnapshotEpoch: epoch})
	if factory == nil {
		t.Fatal("epoch did not provide a strict reader factory")
	}
	queryer, releaseReader, err := factory(ctx, 0)
	if err != nil {
		t.Fatalf("join epoch reader: %v", err)
	}
	rows, err := queryer.QueryContext(ctx, "SELECT epoch_page")
	if err != nil {
		t.Fatalf("epoch reader query: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	releaseReader()
	epoch.Close()
	epoch.Close() // cancellation/teardown paths may both attempt release

	want := []string{
		"begin",
		"query SELECT pg_export_snapshot()",
		"begin",
		"exec SET TRANSACTION SNAPSHOT '00000001-1'",
		"query SELECT epoch_page",
		"rollback",
		"rollback",
	}
	if got := tracker.events(); !strictSnapshotEventPrefix(got, want) {
		t.Fatalf("epoch events = %v, want prefix %v", got, want)
	}
}

func TestMigrationSnapshotEpochRejectsNonPostgres(t *testing.T) {
	reader, writer := openSnapshotSQLite(t, []string{`CREATE TABLE events (id INTEGER PRIMARY KEY)`})
	defer reader.Close()
	defer writer.Close()
	if _, err := BeginStrictSnapshotEpoch(context.Background(), &keysetRuntimeSourcePool{db: reader}); err == nil || !strings.Contains(err.Error(), "requires a PostgreSQL source") {
		t.Fatalf("BeginStrictSnapshotEpoch sqlite error = %v, want fail-closed PostgreSQL error", err)
	}
}

func TestKeysetProducerReleasesWorkerQueryersOnSuccessAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queryer  sourceQueryer
		ranges   []pkRange
		canceled bool
	}{
		{name: "success with no ranges", queryer: noopSnapshotQueryer{}},
		{name: "canceled reader", queryer: noopSnapshotQueryer{}, ranges: []pkRange{{minPK: int64(1), maxPK: int64(2), minInclusive: true}}, canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			acquired, released := 0, 0
			producer := &keysetProducer{
				db:         tc.queryer,
				numReaders: 4,
				pkRanges:   tc.ranges,
				queryerForWorker: func(context.Context, int) (sourceQueryer, func(), error) {
					mu.Lock()
					acquired++
					mu.Unlock()
					return tc.queryer, func() {
						mu.Lock()
						released++
						mu.Unlock()
					}, nil
				},
			}
			ctx := context.Background()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			producer.produce(ctx, pipelineEnv{}, make(chan chunkResult, producer.numReaders))
			mu.Lock()
			defer mu.Unlock()
			if acquired != producer.numReaders || released != producer.numReaders {
				t.Fatalf("worker queryers acquired/released = %d/%d, want %d/%d", acquired, released, producer.numReaders, producer.numReaders)
			}
		})
	}
}

type noopSnapshotQueryer struct{}

func (noopSnapshotQueryer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("query should not run")
}

func (noopSnapshotQueryer) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

type postgresSnapshotRecordingSource struct{ *keysetRuntimeSourcePool }

func (p *postgresSnapshotRecordingSource) DBType() string { return "postgres" }

func strictSnapshotEventPrefix(got, want []string) bool {
	if len(got) < len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

const snapshotRecordingDriverName = "dmt-strict-snapshot-recording"

var snapshotRecordingRegistry = struct {
	sync.Mutex
	next     int
	trackers map[string]*snapshotRecordingTracker
	once     sync.Once
}{trackers: make(map[string]*snapshotRecordingTracker)}

type snapshotRecordingTracker struct {
	sync.Mutex
	eventLog    []string
	importErr   error
	execErrors  map[string]error
	queryErrors map[string]error
}

func (t *snapshotRecordingTracker) record(event string) {
	t.Lock()
	defer t.Unlock()
	t.eventLog = append(t.eventLog, event)
}

func (t *snapshotRecordingTracker) events() []string {
	t.Lock()
	defer t.Unlock()
	return append([]string(nil), t.eventLog...)
}

func (t *snapshotRecordingTracker) count(event string) int {
	t.Lock()
	defer t.Unlock()
	count := 0
	for _, recorded := range t.eventLog {
		if recorded == event {
			count++
		}
	}
	return count
}

func (t *snapshotRecordingTracker) failExec(query string, err error) {
	t.Lock()
	defer t.Unlock()
	if t.execErrors == nil {
		t.execErrors = make(map[string]error)
	}
	t.execErrors[query] = err
}

func (t *snapshotRecordingTracker) failQuery(query string, err error) {
	t.Lock()
	defer t.Unlock()
	if t.queryErrors == nil {
		t.queryErrors = make(map[string]error)
	}
	t.queryErrors[query] = err
}

func openSnapshotRecordingDB(t *testing.T) (*sql.DB, *snapshotRecordingTracker) {
	t.Helper()
	snapshotRecordingRegistry.once.Do(func() { sql.Register(snapshotRecordingDriverName, snapshotRecordingDriver{}) })
	snapshotRecordingRegistry.Lock()
	snapshotRecordingRegistry.next++
	dsn := fmt.Sprintf("snapshot-%d", snapshotRecordingRegistry.next)
	tracker := &snapshotRecordingTracker{}
	snapshotRecordingRegistry.trackers[dsn] = tracker
	snapshotRecordingRegistry.Unlock()

	db, err := sql.Open(snapshotRecordingDriverName, dsn)
	if err != nil {
		t.Fatalf("open recording DB: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		snapshotRecordingRegistry.Lock()
		delete(snapshotRecordingRegistry.trackers, dsn)
		snapshotRecordingRegistry.Unlock()
	})
	return db, tracker
}

type snapshotRecordingDriver struct{}

func (snapshotRecordingDriver) Open(dsn string) (driver.Conn, error) {
	snapshotRecordingRegistry.Lock()
	tracker := snapshotRecordingRegistry.trackers[dsn]
	snapshotRecordingRegistry.Unlock()
	if tracker == nil {
		return nil, fmt.Errorf("unknown snapshot recording DSN %q", dsn)
	}
	return &snapshotRecordingConn{tracker: tracker}, nil
}

type snapshotRecordingConn struct{ tracker *snapshotRecordingTracker }

func (c *snapshotRecordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not expected")
}

func (c *snapshotRecordingConn) Close() error {
	c.tracker.record("close")
	return nil
}

func (c *snapshotRecordingConn) Begin() (driver.Tx, error) { return c.begin() }

func (c *snapshotRecordingConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.begin()
}

func (c *snapshotRecordingConn) begin() (driver.Tx, error) {
	c.tracker.record("begin")
	return snapshotRecordingTx{tracker: c.tracker}, nil
}

func (c *snapshotRecordingConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.tracker.record("query " + query)
	c.tracker.Lock()
	err := c.tracker.queryErrors[query]
	c.tracker.Unlock()
	if err != nil {
		return nil, err
	}
	if query == "SELECT pg_export_snapshot()" {
		return &snapshotRecordingRows{values: []driver.Value{"00000001-1"}}, nil
	}
	if strings.Contains(query, "information_schema.TABLES") {
		return &snapshotRecordingRows{values: []driver.Value{"InnoDB"}}, nil
	}
	return &snapshotRecordingRows{}, nil
}

func (c *snapshotRecordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.tracker.record("exec " + query)
	c.tracker.Lock()
	err := c.tracker.importErr
	if queryErr := c.tracker.execErrors[query]; queryErr != nil {
		err = queryErr
	}
	c.tracker.Unlock()
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

type snapshotRecordingTx struct{ tracker *snapshotRecordingTracker }

func (tx snapshotRecordingTx) Commit() error {
	tx.tracker.record("commit")
	return nil
}

func (tx snapshotRecordingTx) Rollback() error {
	tx.tracker.record("rollback")
	return nil
}

type snapshotRecordingRows struct {
	values []driver.Value
	sent   bool
}

func (r *snapshotRecordingRows) Columns() []string { return []string{"value"} }

func (r *snapshotRecordingRows) Close() error { return nil }

func (r *snapshotRecordingRows) Next(dest []driver.Value) error {
	if r.sent || len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values)
	r.sent = true
	return nil
}
