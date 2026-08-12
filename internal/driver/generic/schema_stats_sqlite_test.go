package generic

import (
	"bytes"
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

type sqliteSchemaStatsHistory struct {
	saved *checkpoint.TuningRecord
}

func (*sqliteSchemaStatsHistory) GetRuntimeAdjustments(int) ([]checkpoint.RuntimeAdjustmentRecord, error) {
	return nil, nil
}

func (*sqliteSchemaStatsHistory) GetTuningHistory(int, string, string) ([]checkpoint.TuningRecord, error) {
	return nil, nil
}

func (h *sqliteSchemaStatsHistory) SaveTuningRecord(record checkpoint.TuningRecord) (int64, error) {
	h.saved = &record
	return 77, nil
}

func (*sqliteSchemaStatsHistory) UpdateTuningResult(int64, float64, float64, int, bool) error {
	return nil
}

func TestSQLiteSchemaStatsAnalyzerArmsAndSavesHistory(t *testing.T) {
	path := fixtureDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	history := &sqliteSchemaStatsHistory{}
	analyzer := driver.NewSmartConfigAnalyzer(db, "sqlite")
	analyzer.SetTargetDBType("sqlite")
	analyzer.SetMemoryEnvelope(8*1024, 4*1024, 2*1024)
	analyzer.SetHistoryProvider(history)
	targetPath := path + ".target"
	analyzer.SetWorkloadIdentity("", 0, path, "", "", 0, targetPath, "")

	got, err := analyzer.Analyze(context.Background(), "")
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if got.TotalTables != 3 || got.TotalRows != 8 {
		t.Fatalf("SQLite analyzer totals = %d/%d, want 3 tables/8 rows", got.TotalTables, got.TotalRows)
	}
	if len(got.DateColumns["line_items"]) == 0 || got.Workers <= 0 || got.ChunkSizeRecommendation <= 0 {
		t.Fatalf("SQLite analyzer did not produce dates and tuning: %+v", got)
	}

	rowID := analyzer.SaveTuningWithActualParams(driver.ActualParams{
		Workers:              got.Workers,
		ChunkSize:            got.ChunkSizeRecommendation,
		ReadAheadBuffers:     got.ReadAheadBuffers,
		WriteAheadWriters:    got.WriteAheadWriters,
		ParallelReaders:      got.ParallelReaders,
		MaxPartitions:        got.MaxPartitions,
		MaxSourceConnections: got.MaxSourceConnections,
		MaxTargetConnections: got.MaxTargetConnections,
	})
	if rowID != 77 || history.saved == nil {
		t.Fatalf("SQLite pending history save = row %d / %#v, want row 77", rowID, history.saved)
	}
	if history.saved.SourceDBType != "sqlite" || history.saved.TargetDBType != "sqlite" ||
		history.saved.SourceDatabase != path || history.saved.TargetDatabase != targetPath ||
		history.saved.SourceHost != "" || history.saved.TargetHost != "" ||
		history.saved.SourcePort != 0 || history.saved.TargetPort != 0 {
		t.Fatalf("SQLite history identity = %+v, want portless source/target paths", history.saved)
	}
}

func TestSQLiteSchemaStatsReaderRealFixture(t *testing.T) {
	ctx := context.Background()
	path := fixtureDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const oddTable = `odd"table`
	if _, err := db.Exec(`
		CREATE TABLE "odd""table" (
			id INTEGER PRIMARY KEY,
			recorded_on DATE,
			fired_at TIME(6),
			stamp TIMESTAMP WITH TIME ZONE,
			note TEXT
		);
		INSERT INTO "odd""table" VALUES
			(1, '2025-01-01', '10:30:00', '2025-01-01 10:30:00+00:00', 'not a date'),
			(2, '2025-01-02', '11:30:00', '2025-01-02 11:30:00+00:00', 'still text');
	`); err != nil {
		t.Fatalf("seed odd-name table: %v", err)
	}

	reader := newSQLiteSchemaStatsReader(testCatalog(t))
	stats, err := reader.TableStats(ctx, db, "ignored", nil)
	if err != nil {
		t.Fatalf("TableStats: %v", err)
	}
	if len(stats) != 4 {
		t.Fatalf("stats = %+v, want four user tables (sqlite_sequence excluded)", stats)
	}

	byName := make(map[string]driver.TableStatRow, len(stats))
	for _, stat := range stats {
		byName[stat.Name] = stat
		if strings.HasPrefix(stat.Name, "sqlite_") {
			t.Errorf("internal table escaped enumeration: %+v", stat)
		}
	}
	wantCounts := map[string]int64{
		"users": 3, "orders": 2, "line_items": 3, oddTable: 2,
	}
	for table, want := range wantCounts {
		got, ok := byName[table]
		if !ok {
			t.Errorf("missing table %q in %+v", table, stats)
			continue
		}
		if got.RowCount != want {
			t.Errorf("%s row count = %d, want %d", table, got.RowCount, want)
		}
		if got.AvgRowSizeBytes <= 0 {
			t.Errorf("%s dbstat width = %d, want positive fixture payload", table, got.AvgRowSizeBytes)
		}
	}

	// Results retain the server-reader contract: row-count descending with a
	// stable name tie-break. This preserves the legacy top-five sample order.
	wantOrder := []string{"line_items", "users", oddTable, "orders"}
	gotOrder := make([]string, len(stats))
	for i := range stats {
		gotOrder[i] = stats[i].Name
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Errorf("stats order = %v, want %v", gotOrder, wantOrder)
	}

	dates, err := reader.DateColumns(ctx, db, "ignored", []string{oddTable, "line_items"})
	if err != nil {
		t.Fatalf("DateColumns: %v", err)
	}
	wantDates := map[string][]string{
		oddTable:     {"recorded_on", "fired_at", "stamp"},
		"line_items": {"updated_at"},
	}
	if !reflect.DeepEqual(dates, wantDates) {
		t.Errorf("date columns = %#v, want %#v", dates, wantDates)
	}

	// Every family recommended by schema stats must also be accepted by the
	// incremental reader that consumes a chosen recommendation at runtime.
	incremental, ok := openReader(t, path).(driver.IncrementalDateReader)
	if !ok {
		t.Fatal("SQLite reader did not expose incremental-date support")
	}
	wantFamilies := map[string]string{
		"recorded_on": "date",
		"fired_at":    "time",
		"stamp":       "timestamp",
	}
	for column, family := range wantFamilies {
		gotColumn, gotFamily, found := incremental.GetDateColumnInfo(ctx, "", oddTable, []string{column})
		if !found || gotColumn != column || gotFamily != family {
			t.Errorf("incremental date %q = (%q, %q, %v), want (%q, %q, true)",
				column, gotColumn, gotFamily, found, column, family)
		}
	}

	// If COUNT interpolation were not identifier-quoted, the embedded quote in
	// oddTable would have failed above. Also prove the fixture remains intact.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("safe table after odd identifier count = %d, %v", count, err)
	}
}

func TestSQLiteSchemaStatsCapAndFilterPrecedePerTableWork(t *testing.T) {
	names := make([]string, 105)
	counts := make(map[string]int64, len(names))
	for i := range names {
		names[i] = fmt.Sprintf("table_%03d", i)
		counts[names[i]] = int64(i + 1)
	}

	t.Run("unfiltered cap", func(t *testing.T) {
		plan := &sqliteStatsRecordingPlan{names: names, counts: counts}
		db := openSQLiteStatsRecordingDB(t, plan)
		reader := newSQLiteSchemaStatsReader(testCatalog(t))

		var logs bytes.Buffer
		oldLevel := logging.GetLevel()
		logging.SetLevel(logging.LevelInfo)
		logging.SetOutput(&logs)
		t.Cleanup(func() {
			logging.SetOutput(os.Stdout)
			logging.SetLevel(oldLevel)
		})

		stats, err := reader.TableStats(context.Background(), db, "", nil)
		if err != nil {
			t.Fatalf("TableStats: %v", err)
		}
		if len(stats) != sqliteSchemaStatsTableCap {
			t.Fatalf("stats count = %d, want cap %d", len(stats), sqliteSchemaStatsTableCap)
		}
		if got := strings.Count(logs.String(), "limited to the first 100"); got != 1 {
			t.Fatalf("cap warnings = %d, logs=%q", got, logs.String())
		}

		allowed := make([]string, len(stats))
		for i := range stats {
			allowed[i] = stats[i].Name
		}
		if _, err := reader.DateColumns(context.Background(), db, "", allowed); err != nil {
			t.Fatalf("DateColumns: %v", err)
		}

		countsSeen, dbstatSeen, pragmaSeen, enumerationClosed := plan.snapshot()
		if !enumerationClosed {
			t.Fatal("per-table query began before enumeration rows closed")
		}
		for _, operation := range []struct {
			name string
			seen map[string]int
		}{{"COUNT", countsSeen}, {"dbstat", dbstatSeen}, {"PRAGMA", pragmaSeen}} {
			if len(operation.seen) != sqliteSchemaStatsTableCap {
				t.Errorf("%s tables = %d, want %d", operation.name, len(operation.seen), sqliteSchemaStatsTableCap)
			}
			for i := sqliteSchemaStatsTableCap; i < len(names); i++ {
				if operation.seen[names[i]] != 0 {
					t.Errorf("%s work reached over-cap table %q", operation.name, names[i])
				}
			}
		}
	})

	t.Run("explicit filter can select beyond cap", func(t *testing.T) {
		plan := &sqliteStatsRecordingPlan{names: names, counts: counts}
		db := openSQLiteStatsRecordingDB(t, plan)
		reader := newSQLiteSchemaStatsReader(testCatalog(t))
		wanted := map[string]bool{"table_002": true, "table_104": true}

		stats, err := reader.TableStats(context.Background(), db, "", func(name string) bool {
			return wanted[name]
		})
		if err != nil {
			t.Fatalf("TableStats: %v", err)
		}
		if got := []string{stats[0].Name, stats[1].Name}; !reflect.DeepEqual(got, []string{"table_104", "table_002"}) {
			t.Fatalf("filtered stats = %+v, want selected tables in row-count order", stats)
		}
		if _, err := reader.DateColumns(context.Background(), db, "", []string{stats[0].Name, stats[1].Name}); err != nil {
			t.Fatalf("DateColumns: %v", err)
		}

		countSeen, dbstatSeen, pragmaSeen, _ := plan.snapshot()
		for _, seen := range []map[string]int{countSeen, dbstatSeen, pragmaSeen} {
			if len(seen) != 2 || seen["table_002"] != 1 || seen["table_104"] != 1 {
				t.Errorf("per-table work escaped explicit filter: %#v", seen)
			}
		}
	})
}

func TestSQLiteSchemaStatsDBStatFallbackAndCancellation(t *testing.T) {
	t.Run("unavailable width is unknown", func(t *testing.T) {
		plan := &sqliteStatsRecordingPlan{
			names:     []string{"items"},
			counts:    map[string]int64{"items": 3},
			dbstatErr: errors.New("no such table: dbstat"),
		}
		reader := newSQLiteSchemaStatsReader(testCatalog(t))
		stats, err := reader.TableStats(context.Background(), openSQLiteStatsRecordingDB(t, plan), "", nil)
		if err != nil {
			t.Fatalf("TableStats: %v", err)
		}
		if len(stats) != 1 || stats[0].RowCount != 3 || stats[0].AvgRowSizeBytes != 0 {
			t.Fatalf("dbstat fallback = %+v, want count retained with unknown width", stats)
		}
	})

	t.Run("cancellation is not downgraded", func(t *testing.T) {
		plan := &sqliteStatsRecordingPlan{
			names:     []string{"items"},
			counts:    map[string]int64{"items": 3},
			dbstatErr: context.Canceled,
		}
		reader := newSQLiteSchemaStatsReader(testCatalog(t))
		_, err := reader.TableStats(context.Background(), openSQLiteStatsRecordingDB(t, plan), "", nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TableStats error = %v, want context.Canceled", err)
		}
	})
}

func TestSQLiteSchemaStatsExcludesVirtualTableShadows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
		CREATE VIRTUAL TABLE docs USING fts5(body);
		INSERT INTO docs(body) VALUES ('searchable');
	`); err != nil {
		t.Fatalf("create FTS fixture: %v", err)
	}

	reader := newSQLiteSchemaStatsReader(testCatalog(t))
	stats, err := reader.TableStats(context.Background(), db, "", nil)
	if err != nil {
		t.Fatalf("TableStats: %v", err)
	}
	if len(stats) != 1 || stats[0].Name != "docs" || stats[0].RowCount != 1 {
		t.Fatalf("FTS stats = %+v, want only the user-visible virtual table", stats)
	}
}

func TestSQLiteDeclaredDateType(t *testing.T) {
	tests := map[string]bool{
		"DATE":                     true,
		"date":                     true,
		"DATETIME(6)":              true,
		"TIMESTAMP WITH TIME ZONE": true,
		"TIME(3)":                  true,
		"TEXT":                     false,
		"UPDATE_COUNTER":           false,
		"":                         false,
	}
	for declared, want := range tests {
		if got := sqliteDeclaredDateType(declared); got != want {
			t.Errorf("sqliteDeclaredDateType(%q) = %v, want %v", declared, got, want)
		}
	}
}

var sqliteStatsRecordingSequence atomic.Uint64

type sqliteStatsRecordingPlan struct {
	mu sync.Mutex

	names     []string
	counts    map[string]int64
	dbstatErr error

	enumerationOpen   bool
	enumerationClosed bool
	countSeen         map[string]int
	dbstatSeen        map[string]int
	pragmaSeen        map[string]int
}

func (p *sqliteStatsRecordingPlan) query(query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	normalized := strings.ToUpper(strings.Join(strings.Fields(query), " "))

	switch {
	case strings.Contains(normalized, "FROM PRAGMA_TABLE_LIST"):
		p.enumerationOpen = true
		rows := make([][]sqldriver.Value, len(p.names))
		for i, name := range p.names {
			rows[i] = []sqldriver.Value{name}
		}
		return &sqliteStatsRecordingRows{
			columns: []string{"name"},
			rows:    rows,
			onClose: func() {
				p.mu.Lock()
				p.enumerationOpen = false
				p.enumerationClosed = true
				p.mu.Unlock()
			},
		}, nil

	case strings.HasPrefix(normalized, "SELECT COUNT(*) FROM"):
		if p.enumerationOpen {
			return nil, errors.New("COUNT started before enumeration rows closed")
		}
		name, err := sqliteStatsCountTable(query)
		if err != nil {
			return nil, err
		}
		if p.countSeen == nil {
			p.countSeen = make(map[string]int)
		}
		p.countSeen[name]++
		return &sqliteStatsRecordingRows{
			columns: []string{"count"},
			rows:    [][]sqldriver.Value{{p.counts[name]}},
		}, nil

	case strings.Contains(normalized, "FROM DBSTAT"):
		if p.enumerationOpen {
			return nil, errors.New("dbstat started before enumeration rows closed")
		}
		name, err := sqliteStatsStringArg(args)
		if err != nil {
			return nil, err
		}
		if p.dbstatSeen == nil {
			p.dbstatSeen = make(map[string]int)
		}
		p.dbstatSeen[name]++
		if p.dbstatErr != nil {
			return nil, p.dbstatErr
		}
		return &sqliteStatsRecordingRows{
			columns: []string{"avg_payload"},
			rows:    [][]sqldriver.Value{{int64(32)}},
		}, nil

	case strings.Contains(normalized, "FROM PRAGMA_TABLE_INFO"):
		if p.enumerationOpen {
			return nil, errors.New("pragma started before enumeration rows closed")
		}
		name, err := sqliteStatsStringArg(args)
		if err != nil {
			return nil, err
		}
		if p.pragmaSeen == nil {
			p.pragmaSeen = make(map[string]int)
		}
		p.pragmaSeen[name]++
		return &sqliteStatsRecordingRows{
			columns: []string{"name", "type"},
			rows: [][]sqldriver.Value{
				{"id", "INTEGER"}, {"happened_at", "DATETIME"}, {"note", "TEXT"},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected query: %s", normalized)
	}
}

func (p *sqliteStatsRecordingPlan) snapshot() (counts, dbstat, pragma map[string]int, enumerationClosed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneIntMap(p.countSeen), cloneIntMap(p.dbstatSeen), cloneIntMap(p.pragmaSeen), p.enumerationClosed
}

func cloneIntMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type sqliteStatsRecordingDriver struct{ plan *sqliteStatsRecordingPlan }

func (d *sqliteStatsRecordingDriver) Open(string) (sqldriver.Conn, error) {
	return &sqliteStatsRecordingConn{plan: d.plan}, nil
}

type sqliteStatsRecordingConn struct{ plan *sqliteStatsRecordingPlan }

func (c *sqliteStatsRecordingConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *sqliteStatsRecordingConn) Close() error { return nil }
func (c *sqliteStatsRecordingConn) Begin() (sqldriver.Tx, error) {
	return nil, errors.New("transactions unsupported")
}
func (c *sqliteStatsRecordingConn) QueryContext(_ context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return c.plan.query(query, args)
}

var _ sqldriver.QueryerContext = (*sqliteStatsRecordingConn)(nil)

type sqliteStatsRecordingRows struct {
	columns []string
	rows    [][]sqldriver.Value
	index   int
	onClose func()
	once    sync.Once
}

func (r *sqliteStatsRecordingRows) Columns() []string { return r.columns }
func (r *sqliteStatsRecordingRows) Close() error {
	r.once.Do(func() {
		if r.onClose != nil {
			r.onClose()
		}
	})
	return nil
}
func (r *sqliteStatsRecordingRows) Next(dest []sqldriver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func openSQLiteStatsRecordingDB(t *testing.T, plan *sqliteStatsRecordingPlan) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("dmt-sqlite-schema-stats-%d", sqliteStatsRecordingSequence.Add(1))
	sql.Register(name, &sqliteStatsRecordingDriver{plan: plan})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func sqliteStatsStringArg(args []sqldriver.NamedValue) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("args = %#v, want one table name", args)
	}
	name, ok := args[0].Value.(string)
	if !ok {
		return "", fmt.Errorf("table arg = %T, want string", args[0].Value)
	}
	return name, nil
}

func sqliteStatsCountTable(query string) (string, error) {
	const prefix = "SELECT COUNT(*) FROM "
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), prefix) {
		return "", fmt.Errorf("not a count query: %q", query)
	}
	quoted := strings.TrimSpace(trimmed[len(prefix):])
	if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
		return "", fmt.Errorf("COUNT identifier is not double-quoted: %q", query)
	}
	return strings.ReplaceAll(quoted[1:len(quoted)-1], `""`, `"`), nil
}
