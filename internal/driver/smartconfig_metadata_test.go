package driver

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
)

const metadataShimDriverName = "dmt-smartconfig-metadata-test"

const (
	metadataTableStatsQuery  = "SELECT table_name, row_count, avg_row_size FROM schema_stats WHERE schema_name = ? ORDER BY row_count DESC"
	metadataDateColumnsQuery = "SELECT table_name, column_name FROM schema_dates WHERE schema_name = ? ORDER BY table_name, ordinal_position"
)

var (
	metadataShimRegisterOnce sync.Once
	metadataShimSequence     atomic.Uint64
	metadataShimPlans        sync.Map
)

type metadataShimPlan struct {
	mu sync.Mutex

	statsRows [][]sqldriver.Value
	dateRows  [][]sqldriver.Value

	dateQueryErr error
	dateRowsErr  error
	dateReady    func() bool

	queryCount        int
	dateQuerySQL      string
	dateQueryArgs     []sqldriver.NamedValue
	dateQueryReceived time.Time
	dateReadyAtQuery  bool
	dateDeadline      time.Time
	dateDeadlineSeen  bool
}

type schemaStatsProviderTestDriver struct {
	*tuningProfileTestDriver
	reader    SchemaStatsReader
	supported bool
}

func (d *schemaStatsProviderTestDriver) SchemaStatsReader() (SchemaStatsReader, bool) {
	return d.reader, d.supported
}

func (p *metadataShimPlan) query(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queryCount++
	switch p.queryCount {
	case 1:
		return &metadataShimRows{
			columns: []string{"table_name", "row_count", "avg_row_size"},
			rows:    cloneMetadataRows(p.statsRows),
		}, nil
	case 2:
		p.dateQuerySQL = query
		p.dateQueryArgs = append([]sqldriver.NamedValue(nil), args...)
		p.dateQueryReceived = time.Now()
		if p.dateReady != nil {
			p.dateReadyAtQuery = p.dateReady()
		}
		p.dateDeadline, p.dateDeadlineSeen = ctx.Deadline()
		if p.dateQueryErr != nil {
			return nil, p.dateQueryErr
		}
		return &metadataShimRows{
			columns:     []string{"table_name", "column_name"},
			rows:        cloneMetadataRows(p.dateRows),
			terminalErr: p.dateRowsErr,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected metadata query %d", p.queryCount)
	}
}

func (p *metadataShimPlan) snapshot() (queries int, deadline time.Time, deadlineSeen bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queryCount, p.dateDeadline, p.dateDeadlineSeen
}

func (p *metadataShimPlan) dateQueryObservation() (query string, args []sqldriver.NamedValue, received time.Time, ready bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dateQuerySQL, append([]sqldriver.NamedValue(nil), p.dateQueryArgs...), p.dateQueryReceived, p.dateReadyAtQuery
}

func cloneMetadataRows(rows [][]sqldriver.Value) [][]sqldriver.Value {
	cloned := make([][]sqldriver.Value, len(rows))
	for i := range rows {
		cloned[i] = append([]sqldriver.Value(nil), rows[i]...)
	}
	return cloned
}

type metadataShimDriver struct{}

func (metadataShimDriver) Open(name string) (sqldriver.Conn, error) {
	value, ok := metadataShimPlans.Load(name)
	if !ok {
		return nil, fmt.Errorf("metadata shim plan %q not found", name)
	}
	return &metadataShimConn{plan: value.(*metadataShimPlan)}, nil
}

type metadataShimConn struct {
	plan *metadataShimPlan
}

func (c *metadataShimConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, errors.New("metadata shim does not support prepared statements")
}
func (c *metadataShimConn) Close() error { return nil }
func (c *metadataShimConn) Begin() (sqldriver.Tx, error) {
	return nil, errors.New("metadata shim does not support transactions")
}
func (c *metadataShimConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return c.plan.query(ctx, query, args)
}

var _ sqldriver.QueryerContext = (*metadataShimConn)(nil)

type metadataShimRows struct {
	columns     []string
	rows        [][]sqldriver.Value
	index       int
	terminalErr error
	errReturned bool
}

func (r *metadataShimRows) Columns() []string { return r.columns }
func (r *metadataShimRows) Close() error      { return nil }
func (r *metadataShimRows) Next(dest []sqldriver.Value) error {
	if r.index < len(r.rows) {
		copy(dest, r.rows[r.index])
		r.index++
		return nil
	}
	if r.terminalErr != nil && !r.errReturned {
		r.errReturned = true
		return r.terminalErr
	}
	return io.EOF
}

func openMetadataShim(t *testing.T, plan *metadataShimPlan) *sql.DB {
	t.Helper()
	metadataShimRegisterOnce.Do(func() {
		sql.Register(metadataShimDriverName, metadataShimDriver{})
	})
	dsn := fmt.Sprintf("plan-%d", metadataShimSequence.Add(1))
	metadataShimPlans.Store(dsn, plan)
	db, err := sql.Open(metadataShimDriverName, dsn)
	if err != nil {
		metadataShimPlans.Delete(dsn)
		t.Fatalf("sql.Open metadata shim: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = db.Close()
		metadataShimPlans.Delete(dsn)
	})
	return db
}

func newMetadataAnalyzer(t *testing.T, plan *metadataShimPlan, dbType string) *SmartConfigAnalyzer {
	t.Helper()
	analyzer := NewSmartConfigAnalyzer(openMetadataShim(t, plan), dbType)
	analyzer.schemaStatsReader = NewQuerySchemaStatsReader(metadataTableStatsQuery, metadataDateColumnsQuery)
	return analyzer
}

func metadataStatsRow(name string, rowCount, avgRowSize int64) []sqldriver.Value {
	return []sqldriver.Value{name, rowCount, avgRowSize}
}

func metadataDateRow(table, column string) []sqldriver.Value {
	return []sqldriver.Value{table, column}
}

func requireValidMetadataTuning(t *testing.T, got *SmartConfigSuggestions) {
	t.Helper()
	if got == nil {
		t.Fatal("Analyze returned nil suggestions")
	}
	if got.Workers <= 0 || got.ChunkSizeRecommendation <= 0 || got.ReadAheadBuffers <= 0 ||
		got.WriteAheadWriters <= 0 || got.ParallelReaders <= 0 {
		t.Fatalf("incomplete tuning after metadata analysis: %+v", got)
	}
	if got.Tier == "" || got.Reasoning == "" {
		t.Fatalf("missing tuning provenance: tier=%q reasoning=%q", got.Tier, got.Reasoning)
	}
}

func actualParamsFromSuggestions(got *SmartConfigSuggestions) ActualParams {
	return ActualParams{
		Workers:              got.Workers,
		ChunkSize:            got.ChunkSizeRecommendation,
		ReadAheadBuffers:     got.ReadAheadBuffers,
		WriteAheadWriters:    got.WriteAheadWriters,
		ParallelReaders:      got.ParallelReaders,
		MaxPartitions:        got.MaxPartitions,
		MaxSourceConnections: got.MaxSourceConnections,
		MaxTargetConnections: got.MaxTargetConnections,
	}
}

func requireBatchedDateQueryShape(t *testing.T, dbType, schema, query string, args []sqldriver.NamedValue) {
	t.Helper()
	if len(args) != 1 || args[0].Value != schema {
		t.Fatalf("%s date query args = %#v, want one schema argument %q", dbType, args, schema)
	}

	normalized := strings.Join(strings.Fields(strings.ToLower(query)), " ")
	selectPair := "select table_name, column_name from"
	orderBy := "order by table_name, ordinal_position"
	staleTablePredicate := "and table_name = ?"
	if !strings.Contains(normalized, selectPair) {
		t.Fatalf("%s date query does not select table/column pairs: %s", dbType, normalized)
	}
	if !strings.Contains(normalized, orderBy) {
		t.Fatalf("%s date query does not preserve table/ordinal order: %s", dbType, normalized)
	}
	if strings.Contains(normalized, staleTablePredicate) {
		t.Fatalf("%s date query retained a per-table predicate: %s", dbType, normalized)
	}
}

func TestAnalyzeServerMetadataQueryCount(t *testing.T) {
	for _, dbType := range []string{"mssql", "postgres", "mysql"} {
		t.Run(dbType, func(t *testing.T) {
			const schema = "public"
			plan := &metadataShimPlan{}
			var wantRows int64
			for i := 0; i < 50; i++ {
				name := fmt.Sprintf("table_%02d", i)
				rows := int64(10_000 - i)
				wantRows += rows
				plan.statsRows = append(plan.statsRows, metadataStatsRow(name, rows, int64(500+i)))
				plan.dateRows = append(plan.dateRows, metadataDateRow(name, "updated_at"))
			}

			analyzer := newMetadataAnalyzer(t, plan, dbType)
			analyzer.SetTargetDBType("postgres")
			analyzer.SetMemoryEnvelope(16*1024, 8*1024, 4*1024)
			got, err := analyzer.Analyze(context.Background(), schema)
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			requireValidMetadataTuning(t, got)
			if got.TotalTables != 50 || got.TotalRows != wantRows {
				t.Fatalf("analyzed totals = %d tables/%d rows, want 50/%d", got.TotalTables, got.TotalRows, wantRows)
			}
			if len(got.DateColumns) != 50 {
				t.Fatalf("date-column tables = %d, want 50", len(got.DateColumns))
			}
			if analyzer.pendingSave == nil {
				t.Fatal("successful server analysis did not arm pending history save")
			}
			if queries, _, _ := plan.snapshot(); queries != 2 {
				t.Fatalf("schema metadata queries = %d, want exactly 2 for 50 tables", queries)
			}
			query, args, _, _ := plan.dateQueryObservation()
			requireBatchedDateQueryShape(t, dbType, schema, query, args)
		})
	}
}

func TestAnalyzeDateFailurePreservesTuningAndHistory(t *testing.T) {
	queryFailure := errors.New("date metadata unavailable")
	tests := []struct {
		name        string
		dateErr     error
		wantTimeout bool
	}{
		{name: "query error", dateErr: queryFailure},
		{name: "date sub-timeout", dateErr: context.DeadlineExceeded, wantTimeout: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := &metadataShimPlan{
				statsRows: [][]sqldriver.Value{
					metadataStatsRow("orders", 1_000, 500),
					metadataStatsRow("audit_log", 200, 750),
				},
				dateQueryErr: tc.dateErr,
			}
			history := &mockHistoryProvider{rowID: 42}
			analyzer := newMetadataAnalyzer(t, plan, "postgres")
			analyzer.SetTargetDBType("postgres")
			analyzer.SetHistoryProvider(history)
			analyzer.SetMemoryEnvelope(16*1024, 8*1024, 4*1024)
			plan.dateReady = func() bool {
				got := analyzer.suggestions
				return analyzer.pendingSave != nil && got != nil && got.Tier != "" && got.Reasoning != "" &&
					got.Workers > 0 && got.ChunkSizeRecommendation > 0
			}

			got, err := analyzer.Analyze(context.Background(), "public")
			if err != nil {
				t.Fatalf("Analyze returned optional date failure: %v", err)
			}
			requireValidMetadataTuning(t, got)
			if len(got.DateColumns) != 0 {
				t.Fatalf("failed date lookup published metadata: %+v", got.DateColumns)
			}
			if len(got.Warnings) != 1 || !strings.Contains(strings.ToLower(got.Warnings[0]), "date") {
				t.Fatalf("date failure warnings = %#v, want exactly one date warning", got.Warnings)
			}
			if !containsMetadataString(got.ExcludeTables, "audit_log") {
				t.Fatalf("date failure lost exclusion suggestions: %+v", got.ExcludeTables)
			}
			if analyzer.pendingSave == nil {
				t.Fatal("date failure cleared pending history save")
			}
			if analyzer.pendingSave.reasoning != got.Reasoning || analyzer.pendingSave.input.TotalTables != 2 {
				t.Fatalf("pending save does not match returned tuning: pending=%+v suggestions=%+v", analyzer.pendingSave, got)
			}
			queries, deadline, deadlineSeen := plan.snapshot()
			if queries != 2 {
				t.Fatalf("schema metadata queries = %d, want 2", queries)
			}
			_, _, received, ready := plan.dateQueryObservation()
			if !ready {
				t.Fatal("date query started before tuning and pending history were ready")
			}
			if tc.wantTimeout {
				if !deadlineSeen {
					t.Fatal("date query did not receive a named child timeout")
				}
				if received.IsZero() {
					t.Fatal("date query receipt time was not captured")
				}
				remaining := deadline.Sub(received)
				if remaining < 9*time.Second || remaining > dateColumnDetectionTimeout {
					t.Fatalf("date query deadline = %v after receipt, want at most 10s", remaining)
				}
			}

			reasoning := got.Reasoning
			if rowID := analyzer.SaveTuningWithActualParams(actualParamsFromSuggestions(got)); rowID != 42 {
				t.Fatalf("history row ID after date failure = %d, want 42", rowID)
			}
			if history.saved == nil || !strings.HasPrefix(history.saved.Reasoning, reasoning+"; ") ||
				!strings.Contains(history.saved.Reasoning, "global policy used directly") || history.saved.TotalTables != 2 {
				t.Fatalf("history after date failure = %+v, want current tuning persisted", history.saved)
			}
		})
	}
}

func TestAnalyzePropagatesParentCancellation(t *testing.T) {
	t.Run("table statistics", func(t *testing.T) {
		plan := &metadataShimPlan{}
		analyzer := newMetadataAnalyzer(t, plan, "postgres")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got, err := analyzer.Analyze(ctx, "public")
		if !errors.Is(err, context.Canceled) || got != nil {
			t.Fatalf("Analyze = (%+v, %v), want nil/context.Canceled", got, err)
		}
		if analyzer.pendingSave != nil {
			t.Fatal("canceled table-stat lookup armed pending history")
		}
	})

	t.Run("date metadata", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		plan := &metadataShimPlan{
			statsRows:    [][]sqldriver.Value{metadataStatsRow("orders", 1_000, 500)},
			dateQueryErr: context.Canceled,
		}
		plan.dateReady = func() bool {
			cancel()
			return true
		}
		history := &mockHistoryProvider{rowID: 42}
		analyzer := newMetadataAnalyzer(t, plan, "postgres")
		analyzer.SetTargetDBType("postgres")
		analyzer.SetHistoryProvider(history)

		got, err := analyzer.Analyze(ctx, "public")
		if !errors.Is(err, context.Canceled) || got != nil {
			t.Fatalf("Analyze = (%+v, %v), want nil/context.Canceled", got, err)
		}
		if analyzer.pendingSave != nil {
			t.Fatal("canceled date lookup retained pending history")
		}
		if rowID := analyzer.SaveTuningWithActualParams(ActualParams{}); rowID != 0 || history.saved != nil {
			t.Fatalf("canceled analysis saved history: row=%d record=%+v", rowID, history.saved)
		}
	})
}

func TestAnalyzeStatsFailureClearsPriorPendingHistory(t *testing.T) {
	plan := &metadataShimPlan{
		statsRows: [][]sqldriver.Value{metadataStatsRow("orders", 1_000, 500)},
		dateRows:  [][]sqldriver.Value{metadataDateRow("orders", "updated_at")},
	}
	history := &mockHistoryProvider{rowID: 42}
	analyzer := newMetadataAnalyzer(t, plan, "postgres")
	analyzer.SetTargetDBType("postgres")
	analyzer.SetHistoryProvider(history)

	first, err := analyzer.Analyze(context.Background(), "public")
	if err != nil {
		t.Fatalf("first Analyze: %v", err)
	}
	if analyzer.pendingSave == nil {
		t.Fatal("successful analysis did not arm pending history")
	}

	// The shim rejects the third query, which is the reused analyzer's next
	// stats lookup. That failure must invalidate the first run's pending save.
	degraded, err := analyzer.Analyze(context.Background(), "public")
	if err != nil {
		t.Fatalf("reused Analyze returned graceful stats failure: %v", err)
	}
	requireValidMetadataTuning(t, degraded)
	if len(degraded.Warnings) != 1 || !strings.Contains(degraded.Warnings[0], "formula-only") {
		t.Fatalf("stats failure warnings = %#v, want one formula-only warning", degraded.Warnings)
	}
	if analyzer.pendingSave != nil {
		t.Fatal("failed stats lookup retained stale pending history")
	}
	if rowID := analyzer.SaveTuningWithActualParams(actualParamsFromSuggestions(first)); rowID != 0 {
		t.Fatalf("stale history save returned row ID %d, want 0", rowID)
	}
	if history.saved != nil {
		t.Fatalf("failed analysis persisted stale history: %+v", history.saved)
	}
}

func TestAnalyzeBatchedDateRankingParity(t *testing.T) {
	plan := &metadataShimPlan{
		statsRows: [][]sqldriver.Value{
			metadataStatsRow("orders", 1_000, 500),
			metadataStatsRow("events", 500, 250),
		},
		dateRows: [][]sqldriver.Value{
			// The catalog query returns ordinal order. Equal-ranked event_date and
			// custom_date must retain that order after higher-ranked columns move.
			metadataDateRow("orders", "event_date"),
			metadataDateRow("orders", "custom_date"),
			metadataDateRow("orders", "created_at"),
			metadataDateRow("orders", "updated_at"),
			metadataDateRow("events", "occurred_at"),
			metadataDateRow("events", "creation_date"),
			metadataDateRow("events", "changed_date"),
			metadataDateRow("events", "modified_at"),
		},
	}
	analyzer := newMetadataAnalyzer(t, plan, "postgres")
	analyzer.SetTargetDBType("postgres")
	got, err := analyzer.Analyze(context.Background(), "public")
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}

	want := map[string][]string{
		"orders": {"updated_at", "created_at", "event_date", "custom_date"},
		"events": {"modified_at", "changed_date", "creation_date", "occurred_at"},
	}
	if !reflect.DeepEqual(got.DateColumns, want) {
		t.Fatalf("ranked date columns = %#v, want %#v", got.DateColumns, want)
	}
	if queries, _, _ := plan.snapshot(); queries != 2 {
		t.Fatalf("schema metadata queries = %d, want 2", queries)
	}
}

func TestAnalyzeBatchedDatesRespectScope(t *testing.T) {
	plan := &metadataShimPlan{
		statsRows: [][]sqldriver.Value{
			metadataStatsRow("Orders", 1_000, 500),
			metadataStatsRow("Archive", 2_000, 900),
			metadataStatsRow("Other", 3_000, 300),
		},
		dateRows: [][]sqldriver.Value{
			metadataDateRow("ORDERS", "updated_at"),
			metadataDateRow("archive", "modified_at"),
			metadataDateRow("ghost", "created_at"),
		},
	}
	analyzer := newMetadataAnalyzer(t, plan, "mysql")
	analyzer.SetTargetDBType("mysql")
	analyzer.SetTableNameFilter([]string{"orders"})
	got, err := analyzer.Analyze(context.Background(), "app")
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if got.TotalTables != 1 || got.TotalRows != 1_000 {
		t.Fatalf("scoped totals = %d/%d, want 1/1000", got.TotalTables, got.TotalRows)
	}
	if len(got.DateColumns) != 1 {
		t.Fatalf("scoped date columns = %+v, want only orders", got.DateColumns)
	}
	for table, columns := range got.DateColumns {
		if !strings.EqualFold(table, "orders") || !reflect.DeepEqual(columns, []string{"updated_at"}) {
			t.Fatalf("scoped date result = %q:%v, want orders:[updated_at]", table, columns)
		}
	}
	if queries, _, _ := plan.snapshot(); queries != 2 {
		t.Fatalf("scope filtering added metadata round trips: got %d queries, want 2", queries)
	}
}

func TestAnalyzeDateRowsErrorDoesNotPublishPartialMetadata(t *testing.T) {
	rowsErr := errors.New("date rows interrupted")
	plan := &metadataShimPlan{
		statsRows:   [][]sqldriver.Value{metadataStatsRow("orders", 1_000, 500)},
		dateRows:    [][]sqldriver.Value{metadataDateRow("orders", "updated_at")},
		dateRowsErr: rowsErr,
	}
	analyzer := newMetadataAnalyzer(t, plan, "mssql")
	analyzer.SetTargetDBType("mssql")
	got, err := analyzer.Analyze(context.Background(), "dbo")
	if err != nil {
		t.Fatalf("Analyze returned optional date rows error: %v", err)
	}
	requireValidMetadataTuning(t, got)
	if len(got.DateColumns) != 0 {
		t.Fatalf("partial date metadata escaped failed row scan: %+v", got.DateColumns)
	}
	if len(got.Warnings) != 1 {
		t.Fatalf("date rows failure warnings = %#v, want exactly one", got.Warnings)
	}
	if analyzer.pendingSave == nil {
		t.Fatal("date rows failure cleared pending history save")
	}
	if queries, _, _ := plan.snapshot(); queries != 2 {
		t.Fatalf("schema metadata queries = %d, want 2", queries)
	}
}

func containsMetadataString(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}
	return false
}

func TestAnalyzeRequiresExplicitUsableSchemaStatsProvider(t *testing.T) {
	tests := []struct {
		name   string
		driver Driver
	}{
		{
			name: "provider interface absent",
			driver: &tuningProfileTestDriver{
				name:     "schema-stats-provider-absent",
				defaults: DriverDefaults{WriteAheadWriters: 2},
			},
		},
		{
			name: "provider explicitly unsupported",
			driver: &schemaStatsProviderTestDriver{
				tuningProfileTestDriver: &tuningProfileTestDriver{name: "schema-stats-provider-false", defaults: DriverDefaults{WriteAheadWriters: 2}},
				supported:               false,
			},
		},
		{
			name: "provider returns nil reader",
			driver: &schemaStatsProviderTestDriver{
				tuningProfileTestDriver: &tuningProfileTestDriver{name: "schema-stats-provider-nil", defaults: DriverDefaults{WriteAheadWriters: 2}},
				supported:               true,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registerTuningProfileTestDriver(t, tc.driver)
			history := &mockHistoryProvider{history: []checkpoint.TuningRecord{{FinalThroughput: 123}}}
			analyzer := NewSmartConfigAnalyzer(nil, tc.driver.Name())
			analyzer.SetTargetDBType(tc.driver.Name())
			analyzer.SetHistoryProvider(history)

			got, err := analyzer.Analyze(context.Background(), "")
			if err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			requireValidMetadataTuning(t, got)
			if len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "formula-only") {
				t.Fatalf("warnings = %#v, want exactly one formula-only warning", got.Warnings)
			}
			if analyzer.pendingSave != nil {
				t.Fatal("unusable provider armed pending history save")
			}
			if history.getCalls != 0 {
				t.Fatalf("formula-only degradation read history %d time(s), want 0", history.getCalls)
			}
		})
	}
}

func TestAnalyzeResolvesDeclaredSchemaStatsProvider(t *testing.T) {
	plan := &metadataShimPlan{
		statsRows: [][]sqldriver.Value{metadataStatsRow("orders", 100, 250)},
		dateRows:  [][]sqldriver.Value{metadataDateRow("orders", "updated_at")},
	}
	d := &schemaStatsProviderTestDriver{
		tuningProfileTestDriver: &tuningProfileTestDriver{name: "schema-stats-provider-supported", defaults: DriverDefaults{WriteAheadWriters: 2}},
		reader:                  NewQuerySchemaStatsReader(metadataTableStatsQuery, metadataDateColumnsQuery),
		supported:               true,
	}
	registerTuningProfileTestDriver(t, d)
	analyzer := NewSmartConfigAnalyzer(openMetadataShim(t, plan), d.Name())
	analyzer.SetTargetDBType(d.Name())

	got, err := analyzer.Analyze(context.Background(), "app")
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if got.TotalTables != 1 || got.TotalRows != 100 || len(got.DateColumns) != 1 {
		t.Fatalf("provider-backed metadata = %+v", got)
	}
	if analyzer.pendingSave == nil || len(got.Warnings) != 0 {
		t.Fatalf("successful provider pending/warnings = %v/%#v", analyzer.pendingSave != nil, got.Warnings)
	}
}
