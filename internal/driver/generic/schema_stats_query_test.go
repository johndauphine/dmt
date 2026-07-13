package generic

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const schemaStatsQueryTestDriver = "dmt-schema-stats-query-test"

var (
	schemaStatsQueryRegisterOnce sync.Once
	schemaStatsQuerySequence     atomic.Uint64
	schemaStatsQueryPlans        sync.Map
)

type schemaStatsQueryObservation struct {
	query string
	args  []sqldriver.NamedValue
}

type schemaStatsQueryPlan struct {
	mu           sync.Mutex
	observations []schemaStatsQueryObservation
}

func (p *schemaStatsQueryPlan) record(query string, args []sqldriver.NamedValue) sqldriver.Rows {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observations = append(p.observations, schemaStatsQueryObservation{
		query: query,
		args:  append([]sqldriver.NamedValue(nil), args...),
	})
	if len(p.observations) == 1 {
		return &schemaStatsQueryRows{
			columns: []string{"table_name", "row_count", "avg_row_size"},
			rows:    [][]sqldriver.Value{{"orders", int64(12), int64(345)}},
		}
	}
	return &schemaStatsQueryRows{
		columns: []string{"table_name", "column_name"},
		rows:    [][]sqldriver.Value{{"orders", "updated_at"}},
	}
}

func (p *schemaStatsQueryPlan) snapshot() []schemaStatsQueryObservation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]schemaStatsQueryObservation(nil), p.observations...)
}

type schemaStatsQueryDriver struct{}

func (schemaStatsQueryDriver) Open(name string) (sqldriver.Conn, error) {
	value, ok := schemaStatsQueryPlans.Load(name)
	if !ok {
		return nil, fmt.Errorf("schema-stats query plan %q not found", name)
	}
	return &schemaStatsQueryConn{plan: value.(*schemaStatsQueryPlan)}, nil
}

type schemaStatsQueryConn struct{ plan *schemaStatsQueryPlan }

func (*schemaStatsQueryConn) Prepare(string) (sqldriver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are not supported")
}
func (*schemaStatsQueryConn) Close() error { return nil }
func (*schemaStatsQueryConn) Begin() (sqldriver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}
func (c *schemaStatsQueryConn) QueryContext(_ context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	return c.plan.record(query, args), nil
}

var _ sqldriver.QueryerContext = (*schemaStatsQueryConn)(nil)

type schemaStatsQueryRows struct {
	columns []string
	rows    [][]sqldriver.Value
	index   int
}

func (r *schemaStatsQueryRows) Columns() []string { return r.columns }
func (*schemaStatsQueryRows) Close() error        { return nil }
func (r *schemaStatsQueryRows) Next(dest []sqldriver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

func openSchemaStatsQueryDB(t *testing.T, plan *schemaStatsQueryPlan) *sql.DB {
	t.Helper()
	schemaStatsQueryRegisterOnce.Do(func() { sql.Register(schemaStatsQueryTestDriver, schemaStatsQueryDriver{}) })
	dsn := fmt.Sprintf("plan-%d", schemaStatsQuerySequence.Add(1))
	schemaStatsQueryPlans.Store(dsn, plan)
	db, err := sql.Open(schemaStatsQueryTestDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		schemaStatsQueryPlans.Delete(dsn)
	})
	return db
}

func TestShippedQuerySchemaStatsCatalogsExecuteFixedShapes(t *testing.T) {
	tests := map[string]struct {
		placeholder string
		tableFrom   string
		dateFrom    string
	}{
		"mssql":      {placeholder: "@p1", tableFrom: "sys.tables", dateFrom: "sys.columns"},
		"postgres":   {placeholder: "$1", tableFrom: "pg_stat_user_tables", dateFrom: "information_schema.columns"},
		"mysql":      {placeholder: "?", tableFrom: "information_schema.tables", dateFrom: "information_schema.columns"},
		"clickhouse": {placeholder: "?", tableFrom: "system.tables", dateFrom: "system.columns"},
	}

	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			reader, ok := NewDriver(cat).SchemaStatsReader()
			if !ok || reader == nil {
				t.Fatal("catalog did not expose query schema stats")
			}
			plan := &schemaStatsQueryPlan{}
			db := openSchemaStatsQueryDB(t, plan)

			stats, err := reader.TableStats(t.Context(), db, "app", nil)
			if err != nil {
				t.Fatalf("TableStats: %v", err)
			}
			if len(stats) != 1 || stats[0].Name != "orders" || stats[0].RowCount != 12 || stats[0].AvgRowSizeBytes != 345 {
				t.Fatalf("fixed table-stat scan = %+v", stats)
			}
			dates, err := reader.DateColumns(t.Context(), db, "app", []string{"orders"})
			if err != nil {
				t.Fatalf("DateColumns: %v", err)
			}
			if !reflect.DeepEqual(dates, map[string][]string{"orders": {"updated_at"}}) {
				t.Fatalf("fixed date scan = %#v", dates)
			}

			observed := plan.snapshot()
			if len(observed) != 2 {
				t.Fatalf("catalog queries = %d, want 2", len(observed))
			}
			for i, observation := range observed {
				if len(observation.args) != 1 || observation.args[0].Value != "app" {
					t.Errorf("query %d args = %#v, want one schema argument", i+1, observation.args)
				}
				if !strings.Contains(observation.query, want.placeholder) {
					t.Errorf("query %d missing %s placeholder:\n%s", i+1, want.placeholder, observation.query)
				}
			}
			if !strings.Contains(strings.ToLower(observed[0].query), want.tableFrom) {
				t.Errorf("table query missing %q:\n%s", want.tableFrom, observed[0].query)
			}
			if !strings.Contains(strings.ToLower(observed[1].query), want.dateFrom) {
				t.Errorf("date query missing %q:\n%s", want.dateFrom, observed[1].query)
			}
		})
	}
}
