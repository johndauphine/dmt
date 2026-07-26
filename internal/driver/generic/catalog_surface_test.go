package generic

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

// Literal expectations for surfaces not covered by the conformance
// DriverCase or the ported behavior suite. These pinned values were
// proven equal to the hand-written sqlite driver by the differential
// tests that ran until #506 removed it.
func TestSQLiteCatalogLiteralSurfaces(t *testing.T) {
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	gen := NewDialect(cat)

	if got := normalizeSQL(gen.PartitionBoundariesQuery("id", "ignored", "Users", 8)); got != `SELECT 1 AS partition_id, MIN("id"), MAX("id"), COUNT(*) FROM "Users"` {
		t.Errorf("PartitionBoundariesQuery = %q", got)
	}
	for _, useStats := range []bool{true, false} {
		if got := gen.RowCountQuery(useStats); got != "SELECT COUNT(*) FROM %s" {
			t.Errorf("RowCountQuery(%v) = %q", useStats, got)
		}
	}
	if got := gen.DateColumnQuery(); got != "SELECT type FROM pragma_table_info(?) WHERE name = ?" {
		t.Errorf("DateColumnQuery = %q", got)
	}
	wantDates := map[string]bool{"datetime": true, "timestamp": true, "date": true, "time": true, "text": true}
	if got := gen.ValidDateTypes(); !reflect.DeepEqual(got, wantDates) {
		t.Errorf("ValidDateTypes = %v", got)
	}
	for _, frag := range []string{"type affinity", "AUTOINCREMENT", "PRAGMA foreign_keys = ON"} {
		if !strings.Contains(gen.AIPromptAugmentation(), frag) {
			t.Errorf("AIPromptAugmentation missing %q", frag)
		}
	}
	// The default converter strategy delegates to the shared table —
	// behavioral check against the (still shared) oracle.
	colTypes := []string{"INTEGER", "bit", "datetime"}
	gotConvs := gen.ValueConverters(colTypes, "postgres")
	wantConvs := driver.DefaultValueConverters(colTypes)
	for i := range gotConvs {
		for _, v := range []any{int64(7), []byte{1}, nil} {
			if !reflect.DeepEqual(applyConv(gotConvs[i], v), applyConv(wantConvs[i], v)) {
				t.Errorf("converter[%d](%v) diverges from DefaultValueConverters", i, v)
			}
		}
	}
}

func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func applyConv(conv func(any) any, v any) any {
	if conv == nil {
		return v
	}
	return conv(v)
}

// Catalog validation must reject the failure modes that would otherwise
// surface mid-migration.
func TestCatalogValidation(t *testing.T) {
	valid, err := catalogFS.ReadFile("catalogs/sqlite.yaml")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("embedded sqlite catalog is valid", func(t *testing.T) {
		if _, err := ParseCatalog(valid); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unknown field is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "name: sqlite", "name: sqlite\ntypo_field: x", 1)
		if _, err := ParseCatalog([]byte(data)); err == nil {
			t.Fatal("expected strict-decode error for unknown field")
		}
	})

	t.Run("unknown arg symbol is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "no_max: [last_pk, limit]", "no_max: [bogus, limit]", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), `unknown symbol "bogus"`) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("missing arg variant is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "with_max_date: [last_pk, max_pk, date_from, limit]", "", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "with_max_date is required") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("malformed composite range query is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "AND {range_clause}{date_clause}", "AND {date_clause}", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "range_query must contain {range_clause}") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unknown dsn strategy is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "dsn_strategy: sqlite_file", "dsn_strategy: nope", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), `dsn_strategy "nope"`) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unknown strict parallel strategy is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "strict_parallel_strategy: none", "strict_parallel_strategy: time_machine", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), `strict_parallel_strategy "time_machine"`) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unknown schema stats strategy is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "schema_stats:\n  strategy: sqlite", "schema_stats:\n  strategy: time_machine", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), `schema_stats.strategy "time_machine"`) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("schema stats capability must match strategy", func(t *testing.T) {
		data := strings.Replace(string(valid), "schema_stats: true", "schema_stats: false", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "capabilities.schema_stats must be true") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("schema stats none strategy rejects capability", func(t *testing.T) {
		data := strings.Replace(string(valid), "schema_stats:\n  strategy: sqlite", "schema_stats:\n  strategy: none", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "capabilities.schema_stats must be false") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("query schema stats requires both statements", func(t *testing.T) {
		data := strings.Replace(string(valid), "schema_stats:\n  strategy: sqlite", "schema_stats:\n  strategy: query", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "schema_stats.table_stats is required") ||
			!strings.Contains(err.Error(), "schema_stats.date_columns is required") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("sqlite schema stats rejects query fields", func(t *testing.T) {
		data := strings.Replace(string(valid), "  strategy: sqlite", "  strategy: sqlite\n  table_stats: SELECT 1", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "sqlite strategy does not accept query fields") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("portless requires zero default port", func(t *testing.T) {
		data := strings.Replace(string(valid), "default_port: 0", "default_port: 1", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "connection.portless") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("zero default port requires portless", func(t *testing.T) {
		data := strings.Replace(string(valid), "portless: true", "portless: false", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), "connection.portless") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unknown catalog name errors", func(t *testing.T) {
		if _, err := LoadCatalog("teradata"); err == nil {
			t.Fatal("expected error for missing catalog")
		}
	})
}

func TestShippedCatalogSchemaStatsAndPortlessDeclarations(t *testing.T) {
	tests := map[string]struct {
		strategy string
		portless bool
		port     int
	}{
		"postgres":   {strategy: "query", port: 5432},
		"mysql":      {strategy: "query", port: 3306},
		"mssql":      {strategy: "query", port: 1433},
		"clickhouse": {strategy: "query", port: 9000},
		"sqlite":     {strategy: "sqlite", portless: true, port: 0},
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			if !cat.Capabilities.SchemaStats || cat.SchemaStats.Strategy != want.strategy {
				t.Fatalf("schema stats declaration = capability:%v strategy:%q, want true/%q",
					cat.Capabilities.SchemaStats, cat.SchemaStats.Strategy, want.strategy)
			}
			if cat.Connection.Portless != want.portless || cat.Connection.DefaultPort != want.port {
				t.Fatalf("connection identity = portless:%v port:%d, want %v/%d",
					cat.Connection.Portless, cat.Connection.DefaultPort, want.portless, want.port)
			}

			d := NewDriver(cat)
			reader, ok := d.SchemaStatsReader()
			if !ok || reader == nil {
				t.Fatal("catalog-declared schema stats did not produce a reader")
			}
			if defaults := d.Defaults(); defaults.Portless != want.portless || defaults.Port != want.port {
				t.Fatalf("driver defaults = portless:%v port:%d, want %v/%d",
					defaults.Portless, defaults.Port, want.portless, want.port)
			}
		})
	}
}

func TestOnlyClickHouseDefaultsCreateSchemaToConnectedDatabase(t *testing.T) {
	for _, name := range []string{"postgres", "mysql", "mssql", "sqlite", "clickhouse"} {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			want := name == "clickhouse"
			if got := cat.Defaults.CreateSchemaDefaultsToDatabase; got != want {
				t.Fatalf("create-schema database fallback = %v, want %v", got, want)
			}
		})
	}
}

func TestShippedSchemaStatsQueriesExcludeEngineArtifactsAndAggregateTables(t *testing.T) {
	clickhouse, err := LoadCatalog("clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	clickhouseStats := strings.ToLower(normalizeSQL(clickhouse.SchemaStats.TableStats))
	for _, want := range []string{
		"engine not like '%view'",
		"not startswith(name, '.inner_id.')",
		"not startswith(name, '.inner.')",
	} {
		if !strings.Contains(clickhouseStats, want) {
			t.Errorf("ClickHouse table-stats query missing %q: %s", want, clickhouseStats)
		}
	}

	mssql, err := LoadCatalog("mssql")
	if err != nil {
		t.Fatal(err)
	}
	mssqlStats := strings.ToLower(normalizeSQL(mssql.SchemaStats.TableStats))
	for _, want := range []string{
		"with partition_stats as",
		"a.type in (1, 3) and a.container_id = p.hobt_id",
		"a.type = 2 and a.container_id = p.partition_id",
		"group by p.object_id, p.index_id, p.partition_number, p.rows",
		"sum(ps.row_count) as row_count",
		"group by t.object_id, t.name",
	} {
		if !strings.Contains(mssqlStats, want) {
			t.Errorf("MSSQL table-stats query missing %q: %s", want, mssqlStats)
		}
	}
	if strings.Contains(mssqlStats, "group by t.name, p.rows") {
		t.Errorf("MSSQL table-stats query still emits per-partition rows: %s", mssqlStats)
	}
	mssqlDates := strings.ToLower(normalizeSQL(mssql.SchemaStats.DateColumns))
	if strings.Contains(mssqlDates, "'timestamp'") {
		t.Errorf("MSSQL date-column query recommends timestamp/rowversion: %s", mssqlDates)
	}
	if !strings.Contains(mssqlDates, "'smalldatetime'") {
		t.Errorf("MSSQL date-column query omits supported smalldatetime: %s", mssqlDates)
	}
}

func TestGenericDriverSchemaStatsSupportComesFromCatalog(t *testing.T) {
	cat, err := LoadCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}

	supported := NewDriver(cat)
	if reader, ok := supported.SchemaStatsReader(); !ok || reader == nil {
		t.Fatal("query catalog did not report schema-stats support")
	}

	withoutStats := *cat
	withoutStats.Capabilities.SchemaStats = false
	unsupported := NewDriver(&withoutStats)
	if reader, ok := unsupported.SchemaStatsReader(); ok || reader != nil {
		t.Fatalf("capability-disabled catalog reported support: reader=%T ok=%v", reader, ok)
	}
}

func TestShippedCatalogStrictParallelStrategies(t *testing.T) {
	tests := map[string]string{
		"postgres":   "exported_snapshot",
		"mysql":      "lock_window_sessions",
		"mssql":      "table_shared_lock",
		"sqlite":     "none",
		"clickhouse": "none",
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			cat, err := LoadCatalog(name)
			if err != nil {
				t.Fatal(err)
			}
			if got := cat.StrictParallelStrategy; got != want {
				t.Fatalf("StrictParallelStrategy = %q, want %q", got, want)
			}
		})
	}
}
