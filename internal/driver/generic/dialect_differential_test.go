package generic

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/sqlite"
)

// The phase-1 acceptance bar (#191): the catalog-driven Dialect must be
// indistinguishable from the hand-written sqlite Dialect it replaces.
// Every method is compared output-for-output across the variant grid —
// stronger than restating the conformance case, because the hand-written
// implementation IS the oracle. When the flip PR deletes the sqlite
// package, this test's expectations fold into the conformance DriverCase.
func TestSQLiteCatalogMatchesHandWrittenDialect(t *testing.T) {
	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatalf("LoadCatalog(sqlite): %v", err)
	}
	gen := NewDialect(cat)
	ref := &sqlite.Dialect{}

	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.UTC)
	dateFilter := &driver.DateFilter{Column: "updated_at", Timestamp: ts}
	const cols = `"id", "name", "updated_at"`

	t.Run("identity and quoting", func(t *testing.T) {
		if got, want := gen.DBType(), ref.DBType(); got != want {
			t.Errorf("DBType: %q != %q", got, want)
		}
		for _, name := range []string{"Users", `weird"name`, `multi""quote`, "with space", ""} {
			if got, want := gen.QuoteIdentifier(name), ref.QuoteIdentifier(name); got != want {
				t.Errorf("QuoteIdentifier(%q): %q != %q", name, got, want)
			}
		}
		for _, schema := range []string{"", "ignored", "main"} {
			if got, want := gen.QualifyTable(schema, "Users"), ref.QualifyTable(schema, "Users"); got != want {
				t.Errorf("QualifyTable(%q): %q != %q", schema, got, want)
			}
		}
		for i := 1; i <= 3; i++ {
			if got, want := gen.ParameterPlaceholder(i), ref.ParameterPlaceholder(i); got != want {
				t.Errorf("ParameterPlaceholder(%d): %q != %q", i, got, want)
			}
		}
		list := []string{"id", `weird"name`, "updated_at"}
		if got, want := gen.ColumnList(list), ref.ColumnList(list); got != want {
			t.Errorf("ColumnList: %q != %q", got, want)
		}
		if got, want := gen.ColumnListForSelect(list, []string{"INTEGER", "TEXT", "TEXT"}, "postgres"),
			ref.ColumnListForSelect(list, []string{"INTEGER", "TEXT", "TEXT"}, "postgres"); got != want {
			t.Errorf("ColumnListForSelect: %q != %q", got, want)
		}
		for _, strict := range []bool{true, false} {
			if got, want := gen.TableHint(strict), ref.TableHint(strict); got != want {
				t.Errorf("TableHint(%v): %q != %q", strict, got, want)
			}
		}
	})

	t.Run("dsn", func(t *testing.T) {
		cases := []struct {
			db   string
			opts map[string]any
		}{
			{"/tmp/data.db", nil},
			{"", nil},
			{":memory:", nil},
			{"file:/tmp/x.db", nil},
			{"/tmp/data.db", map[string]any{"pragmas": []string{"cache_size(-20000)"}}},
		}
		for _, c := range cases {
			got := gen.BuildDSN("h", 1, c.db, "u", "p", c.opts)
			want := ref.BuildDSN("h", 1, c.db, "u", "p", c.opts)
			if got != want {
				t.Errorf("BuildDSN(%q, %v): %q != %q", c.db, c.opts, got, want)
			}
		}
	})

	t.Run("keyset", func(t *testing.T) {
		for _, hasMax := range []bool{false, true} {
			for _, df := range []*driver.DateFilter{nil, dateFilter} {
				gotQ := gen.BuildKeysetQuery(cols, "id", "ignored", "Users", "", hasMax, df)
				wantQ := ref.BuildKeysetQuery(cols, "id", "ignored", "Users", "", hasMax, df)
				if normalizeSQL(gotQ) != normalizeSQL(wantQ) {
					t.Errorf("BuildKeysetQuery(max=%v date=%v):\n  got  %q\n  want %q",
						hasMax, df != nil, normalizeSQL(gotQ), normalizeSQL(wantQ))
				}
				gotA := gen.BuildKeysetArgs(int64(10), int64(100), 25, hasMax, df)
				wantA := ref.BuildKeysetArgs(int64(10), int64(100), 25, hasMax, df)
				if !reflect.DeepEqual(gotA, wantA) {
					t.Errorf("BuildKeysetArgs(max=%v date=%v): %#v != %#v", hasMax, df != nil, gotA, wantA)
				}
			}
		}
	})

	t.Run("row number", func(t *testing.T) {
		for _, c := range []string{cols, `"id" AS pk, "name", "x" as alias`} {
			for _, df := range []*driver.DateFilter{nil, dateFilter} {
				gotQ := gen.BuildRowNumberQuery(c, `"id"`, "ignored", "Users", "", df)
				wantQ := ref.BuildRowNumberQuery(c, `"id"`, "ignored", "Users", "", df)
				if normalizeSQL(gotQ) != normalizeSQL(wantQ) {
					t.Errorf("BuildRowNumberQuery(cols=%q date=%v):\n  got  %q\n  want %q",
						c, df != nil, normalizeSQL(gotQ), normalizeSQL(wantQ))
				}
				gotA := gen.BuildRowNumberArgs(int64(50), 25, df)
				wantA := ref.BuildRowNumberArgs(int64(50), 25, df)
				if !reflect.DeepEqual(gotA, wantA) {
					t.Errorf("BuildRowNumberArgs(date=%v): %#v != %#v", df != nil, gotA, wantA)
				}
			}
		}
	})

	t.Run("metadata queries", func(t *testing.T) {
		gotP := gen.PartitionBoundariesQuery("id", "ignored", "Users", 8)
		wantP := ref.PartitionBoundariesQuery("id", "ignored", "Users", 8)
		if normalizeSQL(gotP) != normalizeSQL(wantP) {
			t.Errorf("PartitionBoundariesQuery: %q != %q", normalizeSQL(gotP), normalizeSQL(wantP))
		}
		for _, useStats := range []bool{true, false} {
			if got, want := gen.RowCountQuery(useStats), ref.RowCountQuery(useStats); got != want {
				t.Errorf("RowCountQuery(%v): %q != %q", useStats, got, want)
			}
		}
		if got, want := gen.DateColumnQuery(), ref.DateColumnQuery(); got != want {
			t.Errorf("DateColumnQuery: %q != %q", got, want)
		}
		if got, want := gen.ValidDateTypes(), ref.ValidDateTypes(); !reflect.DeepEqual(got, want) {
			t.Errorf("ValidDateTypes: %v != %v", got, want)
		}
	})

	t.Run("value converters", func(t *testing.T) {
		colTypes := []string{"INTEGER", "TEXT", "bit", "datetime", "uniqueidentifier"}
		samples := []any{int64(7), "x", []byte{1}, ts, []byte("0123456789abcdef")}
		gotConvs := gen.ValueConverters(colTypes, "postgres")
		wantConvs := ref.ValueConverters(colTypes, "postgres")
		if len(gotConvs) != len(wantConvs) {
			t.Fatalf("converter count: %d != %d", len(gotConvs), len(wantConvs))
		}
		// Functions can't be compared; their behavior on sample values can.
		for i := range gotConvs {
			for _, v := range append(samples, nil) {
				got, want := applyConv(gotConvs[i], v), applyConv(wantConvs[i], v)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("converter[%d](%T %v): %#v != %#v", i, v, v, got, want)
				}
			}
		}
	})

	t.Run("ai augmentation", func(t *testing.T) {
		// Compare trimmed: YAML block scalars and Go raw strings differ
		// in leading/trailing newline shape; the prompt content is what
		// must match.
		if got, want := strings.TrimSpace(gen.AIPromptAugmentation()), strings.TrimSpace(ref.AIPromptAugmentation()); got != want {
			t.Errorf("AIPromptAugmentation:\n  got  %q\n  want %q", got, want)
		}
		if got, want := strings.TrimSpace(gen.AIDropTablePromptAugmentation()), strings.TrimSpace(ref.AIDropTablePromptAugmentation()); got != want {
			t.Errorf("AIDropTablePromptAugmentation:\n  got  %q\n  want %q", got, want)
		}
	})
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

	t.Run("unknown dsn strategy is rejected", func(t *testing.T) {
		data := strings.Replace(string(valid), "dsn_strategy: sqlite_file", "dsn_strategy: nope", 1)
		_, err := ParseCatalog([]byte(data))
		if err == nil || !strings.Contains(err.Error(), `dsn_strategy "nope"`) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("unknown catalog name errors", func(t *testing.T) {
		if _, err := LoadCatalog("teradata"); err == nil {
			t.Fatal("expected error for missing catalog")
		}
	})
}
