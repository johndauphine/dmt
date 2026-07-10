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
	wantDates := map[string]bool{"datetime": true, "timestamp": true, "date": true, "text": true}
	if got := gen.ValidDateTypes(); !reflect.DeepEqual(got, wantDates) {
		t.Errorf("ValidDateTypes = %v", got)
	}
	for _, frag := range []string{"type affinity", "AUTOINCREMENT", "PRAGMA foreign_keys = ON"} {
		if !strings.Contains(gen.AIPromptAugmentation(), frag) {
			t.Errorf("AIPromptAugmentation missing %q", frag)
		}
	}
	if !strings.Contains(gen.AIDropTablePromptAugmentation(), "DROP TABLE IF EXISTS") {
		t.Error("AIDropTablePromptAugmentation missing drop guidance")
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

	t.Run("unknown catalog name errors", func(t *testing.T) {
		if _, err := LoadCatalog("teradata"); err == nil {
			t.Fatal("expected error for missing catalog")
		}
	})
}
