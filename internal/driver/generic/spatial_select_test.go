package generic

import "testing"

// Cross-engine SELECTs must wrap spatial columns in ST_AsText — the
// hand-written postgres/mysql dialects' behavior, regressed by the
// mysql flip (#516) and restored via spatial_select (#509).
func TestColumnListForSelectSpatialWrapping(t *testing.T) {
	cases := []struct {
		catalog  string
		colTypes []string
		target   string
		want     string
	}{
		{"postgres", []string{"int4", "geography", "geometry"}, "mssql",
			`"id", ST_AsText("geo") AS "geo", ST_AsText("shape") AS "shape"`},
		{"postgres", []string{"int4", "geography", "geometry"}, "postgres",
			`"id", "geo", "shape"`},
		{"mysql", []string{"int", "point", "geometrycollection"}, "mssql",
			"`id`, ST_AsText(`geo`) AS `geo`, ST_AsText(`shape`) AS `shape`"},
		{"mysql", []string{"int", "point", "geometrycollection"}, "mysql",
			"`id`, `geo`, `shape`"},
		// sqlite declares no spatial_select — plain list either way.
		{"sqlite", []string{"integer", "text", "blob"}, "mssql",
			`"id", "geo", "shape"`},
	}
	cols := []string{"id", "geo", "shape"}
	for _, tc := range cases {
		cat, err := LoadCatalog(tc.catalog)
		if err != nil {
			t.Fatal(err)
		}
		got := NewDialect(cat).ColumnListForSelect(cols, tc.colTypes, tc.target)
		if got != tc.want {
			t.Errorf("%s→%s: got %q, want %q", tc.catalog, tc.target, got, tc.want)
		}
	}
}
