package generic

import (
	"strings"
	"testing"
)

// TestBuildCompositeKeysetQuery pins the per-engine tuple-keyset SQL shape
// and argument order for #616: row-value comparison for engines that support
// it, an expanded OR-chain for SQL Server, and an unbounded ("1=1") first
// chunk that drops the tuple arguments.
func TestBuildCompositeKeysetQuery(t *testing.T) {
	tests := []struct {
		engine    string
		wantLower string // substring the bounded query must contain
		wantArgs  []any  // bounded args for lastPK (10,20), limit 500
		unbounded []any  // unbounded args (limit only)
	}{
		{"sqlite", `("a", "b") > (?, ?)`, []any{int64(10), int64(20), 500}, []any{500}},
		{"postgres", `("a", "b") > ($1, $2)`, []any{int64(10), int64(20), 500}, []any{500}},
		{"mysql", "(`a`, `b`) > (?, ?)", []any{int64(10), int64(20), 500}, []any{500}},
		{"mssql", `[a] > @p2 OR ([a] = @p3 AND [b] > @p4)`, []any{500, int64(10), int64(10), int64(20)}, []any{500}},
	}
	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			cat, err := LoadCatalog(tc.engine)
			if err != nil {
				t.Fatalf("LoadCatalog: %v", err)
			}
			d := NewDialect(cat)
			q := d.BuildCompositeKeysetQuery("a, b", []string{"a", "b"}, "s", "t", "", true, nil)
			if !strings.Contains(q, tc.wantLower) {
				t.Fatalf("query missing %q:\n%s", tc.wantLower, q)
			}
			if !strings.Contains(q, "ORDER BY") {
				t.Fatalf("query missing ORDER BY: %s", q)
			}
			args := d.BuildCompositeKeysetArgs([]any{int64(10), int64(20)}, 500, true, nil)
			if !argsEqual(args, tc.wantArgs) {
				t.Fatalf("bounded args = %v, want %v", args, tc.wantArgs)
			}
			// Unbounded first chunk.
			qu := d.BuildCompositeKeysetQuery("a, b", []string{"a", "b"}, "s", "t", "", false, nil)
			if !strings.Contains(qu, "1=1") {
				t.Fatalf("unbounded query missing 1=1: %s", qu)
			}
			au := d.BuildCompositeKeysetArgs([]any{int64(10), int64(20)}, 500, false, nil)
			if !argsEqual(au, tc.unbounded) {
				t.Fatalf("unbounded args = %v, want %v", au, tc.unbounded)
			}
		})
	}
}

func argsEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestBuildCompositeKeysetQuerySingleColumn pins the 1-tuple form used by
// single-column non-integer keyset pagination (#629): a parenthesized single
// expression for row-value engines and a bare comparison for the OR-chain.
func TestBuildCompositeKeysetQuerySingleColumn(t *testing.T) {
	tests := []struct {
		engine    string
		wantLower string
		wantArgs  []any
	}{
		{"sqlite", `("code") > (?)`, []any{"abc", 500}},
		{"postgres", `("code") > ($1)`, []any{"abc", 500}},
		{"mysql", "(`code`) > (?)", []any{"abc", 500}},
		{"mssql", `([code] > @p2)`, []any{500, "abc"}},
	}
	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			cat, err := LoadCatalog(tc.engine)
			if err != nil {
				t.Fatalf("LoadCatalog: %v", err)
			}
			d := NewDialect(cat)
			q := d.BuildCompositeKeysetQuery("code, payload", []string{"code"}, "s", "t", "", true, nil)
			if !strings.Contains(q, tc.wantLower) {
				t.Fatalf("query missing %q:\n%s", tc.wantLower, q)
			}
			args := d.BuildCompositeKeysetArgs([]any{"abc"}, 500, true, nil)
			if !argsEqual(args, tc.wantArgs) {
				t.Fatalf("args = %v, want %v", args, tc.wantArgs)
			}
			// Unbounded first chunk drops the comparison and its arg.
			qu := d.BuildCompositeKeysetQuery("code, payload", []string{"code"}, "s", "t", "", false, nil)
			if !strings.Contains(qu, "1=1") {
				t.Fatalf("unbounded query missing 1=1: %s", qu)
			}
			au := d.BuildCompositeKeysetArgs([]any{"abc"}, 500, false, nil)
			if len(au) != 1 || au[0] != 500 {
				t.Fatalf("unbounded args = %v, want [500]", au)
			}
		})
	}
}

func TestBuildCompositeKeysetRangeQuery(t *testing.T) {
	tests := []struct {
		engine        string
		wantLower     string
		wantRange     string
		boundedArgs   []any
		unboundedArgs []any
	}{
		{"sqlite", `("a", "b") > (?, ?)`, `"a" >= ? AND "a" <= ?`, []any{int64(10), int64(20), int64(1), int64(9), 500}, []any{int64(1), int64(9), 500}},
		{"postgres", `("a", "b") > ($1, $2)`, `"a" >= $3 AND "a" <= $4`, []any{int64(10), int64(20), int64(1), int64(9), 500}, []any{int64(1), int64(9), 500}},
		{"mysql", "(`a`, `b`) > (?, ?)", "`a` >= ? AND `a` <= ?", []any{int64(10), int64(20), int64(1), int64(9), 500}, []any{int64(1), int64(9), 500}},
		{"mssql", "[a] > @p2 OR ([a] = @p3 AND [b] > @p4)", "[a] >= @p5 AND [a] <= @p6", []any{500, int64(10), int64(10), int64(20), int64(1), int64(9)}, []any{500, int64(1), int64(9)}},
	}
	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			cat, err := LoadCatalog(tc.engine)
			if err != nil {
				t.Fatalf("LoadCatalog: %v", err)
			}
			d := NewDialect(cat)
			if !d.SupportsCompositeRangeKeyset() {
				t.Fatal("range template not declared")
			}
			q := d.BuildCompositeKeysetRangeQuery("a, b", []string{"a", "b"}, "s", "t", "", true, true, nil)
			if !strings.Contains(q, tc.wantLower) || !strings.Contains(q, tc.wantRange) {
				t.Fatalf("range query missing tuple or range clause:\n%s", q)
			}
			if got := d.BuildCompositeKeysetRangeArgs([]any{int64(10), int64(20)}, 1, 9, 500, true, nil); !argsEqual(got, tc.boundedArgs) {
				t.Fatalf("bounded range args = %v, want %v", got, tc.boundedArgs)
			}
			qu := d.BuildCompositeKeysetRangeQuery("a, b", []string{"a", "b"}, "s", "t", "", false, true, nil)
			if !strings.Contains(qu, "1=1") {
				t.Fatalf("unbounded range query missing 1=1:\n%s", qu)
			}
			if got := d.BuildCompositeKeysetRangeArgs([]any{int64(10), int64(20)}, 1, 9, 500, false, nil); !argsEqual(got, tc.unboundedArgs) {
				t.Fatalf("unbounded range args = %v, want %v", got, tc.unboundedArgs)
			}
		})
	}

	cat, err := LoadCatalog("sqlite")
	if err != nil {
		t.Fatalf("LoadCatalog(sqlite): %v", err)
	}
	q := NewDialect(cat).BuildCompositeKeysetRangeQuery("a, b", []string{"a", "b"}, "", "t", "", false, false, nil)
	if !strings.Contains(q, `"a" > ? AND "a" <= ?`) {
		t.Fatalf("later split range must exclude the shared lower boundary:\n%s", q)
	}
}
