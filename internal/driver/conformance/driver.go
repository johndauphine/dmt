package conformance

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

// DriverCase describes one driver's conformance expectations.
//
// Required fields are Name, Driver, QuoteName, QuoteNameWant, QualifiedTable,
// QualifiedNameWant, PlaceholderIndex, PlaceholderWant, ColumnList, and
// ColumnListWant. RequireSchemaQualification also requires QualifiedSchema.
// Optional fields add dialect-specific assertions.
type DriverCase struct {
	Name string

	Driver driver.Driver

	Aliases []string

	QuoteName     string
	QuoteNameWant string

	QualifiedSchema   string
	QualifiedTable    string
	QualifiedNameWant string

	PlaceholderIndex int
	PlaceholderWant  string

	ColumnList     []string
	ColumnListWant string

	DateType string

	// Optional expectations.
	SchemaIgnoredInQualification bool
	RequireSchemaQualification   bool
	RequireAIPromptAugmentation  bool

	Pagination *PaginationCase
}

// PaginationCase describes dialect-specific pagination SQL/arg expectations.
// It is intentionally exact about parameter order because incremental reads
// combine PK bounds, limits, and date filters differently across dialects.
type PaginationCase struct {
	Columns    string
	PKColumn   string
	Schema     string
	Table      string
	TableHint  string
	OrderBy    string
	DateFilter *driver.DateFilter

	LastPK any
	MaxPK  any
	Limit  int
	RowNum int64

	KeysetNoMaxQuery   string
	KeysetNoMaxArgs    []any
	KeysetWithMaxQuery string
	KeysetWithMaxArgs  []any
	RowNumberQuery     string
	RowNumberArgs      []any
}

// RunDriverConformance runs the shared driver/dialect contract checks.
func RunDriverConformance(t *testing.T, tc DriverCase) {
	t.Helper()
	validateCase(t, tc)

	t.Run("registered", func(t *testing.T) {
		got, err := driver.Get(tc.Name)
		if err != nil {
			t.Fatalf("driver.Get(%q): %v", tc.Name, err)
		}
		if got.Name() != tc.Name {
			t.Fatalf("registered driver name = %q, want %q", got.Name(), tc.Name)
		}
		if driver.Canonicalize(strings.ToUpper(tc.Name)) != tc.Name {
			t.Fatalf("Canonicalize(%q) = %q, want %q", strings.ToUpper(tc.Name), driver.Canonicalize(strings.ToUpper(tc.Name)), tc.Name)
		}
		for _, alias := range tc.Aliases {
			got, err := driver.Get(alias)
			if err != nil {
				t.Fatalf("driver.Get(alias %q): %v", alias, err)
			}
			if got.Name() != tc.Name {
				t.Fatalf("alias %q resolved to %q, want %q", alias, got.Name(), tc.Name)
			}
			if driver.Canonicalize(strings.ToUpper(alias)) != tc.Name {
				t.Fatalf("Canonicalize(%q) = %q, want %q", strings.ToUpper(alias), driver.Canonicalize(strings.ToUpper(alias)), tc.Name)
			}
		}
	})

	t.Run("driver metadata is available without live DB", func(t *testing.T) {
		if tc.Driver.Name() != tc.Name {
			t.Fatalf("Driver.Name() = %q, want %q", tc.Driver.Name(), tc.Name)
		}
		if aliases := tc.Driver.Aliases(); !sameStrings(aliases, tc.Aliases) {
			t.Fatalf("Driver.Aliases() = %v, want %v", aliases, tc.Aliases)
		}
		defaults := tc.Driver.Defaults()
		if defaults.WriteAheadWriters <= 0 {
			t.Fatalf("Defaults().WriteAheadWriters = %d, want > 0", defaults.WriteAheadWriters)
		}
		if defaults.OptimumBulkChunkBytes < 0 {
			t.Fatalf("Defaults().OptimumBulkChunkBytes = %d, want >= 0", defaults.OptimumBulkChunkBytes)
		}
		if tc.Driver.Dialect() == nil {
			t.Fatal("Driver.Dialect() returned nil")
		}
	})

	t.Run("nil db hooks do not panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("driver hook panicked with nil db: %v", r)
			}
		}()
		_ = tc.Driver.ProbeTarget(context.Background(), nil)
		findings := tc.Driver.PreFlight(context.Background(), nil, driver.PreFlightRequest{
			Side:       driver.PreFlightSideTarget,
			Schema:     tc.QualifiedSchema,
			TargetMode: "drop_recreate",
			Workers:    1,
		})
		for i, f := range findings {
			if f.Severity == "" || f.Check == "" || f.Side == "" || f.Message == "" {
				t.Fatalf("PreFlight finding %d has empty required fields: %+v", i, f)
			}
		}
	})

	t.Run("dialect basics", func(t *testing.T) {
		d := tc.Driver.Dialect()
		if d.DBType() != tc.Name {
			t.Fatalf("Dialect.DBType() = %q, want %q", d.DBType(), tc.Name)
		}
		if got := d.QuoteIdentifier(tc.QuoteName); got != tc.QuoteNameWant {
			t.Fatalf("QuoteIdentifier(%q) = %q, want %q", tc.QuoteName, got, tc.QuoteNameWant)
		}
		if got := d.QualifyTable(tc.QualifiedSchema, tc.QualifiedTable); got != tc.QualifiedNameWant {
			t.Fatalf("QualifyTable(%q, %q) = %q, want %q",
				tc.QualifiedSchema, tc.QualifiedTable, got, tc.QualifiedNameWant)
		}
		if got := d.ParameterPlaceholder(tc.PlaceholderIndex); got != tc.PlaceholderWant {
			t.Fatalf("ParameterPlaceholder(%d) = %q, want %q", tc.PlaceholderIndex, got, tc.PlaceholderWant)
		}
		if got := d.ColumnList(tc.ColumnList); got != tc.ColumnListWant {
			t.Fatalf("ColumnList(%v) = %q, want %q", tc.ColumnList, got, tc.ColumnListWant)
		}
		if got := d.ColumnListForSelect(tc.ColumnList, nil, tc.Name); got != tc.ColumnListWant {
			t.Fatalf("ColumnListForSelect(%v, nil, %q) = %q, want %q", tc.ColumnList, tc.Name, got, tc.ColumnListWant)
		}
		if got := d.TableHint(false); strings.TrimSpace(got) != got {
			t.Fatalf("TableHint(false) has surrounding whitespace: %q", got)
		}
	})

	t.Run("dialect metadata", func(t *testing.T) {
		d := tc.Driver.Dialect()
		if query := d.RowCountQuery(false); !strings.Contains(query, "%s") {
			t.Fatalf("RowCountQuery(false) = %q, want table-name format placeholder", query)
		}
		if query := d.DateColumnQuery(); strings.TrimSpace(query) == "" {
			t.Fatal("DateColumnQuery() returned empty query")
		}
		dateTypes := d.ValidDateTypes()
		if len(dateTypes) == 0 {
			t.Fatal("ValidDateTypes() returned no types")
		}
		if tc.DateType != "" && !dateTypes[tc.DateType] {
			t.Fatalf("ValidDateTypes()[%q] = false, map = %v", tc.DateType, dateTypes)
		}
	})

	t.Run("optional expectations", func(t *testing.T) {
		d := tc.Driver.Dialect()
		if tc.SchemaIgnoredInQualification {
			if got := d.QualifyTable("different_schema", tc.QualifiedTable); got != tc.QualifiedNameWant {
				t.Fatalf("schema-ignoring dialect changed qualification with schema: got %q, want %q", got, tc.QualifiedNameWant)
			}
		}
		if tc.RequireSchemaQualification {
			got := d.QualifyTable(tc.QualifiedSchema, tc.QualifiedTable)
			if !strings.Contains(got, tc.QualifiedSchema) {
				t.Fatalf("schema-qualified dialect result %q does not include schema %q", got, tc.QualifiedSchema)
			}
		}
		if tc.RequireAIPromptAugmentation && strings.TrimSpace(d.AIPromptAugmentation()) == "" {
			t.Fatal("AIPromptAugmentation() is required for this dialect but was empty")
		}
	})

	if tc.Pagination != nil {
		t.Run("pagination with date filters", func(t *testing.T) {
			runPaginationConformance(t, tc.Driver.Dialect(), *tc.Pagination)
		})
	}
}

func validateCase(t *testing.T, tc DriverCase) {
	t.Helper()
	if tc.Name == "" {
		t.Fatal("DriverCase.Name is required")
	}
	if tc.Driver == nil {
		t.Fatal("DriverCase.Driver is required")
	}
	required := map[string]string{
		"QuoteName":         tc.QuoteName,
		"QuoteNameWant":     tc.QuoteNameWant,
		"QualifiedTable":    tc.QualifiedTable,
		"QualifiedNameWant": tc.QualifiedNameWant,
		"PlaceholderWant":   tc.PlaceholderWant,
		"ColumnListWant":    tc.ColumnListWant,
	}
	for field, value := range required {
		if value == "" {
			t.Fatalf("DriverCase.%s is required", field)
		}
	}
	if len(tc.ColumnList) == 0 {
		t.Fatal("DriverCase.ColumnList is required")
	}
	if tc.PlaceholderIndex <= 0 {
		t.Fatal("DriverCase.PlaceholderIndex must be greater than zero")
	}
	if tc.RequireSchemaQualification && tc.QualifiedSchema == "" {
		t.Fatal("DriverCase.QualifiedSchema is required when RequireSchemaQualification is true")
	}
}

func runPaginationConformance(t *testing.T, d driver.Dialect, pc PaginationCase) {
	t.Helper()
	validatePaginationCase(t, pc)

	t.Run("keyset without max pk", func(t *testing.T) {
		gotQuery := d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, false, false, pc.DateFilter)
		assertSQL(t, gotQuery, pc.KeysetNoMaxQuery)

		gotArgs := d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, false, pc.DateFilter)
		assertArgs(t, gotArgs, pc.KeysetNoMaxArgs)
	})

	t.Run("keyset with max pk", func(t *testing.T) {
		gotQuery := d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, true, false, pc.DateFilter)
		assertSQL(t, gotQuery, pc.KeysetWithMaxQuery)

		gotArgs := d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, true, pc.DateFilter)
		assertArgs(t, gotArgs, pc.KeysetWithMaxArgs)
	})

	t.Run("keyset lower-bound semantics", func(t *testing.T) {
		quotedPK := d.QuoteIdentifier(pc.PKColumn)
		exclusive := d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, true, false, pc.DateFilter)
		inclusive := d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, true, true, pc.DateFilter)
		if !strings.Contains(exclusive, quotedPK+" > ") {
			t.Fatalf("exclusive query lacks strict lower bound for %s:\n%s", quotedPK, exclusive)
		}
		if !strings.Contains(inclusive, quotedPK+" >= ") {
			t.Fatalf("inclusive query lacks inclusive lower bound for %s:\n%s", quotedPK, inclusive)
		}
	})

	t.Run("row number", func(t *testing.T) {
		gotQuery := d.BuildRowNumberQuery(pc.Columns, pc.OrderBy, pc.Schema, pc.Table, pc.TableHint, pc.DateFilter)
		assertSQL(t, gotQuery, pc.RowNumberQuery)

		gotArgs := d.BuildRowNumberArgs(pc.RowNum, pc.Limit, pc.DateFilter)
		assertArgs(t, gotArgs, pc.RowNumberArgs)
	})

	// The dated cases above pin exact SQL; the date-free variants are
	// only checked for the universal invariant that every placeholder in
	// the query has exactly one bound argument (a mismatched catalog args
	// list slipped past the dated-only cases once).
	t.Run("placeholder/arg parity across variants", func(t *testing.T) {
		variants := []struct {
			name  string
			query string
			args  []any
		}{
			{"keyset no max no date",
				d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, false, false, nil),
				d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, false, nil)},
			{"keyset with max no date",
				d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, true, false, nil),
				d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, true, nil)},
			{"keyset no max dated",
				d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, false, false, pc.DateFilter),
				d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, false, pc.DateFilter)},
			{"keyset with max dated",
				d.BuildKeysetQuery(pc.Columns, pc.PKColumn, pc.Schema, pc.Table, pc.TableHint, true, false, pc.DateFilter),
				d.BuildKeysetArgs(pc.LastPK, pc.MaxPK, pc.Limit, true, pc.DateFilter)},
			{"row number no date",
				d.BuildRowNumberQuery(pc.Columns, pc.OrderBy, pc.Schema, pc.Table, pc.TableHint, nil),
				d.BuildRowNumberArgs(pc.RowNum, pc.Limit, nil)},
			{"row number dated",
				d.BuildRowNumberQuery(pc.Columns, pc.OrderBy, pc.Schema, pc.Table, pc.TableHint, pc.DateFilter),
				d.BuildRowNumberArgs(pc.RowNum, pc.Limit, pc.DateFilter)},
		}
		for _, v := range variants {
			assertPlaceholderParity(t, d, v.name, v.query, v.args)
		}
	})
}

// assertPlaceholderParity verifies every bound argument has a matching
// placeholder in the query and vice versa. Named arguments (sql.Named,
// used by MSSQL) are matched by @name; positional schemes ("?") by
// occurrence count; numbered schemes ($1/@p1) by consecutive presence.
func assertPlaceholderParity(t *testing.T, d driver.Dialect, name, query string, args []any) {
	t.Helper()
	named := 0
	for _, a := range args {
		if na, ok := a.(sql.NamedArg); ok {
			named++
			if !strings.Contains(query, "@"+na.Name) {
				t.Errorf("%s: arg @%s has no placeholder in query\nquery: %s", name, na.Name, query)
			}
		}
	}
	if named > 0 {
		if named != len(args) {
			t.Errorf("%s: mixed named and positional args: %v", name, args)
		}
		return
	}

	var bound int
	if p := d.ParameterPlaceholder(1); p == d.ParameterPlaceholder(2) {
		bound = strings.Count(query, p)
	} else {
		for strings.Contains(query, d.ParameterPlaceholder(bound+1)) {
			bound++
		}
	}
	if bound != len(args) {
		t.Errorf("%s: query binds %d placeholders but args has %d values\nquery: %s\nargs: %v",
			name, bound, len(args), query, args)
	}
}

func validatePaginationCase(t *testing.T, pc PaginationCase) {
	t.Helper()
	required := map[string]string{
		"Columns":            pc.Columns,
		"PKColumn":           pc.PKColumn,
		"Table":              pc.Table,
		"OrderBy":            pc.OrderBy,
		"KeysetNoMaxQuery":   pc.KeysetNoMaxQuery,
		"KeysetWithMaxQuery": pc.KeysetWithMaxQuery,
		"RowNumberQuery":     pc.RowNumberQuery,
	}
	for field, value := range required {
		if value == "" {
			t.Fatalf("PaginationCase.%s is required", field)
		}
	}
	if pc.DateFilter == nil {
		t.Fatal("PaginationCase.DateFilter is required")
	}
	if pc.Limit <= 0 {
		t.Fatal("PaginationCase.Limit must be greater than zero")
	}
	if len(pc.KeysetNoMaxArgs) == 0 {
		t.Fatal("PaginationCase.KeysetNoMaxArgs is required")
	}
	if len(pc.KeysetWithMaxArgs) == 0 {
		t.Fatal("PaginationCase.KeysetWithMaxArgs is required")
	}
	if len(pc.RowNumberArgs) == 0 {
		t.Fatal("PaginationCase.RowNumberArgs is required")
	}
}

func assertSQL(t *testing.T, got, want string) {
	t.Helper()
	got = normalizeSQL(got)
	want = normalizeSQL(want)
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
	}
}

func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func assertArgs(t *testing.T, got, want []any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(got))
	for _, s := range got {
		counts[s]++
	}
	for _, s := range want {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}

// WriterCapabilities declares which optional writer capabilities an
// engine is expected to implement (#460). The conformance harness
// asserts declared == actual via type assertion, so a capability
// silently gained or lost by a refactor fails the driver's tests.
type WriterCapabilities struct {
	// ConstraintWriter: can the engine ADD CONSTRAINT (FK/CHECK) after
	// CREATE TABLE? SQLite cannot — constraints are inline-only there.
	ConstraintWriter bool
	// Upserter: staging+MERGE / ON CONFLICT incremental writes.
	// target_mode=upsert is rejected up front without it (#476).
	Upserter bool
	// SequenceResetter: post-transfer identity/sequence reset; engines
	// without sequences skip the phase (#476).
	SequenceResetter bool
}

// CheckWriterCapabilities asserts the writer's optional capabilities
// match the declaration. Pass a typed nil writer pointer — only the
// type's method set matters, no connection is made.
func CheckWriterCapabilities(t *testing.T, w driver.Writer, want WriterCapabilities) {
	t.Helper()
	if _, got := w.(driver.ConstraintWriter); got != want.ConstraintWriter {
		t.Errorf("ConstraintWriter capability = %v, want %v — update the capability matrix or the writer's method set deliberately", got, want.ConstraintWriter)
	}
	if _, got := w.(driver.Upserter); got != want.Upserter {
		t.Errorf("Upserter capability = %v, want %v — update the capability matrix or the writer's method set deliberately", got, want.Upserter)
	}
	if _, got := w.(driver.SequenceResetter); got != want.SequenceResetter {
		t.Errorf("SequenceResetter capability = %v, want %v — update the capability matrix or the writer's method set deliberately", got, want.SequenceResetter)
	}
}

// ReaderCapabilities declares the optional reader capabilities (#476).
type ReaderCapabilities struct {
	// IncrementalDateReader: date-updated column discovery + high
	// watermark reads for incremental sync. Absence degrades affected
	// tables to full sync.
	IncrementalDateReader bool
}

// CheckReaderCapabilities asserts the reader's optional capabilities
// match the declaration; pass a typed nil reader pointer.
func CheckReaderCapabilities(t *testing.T, r driver.Reader, want ReaderCapabilities) {
	t.Helper()
	if _, got := r.(driver.IncrementalDateReader); got != want.IncrementalDateReader {
		t.Errorf("IncrementalDateReader capability = %v, want %v — update the capability matrix or the reader's method set deliberately", got, want.IncrementalDateReader)
	}
}
