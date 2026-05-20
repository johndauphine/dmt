package conformance

import (
	"context"
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
