package mssql

import (
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestDriverRegistration(t *testing.T) {
	// The driver should be registered via init()
	d, err := driver.Get("mssql")
	if err != nil {
		t.Fatalf("Failed to get mssql driver: %v", err)
	}

	if d.Name() != "mssql" {
		t.Errorf("Expected driver name 'mssql', got %q", d.Name())
	}

	// Test aliases
	for _, alias := range []string{"sqlserver", "sql-server"} {
		d, err := driver.Get(alias)
		if err != nil {
			t.Errorf("Failed to get driver by alias %q: %v", alias, err)
			continue
		}
		if d.Name() != "mssql" {
			t.Errorf("Expected driver name 'mssql' for alias %q, got %q", alias, d.Name())
		}
	}
}

func TestDialect(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		method   func() string
		expected string
	}{
		{"DBType", dialect.DBType, "mssql"},
		{"QuoteIdentifier", func() string { return dialect.QuoteIdentifier("test") }, `[test]`},
		{"QualifyTable", func() string { return dialect.QualifyTable("dbo", "users") }, `[dbo].[users]`},
		{"ParameterPlaceholder", func() string { return dialect.ParameterPlaceholder(1) }, "@p1"},
		{"TableHint", func() string { return dialect.TableHint(false) }, "WITH (NOLOCK)"},
		{"TableHintStrict", func() string { return dialect.TableHint(true) }, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.method()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestAvailableDrivers(t *testing.T) {
	available := driver.Available()
	found := false
	for _, name := range available {
		if name == "mssql" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("MSSQL driver not in available list: %v", available)
	}
}

func TestBuildAddColumnSQL(t *testing.T) {
	w := &Writer{
		dialect:    &Dialect{},
		typeMapper: driver.NewDeterministicMapper(),
		sourceType: "postgres",
	}
	got, err := w.buildAddColumnSQL(
		&driver.Table{Name: "Users"},
		&driver.Column{Name: "Email", DataType: "varchar", MaxLength: 255},
		"dbo",
	)
	if err != nil {
		t.Fatalf("buildAddColumnSQL returned error: %v", err)
	}

	for _, want := range []string{
		"ALTER TABLE [dbo].[Users] ADD",
		"[Email]",
		"NULL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("SQL %q missing %q", got, want)
		}
	}
}

func TestBuildDropColumnNotNullSQL(t *testing.T) {
	w := &Writer{
		dialect:    &Dialect{},
		typeMapper: driver.NewDeterministicMapper(),
		sourceType: "postgres",
	}
	got, err := w.buildDropColumnNotNullSQL(
		&driver.Table{Name: "Users"},
		&driver.Column{Name: "Email", DataType: "varchar", MaxLength: 255},
		"dbo",
	)
	if err != nil {
		t.Fatalf("buildDropColumnNotNullSQL returned error: %v", err)
	}

	for _, want := range []string{
		"ALTER TABLE [dbo].[Users] ALTER COLUMN",
		"[Email]",
		"VARCHAR(255)",
		"NULL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("SQL %q missing %q", got, want)
		}
	}
}

func TestBuildAlterColumnTypeSQL(t *testing.T) {
	w := &Writer{
		dialect:    &Dialect{},
		typeMapper: driver.NewDeterministicMapper(),
		sourceType: "postgres",
	}
	got, err := w.buildAlterColumnTypeSQL(
		&driver.Table{Name: "Users"},
		&driver.Column{Name: "Email", DataType: "varchar", MaxLength: 512, IsNullable: false},
		"dbo",
	)
	if err != nil {
		t.Fatalf("buildAlterColumnTypeSQL returned error: %v", err)
	}

	for _, want := range []string{
		"ALTER TABLE [dbo].[Users] ALTER COLUMN",
		"[Email]",
		"VARCHAR(512)",
		"NOT NULL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("SQL %q missing %q", got, want)
		}
	}
}

func TestFormatDateFilterTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 123456700, time.FixedZone("CDT", -5*60*60))

	got := formatDateFilterTimestamp(ts)
	want := "2024-06-15T15:30:00.1234567"
	if got != want {
		t.Fatalf("formatDateFilterTimestamp() = %q, want %q", got, want)
	}
}

func TestQuoteIdentifierWithSpecialChars(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "[simple]"},
		{"with space", "[with space]"},
		{"with]bracket", "[with]]bracket]"},
		{"schema.table", "[schema.table]"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := dialect.QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBuildDSNEncrypt(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		encrypt  bool
		expected string
	}{
		{"encrypt true", true, "&encrypt=true"},
		{"encrypt false uses disable", false, "&encrypt=disable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := dialect.BuildDSN("localhost", 1433, "testdb", "sa", "pass", map[string]any{
				"encrypt": tt.encrypt,
			})
			if !strings.Contains(dsn, tt.expected) {
				t.Errorf("BuildDSN with encrypt=%v should contain %q, got %q", tt.encrypt, tt.expected, dsn)
			}
		})
	}
}

func TestIsASCIINumeric(t *testing.T) {
	tests := []struct {
		input    []byte
		expected bool
	}{
		{[]byte("123"), true},
		{[]byte("-45.67"), true},
		{[]byte("+0.5"), true},
		{[]byte("1.5E+10"), true},
		{[]byte("1e-5"), true},
		{[]byte(".5"), true},
		{[]byte(""), false},
		{[]byte("."), false},
		{[]byte("+-1"), false},
		{[]byte("1.2.3"), false},
		{[]byte("abc"), false},
		{[]byte{0x01, 0x02, 0x03}, false}, // binary data
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := isASCIINumeric(tt.input)
			if result != tt.expected {
				t.Errorf("isASCIINumeric(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
