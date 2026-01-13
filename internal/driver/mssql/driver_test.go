package mssql

import (
	"testing"

	"github.com/johndauphine/data-transfer-tool/internal/driver"
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

func TestTypeMapper(t *testing.T) {
	mapper := &TypeMapper{}

	tests := []struct {
		source   string
		dataType string
		expected string
	}{
		{"postgres", "boolean", "bit"},
		{"postgres", "integer", "int"},
		{"postgres", "bigint", "bigint"},
		{"postgres", "text", "nvarchar(max)"},
		{"postgres", "timestamp", "datetime2"},
		{"postgres", "uuid", "uniqueidentifier"},
		{"mssql", "int", "int"},
		{"mssql", "bigint", "bigint"},
		{"mssql", "bit", "bit"},
	}

	for _, tt := range tests {
		t.Run(tt.source+"_"+tt.dataType, func(t *testing.T) {
			result := mapper.MapType(driver.TypeInfo{
				SourceDBType: tt.source,
				TargetDBType: "mssql",
				DataType:     tt.dataType,
			})
			if result != tt.expected {
				t.Errorf("MapType(%s, %s) = %q, want %q", tt.source, tt.dataType, result, tt.expected)
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
