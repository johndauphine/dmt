// DEFAULT translation tests. Ported from UVG ddl.rs lines 692-820 +
// codegen/mod.rs lines 545-577 (the strip helpers).

package ddl

import "testing"

func TestStripPostgresTypecast(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"0::integer", "0"},
		{"'hello'::character varying", "'hello'"},
		{"now()", "now()"},
		// :: inside a function arg should be preserved (UVG corpus)
		{"nextval('seq'::regclass)", "nextval('seq'::regclass)"},
		{"3.14::numeric(10,2)", "3.14"},
		{"'  spaced  '::text", "'  spaced  '"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := stripPostgresTypecast(tc.input); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStripMSSQLParens(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"((0))", "0"},
		{"((1))", "1"},
		{"(N'hello')", "'hello'"},
		{"(getdate())", "getdate()"},
		{"value", "value"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := stripMSSQLParens(tc.input); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslateDefaultFunction_CurrentTimestamp(t *testing.T) {
	tests := []struct {
		input, target, want string
	}{
		{"now()", DialectMySQL, "CURRENT_TIMESTAMP"},
		{"GETDATE()", DialectPostgres, "now()"},
		{"CURRENT_TIMESTAMP", DialectMSSQL, "GETDATE()"},
		{"sysdatetime()", DialectPostgres, "now()"},
	}
	for _, tc := range tests {
		t.Run(tc.input+"_to_"+tc.target, func(t *testing.T) {
			if got := translateDefaultFunction(tc.input, tc.target); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslateDefaultFunction_UUID(t *testing.T) {
	tests := []struct {
		input, target, want string
	}{
		{"gen_random_uuid()", DialectMySQL, "(UUID())"},
		{"NEWID()", DialectPostgres, "gen_random_uuid()"},
		{"uuid()", DialectMSSQL, "NEWID()"},
	}
	for _, tc := range tests {
		t.Run(tc.input+"_to_"+tc.target, func(t *testing.T) {
			if got := translateDefaultFunction(tc.input, tc.target); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTranslateDefaultFunction_Passthrough(t *testing.T) {
	// Anything not in the function-translation table passes through
	// unchanged — caller (FormatDDLDefault) handles literal quoting.
	tests := []string{"'hello'", "42", "vendor_specific_function()"}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if got := translateDefaultFunction(input, DialectPostgres); got != input {
				t.Errorf("got %q, want %q (passthrough)", got, input)
			}
		})
	}
}

func TestEnsureDefaultQuoting(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"member", "'member'"},
		{"active", "'active'"},
		{"'member'", "'member'"},
		{`"double"`, `"double"`},
		{"0", "0"},
		{"3.14", "3.14"},
		{"-42", "-42"},
		{"1.5e10", "1.5e10"},
		{"NULL", "NULL"},
		{"null", "null"},
		{"true", "true"},
		{"false", "false"},
		{"now()", "now()"},
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"current_timestamp", "current_timestamp"},
		// Embedded single quote → doubled
		{"it's", "'it''s'"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := ensureDefaultQuoting(tc.input); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatDDLDefault_BooleanLiteralTranslation(t *testing.T) {
	// MSSQL "((1))" → PG true (boolean column hint set)
	if got := FormatDDLDefault("((1))", DialectMSSQL, DialectPostgres, true); got != "true" {
		t.Errorf("MSSQL ((1)) → PG bool: got %q, want true", got)
	}
	if got := FormatDDLDefault("((0))", DialectMSSQL, DialectPostgres, true); got != "false" {
		t.Errorf("MSSQL ((0)) → PG bool: got %q, want false", got)
	}
	// PG "true" → MSSQL "1" / MySQL "1"
	if got := FormatDDLDefault("true", DialectPostgres, DialectMSSQL, true); got != "1" {
		t.Errorf("PG true → MSSQL bool: got %q, want 1", got)
	}
	if got := FormatDDLDefault("true", DialectPostgres, DialectMySQL, true); got != "1" {
		t.Errorf("PG true → MySQL bool: got %q, want 1", got)
	}
	if got := FormatDDLDefault("false", DialectPostgres, DialectMSSQL, true); got != "0" {
		t.Errorf("PG false → MSSQL bool: got %q, want 0", got)
	}
}

func TestFormatDDLDefault_IntegerNotConvertedToBoolean(t *testing.T) {
	// Critical: when isBoolean=false, MSSQL "((0))" and "((1))" must
	// stay as integers — they're integer DEFAULT values, not booleans.
	// UVG had this exact regression test (line 790).
	if got := FormatDDLDefault("((0))", DialectMSSQL, DialectPostgres, false); got != "0" {
		t.Errorf("integer DEFAULT 0 should NOT translate to bool: got %q, want 0", got)
	}
	if got := FormatDDLDefault("((1))", DialectMSSQL, DialectPostgres, false); got != "1" {
		t.Errorf("integer DEFAULT 1 should NOT translate to bool: got %q, want 1", got)
	}
}

func TestFormatDDLDefault_FunctionTranslation(t *testing.T) {
	// PG now() → MySQL CURRENT_TIMESTAMP
	if got := FormatDDLDefault("now()", DialectPostgres, DialectMySQL, false); got != "CURRENT_TIMESTAMP" {
		t.Errorf("got %q, want CURRENT_TIMESTAMP", got)
	}
	// MSSQL ((getdate())) → PG now() — strip parens, translate function
	if got := FormatDDLDefault("(getdate())", DialectMSSQL, DialectPostgres, false); got != "now()" {
		t.Errorf("got %q, want now()", got)
	}
}

func TestFormatDDLDefault_StringLiteralWithTypecast(t *testing.T) {
	// PG 'hello'::character varying → MySQL 'hello' (strip cast, keep quote)
	if got := FormatDDLDefault("'hello'::character varying", DialectPostgres, DialectMySQL, false); got != "'hello'" {
		t.Errorf("got %q, want 'hello'", got)
	}
}

func TestFormatDDLDefault_EmptyInput(t *testing.T) {
	if got := FormatDDLDefault("", DialectPostgres, DialectMySQL, false); got != "" {
		t.Errorf("empty input should return empty; got %q", got)
	}
}
