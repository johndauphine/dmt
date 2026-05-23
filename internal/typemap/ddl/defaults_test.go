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

// TestStripPostgresTypecast_StackedCasts — Copilot review on PR #188.
// Original implementation stripped only the outermost ::TYPE in one
// pass; "'hello'::varchar::text" became "'hello'::varchar" with the
// inner cast still on it, which would break MSSQL/MySQL emission
// (the :: cast syntax is PG-specific). Now strips recursively until
// no more outer casts remain.
func TestStripPostgresTypecast_StackedCasts(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"'hello'::varchar::text", "'hello'"},
		{"0::integer::bigint", "0"},
		{"'a'::text::varchar::char", "'a'"},
		// Inner :: inside a function arg still survives across recursion
		{"nextval('seq'::regclass)::bigint", "nextval('seq'::regclass)"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := stripPostgresTypecast(tc.input); got != tc.want {
				t.Errorf("got %q, want %q (#188 recursive-strip regression)", got, tc.want)
			}
		})
	}
}

// TestIsNumericLiteral_RejectsMalformed — Copilot review on PR #188.
// Original byte-walk implementation accepted invalid forms like "1e"
// and "1e+" (digits followed by exponent marker but no exponent
// digits). ensureDefaultQuoting would then skip quoting on those,
// emitting invalid SQL. Now defers to strconv.ParseFloat which
// rejects them.
func TestIsNumericLiteral_RejectsMalformed(t *testing.T) {
	rejected := []string{"1e", "1e+", "1e-", "e10", ".", "+", "-", ""}
	for _, s := range rejected {
		t.Run("rejects_"+s, func(t *testing.T) {
			if isNumericLiteral(s) {
				t.Errorf("%q should NOT be a valid numeric literal (#188 regression)", s)
			}
		})
	}

	// Inf/NaN are valid float64 from strconv.ParseFloat but not valid
	// SQL numeric defaults — must also be rejected.
	for _, s := range []string{"Inf", "+Inf", "-Inf", "NaN"} {
		t.Run("rejects_"+s, func(t *testing.T) {
			if isNumericLiteral(s) {
				t.Errorf("%q should NOT be a valid numeric literal", s)
			}
		})
	}

	accepted := []string{"0", "1", "-1", "3.14", "-3.14", "1e10", "1.5e-10", "+42"}
	for _, s := range accepted {
		t.Run("accepts_"+s, func(t *testing.T) {
			if !isNumericLiteral(s) {
				t.Errorf("%q should be a valid numeric literal", s)
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

// TestStripMSSQLParens_DoesNotOverStrip — Codex review on PR #188.
// Original implementation looped while s starts with '(' and ends
// with ')', without checking that the outer pair actually wraps the
// whole expression. "((1)+(2))" stripped to "(1)+(2)" then to
// "1)+(2" — invalid. Now: only strip when the outer parens enclose
// everything (paren-depth scan with quote tracking).
func TestStripMSSQLParens_DoesNotOverStrip(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"((1)+(2))", "(1)+(2)"},         // outer wraps, but inner pair doesn't span — strip once only
		{"((1)+(2)+(3))", "(1)+(2)+(3)"}, // same shape, three terms
		{"(a)+(b)", "(a)+(b)"},           // no outer wrapping — leave alone
		{"((((0))))", "0"},               // multiple safe layers — strip all
		{"('a)b')", "'a)b'"},             // ')' inside quoted literal must not confuse depth tracking
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := stripMSSQLParens(tc.input); got != tc.want {
				t.Errorf("got %q, want %q (#188 regression)", got, tc.want)
			}
		})
	}
}

// TestTranslateDefaultFunction_CurrentTimestamp covers the issue #169
// translation table (PG and MySQL → CURRENT_TIMESTAMP, MSSQL →
// GETDATE()). Both PG and MySQL use the SQL-standard form for
// portability; MSSQL uses GETDATE() because the standard form isn't
// recognized as the "default current time" sentinel by all MSSQL
// versions. Earlier UVG-derived behavior emitted now() for PG —
// updated per Copilot review on PR #188.
func TestTranslateDefaultFunction_CurrentTimestamp(t *testing.T) {
	tests := []struct {
		input, target, want string
	}{
		{"now()", DialectMySQL, "CURRENT_TIMESTAMP"},
		{"now()", DialectPostgres, "CURRENT_TIMESTAMP"},
		{"GETDATE()", DialectPostgres, "CURRENT_TIMESTAMP"},
		{"GETDATE()", DialectMySQL, "CURRENT_TIMESTAMP"},
		{"CURRENT_TIMESTAMP", DialectMSSQL, "GETDATE()"},
		{"CURRENT_TIMESTAMP", DialectPostgres, "CURRENT_TIMESTAMP"},
		{"sysdatetime()", DialectPostgres, "CURRENT_TIMESTAMP"},
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
	// MSSQL ((getdate())) → PG CURRENT_TIMESTAMP — strip parens, translate
	// function (per #169 translation table).
	if got := FormatDDLDefault("(getdate())", DialectMSSQL, DialectPostgres, false); got != "CURRENT_TIMESTAMP" {
		t.Errorf("got %q, want CURRENT_TIMESTAMP", got)
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
