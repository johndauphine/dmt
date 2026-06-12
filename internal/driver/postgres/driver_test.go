package postgres

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestDialect(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		method   func() string
		expected string
	}{
		{"DBType", dialect.DBType, "postgres"},
		{"QuoteIdentifier", func() string { return dialect.QuoteIdentifier("test") }, `"test"`},
		{"QualifyTable", func() string { return dialect.QualifyTable("public", "users") }, `"public"."users"`},
		{"ParameterPlaceholder", func() string { return dialect.ParameterPlaceholder(1) }, "$1"},
		{"TableHint", func() string { return dialect.TableHint(false) }, ""},
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

func TestBuildAddColumnSQL(t *testing.T) {
	w := &Writer{
		dialect:    &Dialect{},
		typeMapper: driver.NewDeterministicMapper(),
		sourceType: "postgres",
	}
	got, err := w.buildAddColumnSQL(
		&driver.Table{Name: "Users"},
		&driver.Column{Name: "Email Address", DataType: "varchar", MaxLength: 255},
		"public",
	)
	if err != nil {
		t.Fatalf("buildAddColumnSQL returned error: %v", err)
	}

	for _, want := range []string{
		`ALTER TABLE "public"."users" ADD COLUMN`,
		`"email_address"`,
		"NULL",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("SQL %q missing %q", got, want)
		}
	}
}

func TestBuildDropColumnNotNullSQL(t *testing.T) {
	w := &Writer{dialect: &Dialect{}}
	got, err := w.buildDropColumnNotNullSQL(
		&driver.Table{Name: "Users"},
		&driver.Column{Name: "Email Address"},
		"public",
	)
	if err != nil {
		t.Fatalf("buildDropColumnNotNullSQL returned error: %v", err)
	}

	want := `ALTER TABLE "public"."users" ALTER COLUMN "email_address" DROP NOT NULL`
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
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
		&driver.Column{Name: "Email Address", DataType: "varchar", MaxLength: 512},
		"public",
	)
	if err != nil {
		t.Fatalf("buildAlterColumnTypeSQL returned error: %v", err)
	}

	want := `ALTER TABLE "public"."users" ALTER COLUMN "email_address" TYPE VARCHAR(512)`
	if got != want {
		t.Fatalf("SQL = %q, want %q", got, want)
	}
}

func TestEstimateRowBytes(t *testing.T) {
	tests := []struct {
		name string
		rows [][]any
		want int // minimum expected
	}{
		{"empty rows", nil, 64},
		{"narrow int rows", [][]any{{1, 2, 3}}, 64},       // 3*8=24, clamped to 64
		{"string rows", [][]any{{"hello world", 42}}, 64}, // 11+8=19, clamped to 64
		{"wide rows", [][]any{{string(make([]byte, 10000))}}, 10000},
		{"mixed", [][]any{
			{string(make([]byte, 500)), 1, true},
			{string(make([]byte, 300)), 2, false},
		}, 200}, // avg ~(516+316)/2 = 416
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowBytes(tt.rows, 10)
			if got < tt.want {
				t.Errorf("estimateRowBytes() = %d, want >= %d", got, tt.want)
			}
		})
	}
}

func TestCopyBatchSize(t *testing.T) {
	targetBytes := fallbackCopyBytes // 3 MB

	// Narrow rows (~64 bytes): 3MB/64 = 49152, under maxCopyBatchRows (50000)
	narrow := make([][]any, 100000)
	for i := range narrow {
		narrow[i] = []any{i, i + 1}
	}
	got := copyBatchSize(narrow, targetBytes)
	if got < 40000 || got > 50000 {
		t.Errorf("narrow rows: copyBatchSize() = %d, want in [40000, 50000]", got)
	}

	// Wide rows (~10KB each): 3MB / ~10008 bytes ≈ 314
	wide := make([][]any, 1000)
	for i := range wide {
		wide[i] = []any{string(make([]byte, 10000)), i}
	}
	got = copyBatchSize(wide, targetBytes)
	if got < 200 || got > 400 {
		t.Errorf("wide rows: copyBatchSize() = %d, want in [200, 400]", got)
	}

	// Very wide rows (~100KB each): 3MB / 102400 = ~30, clamped to minCopyBatchRows (100)
	veryWide := make([][]any, 10)
	for i := range veryWide {
		veryWide[i] = []any{string(make([]byte, 100000)), string(make([]byte, 2400))}
	}
	got = copyBatchSize(veryWide, targetBytes)
	if got != minCopyBatchRows {
		t.Errorf("very wide rows: copyBatchSize() = %d, want %d", got, minCopyBatchRows)
	}
}

func TestAIPromptAugmentation(t *testing.T) {
	dialect := &Dialect{}
	aug := dialect.AIPromptAugmentation()

	// Verify the augmentation contains critical PostgreSQL identifier rules
	checks := []string{
		"CRITICAL PostgreSQL identifier rules",
		"MUST preserve the exact spelling and underscores from the source name",
		"ONLY allowed transformation is lowercasing letters",
		"Do NOT abbreviate, shorten, remove underscores, or change any non-letter characters",
		"user_id → user_id (NOT userid)",
		"LastEditorDisplayName → lasteditordisplayname",
		"created_at → created_at (NOT createdat)",
		"Do NOT use double-quotes around identifiers",
	}

	for _, check := range checks {
		if !strings.Contains(aug, check) {
			t.Errorf("AIPromptAugmentation should contain %q", check)
		}
	}
}
