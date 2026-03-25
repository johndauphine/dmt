package postgres

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

func TestDriverRegistration(t *testing.T) {
	// The driver should be registered via init()
	d, err := driver.Get("postgres")
	if err != nil {
		t.Fatalf("Failed to get postgres driver: %v", err)
	}

	if d.Name() != "postgres" {
		t.Errorf("Expected driver name 'postgres', got %q", d.Name())
	}

	// Test aliases
	for _, alias := range []string{"postgresql", "pg"} {
		d, err := driver.Get(alias)
		if err != nil {
			t.Errorf("Failed to get driver by alias %q: %v", alias, err)
			continue
		}
		if d.Name() != "postgres" {
			t.Errorf("Expected driver name 'postgres' for alias %q, got %q", alias, d.Name())
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

func TestAvailableDrivers(t *testing.T) {
	available := driver.Available()
	found := false
	for _, name := range available {
		if name == "postgres" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("PostgreSQL driver not in available list: %v", available)
	}
}

func TestEstimateAvgRowBytes(t *testing.T) {
	tests := []struct {
		name string
		rows [][]any
		want int // minimum expected
	}{
		{"empty rows", nil, 64},
		{"narrow int rows", [][]any{{1, 2, 3}}, 64},            // 3*8=24, clamped to 64
		{"string rows", [][]any{{"hello world", 42}}, 64},      // 11+8=19, clamped to 64
		{"wide rows", [][]any{{string(make([]byte, 10000))}}, 10000},
		{"mixed", [][]any{
			{string(make([]byte, 500)), 1, true},
			{string(make([]byte, 300)), 2, false},
		}, 200}, // avg ~(516+316)/2 = 416
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateAvgRowBytes(tt.rows, 10)
			if got < tt.want {
				t.Errorf("estimateAvgRowBytes() = %d, want >= %d", got, tt.want)
			}
		})
	}
}

func TestCopyBatchSize(t *testing.T) {
	targetBytes := fallbackCopyBytes // 1 MB

	// Narrow rows (~64 bytes): 1MB/64 = 16384, clamped to maxCopyBatchRows (10000)
	narrow := make([][]any, 100)
	for i := range narrow {
		narrow[i] = []any{i, i + 1}
	}
	got := copyBatchSize(narrow, targetBytes)
	if got != maxCopyBatchRows {
		t.Errorf("narrow rows: copyBatchSize() = %d, want %d", got, maxCopyBatchRows)
	}

	// Wide rows (~10KB each): 1MB / ~10008 bytes ≈ 104
	wide := make([][]any, 100)
	for i := range wide {
		wide[i] = []any{string(make([]byte, 10000)), i}
	}
	got = copyBatchSize(wide, targetBytes)
	if got < minCopyBatchRows || got > 200 {
		t.Errorf("wide rows: copyBatchSize() = %d, want in [%d, 200]", got, minCopyBatchRows)
	}

	// Very wide rows (~100KB each): 1MB / 102400 = ~10, clamped to minCopyBatchRows (100)
	veryWide := make([][]any, 10)
	for i := range veryWide {
		veryWide[i] = []any{string(make([]byte, 100000)), string(make([]byte, 2400))}
	}
	got = copyBatchSize(veryWide, targetBytes)
	if got != minCopyBatchRows {
		t.Errorf("very wide rows: copyBatchSize() = %d, want %d", got, minCopyBatchRows)
	}

	// Larger TCP buffer should allow bigger batches
	got = copyBatchSize(narrow, 5<<20) // 5 MB
	if got != maxCopyBatchRows {
		t.Errorf("5MB budget narrow rows: copyBatchSize() = %d, want %d", got, maxCopyBatchRows)
	}
	got = copyBatchSize(wide, 5<<20) // 5 MB — ~10008 bytes/row → ~523 rows
	if got < 400 || got > 600 {
		t.Errorf("5MB budget wide rows: copyBatchSize() = %d, want in [400, 600]", got)
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
