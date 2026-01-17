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

func TestAIPromptAugmentation(t *testing.T) {
	dialect := &Dialect{}
	aug := dialect.AIPromptAugmentation()

	// Verify the augmentation contains critical PostgreSQL identifier rules
	checks := []string{
		"CRITICAL PostgreSQL identifier rules",
		"EXACT lowercase version of the source name",
		"Do NOT abbreviate, shorten, or modify names",
		"LastEditorDisplayName → lasteditordisplayname",
		"LastEditorUserId → lasteditoruserid",
		"Do NOT use double-quotes around identifiers",
	}

	for _, check := range checks {
		if !strings.Contains(aug, check) {
			t.Errorf("AIPromptAugmentation should contain %q", check)
		}
	}
}
