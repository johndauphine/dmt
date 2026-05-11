package errordiag

import (
	"regexp"
	"strings"
	"testing"
)

func TestLookup_UnknownDriverReturnsFalse(t *testing.T) {
	if _, ok := Lookup("oracle", "anything"); ok {
		t.Fatalf("Lookup on unregistered driver should return ok=false")
	}
}

func TestLookup_NonMatchingErrorReturnsFalse(t *testing.T) {
	if _, ok := Lookup("postgres", "some bizarre error that no pattern should catch xyzzy"); ok {
		t.Fatalf("Lookup on non-matching error should return ok=false")
	}
}

func TestLookup_CaseInsensitiveDriverName(t *testing.T) {
	// Same payload should match under any casing of the driver name.
	const errMsg = `pq: duplicate key value violates unique constraint "users_pkey"`
	for _, name := range []string{"postgres", "POSTGRES", "Postgres", "PostgreSQL", "pg"} {
		if _, ok := Lookup(name, errMsg); !ok {
			t.Errorf("Lookup(%q) should match a registered PG pattern", name)
		}
	}
}

func TestLookup_FirstMatchWins(t *testing.T) {
	// Register two patterns on a unique driver name; the first should win.
	driver := "test_priority_driver"
	Register(driver, []Pattern{
		{
			Name:  "first",
			Regex: regexp.MustCompile(`error: foo`),
			Diagnose: func(_ []string) Diagnosis {
				return Diagnosis{Cause: "first wins", Confidence: "high", Category: "other"}
			},
		},
		{
			Name:  "second",
			Regex: regexp.MustCompile(`foo`), // also matches, but registered second
			Diagnose: func(_ []string) Diagnosis {
				return Diagnosis{Cause: "second", Confidence: "high", Category: "other"}
			},
		},
	})
	m, ok := Lookup(driver, "error: foo here")
	if !ok || m.PatternName != "first" || m.Diagnosis.Cause != "first wins" {
		t.Fatalf("expected first registered pattern to win; got ok=%v pattern=%q cause=%q",
			ok, m.PatternName, m.Diagnosis.Cause)
	}
}

func TestPatternCount_ReturnsRegisteredCount(t *testing.T) {
	// Confirm each shipped driver has the minimum-viable coverage promised
	// by issue #173 (≥20 patterns per driver).
	for _, driver := range []string{"postgres", "mssql", "mysql"} {
		if got := PatternCount(driver); got < 20 {
			t.Errorf("%s catalog has only %d patterns; #173 requires ≥20", driver, got)
		}
	}
}

func TestRegister_AppendsToExistingCatalog(t *testing.T) {
	driver := "test_append_driver"
	Register(driver, []Pattern{{
		Name:  "a",
		Regex: regexp.MustCompile(`A`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{Cause: "a", Confidence: "high", Category: "other"}
		},
	}})
	Register(driver, []Pattern{{
		Name:  "b",
		Regex: regexp.MustCompile(`B`),
		Diagnose: func(_ []string) Diagnosis {
			return Diagnosis{Cause: "b", Confidence: "high", Category: "other"}
		},
	}})
	if PatternCount(driver) != 2 {
		t.Fatalf("expected 2 patterns after two Register calls, got %d", PatternCount(driver))
	}
	if m, ok := Lookup(driver, "B"); !ok || m.PatternName != "b" {
		t.Fatalf("expected second Register to be reachable; got ok=%v pattern=%q", ok, m.PatternName)
	}
}

func TestDiagnosis_FieldsAreNormalized(t *testing.T) {
	// Spot-check a known PG pattern returns a well-formed diagnosis.
	m, ok := Lookup("postgres", `pq: duplicate key value violates unique constraint "users_pkey"`)
	if !ok {
		t.Fatal("expected duplicate-key pattern to match")
	}
	if m.Diagnosis.Cause == "" {
		t.Error("Cause should be non-empty")
	}
	if len(m.Diagnosis.Suggestions) == 0 {
		t.Error("Suggestions should be non-empty")
	}
	switch m.Diagnosis.Confidence {
	case "high", "medium", "low":
	default:
		t.Errorf("Confidence %q not in {high,medium,low}", m.Diagnosis.Confidence)
	}
	switch m.Diagnosis.Category {
	case "type_mismatch", "constraint", "permission", "connection", "data_quality", "other":
	default:
		t.Errorf("Category %q not in the documented set", m.Diagnosis.Category)
	}
}

// validCategories is the set ErrorDiagnosis.Category may take; mirrored
// from driver.ErrorDiagnosis's normalizer. Per-driver test files use
// this to assert every catalog entry produces a well-formed value.
var validCategories = map[string]struct{}{
	"type_mismatch": {},
	"constraint":    {},
	"permission":    {},
	"connection":    {},
	"data_quality":  {},
	"other":         {},
}

// validConfidences is the set ErrorDiagnosis.Confidence may take.
var validConfidences = map[string]struct{}{
	"high":   {},
	"medium": {},
	"low":    {},
}

// assertDiagnosisShape verifies a diagnosis satisfies the basic
// well-formedness contract: non-empty cause, at least one suggestion,
// confidence and category in the documented enum sets.
func assertDiagnosisShape(t *testing.T, d Diagnosis, ctx string) {
	t.Helper()
	if strings.TrimSpace(d.Cause) == "" {
		t.Errorf("[%s] Cause is empty", ctx)
	}
	if len(d.Suggestions) == 0 {
		t.Errorf("[%s] Suggestions is empty", ctx)
	}
	for i, s := range d.Suggestions {
		if strings.TrimSpace(s) == "" {
			t.Errorf("[%s] Suggestions[%d] is empty", ctx, i)
		}
	}
	if _, ok := validConfidences[d.Confidence]; !ok {
		t.Errorf("[%s] Confidence %q not in valid set", ctx, d.Confidence)
	}
	if _, ok := validCategories[d.Category]; !ok {
		t.Errorf("[%s] Category %q not in valid set", ctx, d.Category)
	}
}
