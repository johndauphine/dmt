package orchestrator

import (
	"reflect"
	"testing"

	"github.com/johndauphine/dmt/internal/source"
)

func TestDateColumnCandidatesForTableUsesEffectiveColumns(t *testing.T) {
	table := source.Table{
		Name: "Users",
		Columns: []source.Column{
			{Name: "id"},
			{Name: "modified_at"},
		},
	}

	got := dateColumnCandidatesForTable(&table, []string{"updated_at", "Modified_At", "created_at"})
	want := []string{"Modified_At"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dateColumnCandidatesForTable() = %v, want %v", got, want)
	}
}

func TestDateColumnCandidatesForTableKeepsCandidatesWhenColumnsUnknown(t *testing.T) {
	table := source.Table{Name: "Users"}
	candidates := []string{"updated_at", "created_at"}

	got := dateColumnCandidatesForTable(&table, candidates)
	if !reflect.DeepEqual(got, candidates) {
		t.Fatalf("dateColumnCandidatesForTable() = %v, want %v", got, candidates)
	}
}
