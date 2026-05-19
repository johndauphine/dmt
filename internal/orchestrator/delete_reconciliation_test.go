package orchestrator

import (
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/source"
)

func TestPreviewDeleteReconciliationDisabled(t *testing.T) {
	state := &deletePreviewState{}
	orch := &Orchestrator{
		config: deletePreviewConfig(false),
		state:  state,
	}

	got, err := orch.previewDeleteReconciliation(nil, time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got != nil {
		t.Fatalf("previewDeleteReconciliation() = %#v, want nil", got)
	}
	if state.calls != 0 {
		t.Fatalf("state calls = %d, want 0", state.calls)
	}
}

func TestPreviewDeleteReconciliationDueWithoutPriorSuccess(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{},
	}

	got, err := orch.previewDeleteReconciliation(deletePreviewTables(), time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got == nil {
		t.Fatal("previewDeleteReconciliation() = nil, want preview")
	}
	if !got.Due {
		t.Fatal("Due = false, want true")
	}
	if got.Reason != "no previous successful reconciliation" {
		t.Fatalf("Reason = %q", got.Reason)
	}
	if got.EligibleTables != 2 || got.SkippedNoPKTables != 1 {
		t.Fatalf("eligible/skipped = %d/%d, want 2/1", got.EligibleTables, got.SkippedNoPKTables)
	}
}

func TestPreviewDeleteReconciliationUsesInterval(t *testing.T) {
	last := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		now  time.Time
		due  bool
		want string
	}{
		{
			name: "not due",
			now:  last.Add(23 * time.Hour),
			want: "interval has not elapsed",
		},
		{
			name: "due",
			now:  last.Add(24 * time.Hour),
			due:  true,
			want: "interval elapsed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orch := &Orchestrator{
				config: deletePreviewConfig(true),
				state: &deletePreviewState{
					state: &checkpoint.DeleteReconciliationState{
						LastSuccessAt: last,
					},
				},
			}

			got, err := orch.previewDeleteReconciliation(deletePreviewTables(), tt.now)
			if err != nil {
				t.Fatalf("previewDeleteReconciliation() error: %v", err)
			}
			if got.Due != tt.due {
				t.Fatalf("Due = %t, want %t", got.Due, tt.due)
			}
			if got.Reason != tt.want {
				t.Fatalf("Reason = %q, want %q", got.Reason, tt.want)
			}
			if got.LastSuccessAt == nil || !got.LastSuccessAt.Equal(last) {
				t.Fatalf("LastSuccessAt = %v, want %s", got.LastSuccessAt, last)
			}
			wantNext := last.Add(24 * time.Hour)
			if got.NextDueAt == nil || !got.NextDueAt.Equal(wantNext) {
				t.Fatalf("NextDueAt = %v, want %s", got.NextDueAt, wantNext)
			}
		})
	}
}

func TestPreviewDeleteReconciliationNoEligibleTables(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{},
	}

	got, err := orch.previewDeleteReconciliation([]source.Table{{Name: "logs"}}, time.Now())
	if err != nil {
		t.Fatalf("previewDeleteReconciliation() error: %v", err)
	}
	if got.Due {
		t.Fatal("Due = true, want false")
	}
	if got.Reason != "no eligible primary-key tables" {
		t.Fatalf("Reason = %q", got.Reason)
	}
}

func TestPreviewDeleteReconciliationPropagatesStateErrors(t *testing.T) {
	orch := &Orchestrator{
		config: deletePreviewConfig(true),
		state:  &deletePreviewState{err: errors.New("boom")},
	}

	_, err := orch.previewDeleteReconciliation(deletePreviewTables(), time.Now())
	if err == nil {
		t.Fatal("previewDeleteReconciliation() error = nil, want error")
	}
}

func deletePreviewConfig(enabled bool) *config.Config {
	cfg := &config.Config{}
	cfg.Source.Schema = "dbo"
	cfg.Target.Schema = "public"
	if enabled {
		cfg.Migration.Deletes = &config.DeleteConfig{
			Mode: config.DeleteModeReconcile,
			Reconcile: config.DeleteReconcileConfig{
				Interval: "24h",
			},
		}
	}
	return cfg
}

func deletePreviewTables() []source.Table {
	return []source.Table{
		{Name: "users", PrimaryKey: []string{"id"}},
		{Name: "orders", PrimaryKey: []string{"id"}},
		{Name: "logs"},
	}
}

type deletePreviewState struct {
	checkpoint.StateBackend
	state *checkpoint.DeleteReconciliationState
	err   error
	calls int
}

func (s *deletePreviewState) GetDeleteReconciliationState(
	string,
	string,
) (*checkpoint.DeleteReconciliationState, error) {
	s.calls++
	return s.state, s.err
}
