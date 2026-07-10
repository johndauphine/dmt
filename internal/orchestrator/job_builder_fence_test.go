package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/source"
	"github.com/johndauphine/dmt/internal/transfer"
)

// fenceSourcePool is a minimal IncrementalDateReader: it reports a date column
// and a controllable source high watermark, so a test can simulate the source
// max advancing between a run and its resume.
type fenceSourcePool struct {
	pool.SourcePool
	maxDate *time.Time
}

func (p *fenceSourcePool) DBType() string { return "sqlite" }

func (p *fenceSourcePool) GetDateColumnInfo(_ context.Context, _, _ string, _ []string) (string, string, bool) {
	return "updated_at", "datetime", true
}

func (p *fenceSourcePool) GetMaxDateColumnValue(_ context.Context, _, _, _ string) (*time.Time, error) {
	return p.maxDate, nil
}

// TestIncrementalFenceIsImmutableAcrossResume is the #647 invariant: within one
// run, the upper fence is sampled once and reused on resume — even if the
// source max date advances while the run is down — so the sync watermark can
// never move past unfinished work. A later, separate run re-samples.
func TestIncrementalFenceIsImmutableAcrossResume(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	h2 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) // source advances during the run

	table := source.Table{Schema: "dbo", Name: "orders", RowCount: 10}
	// A previous completed sync established the lower bound T0.
	if err := state.UpdateSyncTimestamp(table.Schema, table.Name, "public", t0); err != nil {
		t.Fatalf("seed sync timestamp: %v", err)
	}

	cfg := &config.Config{Migration: config.MigrationConfig{
		TargetMode:         "upsert",
		DateUpdatedColumns: []string{"updated_at"},
	}}
	cfg.Target.Schema = "public"

	src := &fenceSourcePool{maxDate: &h1}
	b := NewJobBuilder(src, state, cfg)

	// Fresh run: samples H1, persists it, and advances the watermark to H1.
	const runID = "run-1"
	if err := state.CreateRun(runID, "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	tblFresh := table
	res1 := newFenceResult()
	df1, err := b.buildDateFilter(context.Background(), runID, &tblFresh, true, true, res1)
	if err != nil {
		t.Fatal(err)
	}
	if df1 == nil || !df1.Timestamp.Equal(t0) {
		t.Fatalf("fresh build: date filter = %+v, want lower bound T0 %v", df1, t0)
	}
	if got := res1.TableSyncWatermarks[table.Name]; !got.Equal(h1) {
		t.Fatalf("fresh watermark = %v, want H1 %v", got, h1)
	}
	persisted, _ := state.GetIncrementalFence(runID, table.Schema, table.Name, "public")
	if persisted == nil || !persisted.Equal(h1) {
		t.Fatalf("persisted fence = %v, want H1 %v", persisted, h1)
	}

	// Source advances to H2 while the run is down; a resume of the SAME run must
	// reuse H1, not re-sample H2 — otherwise the watermark would advance past a
	// row updated behind the cursor and lose it permanently (#647).
	src.maxDate = &h2
	tblResume := table
	res2 := newFenceResult()
	df2, err := b.buildDateFilter(context.Background(), runID, &tblResume, true, true, res2)
	if err != nil {
		t.Fatal(err)
	}
	if df2 == nil {
		t.Fatal("resume build returned nil date filter")
	}
	if got, ok := res2.TableSyncWatermarks[table.Name]; ok && !got.Equal(h1) {
		t.Fatalf("resume watermark = %v, want H1 %v — must not advance past unfinished work", got, h1)
	}

	// A later, separate run re-samples the (now advanced) source and advances
	// the watermark to H2.
	tblNext := table
	res3 := newFenceResult()
	if err := state.CreateRun("run-2", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	df3, err := b.buildDateFilter(context.Background(), "run-2", &tblNext, true, true, res3)
	if err != nil {
		t.Fatal(err)
	}
	if df3 == nil {
		t.Fatal("next-run build returned nil date filter")
	}
	if got := res3.TableSyncWatermarks[table.Name]; !got.Equal(h2) {
		t.Fatalf("next-run watermark = %v, want freshly sampled H2 %v", got, h2)
	}
}

// TestIncrementalFenceLowerBoundIsPriorSync confirms the incremental read uses
// the prior sync as its lower bound (updated_at > T0), while the sampled fence
// H1 governs only how far the watermark advances.
func TestIncrementalFenceLowerBoundIsPriorSync(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatalf("checkpoint.New: %v", err)
	}
	defer state.Close()

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	table := source.Table{Schema: "dbo", Name: "orders", RowCount: 10}
	if err := state.UpdateSyncTimestamp(table.Schema, table.Name, "public", t0); err != nil {
		t.Fatalf("seed sync timestamp: %v", err)
	}

	cfg := &config.Config{Migration: config.MigrationConfig{
		TargetMode:         "upsert",
		DateUpdatedColumns: []string{"updated_at"},
	}}
	cfg.Target.Schema = "public"
	b := NewJobBuilder(&fenceSourcePool{maxDate: &h1}, state, cfg)

	res := newFenceResult()
	tbl := table
	if err := state.CreateRun("run-1", "dbo", "public", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	df, err := b.buildDateFilter(context.Background(), "run-1", &tbl, true, true, res)
	if err != nil {
		t.Fatal(err)
	}
	if df == nil || !df.Timestamp.Equal(t0) {
		t.Fatalf("lower bound = %v, want prior sync T0 %v", df, t0)
	}
	if got := res.TableSyncWatermarks[table.Name]; !got.Equal(h1) {
		t.Fatalf("watermark fence = %v, want H1 %v", got, h1)
	}
}

type fenceFaultState struct {
	checkpoint.StateBackend
	lastSync *time.Time
	getErr   error
	setErr   error
}

func (s *fenceFaultState) GetLastSyncTimestamp(string, string, string) (*time.Time, error) {
	return s.lastSync, nil
}

func (s *fenceFaultState) GetIncrementalFence(string, string, string, string) (*time.Time, error) {
	return nil, s.getErr
}

func (s *fenceFaultState) SetIncrementalFence(string, string, string, string, time.Time) error {
	return s.setErr
}

func TestIncrementalFenceStateFailuresFailClosed(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	table := source.Table{Schema: "dbo", Name: "orders", RowCount: 10}
	cfg := &config.Config{Migration: config.MigrationConfig{
		TargetMode:         "upsert",
		DateUpdatedColumns: []string{"updated_at"},
	}}
	cfg.Target.Schema = "public"

	for _, tc := range []struct {
		name   string
		getErr error
		setErr error
	}{
		{name: "fence read", getErr: errors.New("checkpoint locked")},
		{name: "fence write", setErr: errors.New("checkpoint disk full")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &fenceFaultState{lastSync: &t0, getErr: tc.getErr, setErr: tc.setErr}
			builder := NewJobBuilder(&fenceSourcePool{maxDate: &h1}, state, cfg)
			result := newFenceResult()
			tbl := table
			filter, err := builder.buildDateFilter(context.Background(), "run-1", &tbl, true, true, result)
			if filter != nil || !checkpoint.IsRequiredWriteError(err) {
				t.Fatalf("buildDateFilter = (%+v, %v), want fail-closed state error", filter, err)
			}
			if _, ok := result.TableSyncWatermarks[table.Name]; ok {
				t.Fatal("state failure exposed an unpersisted sync watermark")
			}
		})
	}
}

func newFenceResult() *BuildResult {
	return &BuildResult{
		TableDateFilters:    map[string]*transfer.DateFilter{},
		TableSyncWatermarks: map[string]time.Time{},
	}
}
