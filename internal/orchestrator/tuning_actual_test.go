package orchestrator

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
	"github.com/johndauphine/dmt/v5/internal/logging"
)

type resizableTuningSource struct {
	driver.Reader
	max       int
	clamp     int
	requested int
	events    *[]string
}

func (p *resizableTuningSource) MaxConns() int { return p.max }
func (p *resizableTuningSource) ResizeConnectionPool(maxConns int) int {
	if p.events != nil {
		*p.events = append(*p.events, "resize-source")
	}
	p.requested = maxConns
	p.max = maxConns
	if p.clamp > 0 && p.max > p.clamp {
		p.max = p.clamp
	}
	return p.max
}

type resizableTuningTarget struct {
	driver.Writer
	max       int
	clamp     int
	requested int
	events    *[]string
}

func (p *resizableTuningTarget) MaxConns() int { return p.max }
func (p *resizableTuningTarget) ResizeConnectionPool(maxConns int) int {
	if p.events != nil {
		*p.events = append(*p.events, "resize-target")
	}
	p.requested = maxConns
	p.max = maxConns
	if p.clamp > 0 && p.max > p.clamp {
		p.max = p.clamp
	}
	return p.max
}

type fixedTuningPool struct{ max int }

func (p fixedTuningPool) MaxConns() int { return p.max }

type fixedTuningSource struct {
	driver.Reader
	max int
}

func (p fixedTuningSource) MaxConns() int { return p.max }

type fixedTuningTarget struct {
	driver.Writer
	max int
}

func (p fixedTuningTarget) MaxConns() int { return p.max }

type recordingTuningSaver struct {
	events *[]string
	actual driver.ActualParams
	rowID  int64
}

func (s *recordingTuningSaver) SaveTuningWithActualParams(actual driver.ActualParams) int64 {
	if s.events != nil {
		*s.events = append(*s.events, "save")
	}
	s.actual = actual
	return s.rowID
}

func TestApplyLiveConnectionPoolLimitsUsesAcceptedLimits(t *testing.T) {
	source := &resizableTuningSource{max: 8}
	target := &resizableTuningTarget{max: 8, clamp: 1}
	orch := &Orchestrator{
		config: &config.Config{Migration: config.MigrationConfig{
			MaxSourceConnections: 20,
			MaxTargetConnections: 16,
		}},
		sourcePool: source,
		targetPool: target,
	}

	gotSource, gotTarget := orch.applyLiveConnectionPoolLimits()
	if gotSource != 20 || gotTarget != 1 {
		t.Fatalf("live limits = source %d target %d, want 20/1", gotSource, gotTarget)
	}
	if source.requested != 20 || target.requested != 16 {
		t.Fatalf("resize requests = source %d target %d, want 20/16", source.requested, target.requested)
	}
}

func TestResizeConnectionPoolWithoutCapabilityPreservesLiveLimit(t *testing.T) {
	if got := resizeConnectionPool("source", fixedTuningPool{max: 7}, 20); got != 7 {
		t.Fatalf("non-resizable live limit = %d, want 7", got)
	}
}

func TestSaveTuningWithLivePoolsOrdersResizeAndPersistsActual(t *testing.T) {
	t.Run("engine accepted limits", func(t *testing.T) {
		var events []string
		source := &resizableTuningSource{max: 8, clamp: 12, events: &events}
		target := &resizableTuningTarget{max: 8, clamp: 1, events: &events}
		saver := &recordingTuningSaver{events: &events, rowID: 42}
		orch := &Orchestrator{
			config: &config.Config{Migration: config.MigrationConfig{
				MaxSourceConnections: 20,
				MaxTargetConnections: 16,
			}},
			sourcePool: source,
			targetPool: target,
		}

		if rowID := orch.saveTuningWithLivePools(saver, checkpoint.TuningRecord{}); rowID != 42 {
			t.Fatalf("saved row ID = %d, want 42", rowID)
		}
		wantEvents := []string{"resize-source", "resize-target", "save"}
		if len(events) != len(wantEvents) {
			t.Fatalf("events = %v, want %v", events, wantEvents)
		}
		for i := range wantEvents {
			if events[i] != wantEvents[i] {
				t.Fatalf("events = %v, want %v", events, wantEvents)
			}
		}
		if saver.actual.MaxSourceConnections != 12 || saver.actual.MaxTargetConnections != 1 {
			t.Fatalf("saved live limits = %d/%d, want 12/1",
				saver.actual.MaxSourceConnections, saver.actual.MaxTargetConnections)
		}
	})

	t.Run("non-resizable pools", func(t *testing.T) {
		var logs bytes.Buffer
		logging.SetOutput(&logs)
		defer logging.SetOutput(os.Stdout)

		var events []string
		saver := &recordingTuningSaver{events: &events, rowID: 7}
		orch := &Orchestrator{
			config: &config.Config{Migration: config.MigrationConfig{
				MaxSourceConnections: 20,
				MaxTargetConnections: 16,
			}},
			sourcePool: fixedTuningSource{max: 6},
			targetPool: fixedTuningTarget{max: 3},
		}

		if rowID := orch.saveTuningWithLivePools(saver, checkpoint.TuningRecord{}); rowID != 7 {
			t.Fatalf("saved row ID = %d, want 7", rowID)
		}
		if len(events) != 1 || events[0] != "save" {
			t.Fatalf("events = %v, want [save]", events)
		}
		if saver.actual.MaxSourceConnections != 6 || saver.actual.MaxTargetConnections != 3 {
			t.Fatalf("saved fallback limits = %d/%d, want 6/3",
				saver.actual.MaxSourceConnections, saver.actual.MaxTargetConnections)
		}
		if count := strings.Count(logs.String(), "does not support live resizing"); count != 2 {
			t.Fatalf("non-resizable warning count = %d, want one per mismatched pool; logs:\n%s", count, logs.String())
		}
	})
}

func TestActualTuningParamsCarriesLivePoolsAndRegime(t *testing.T) {
	cfg := &config.Config{Migration: config.MigrationConfig{
		Workers:           9,
		ChunkSize:         1200,
		ReadAheadBuffers:  5,
		WriteAheadWriters: 3,
		ParallelReaders:   4,
		MaxPartitions:     7,
	}}
	regime := checkpoint.TuningRecord{
		TargetSharedBuffersMB:   2048,
		TargetSyncCommit:        "on",
		TargetFsync:             "on",
		TargetFullPageWrites:    "on",
		TargetMaxWALSizeMB:      4096,
		TargetWALLevel:          "replica",
		SourceMaxServerMemoryMB: 8192,
	}

	got := actualTuningParams(cfg, 41, 23, regime)
	if got.Workers != 9 || got.ChunkSize != 1200 || got.ReadAheadBuffers != 5 ||
		got.WriteAheadWriters != 3 || got.ParallelReaders != 4 || got.MaxPartitions != 7 {
		t.Fatalf("core actual params not carried: %+v", got)
	}
	if got.MaxSourceConnections != 41 || got.MaxTargetConnections != 23 {
		t.Fatalf("live pools = %d/%d, want 41/23", got.MaxSourceConnections, got.MaxTargetConnections)
	}
	if got.TargetSharedBuffersMB != 2048 || got.TargetSyncCommit != "on" ||
		got.TargetFsync != "on" || got.TargetFullPageWrites != "on" ||
		got.TargetMaxWALSizeMB != 4096 || got.TargetWALLevel != "replica" ||
		got.SourceMaxServerMemoryMB != 8192 {
		t.Fatalf("regime actual params not carried: %+v", got)
	}
}
