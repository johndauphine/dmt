package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/systemmemory"
)

type fakeMemoryReader struct {
	snapshot systemmemory.Snapshot
	err      error
	reads    int
}

func (r *fakeMemoryReader) Read() (systemmemory.Snapshot, error) {
	r.reads++
	return r.snapshot, r.err
}

func TestResolveMemoryEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   systemmemory.Snapshot
		maxMemory  int64
		want       MemoryEnvelope
		wantErrSub string
	}{
		{
			name:     "automatic budget",
			snapshot: systemmemory.Snapshot{CapacityMB: 8192, AvailableMB: 4096, Source: "host"},
			want:     MemoryEnvelope{CapacityMB: 8192, AvailableMB: 4096, BudgetMB: 2867, Source: "host"},
		},
		{
			name:      "stricter user ceiling",
			snapshot:  systemmemory.Snapshot{CapacityMB: 8192, AvailableMB: 4096, Source: "host"},
			maxMemory: 1024,
			want:      MemoryEnvelope{CapacityMB: 8192, AvailableMB: 4096, BudgetMB: 1024, Source: "host"},
		},
		{
			name:      "oversized user ceiling",
			snapshot:  systemmemory.Snapshot{CapacityMB: 8192, AvailableMB: 4096, Source: "host"},
			maxMemory: 9999,
			want:      MemoryEnvelope{CapacityMB: 8192, AvailableMB: 4096, BudgetMB: 2867, Source: "host"},
		},
		{
			name:     "small container has no one GiB floor",
			snapshot: systemmemory.Snapshot{CapacityMB: 512, AvailableMB: 512, Source: "cgroup-v2"},
			want:     MemoryEnvelope{CapacityMB: 512, AvailableMB: 512, BudgetMB: 358, Source: "cgroup-v2"},
		},
		{
			name:       "invalid capacity",
			snapshot:   systemmemory.Snapshot{CapacityMB: 0, AvailableMB: 400, Source: "host"},
			wantErrSub: "invalid effective capacity",
		},
		{
			name:       "capacity overflows byte conversion",
			snapshot:   systemmemory.Snapshot{CapacityMB: maxMemoryEnvelopeMB + 1, AvailableMB: 400, Source: "host"},
			wantErrSub: "safe byte-conversion maximum",
		},
		{
			name:       "invalid availability",
			snapshot:   systemmemory.Snapshot{CapacityMB: 512, AvailableMB: 0, Source: "host"},
			wantErrSub: "invalid effective availability",
		},
		{
			name:       "availability exceeds capacity",
			snapshot:   systemmemory.Snapshot{CapacityMB: 512, AvailableMB: 513, Source: "host"},
			wantErrSub: "exceeds effective capacity",
		},
		{
			name:       "automatic budget floors to zero",
			snapshot:   systemmemory.Snapshot{CapacityMB: 1, AvailableMB: 1, Source: "cgroup-v2"},
			wantErrSub: "automatic memory budget is not positive",
		},
		{
			name:       "negative user ceiling",
			snapshot:   systemmemory.Snapshot{CapacityMB: 512, AvailableMB: 400, Source: "host"},
			maxMemory:  -1,
			wantErrSub: "must not be negative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveMemoryEnvelope(tc.snapshot, tc.maxMemory)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("resolveMemoryEnvelope() error = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveMemoryEnvelope() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveMemoryEnvelope() = %+v, want %+v", got, tc.want)
			}
			if got.BudgetMB > got.AvailableMB {
				t.Fatalf("budget %d MB exceeds available %d MB", got.BudgetMB, got.AvailableMB)
			}
		})
	}
}

func TestApplyDefaultsMemoryEnvelopeReadFailureIsConservative(t *testing.T) {
	withEmptySecretsFile(t)
	wantErr := errors.New("finite cgroup current file unreadable")
	for _, maxMemory := range []int64{0, 128} {
		t.Run(func() string {
			if maxMemory == 0 {
				return "without user ceiling"
			}
			return "with user ceiling"
		}(), func(t *testing.T) {
			reader := &fakeMemoryReader{err: wantErr}
			cfg := &Config{
				Migration:    MigrationConfig{MaxMemoryMB: maxMemory},
				memoryReader: reader,
			}
			err := cfg.applyDefaults()
			if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
				t.Fatalf("applyDefaults() error = %v, want detector error", err)
			}
			if reader.reads != 1 {
				t.Fatalf("memory reader calls = %d, want exactly 1", reader.reads)
			}
			if cfg.autoConfig.MemoryEnvelope != (MemoryEnvelope{}) {
				t.Fatalf("failed detection published envelope %+v", cfg.autoConfig.MemoryEnvelope)
			}
		})
	}
}

func TestApplyDefaultsPublishesEnvelopeAndUsesLegacyBuffers(t *testing.T) {
	withEmptySecretsFile(t)
	reader := &fakeMemoryReader{snapshot: systemmemory.Snapshot{
		CapacityMB:  8192,
		AvailableMB: 8192,
		Source:      "cgroup-v2",
	}}
	cfg := &Config{
		Migration: MigrationConfig{
			Workers:     4,
			ChunkSize:   50_000,
			MaxMemoryMB: 4096,
		},
		memoryReader: reader,
	}

	if err := cfg.applyDefaults(); err != nil {
		t.Fatalf("applyDefaults() error = %v", err)
	}
	want := MemoryEnvelope{CapacityMB: 8192, AvailableMB: 8192, BudgetMB: 4096, Source: "cgroup-v2"}
	if cfg.autoConfig.MemoryEnvelope != want {
		t.Fatalf("MemoryEnvelope = %+v, want %+v", cfg.autoConfig.MemoryEnvelope, want)
	}
	if cfg.autoConfig.AvailableMemoryMB != want.AvailableMB || cfg.autoConfig.EffectiveMaxMemoryMB != want.BudgetMB {
		t.Fatalf("compatibility projections = available %d/effective %d, want %d/%d",
			cfg.autoConfig.AvailableMemoryMB, cfg.autoConfig.EffectiveMaxMemoryMB, want.AvailableMB, want.BudgetMB)
	}
	if reader.reads != 1 {
		t.Fatalf("memory reader calls = %d, want exactly 1", reader.reads)
	}
	// The restored legacy formula uses half of the exact available/user-capped
	// memory input, while the retained envelope remains the final hard clamp.
	if cfg.Migration.ReadAheadBuffers != 21 {
		t.Fatalf("ReadAheadBuffers = %d, want legacy target-memory value 21", cfg.Migration.ReadAheadBuffers)
	}

	dump := cfg.DebugDump()
	for _, wantLine := range []string{
		"Memory Capacity: 8192 MB",
		"Memory Available: 8192 MB",
		"Memory Budget: 4096 MB",
		"Memory Source: cgroup-v2",
		"Max Memory Limit: 4096 MB (user ceiling)",
	} {
		if !strings.Contains(dump, wantLine) {
			t.Errorf("DebugDump() missing %q:\n%s", wantLine, dump)
		}
	}
}
