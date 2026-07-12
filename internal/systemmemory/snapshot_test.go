package systemmemory

import (
	"math"
	"testing"
)

func TestSnapshotEffectivePressure(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   Snapshot
		want       float64
		wantSource string
		wantOK     bool
	}{
		{
			name:       "host only",
			snapshot:   Snapshot{HostCapacityMB: 16_000, HostAvailableMB: 4_000, Source: "host"},
			want:       75,
			wantSource: "host",
			wantOK:     true,
		},
		{
			name: "cgroup is more constrained",
			snapshot: Snapshot{
				HostCapacityMB: 16_000, HostAvailableMB: 12_000,
				CgroupLimitMB: 8_000, CgroupCurrentMB: 6_000,
				Source: "cgroup-v2",
			},
			want:       75,
			wantSource: "cgroup-v2",
			wantOK:     true,
		},
		{
			name: "host is more constrained",
			snapshot: Snapshot{
				HostCapacityMB: 16_000, HostAvailableMB: 4_000,
				CgroupLimitMB: 8_000, CgroupCurrentMB: 2_000,
				Source: "cgroup-v1",
			},
			want:       75,
			wantSource: "host",
			wantOK:     true,
		},
		{
			name: "current over limit clamps to 100",
			snapshot: Snapshot{
				CgroupLimitMB: 512, CgroupCurrentMB: 600,
				Source: "cgroup-v2",
			},
			want:       100,
			wantSource: "cgroup-v2",
			wantOK:     true,
		},
		{name: "unknown", snapshot: Snapshot{}, wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, source, ok := test.snapshot.EffectivePressure()
			if ok != test.wantOK {
				t.Fatalf("ok = %v, want %v", ok, test.wantOK)
			}
			if !test.wantOK {
				return
			}
			if math.Abs(got-test.want) > 0.001 {
				t.Errorf("pressure = %.3f, want %.3f", got, test.want)
			}
			if source != test.wantSource {
				t.Errorf("source = %q, want %q", source, test.wantSource)
			}
		})
	}
}

func TestSnapshotEffectivePressureDoesNotCombineEffectiveMinima(t *testing.T) {
	// The host is 75% used and the cgroup is 25% used. Independently taking
	// effective capacity=min(16GB, 8GB) and available=min(4GB, 6GB) would
	// manufacture 50%, which describes neither domain.
	snapshot := Snapshot{
		CapacityMB: 8_000, AvailableMB: 4_000,
		HostCapacityMB: 16_000, HostAvailableMB: 4_000,
		CgroupLimitMB: 8_000, CgroupCurrentMB: 2_000,
		Source: "cgroup-v2",
	}

	got, source, ok := snapshot.EffectivePressure()
	if !ok {
		t.Fatal("EffectivePressure returned ok=false")
	}
	if got != 75 || source != "host" {
		t.Fatalf("EffectivePressure = (%.1f, %q), want (75.0, host)", got, source)
	}
}
