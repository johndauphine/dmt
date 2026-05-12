package mysql

import "testing"

func TestHardChunkLimitFromPacket(t *testing.T) {
	tests := []struct {
		name             string
		maxAllowedPacket int64
		avgRowBytes      int64
		want             int
	}{
		{
			name:             "default 4MB packet, 500-byte rows",
			maxAllowedPacket: 4 * 1024 * 1024,
			avgRowBytes:      500,
			// (4MB * 0.8) / 500 = 3355443 / 500 = 6710
			want: 6710,
		},
		{
			name:             "64MB packet, 8KB JSON rows",
			maxAllowedPacket: 64 * 1024 * 1024,
			avgRowBytes:      8 * 1024,
			// (64MB * 0.8) / 8KB = 53687091 / 8192 = 6553
			want: 6553,
		},
		{
			name:             "1GB packet, 1KB rows",
			maxAllowedPacket: 1024 * 1024 * 1024,
			avgRowBytes:      1024,
			// (1GB * 0.8) / 1KB = 858993459 / 1024 = 838860
			want: 838860,
		},
		{
			name:             "zero packet returns zero",
			maxAllowedPacket: 0,
			avgRowBytes:      500,
			want:             0,
		},
		{
			name:             "zero avgRowBytes returns zero",
			maxAllowedPacket: 4 * 1024 * 1024,
			avgRowBytes:      0,
			want:             0,
		},
		{
			name:             "negative inputs return zero",
			maxAllowedPacket: -1,
			avgRowBytes:      500,
			want:             0,
		},
		{
			// Codex review on #166: when avg row exceeds 80% of the
			// packet, integer division returns 0 — but treating that as
			// "no cap" disables packet protection precisely when needed.
			// Clamp to 1: one row per chunk fits and is the smallest
			// meaningful unit.
			name:             "row larger than packet budget — clamped to 1",
			maxAllowedPacket: 4 * 1024 * 1024,
			avgRowBytes:      3_500_000, // > 80% of 4MB
			want:             1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := HardChunkLimitFromPacket(tc.maxAllowedPacket, tc.avgRowBytes); got != tc.want {
				t.Errorf("HardChunkLimitFromPacket(%d, %d) = %d, want %d",
					tc.maxAllowedPacket, tc.avgRowBytes, got, tc.want)
			}
		})
	}
}

// TestHardChunkLimitFromPacket_RealWorldExample anchors the calculation
// against the exact #166 motivating scenario: MySQL with default
// max_allowed_packet=4MB and 8KB JSON rows would error out at
// chunk_size=50000 (50000 * 8KB = 400MB per packet, far exceeding 4MB).
// HardChunkLimitFromPacket should cap chunk_size well below 500 rows
// in that scenario.
func TestHardChunkLimitFromPacket_PreventsDefault50000Crash(t *testing.T) {
	got := HardChunkLimitFromPacket(4*1024*1024, 8*1024)
	if got > 500 {
		t.Errorf("default-MySQL + 8KB-row scenario should cap chunk_size near ~400 rows; got %d (would still crash at 50000-row default)", got)
	}
	if got <= 0 {
		t.Error("calculation should return a positive cap for valid inputs")
	}
}

func TestProbeTarget_NilDBReturnsEmpty(t *testing.T) {
	d := &Driver{}
	probe := d.ProbeTarget(nil, nil)
	if probe.MaxAllowedPacket != 0 {
		t.Errorf("nil DB should return empty probe; got MaxAllowedPacket=%d", probe.MaxAllowedPacket)
	}
}

func TestDriverDefaults_OptimumBulkChunkBytesIsUnmeasured(t *testing.T) {
	// MySQL's OptimumBulkChunkBytes stays 0 until a target-side
	// throughput sweep populates it (#166's separate measurement issue).
	d := &Driver{}
	if d.Defaults().OptimumBulkChunkBytes != 0 {
		t.Errorf("MySQL OptimumBulkChunkBytes should remain 0 (unmeasured) until a sweep populates it; got %d",
			d.Defaults().OptimumBulkChunkBytes)
	}
}
