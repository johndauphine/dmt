package generic

import "testing"

// COPY batch-sizing tests ported from the hand-written postgres driver
// with its removal (#509 cleanup) — they pin the p90 row-size estimate
// and the row-count clamps on the pgx COPY strategies.

func TestPgEstimateRowBytes(t *testing.T) {
	tests := []struct {
		name string
		rows [][]any
		want int // minimum expected
	}{
		{"empty rows", nil, 64},
		{"narrow int rows", [][]any{{1, 2, 3}}, 64},       // 3*8=24, clamped to 64
		{"string rows", [][]any{{"hello world", 42}}, 64}, // 11+8=19, clamped to 64
		{"wide rows", [][]any{{string(make([]byte, 10000))}}, 10000},
		{"mixed", [][]any{
			{string(make([]byte, 500)), 1, true},
			{string(make([]byte, 300)), 2, false},
		}, 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pgEstimateRowBytes(tt.rows, 10)
			if got < tt.want {
				t.Errorf("pgEstimateRowBytes() = %d, want >= %d", got, tt.want)
			}
		})
	}
}

func TestPgCopyBatchSize(t *testing.T) {
	targetBytes := pgFallbackCopyBytes // 3 MB

	// Narrow rows (~64 bytes): 3MB/64 = 49152, under pgMaxCopyBatchRows.
	narrow := make([][]any, 100000)
	for i := range narrow {
		narrow[i] = []any{i, i + 1}
	}
	got := pgCopyBatchSize(narrow, targetBytes)
	if got < 40000 || got > 50000 {
		t.Errorf("narrow rows: pgCopyBatchSize() = %d, want in [40000, 50000]", got)
	}

	// Wide rows (~10KB each): 3MB / ~10008 bytes ≈ 314.
	wide := make([][]any, 1000)
	for i := range wide {
		wide[i] = []any{string(make([]byte, 10000)), i}
	}
	got = pgCopyBatchSize(wide, targetBytes)
	if got < 200 || got > 400 {
		t.Errorf("wide rows: pgCopyBatchSize() = %d, want in [200, 400]", got)
	}

	// Very wide rows (~100KB each): clamped to the floor — degenerate
	// single-row COPY calls are still avoided.
	veryWide := make([][]any, 10)
	for i := range veryWide {
		veryWide[i] = []any{string(make([]byte, 100000)), string(make([]byte, 2400))}
	}
	got = pgCopyBatchSize(veryWide, targetBytes)
	if got != pgMinCopyBatchRows {
		t.Errorf("very wide rows: pgCopyBatchSize() = %d, want %d", got, pgMinCopyBatchRows)
	}
}
