package transfer

import (
	"context"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/driver"
)

func TestRangeStateRoundTrip(t *testing.T) {
	states := []readerCheckpointState{
		{lastPK: int64(500), maxPK: int64(1000), lastPKInclusive: true, complete: false},
		{lastPK: "pk-abc", maxPK: "pk-zzz", complete: true},
		{lastPK: int64(7), maxPK: nil, complete: false}, // unbounded final range
	}
	encoded := encodeKeysetRangeState(states)
	if encoded == "" {
		t.Fatal("encode returned empty")
	}
	got := decodeKeysetRangeState(encoded)
	if len(got) != 3 {
		t.Fatalf("decoded %d ranges, want 3", len(got))
	}
	// Integer PKs round-trip as int64 (precision-preserving, unlike the
	// legacy last_pk column's float64 convention — codex review).
	if got[0].lastPK != int64(500) || got[0].maxPK != int64(1000) || !got[0].lastPKInclusive || got[0].complete {
		t.Errorf("range 0 = %+v", got[0])
	}
	if got[1].lastPK != "pk-abc" || got[1].maxPK != "pk-zzz" || !got[1].complete {
		t.Errorf("range 1 = %+v", got[1])
	}
	if got[2].lastPK != int64(7) || got[2].maxPK != nil || got[2].complete {
		t.Errorf("range 2 = %+v", got[2])
	}
}

func TestRangeStateRoundTripPreservesBigInt(t *testing.T) {
	const big = int64(1) << 60 // beyond float64's exact-integer range
	states := []readerCheckpointState{{lastPK: big + 1, maxPK: big + 3}}
	got := decodeKeysetRangeState(encodeKeysetRangeState(states))
	if len(got) != 1 || got[0].lastPK != big+1 || got[0].maxPK != big+3 {
		t.Fatalf("BIGINT bounds rounded: %+v", got)
	}
}

func TestDecodeRangeStateMalformedFallsBackToLegacy(t *testing.T) {
	for _, in := range []string{"", "not json", "{}", "[]"} {
		if got := decodeKeysetRangeState(in); got != nil {
			t.Errorf("decode(%q) = %v, want nil (legacy fallback)", in, got)
		}
	}
}

func TestRestoredPKRanges(t *testing.T) {
	ranges, completed := restoredPKRanges([]resumeRange{
		{lastPK: float64(50), maxPK: float64(50), complete: true},
		{lastPK: nil, maxPK: float64(100), lastPKInclusive: true},
	}, int64(1))
	if len(ranges) != 2 || len(completed) != 2 {
		t.Fatalf("got %d ranges / %d flags", len(ranges), len(completed))
	}
	if ranges[0].minPK != float64(50) || !completed[0] {
		t.Errorf("range 0 = %+v completed=%v", ranges[0], completed[0])
	}
	// nil watermark falls back to the table minimum
	if ranges[1].minPK != int64(1) || !ranges[1].minInclusive || completed[1] {
		t.Errorf("range 1 = %+v completed=%v", ranges[1], completed[1])
	}
}

// rangeStateRecordingSaver captures the most recent persisted range state.
type rangeStateRecordingSaver struct {
	mu             sync.Mutex
	lastRangeState string
}

func (s *rangeStateRecordingSaver) SaveProgress(_ int64, _ string, _ *int, _ any, _, _ int64, rangeState string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRangeState = rangeState
	return nil
}

func (s *rangeStateRecordingSaver) GetProgress(int64) (any, int64, string, error) {
	return nil, 0, "", nil
}

// TestKeysetResumeSkipsCompletedRanges is the #464 acceptance test: a
// resumed transfer with one completed and one half-done range transfers
// only the incomplete remainder, and the final persisted range state
// reports every range complete.
func TestKeysetResumeSkipsCompletedRanges(t *testing.T) {
	const totalRows = 100

	db := seedKeysetRuntimeTunerDB(t, totalRows)
	srcPool := &keysetRuntimeSourcePool{db: db}

	tuner := NewRuntimeTuner(RuntimeSnapshot{
		ChunkSize:           10,
		WriteAheadWriters:   1,
		CheckpointFrequency: 1,
	})
	tgtPool := &keysetRuntimeTargetPool{tuner: tuner, updateTo: 10}
	saver := &rangeStateRecordingSaver{}

	table := driver.Table{
		Name: "items",
		Columns: []driver.Column{
			{Name: "id", DataType: "integer"},
			{Name: "payload", DataType: "text"},
		},
		PrimaryKey:       []string{"id"},
		RowCount:         totalRows,
		EstimatedRowSize: 32,
	}
	table.PopulatePKColumns()

	cfg := &config.Config{
		Target: config.TargetConfig{Schema: ""},
		Migration: config.MigrationConfig{
			ChunkSize:           10,
			ParallelReaders:     2,
			WriteAheadWriters:   1,
			TargetMode:          "drop_recreate",
			CheckpointFrequency: 1,
		},
	}
	job := Job{Table: table, TaskID: 464, Saver: saver}

	// A previous segment finished rows 1-50 and acked through 70 in the
	// second range — only 71..100 should move on resume.
	resumeRanges := []resumeRange{
		{lastPK: float64(50), maxPK: float64(50), complete: true},
		{lastPK: float64(70), maxPK: float64(100), complete: false},
	}

	stats, err := executeKeysetPagination(
		context.Background(), srcPool, tgtPool, cfg, job,
		[]string{"id", "payload"}, []string{"id", "payload"},
		[]string{"integer", "text"}, []int{0, 0},
		nil, nil, 70, resumeRanges, "items", tuner, nil,
	)
	if err != nil {
		t.Fatalf("executeKeysetPagination: %v", err)
	}

	tgtPool.mu.Lock()
	written := append([]int(nil), tgtPool.ids...)
	tgtPool.mu.Unlock()
	if len(written) != 30 {
		t.Fatalf("wrote %d rows, want 30 (71..100); ids=%v", len(written), written)
	}
	for _, id := range written {
		if id <= 70 {
			t.Fatalf("row %d re-transferred despite per-range watermark", id)
		}
	}
	if stats.Rows != totalRows {
		t.Errorf("stats.Rows = %d, want %d (resumeRowsDone + written)", stats.Rows, totalRows)
	}

	final := decodeKeysetRangeState(saver.lastRangeState)
	if len(final) != 2 {
		t.Fatalf("final range state has %d ranges, want 2 (%q)", len(final), saver.lastRangeState)
	}
	for i, rr := range final {
		if !rr.complete {
			t.Errorf("final range %d not marked complete: %+v", i, rr)
		}
	}
}
