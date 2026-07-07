package transfer

import (
	"reflect"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/source"
)

// savedProgress captures one SaveProgress call for assertions.
type savedProgress struct {
	lastPK     any
	rowsDone   int64
	rowsTotal  int64
	rangeState string
}

type fakeSaver struct {
	mu    sync.Mutex
	saves []savedProgress
}

func (f *fakeSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves = append(f.saves, savedProgress{lastPK: lastPK, rowsDone: rowsDone, rowsTotal: rowsTotal, rangeState: rangeState})
	return nil
}

func (f *fakeSaver) GetProgress(taskID int64) (any, int64, string, error) {
	return nil, 0, "", nil
}

func (f *fakeSaver) recorded() []savedProgress {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]savedProgress(nil), f.saves...)
}

func TestKeysetCheckpointCoordinatorOutOfOrderAcks(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{
		Table:  source.Table{Name: "Things", RowCount: 200},
		TaskID: 1,
		Saver:  saver,
	}
	pkRanges := []pkRange{
		{minPK: int64(0), maxPK: int64(99)},
		{minPK: int64(100), maxPK: int64(199)},
	}

	coord := newKeysetCheckpointCoordinator(job.Saver, job, pkRanges, nil, 0, func() int { return 1 })
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// Reader 1 finishes first; checkpoint should remain at reader 0's last PK.
	coord.onAck(writeAck{readerID: 1, seq: 0, lastPK: int64(150)})
	// Reader 0 sends seq 1 before seq 0; should be buffered.
	coord.onAck(writeAck{readerID: 0, seq: 1, lastPK: int64(60)})
	// Reader 0 sends seq 0; should process seq 0 then seq 1.
	coord.onAck(writeAck{readerID: 0, seq: 0, lastPK: int64(40)})

	got := make([]int64, 0)
	for _, save := range saver.recorded() {
		value, ok := parseNumericPK(save.lastPK)
		if !ok {
			t.Fatalf("expected numeric checkpoint, got %T", save.lastPK)
		}
		got = append(got, value)
	}

	want := []int64{0, 40, 60}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want %v", got, want)
	}
}

// Regression test for #632: persisted rows_done must count only rows whose
// acks were applied in sequence order — exactly what the simultaneously
// persisted watermarks cover. The old accounting used the pool's write
// counter, which includes chunks written but not yet sequenced (rows beyond
// the watermark); a retry replays those rows and counts them again,
// inflating rows_done past the real table count.
func TestKeysetCheckpointRowsDoneExcludesUnsequencedAcks(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{
		Table:  source.Table{Name: "Posts", RowCount: 200},
		TaskID: 1,
		Saver:  saver,
	}
	pkRanges := []pkRange{
		{minPK: int64(0), maxPK: int64(99)},
		{minPK: int64(100), maxPK: int64(199)},
	}

	coord := newKeysetCheckpointCoordinator(job.Saver, job, pkRanges, nil, 0, func() int { return 1 })
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// Range 1 chunk applied in order: its 5 rows are checkpointed.
	coord.onAck(writeAck{readerID: 1, seq: 0, lastPK: int64(150), rows: 5})
	// Range 0 seq 1 arrives before seq 0: written to the target, but beyond
	// the watermark — it must contribute nothing until sequenced.
	coord.onAck(writeAck{readerID: 0, seq: 1, lastPK: int64(60), rows: 10})
	// Another in-order range 1 chunk: rows_done grows by its 5 rows only,
	// not by the 10 buffered rows (the old counter reported 20 here).
	coord.onAck(writeAck{readerID: 1, seq: 1, lastPK: int64(199), rows: 5})
	// Range 0 seq 0 arrives: seq 0 applies, then the buffered seq 1 drains.
	coord.onAck(writeAck{readerID: 0, seq: 0, lastPK: int64(40), rows: 2})

	var got []int64
	for _, save := range saver.recorded() {
		got = append(got, save.rowsDone)
	}
	want := []int64{5, 10, 12, 22}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows_done sequence = %v, want %v (buffered out-of-order acks must not count)", got, want)
	}
}

// Same #632 invariant for the ROW_NUMBER coordinator, including the resume
// base: rows_done = resumeRowsDone + in-order acked rows.
func TestRowNumberCheckpointRowsDoneExcludesUnsequencedAcks(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{
		Table:  source.Table{Name: "Orders", RowCount: 10},
		TaskID: 1,
		Saver:  saver,
	}

	coord := newRowNumberCheckpointCoordinator(job.Saver, job, nil, 10, 3, 3, func() int { return 1 })
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// seq 1 written first: buffered, no save, no rows counted.
	coord.onAck(writeAck{seq: 1, rowNum: 7, rows: 2})
	if len(saver.recorded()) != 0 {
		t.Fatalf("buffered out-of-order ack must not checkpoint, got %d saves", len(saver.recorded()))
	}
	// seq 0 arrives: applies seq 0, then drains the buffered seq 1.
	coord.onAck(writeAck{seq: 0, rowNum: 5, rows: 2})

	saves := saver.recorded()
	if len(saves) != 2 {
		t.Fatalf("expected 2 saves, got %d", len(saves))
	}
	if saves[0].rowsDone != 5 || saves[0].lastPK != int64(5) {
		t.Fatalf("first save = (rowNum=%v rowsDone=%d), want (5, 5)", saves[0].lastPK, saves[0].rowsDone)
	}
	if saves[1].rowsDone != 7 || saves[1].lastPK != int64(7) {
		t.Fatalf("second save = (rowNum=%v rowsDone=%d), want (7, 7)", saves[1].lastPK, saves[1].rowsDone)
	}
}

// Same #632 invariant for the composite (tuple) keyset coordinator.
func TestCompositeCheckpointRowsDoneExcludesUnsequencedAcks(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{
		Table:  source.Table{Name: "Ledger", RowCount: 10},
		TaskID: 1,
		Saver:  saver,
	}

	coord := newCompositeCheckpointCoordinator(job.Saver, job, nil, 10, 0, func() int { return 1 })
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// seq 1 written first: buffered, no save, no rows counted.
	coord.onAck(writeAck{seq: 1, lastPK: []any{"b", int64(2)}, rows: 4})
	if len(saver.recorded()) != 0 {
		t.Fatalf("buffered out-of-order ack must not checkpoint, got %d saves", len(saver.recorded()))
	}
	// seq 0 arrives: applies seq 0, then drains the buffered seq 1.
	coord.onAck(writeAck{seq: 0, lastPK: []any{"a", int64(9)}, rows: 3})

	saves := saver.recorded()
	if len(saves) != 2 {
		t.Fatalf("expected 2 saves, got %d", len(saves))
	}
	if saves[0].rowsDone != 3 {
		t.Fatalf("first save rowsDone = %d, want 3", saves[0].rowsDone)
	}
	if saves[1].rowsDone != 7 {
		t.Fatalf("second save rowsDone = %d, want 7", saves[1].rowsDone)
	}
}
