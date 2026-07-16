package transfer

import (
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/internal/source"
)

func TestAckSequencerReleasesSlotsOnlyAfterSequenceGapCloses(t *testing.T) {
	var seq ackSequencer
	var applied []int64

	if got := seq.feed(writeAck{seq: 1}, func(ack writeAck) {
		applied = append(applied, ack.seq)
	}); got != (ackRelease{}) {
		t.Fatalf("out-of-order release = %+v, want zero", got)
	}
	if got := seq.feed(writeAck{seq: 2}, func(ack writeAck) {
		applied = append(applied, ack.seq)
	}); got != (ackRelease{}) {
		t.Fatalf("second out-of-order release = %+v, want zero", got)
	}

	got := seq.feed(writeAck{seq: 0}, func(ack writeAck) {
		applied = append(applied, ack.seq)
	})
	if got != (ackRelease{jobs: 3}) {
		t.Fatalf("gap-closing release = %+v, want 3 jobs", got)
	}
	if !reflect.DeepEqual(applied, []int64{0, 1, 2}) {
		t.Fatalf("apply order = %v, want [0 1 2]", applied)
	}
}

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

func TestParseNumericPKRequiresExactInt64(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  int64
		ok    bool
	}{
		{name: "mysql aggregate bytes", value: []byte("9223372036854775807"), want: 9223372036854775807, ok: true},
		{name: "integer float", value: float64(42), want: 42, ok: true},
		{name: "fractional float", value: 42.5},
		{name: "out of range float", value: math.Exp2(63)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseNumericPK(tt.value)
			if ok != tt.ok || ok && got != tt.want {
				t.Fatalf("parseNumericPK(%#v) = (%d, %v), want (%d, %v)", tt.value, got, ok, tt.want, tt.ok)
			}
		})
	}
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

func TestKeysetCheckpointCoordinatorDoesNotPersistUntransferredInclusiveBound(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{Table: source.Table{Name: "Things", RowCount: 2}, TaskID: 1, Saver: saver}
	coord := newKeysetCheckpointCoordinator(job.Saver, job, []pkRange{{
		minPK:        int64(-10),
		maxPK:        int64(-9),
		minInclusive: true,
	}}, nil, 0, func() int { return 1 })

	if got := coord.safeCheckpoint(); got != nil {
		t.Fatalf("safe checkpoint before first ack = %v, want nil", got)
	}
	coord.onAck(writeAck{readerID: 0, seq: 0, lastPK: int64(-10), rows: 1})
	saves := saver.recorded()
	if len(saves) != 1 || saves[0].lastPK != int64(-10) {
		t.Fatalf("saves after first ack = %+v, want exclusive checkpoint -10", saves)
	}
	ranges := decodeKeysetRangeState(saves[0].rangeState)
	if len(ranges) != 1 || ranges[0].lastPKInclusive {
		t.Fatalf("saved range state = %+v, want exclusive acknowledged watermark", ranges)
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

// Parallel tuple ranges are acknowledged out of source order. The legacy
// last_pk fallback must therefore stay at the first incomplete range, while
// the richer envelope can mark lower ranges complete as their final marker and
// last write ack meet. This lets an older binary replay safely instead of
// skipping an unfinished lower range (#667).
func TestCompositeRangeCheckpointCoordinatorAdvancesSafeLegacyTuple(t *testing.T) {
	saver := &fakeSaver{}
	job := Job{Table: source.Table{Name: "Ledger", RowCount: 30}, TaskID: 667, Saver: saver}
	ranges := []compositeResumeRange{
		{min: 1, max: 10, minInclusive: true},
		{min: 10, max: 20},
		{min: 20, max: 30},
	}
	coord := newCompositeRangeCheckpointCoordinator(saver, job, ranges, nil, 30, 0, func() int { return 1 })
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// The highest range has an acknowledged watermark, but it remains
	// unfinished. Its marker never arrives in this simulated crash window.
	coord.onAck(writeAck{readerID: 2, seq: 0, lastPK: []any{int64(25), int64(1)}, rows: 1})

	// The middle range's marker reaches the consumer before its asynchronous
	// write ack. It must not be marked complete until that ack is sequenced.
	coord.markRangeDone(1, 1)
	if coord.states[1].complete {
		t.Fatal("range completed before its final write ack")
	}
	coord.onAck(writeAck{readerID: 1, seq: 0, lastPK: []any{int64(15), int64(1)}, rows: 1})
	if !coord.states[1].complete {
		t.Fatal("range did not complete after final marker and ack")
	}

	// The lower range becomes the safe legacy frontier first.
	coord.onAck(writeAck{readerID: 0, seq: 0, lastPK: []any{int64(5), int64(1)}, rows: 1})
	coord.markRangeDone(0, 1)

	saves := saver.recorded()
	if len(saves) == 0 {
		t.Fatal("expected periodic checkpoint")
	}
	last := saves[len(saves)-1]
	legacy, ok := last.lastPK.([]any)
	if !ok || len(legacy) != 2 || legacy[0] != int64(25) {
		t.Fatalf("legacy last_pk = %#v, want lower-complete safe frontier [25 1]", last.lastPK)
	}
	state := decodeCompositeRangeState(last.rangeState)
	if len(state) != 3 || !state[0].complete || !state[1].complete || state[2].complete {
		t.Fatalf("range state = %#v, want first two complete only", state)
	}
}
