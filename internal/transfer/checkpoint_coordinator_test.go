package transfer

import (
	"reflect"
	"sync"
	"testing"

	"github.com/johndauphine/data-transfer-tool/internal/source"
)

type fakeSaver struct {
	mu      sync.Mutex
	lastPKs []any
}

func (f *fakeSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastPKs = append(f.lastPKs, lastPK)
	return nil
}

func (f *fakeSaver) GetProgress(taskID int64) (any, int64, error) {
	return nil, 0, nil
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
	var totalWritten int64

	coord := newKeysetCheckpointCoordinator(job, pkRanges, 0, &totalWritten, 1)
	if coord == nil {
		t.Fatal("expected checkpoint coordinator")
	}

	// Reader 1 finishes first; checkpoint should remain at reader 0's last PK.
	coord.onAck(writeAck{readerID: 1, seq: 0, lastPK: int64(150)})
	// Reader 0 sends seq 1 before seq 0; should be buffered.
	coord.onAck(writeAck{readerID: 0, seq: 1, lastPK: int64(60)})
	// Reader 0 sends seq 0; should process seq 0 then seq 1.
	coord.onAck(writeAck{readerID: 0, seq: 0, lastPK: int64(40)})

	saver.mu.Lock()
	defer saver.mu.Unlock()

	got := make([]int64, 0, len(saver.lastPKs))
	for _, pk := range saver.lastPKs {
		value, ok := parseNumericPK(pk)
		if !ok {
			t.Fatalf("expected numeric checkpoint, got %T", pk)
		}
		got = append(got, value)
	}

	want := []int64{0, 40, 60}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoints = %v, want %v", got, want)
	}
}
