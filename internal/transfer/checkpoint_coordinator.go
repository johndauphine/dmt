package transfer

import (
	"sync/atomic"

	"github.com/johndauphine/dmt/internal/logging"
)

// ackSequencer delivers write acks to apply in seq order (0, 1, 2, …),
// buffering out-of-order arrivals from the parallel writer pool. Extracted
// (#614) so the keyset coordinator (one sequencer per PK range) and the
// ROW_NUMBER coordinator (a single sequencer) share one ordering
// implementation. Not safe for concurrent use — both coordinators run on
// the single ack-processor goroutine.
type ackSequencer struct {
	nextSeq int64
	pending map[int64]writeAck
}

// feed applies ack if it is next in sequence, then drains any buffered
// successors; out-of-order acks are parked until their turn.
func (s *ackSequencer) feed(ack writeAck, apply func(writeAck)) {
	if ack.seq != s.nextSeq {
		if s.pending == nil {
			s.pending = make(map[int64]writeAck)
		}
		s.pending[ack.seq] = ack
		return
	}
	for {
		apply(ack)
		s.nextSeq++
		next, ok := s.pending[s.nextSeq]
		if !ok {
			return
		}
		delete(s.pending, s.nextSeq)
		ack = next
	}
}

type keysetCheckpointCoordinator struct {
	saver          ProgressSaver
	taskID         int64
	tableName      string
	partitionID    *int
	rowsTotal      int64
	resumeRowsDone int64
	totalWritten   *int64
	checkpointFreq func() int

	states          []readerCheckpointState
	completedChunks int
}

// completed marks ranges already finished by a previous run segment
// (#464 resume); nil means a fresh transfer with no completed ranges.
func newKeysetCheckpointCoordinator(job Job, pkRanges []pkRange, completed []bool, resumeRowsDone int64, totalWritten *int64, checkpointFreq func() int) *keysetCheckpointCoordinator {
	if job.Saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq == nil {
		checkpointFreq = func() int { return 10 }
	}

	var partID *int
	rowsTotal := job.Table.RowCount
	if job.Partition != nil {
		partID = &job.Partition.PartitionID
		rowsTotal = job.Partition.RowCount
	}

	states := make([]readerCheckpointState, len(pkRanges))
	for i, pkr := range pkRanges {
		states[i].lastPK = pkr.minPK
		states[i].maxPK = pkr.maxPK
		if lastPKInt, ok := parseNumericPK(pkr.minPK); ok {
			states[i].lastPKInt = lastPKInt
		}
		if maxPKInt, ok := parseNumericPK(pkr.maxPK); ok {
			states[i].maxPKInt = maxPKInt
			states[i].maxOK = true
			if states[i].lastPKInt >= maxPKInt {
				states[i].complete = true
			}
		}
		if completed != nil && completed[i] {
			states[i].complete = true
		}
	}

	return &keysetCheckpointCoordinator{
		saver:          job.Saver,
		taskID:         job.TaskID,
		tableName:      job.Table.Name,
		partitionID:    partID,
		rowsTotal:      rowsTotal,
		resumeRowsDone: resumeRowsDone,
		totalWritten:   totalWritten,
		checkpointFreq: checkpointFreq,
		states:         states,
	}
}

func (c *keysetCheckpointCoordinator) onAck(ack writeAck) {
	if c == nil {
		return
	}
	if ack.readerID < 0 || ack.readerID >= len(c.states) {
		return
	}
	state := &c.states[ack.readerID]
	state.seq.feed(ack, func(a writeAck) {
		c.applyAck(state, a)
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 {
			safeLastPK := c.safeCheckpoint()
			if safeLastPK != nil {
				rowsDone := c.resumeRowsDone + atomic.LoadInt64(c.totalWritten)
				if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, safeLastPK, rowsDone, c.rowsTotal, encodeKeysetRangeState(c.states)); err != nil {
					logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
				}
			}
		}
	})
}

func (c *keysetCheckpointCoordinator) applyAck(state *readerCheckpointState, ack writeAck) {
	if pkInt, ok := parseNumericPK(ack.lastPK); ok {
		state.lastPK = ack.lastPK
		state.lastPKInt = pkInt
		if state.maxOK && pkInt >= state.maxPKInt {
			state.complete = true
		}
	} else {
		state.lastPK = ack.lastPK
	}
}

func (c *keysetCheckpointCoordinator) safeCheckpoint() any {
	if c == nil || len(c.states) == 0 {
		return nil
	}
	idx := 0
	for idx < len(c.states)-1 && c.states[idx].complete {
		idx++
	}
	return c.states[idx].lastPK
}

// rangeState renders the current per-range watermarks for persistence;
// "" when the coordinator is absent (no saver configured).
func (c *keysetCheckpointCoordinator) rangeState() string {
	if c == nil {
		return ""
	}
	return encodeKeysetRangeState(c.states)
}

func (c *keysetCheckpointCoordinator) finalCheckpoint(fallback any) any {
	if c == nil {
		return fallback
	}
	if safeLastPK := c.safeCheckpoint(); safeLastPK != nil {
		return safeLastPK
	}
	return fallback
}

// rowNumberCheckpointCoordinator persists ROW_NUMBER progress from ordered
// write acks. The single-reader strategy has one sequence, so one sequencer
// and a single row-number watermark suffice (extracted from an inline ack
// closure in #614).
type rowNumberCheckpointCoordinator struct {
	saver          ProgressSaver
	taskID         int64
	tableName      string
	partitionID    *int
	rowsTotal      int64
	resumeRowsDone int64
	written        func() int64
	checkpointFreq func() int

	seq             ackSequencer
	completedChunks int
	lastRowNum      int64
}

func newRowNumberCheckpointCoordinator(job Job, partitionID *int, rowsTotal, initialRowNum, resumeRowsDone int64, written func() int64, checkpointFreq func() int) *rowNumberCheckpointCoordinator {
	if job.Saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq == nil {
		checkpointFreq = func() int { return 10 }
	}
	return &rowNumberCheckpointCoordinator{
		saver:          job.Saver,
		taskID:         job.TaskID,
		tableName:      job.Table.Name,
		partitionID:    partitionID,
		rowsTotal:      rowsTotal,
		resumeRowsDone: resumeRowsDone,
		written:        written,
		checkpointFreq: checkpointFreq,
		lastRowNum:     initialRowNum,
	}
}

func (c *rowNumberCheckpointCoordinator) onAck(ack writeAck) {
	if c == nil {
		return
	}
	c.seq.feed(ack, func(a writeAck) {
		c.lastRowNum = a.rowNum
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 {
			rowsDone := c.resumeRowsDone + c.written()
			if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, c.lastRowNum, rowsDone, c.rowsTotal, ""); err != nil {
				logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
			}
		}
	})
}

// finalRowNum returns the last acked row number, or fallback when the
// coordinator is absent (no saver configured).
func (c *rowNumberCheckpointCoordinator) finalRowNum(fallback int64) int64 {
	if c == nil {
		return fallback
	}
	return c.lastRowNum
}
