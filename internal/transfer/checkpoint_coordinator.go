package transfer

import (
	"github.com/johndauphine/dmt/v5/internal/logging"
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
// successors; out-of-order acks are parked until their turn. The return value
// carries the job count whose bounded ordered-ack slots are no longer pending.
// Full chunk byte reservations have already been released by the writer after
// successful ack delivery.
func (s *ackSequencer) feed(ack writeAck, apply func(writeAck)) (released ackRelease) {
	if ack.seq < s.nextSeq {
		// A defensive duplicate/stale acknowledgement cannot advance the
		// watermark, but its independently acquired ack slot must not leak.
		return ackRelease{jobs: 1}
	}
	if ack.seq != s.nextSeq {
		if s.pending == nil {
			s.pending = make(map[int64]writeAck)
		}
		if _, exists := s.pending[ack.seq]; exists {
			// Exactly-once writers should never produce this, but retaining both
			// is impossible in a seq-keyed map. Keep the first and release the
			// duplicate's ack slot.
			return ackRelease{jobs: 1}
		}
		s.pending[ack.seq] = ack
		return ackRelease{}
	}
	for {
		apply(ack)
		released.jobs++
		s.nextSeq++
		next, ok := s.pending[s.nextSeq]
		if !ok {
			return released
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
	checkpointFreq func() int

	states          []readerCheckpointState
	completedChunks int

	// ackedRows counts rows from acks applied in sequence order — exactly
	// the rows the persisted range watermarks cover. The pool's write
	// counter must NOT feed rows_done: it runs ahead of the watermark
	// (writers count a chunk before its ack is sequenced), and rows counted
	// beyond the watermark are replayed and counted again on retry,
	// inflating rows_done past the real table count (#632).
	ackedRows int64
}

// completed marks ranges already finished by a previous run segment
// (#464 resume); nil means a fresh transfer with no completed ranges. saver
// is the persistence sink for periodic checkpoints — the runner passes an
// asyncSaver so these mid-transfer saves don't stall ack processing (#620).
func newKeysetCheckpointCoordinator(saver ProgressSaver, job Job, pkRanges []pkRange, completed []bool, resumeRowsDone int64, checkpointFreq func() int) *keysetCheckpointCoordinator {
	if saver == nil || job.TaskID <= 0 {
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
		states[i].lastPKInclusive = pkr.minInclusive
		if lastPKInt, ok := parseNumericPK(pkr.minPK); ok {
			states[i].lastPKInt = lastPKInt
		}
		if maxPKInt, ok := parseNumericPK(pkr.maxPK); ok {
			states[i].maxPKInt = maxPKInt
			states[i].maxOK = true
			if !states[i].lastPKInclusive && states[i].lastPKInt >= maxPKInt {
				states[i].complete = true
			}
		}
		if completed != nil && completed[i] {
			states[i].complete = true
		}
	}

	return &keysetCheckpointCoordinator{
		saver:          saver,
		taskID:         job.TaskID,
		tableName:      job.Table.Name,
		partitionID:    partID,
		rowsTotal:      rowsTotal,
		resumeRowsDone: resumeRowsDone,
		checkpointFreq: checkpointFreq,
		states:         states,
	}
}

func (c *keysetCheckpointCoordinator) onAck(ack writeAck) ackRelease {
	if c == nil {
		return ackRelease{jobs: 1}
	}
	if ack.readerID < 0 || ack.readerID >= len(c.states) {
		return ackRelease{jobs: 1}
	}
	state := &c.states[ack.readerID]
	return state.seq.feed(ack, func(a writeAck) {
		c.applyAck(state, a)
		c.ackedRows += a.rows
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 {
			safeLastPK := c.safeCheckpoint()
			if safeLastPK != nil {
				rowsDone := c.resumeRowsDone + c.ackedRows
				if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, safeLastPK, rowsDone, c.rowsTotal, encodeKeysetRangeState(c.states)); err != nil {
					logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
				}
			}
		}
	})
}

func (c *keysetCheckpointCoordinator) applyAck(state *readerCheckpointState, ack writeAck) {
	state.lastPKInclusive = false
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
	if c.states[idx].lastPKInclusive {
		// A legacy last_pk has no inclusivity bit. Do not persist this fresh,
		// unacknowledged bound as though it were an exclusive watermark: if
		// range_state were later unavailable, resume would skip the bound row.
		// This intentionally delays periodic saves until the first range acks.
		return nil
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
	checkpointFreq func() int

	seq             ackSequencer
	completedChunks int
	lastRowNum      int64

	// ackedRows counts rows from acks applied in sequence order — the rows
	// the persisted lastRowNum watermark covers. See the keyset
	// coordinator's field for why the pool's write counter must not feed
	// rows_done (#632).
	ackedRows int64
}

func newRowNumberCheckpointCoordinator(saver ProgressSaver, job Job, partitionID *int, rowsTotal, initialRowNum, resumeRowsDone int64, checkpointFreq func() int) *rowNumberCheckpointCoordinator {
	if saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq == nil {
		checkpointFreq = func() int { return 10 }
	}
	return &rowNumberCheckpointCoordinator{
		saver:          saver,
		taskID:         job.TaskID,
		tableName:      job.Table.Name,
		partitionID:    partitionID,
		rowsTotal:      rowsTotal,
		resumeRowsDone: resumeRowsDone,
		checkpointFreq: checkpointFreq,
		lastRowNum:     initialRowNum,
	}
}

func (c *rowNumberCheckpointCoordinator) onAck(ack writeAck) ackRelease {
	if c == nil {
		return ackRelease{jobs: 1}
	}
	return c.seq.feed(ack, func(a writeAck) {
		c.lastRowNum = a.rowNum
		c.ackedRows += a.rows
		c.completedChunks++
		freq := c.checkpointFreq()
		if freq <= 0 {
			freq = 10
		}
		if c.completedChunks%freq == 0 {
			rowsDone := c.resumeRowsDone + c.ackedRows
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
