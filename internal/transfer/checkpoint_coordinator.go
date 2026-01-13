package transfer

import (
	"sync/atomic"

	"github.com/johndauphine/data-transfer-tool/internal/logging"
)

type keysetCheckpointCoordinator struct {
	saver          ProgressSaver
	taskID         int64
	tableName      string
	partitionID    *int
	rowsTotal      int64
	resumeRowsDone int64
	totalWritten   *int64
	checkpointFreq int

	states          []readerCheckpointState
	completedChunks int
}

func newKeysetCheckpointCoordinator(job Job, pkRanges []pkRange, resumeRowsDone int64, totalWritten *int64, checkpointFreq int) *keysetCheckpointCoordinator {
	if job.Saver == nil || job.TaskID <= 0 {
		return nil
	}
	if checkpointFreq <= 0 {
		checkpointFreq = 10
	}

	var partID *int
	rowsTotal := job.Table.RowCount
	if job.Partition != nil {
		partID = &job.Partition.PartitionID
		rowsTotal = job.Partition.RowCount
	}

	states := make([]readerCheckpointState, len(pkRanges))
	for i, pkr := range pkRanges {
		states[i].pending = make(map[int64]writeAck)
		states[i].lastPK = pkr.minPK
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
	if ack.seq != state.nextSeq {
		state.pending[ack.seq] = ack
		return
	}

	for {
		c.applyAck(state, ack)
		c.completedChunks++
		if c.completedChunks%c.checkpointFreq == 0 {
			safeLastPK := c.safeCheckpoint()
			if safeLastPK != nil {
				rowsDone := c.resumeRowsDone + atomic.LoadInt64(c.totalWritten)
				if err := c.saver.SaveProgress(c.taskID, c.tableName, c.partitionID, safeLastPK, rowsDone, c.rowsTotal); err != nil {
					logging.Warn("Checkpoint save failed for %s: %v", c.tableName, err)
				}
			}
		}

		state.nextSeq++
		next, ok := state.pending[state.nextSeq]
		if !ok {
			break
		}
		delete(state.pending, state.nextSeq)
		ack = next
	}
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

func (c *keysetCheckpointCoordinator) finalCheckpoint(fallback any) any {
	if c == nil {
		return fallback
	}
	if safeLastPK := c.safeCheckpoint(); safeLastPK != nil {
		return safeLastPK
	}
	return fallback
}
