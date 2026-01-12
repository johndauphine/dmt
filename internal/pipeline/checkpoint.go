package pipeline

import (
	"strconv"
	"sync/atomic"

	"github.com/johndauphine/mssql-pg-migrate/internal/logging"
)

// keysetCheckpointCoordinator coordinates checkpointing for keyset pagination
// with multiple parallel readers.
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

// readerCheckpointState tracks the state of a single reader for checkpointing.
type readerCheckpointState struct {
	lastPK    any
	lastPKInt int64
	maxPKInt  int64
	maxOK     bool
	complete  bool
	nextSeq   int64
	pending   map[int64]writeAck
}

// newKeysetCheckpointCoordinator creates a new checkpoint coordinator.
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

// onAck handles a write acknowledgement.
// THREAD SAFETY: This method is called from a single goroutine via startAckProcessor,
// which reads from the ackChan sequentially. No concurrent access to state fields occurs.
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

// applyAck applies a single ack to the reader state.
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

// safeCheckpoint returns the safe checkpoint position across all readers.
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

// finalCheckpoint returns the final checkpoint value, using fallback if needed.
func (c *keysetCheckpointCoordinator) finalCheckpoint(fallback any) any {
	if c == nil {
		return fallback
	}
	if safeLastPK := c.safeCheckpoint(); safeLastPK != nil {
		return safeLastPK
	}
	return fallback
}

// parseNumericPK parses a primary key value as an int64.
func parseNumericPK(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// parseResumeRowNum parses a resume position for row number pagination.
func parseResumeRowNum(lastPK any) (int64, bool) {
	return parseNumericPK(lastPK)
}

// splitPKRange divides a PK range into n sub-ranges for parallel reading.
func splitPKRange(minPK, maxPK any, n int) []pkRange {
	if n <= 1 {
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	// Convert to int64 for range splitting
	var minVal, maxVal int64
	switch v := minPK.(type) {
	case int:
		minVal = int64(v)
	case int32:
		minVal = int64(v)
	case int64:
		minVal = v
	default:
		// Can't split non-integer PKs, use single range
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	switch v := maxPK.(type) {
	case int:
		maxVal = int64(v)
	case int32:
		maxVal = int64(v)
	case int64:
		maxVal = v
	default:
		return []pkRange{{minPK: minPK, maxPK: maxPK}}
	}

	// Calculate range size per reader
	totalRange := maxVal - minVal
	if totalRange <= 0 {
		return []pkRange{{minPK: minPK, maxPK: maxPK}}
	}

	rangeSize := totalRange / int64(n)
	if rangeSize < 1 {
		rangeSize = 1
		n = int(totalRange) // Reduce readers if range is small
	}

	ranges := make([]pkRange, 0, n)
	for i := 0; i < n; i++ {
		var rangeMin, rangeMax int64
		if i == 0 {
			rangeMin = minVal - 1 // First range: start before minVal for > comparison
		} else {
			rangeMin = minVal + int64(i)*rangeSize // Subsequent ranges: start at boundary
		}
		rangeMax = minVal + int64(i+1)*rangeSize
		if i == n-1 {
			rangeMax = maxVal // Last reader gets remainder
		}
		ranges = append(ranges, pkRange{
			minPK: rangeMin,
			maxPK: rangeMax,
		})
	}

	return ranges
}

// decrementPK returns a value that is less than the given PK value.
func decrementPK(pk any) any {
	switch v := pk.(type) {
	case int64:
		return v - 1
	case int32:
		return v - 1
	case int:
		return v - 1
	default:
		return pk
	}
}
