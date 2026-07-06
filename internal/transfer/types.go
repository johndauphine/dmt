package transfer

import (
	"context"
	"fmt"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/source"
	"time"
)

// sendChunkOrCancel pushes r to ch unless ctx is cancelled first. It exists
// because reader goroutines must not block forever on a bare
// `chunkChan <- r` when the consumer side stops draining — the original
// (#250) pre-fix code did exactly that, leaking reader goroutines and
// holding source DB cursors after writer failure. Returns true if the
// chunk landed in the channel, false if ctx was cancelled and the reader
// should stop producing.
func sendChunkOrCancel(ctx context.Context, ch chan<- chunkResult, r chunkResult) bool {
	select {
	case ch <- r:
		return true
	case <-ctx.Done():
		return false
	}
}

// ProgressSaver is an interface for saving transfer progress
type ProgressSaver interface {
	// rangeState carries the keyset per-range watermarks as opaque JSON
	// (#464); "" for ROW_NUMBER transfers and legacy rows.
	SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error
	GetProgress(taskID int64) (lastPK any, rowsDone int64, rangeState string, err error)
}

// DateFilter is an alias for driver.DateFilter for backward compatibility
type DateFilter = driver.DateFilter

// Job represents a data transfer job
type Job struct {
	Table      source.Table
	Partition  *source.Partition
	TaskID     int64         // For chunk-level resume
	Saver      ProgressSaver // For saving progress (nil to disable)
	DateFilter *DateFilter   // Optional date filter for incremental sync (upsert mode)

	// IsResume is true when this job is dispatched as part of a Resume()
	// run rather than a fresh Run(). It exists because per-partition
	// checkpoint state alone cannot tell a writer "you might be replaying
	// already-committed rows": a partition may have committed chunks to
	// the target but crashed before the first checkpoint flush, leaving
	// resumeLastPK == nil. ROW_NUMBER pagination uses this flag to enable
	// the idempotent-on-dup writer path (#227 codex follow-up) for ALL
	// partitions of the table on resume, not just those with saved progress.
	IsResume bool

	// ReplayPossible is true when this execution may replay rows already
	// committed by an earlier in-process attempt. Fresh first attempts keep
	// plain INSERT behavior; retries can opt into ROW_NUMBER duplicate-safe
	// writes without pretending to be a cross-process resume.
	ReplayPossible bool

	// MemBudget is the shared in-flight byte budget (#617). The orchestrator
	// creates one per migration and sets the same pointer on every table
	// job, so concurrent pipelines divide it by contention. Nil disables
	// byte-based admission control (channel-depth limiting only).
	MemBudget *MemBudget
}

// chunkResult holds a chunk of data for the read-ahead pipeline
type chunkResult struct {
	rows      [][]any
	lastPK    any
	rowNum    int64 // for ROW_NUMBER pagination progress tracking
	readerID  int
	seq       int64
	bytes     int64 // scanned Go heap size, reserved against MemBudget (#617)
	queryTime time.Duration
	scanTime  time.Duration
	readEnd   time.Time // when this chunk finished reading
	err       error
	done      bool // signals end of data
}

type writeJob struct {
	rows     [][]any
	lastPK   any
	rowNum   int64
	readerID int
	seq      int64
	bytes    int64 // MemBudget bytes to release once this chunk is written (#617)
}

type writeAck struct {
	readerID int
	seq      int64
	lastPK   any
	rowNum   int64
}

type readerCheckpointState struct {
	lastPK    any
	maxPK     any // original range upper bound, persisted in range_state (#464)
	lastPKInt int64
	maxPKInt  int64
	maxOK     bool
	complete  bool
	seq       ackSequencer // per-range ordered-ack delivery (#614)
}

// writeResult holds the result of a parallel write operation
type writeResult struct {
	writeTime time.Duration
	rowCount  int64
	err       error
}

// TransferStats tracks timing statistics for profiling
type TransferStats struct {
	QueryTime time.Duration
	ScanTime  time.Duration
	WriteTime time.Duration
	Rows      int64
}

// pkRange represents a primary key range for parallel reading
type pkRange struct {
	minPK any // inclusive (start from > minPK)
	maxPK any // inclusive (read up to <= maxPK)
}

// splitPKRange divides a PK range into n sub-ranges for parallel reading
// Note: minPK should be the actual minimum PK value; this function handles
// the decrement needed for the > comparison in WHERE clauses
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
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	if maxVal <= minVal {
		return []pkRange{{minPK: decrementPK(minPK), maxPK: maxPK}}
	}

	totalRange := pkRangeDistance(minVal, maxVal)
	rangeSize := totalRange / uint64(n)
	if rangeSize < 1 {
		rangeSize = 1
		n = int(totalRange) // Reduce readers if range is small
	}

	ranges := make([]pkRange, 0, n)
	for i := 0; i < n; i++ {
		var rangeMin, rangeMax int64
		if i == 0 {
			rangeMin = decrementInt64PK(minVal) // First range: start before minVal for > comparison
		} else {
			rangeMin = addPKOffset(minVal, uint64(i)*rangeSize) // Subsequent ranges: start at boundary
		}
		rangeMax = addPKOffset(minVal, uint64(i+1)*rangeSize)
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

func pkRangeDistance(minVal, maxVal int64) uint64 {
	return uint64(maxVal) - uint64(minVal)
}

func addPKOffset(minVal int64, offset uint64) int64 {
	return int64(uint64(minVal) + offset)
}

func decrementInt64PK(pk int64) int64 {
	if pk == minInt64 {
		return pk
	}
	return pk - 1
}

func (s *TransferStats) String() string {
	total := s.QueryTime + s.ScanTime + s.WriteTime
	if total == 0 {
		return "no data"
	}
	return fmt.Sprintf("query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%), rows=%d",
		s.QueryTime.Seconds(), float64(s.QueryTime)/float64(total)*100,
		s.ScanTime.Seconds(), float64(s.ScanTime)/float64(total)*100,
		s.WriteTime.Seconds(), float64(s.WriteTime)/float64(total)*100,
		s.Rows)
}

// decrementPK returns a value less than the given PK value when one is representable.
func decrementPK(pk any) any {
	switch v := pk.(type) {
	case int64:
		return decrementInt64PK(v)
	case int32:
		if v == minInt32 {
			return v
		}
		return v - 1
	case int:
		if v == minInt() {
			return v
		}
		return v - 1
	default:
		return pk
	}
}

const (
	minInt64 = -1 << 63
	minInt32 = -1 << 31
)

func minInt() int {
	return -int(^uint(0)>>1) - 1
}
