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

	// MemGuard is the shared heap-pressure guard (#666). The orchestrator
	// creates one per migration so concurrent pipelines elect only one forced
	// GC leader. Nil makes direct callers retain the per-pipeline fallback.
	MemGuard *MemoryGuard

	// StrictSnapshotEpoch is an orchestrator-owned PostgreSQL or SQL Server
	// snapshot shared across every job in one transfer phase. Nil retains the
	// table-scoped strict snapshot behavior.
	StrictSnapshotEpoch *StrictSnapshotEpoch

	// AuditEvent records an operator-visible transfer event in the owning run's
	// audit log. It is optional so direct transfer callers do not need to own an
	// auditor; the orchestrator installs it on scheduled jobs (#665).
	AuditEvent func(typeName string, fields map[string]any)
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
	// rangeDone is a producer-only control record for a parallel range. seq is
	// the next sequence number after its final data chunk; it never reaches a
	// writer. The checkpoint coordinator waits for every smaller sequence ack
	// before treating the range as durably complete.
	rangeDone bool
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
	rows     int64 // chunk row count, accumulated into rows_done in ack order (#632)
}

type readerCheckpointState struct {
	lastPK          any
	maxPK           any // original range upper bound, persisted in range_state (#464)
	lastPKInclusive bool
	lastPKInt       int64
	maxPKInt        int64
	maxOK           bool
	complete        bool
	seq             ackSequencer // per-range ordered-ack delivery (#614)
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
	minPK        any  // lower bound
	maxPK        any  // inclusive (read up to <= maxPK)
	minInclusive bool // true only when the lower-bound row has not been acknowledged
}

// splitPKRange divides a PK range into n sub-ranges for parallel reading
// without inventing a predecessor sentinel for the first lower bound.
// includeMin is true for fresh work and false for an acknowledged resume
// watermark. Every later sub-range starts exclusively after the preceding
// range's inclusive maximum.
func splitPKRange(minPK, maxPK any, n int, includeMin bool) []pkRange {
	if n <= 1 {
		return []pkRange{{minPK: minPK, maxPK: maxPK, minInclusive: includeMin}}
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
		return []pkRange{{minPK: minPK, maxPK: maxPK, minInclusive: includeMin}}
	}

	switch v := maxPK.(type) {
	case int:
		maxVal = int64(v)
	case int32:
		maxVal = int64(v)
	case int64:
		maxVal = v
	default:
		return []pkRange{{minPK: minPK, maxPK: maxPK, minInclusive: includeMin}}
	}

	if maxVal <= minVal {
		return []pkRange{{minPK: minPK, maxPK: maxPK, minInclusive: includeMin}}
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
			rangeMin = minVal
		} else {
			rangeMin = addPKOffset(minVal, uint64(i)*rangeSize) // Subsequent ranges: start at boundary
		}
		rangeMax = addPKOffset(minVal, uint64(i+1)*rangeSize)
		if i == n-1 {
			rangeMax = maxVal // Last reader gets remainder
		}
		ranges = append(ranges, pkRange{
			minPK:        rangeMin,
			maxPK:        rangeMax,
			minInclusive: i == 0 && includeMin,
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
