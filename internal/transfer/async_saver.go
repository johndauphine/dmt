package transfer

import (
	"fmt"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
)

// progressSnapshot is one complete SaveProgress call captured for async
// persistence.
type progressSnapshot struct {
	taskID      int64
	tableName   string
	partitionID *int
	lastPK      any
	rowsDone    int64
	rowsTotal   int64
	rangeState  string
}

// asyncSaverFailThreshold is the number of consecutive failed saves after
// which close() surfaces the saver as unhealthy. A slow saver stays silent;
// a persistently broken one (disk full, permission loss) is reported so the
// run doesn't quietly lose all resumability.
const asyncSaverFailThreshold = 5

// asyncSaver decouples periodic checkpoint persistence from the
// ack-processing goroutine (#620). Checkpoints are monotonic watermarks, so
// only the newest pending snapshot matters: a single-slot mailbox coalesces
// bursts, and a slow SaveProgress (SQLite fsync, YAML-file rewrite) no
// longer stalls ack draining — which, through the bounded ackChan, would
// otherwise backpressure the writers themselves.
//
// It implements ProgressSaver, so a coordinator that "calls SaveProgress"
// gets async behavior transparently. The final end-of-transfer checkpoint
// is written through the raw saver instead (synchronous + durable), after
// close() has flushed and joined this one.
type asyncSaver struct {
	inner   ProgressSaver
	mailbox chan progressSnapshot
	done    chan struct{}

	mu          sync.Mutex
	lastErr     error
	consecFails int
	closed      bool
}

func newAsyncSaver(inner ProgressSaver) *asyncSaver {
	return &asyncSaver{
		inner:   inner,
		mailbox: make(chan progressSnapshot, 1),
		done:    make(chan struct{}),
	}
}

// start launches the single background persistence goroutine.
func (s *asyncSaver) start() {
	go func() {
		defer close(s.done)
		for snap := range s.mailbox {
			s.persist(snap)
		}
	}()
}

func (s *asyncSaver) persist(snap progressSnapshot) {
	err := s.inner.SaveProgress(snap.taskID, snap.tableName, snap.partitionID, snap.lastPK, snap.rowsDone, snap.rowsTotal, snap.rangeState)
	s.mu.Lock()
	if err != nil {
		s.consecFails++
		s.lastErr = err
		logging.Warn("Checkpoint save failed for %s: %v", snap.tableName, err)
	} else {
		s.consecFails = 0
		s.lastErr = nil
	}
	s.mu.Unlock()
}

// SaveProgress posts to the single-slot mailbox, replacing any not-yet-
// persisted snapshot (latest wins — checkpoints are monotonic). It never
// blocks the caller and always reports success; real persistence errors are
// tracked internally and surfaced by close(). There is a single caller (the
// ack-processing goroutine), so the drain-and-retry loop makes at most two
// iterations even with the persistence goroutine consuming concurrently.
func (s *asyncSaver) SaveProgress(taskID int64, tableName string, partitionID *int, lastPK any, rowsDone, rowsTotal int64, rangeState string) error {
	snap := progressSnapshot{
		taskID:      taskID,
		tableName:   tableName,
		partitionID: partitionID,
		lastPK:      lastPK,
		rowsDone:    rowsDone,
		rowsTotal:   rowsTotal,
		rangeState:  rangeState,
	}
	for {
		select {
		case s.mailbox <- snap:
			return nil
		default:
			// Mailbox full — discard the stale pending snapshot and retry.
			select {
			case <-s.mailbox:
			default:
			}
		}
	}
}

func (s *asyncSaver) GetProgress(taskID int64) (any, int64, string, error) {
	return s.inner.GetProgress(taskID)
}

// close flushes the mailbox goroutine and joins it, so no async write can
// land after the caller's subsequent synchronous final save. It returns an
// error only when the saver failed on many consecutive attempts. Safe to
// call once; subsequent calls are no-ops.
func (s *asyncSaver) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	close(s.mailbox)
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consecFails >= asyncSaverFailThreshold {
		return fmt.Errorf("checkpoint saver failed %d consecutive times: %w", s.consecFails, s.lastErr)
	}
	return nil
}
