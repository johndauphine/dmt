package transfer

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

// memoryGuard throttles pipeline readers when heap usage exceeds a threshold.
// This prevents memory ballooning when actual row sizes exceed the static
// estimates used for pipeline buffer sizing (e.g., TEXT columns with large
// content vs. the default 256-byte estimate).
type memoryGuard struct {
	limitBytes uint64 // memory limit from config
	threshold  uint64 // pause readers when HeapAlloc exceeds this (80% of limit)
	gcActive   atomic.Bool
}

// newMemoryGuard creates a guard that pauses readers when heap usage exceeds
// 80% of the configured memory limit. Returns nil if no limit is configured.
func newMemoryGuard(effectiveMaxMemoryMB int64) *memoryGuard {
	if effectiveMaxMemoryMB <= 0 {
		return nil
	}
	limitBytes := uint64(effectiveMaxMemoryMB) * 1024 * 1024
	return &memoryGuard{
		limitBytes: limitBytes,
		threshold:  limitBytes * 80 / 100, // pause at 80% of limit
	}
}

// waitIfNeeded checks heap usage and blocks until memory drops below the
// threshold. Called by readers before producing each chunk. Returns false
// if the context was cancelled while waiting.
func (mg *memoryGuard) waitIfNeeded(ctx context.Context) bool {
	if mg == nil {
		return true
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	if ms.HeapAlloc < mg.threshold {
		return true
	}

	// Memory pressure — pause and let GC + writers catch up.
	// Only the first goroutine to enter triggers GC; others just wait and
	// recheck, avoiding repeated stop-the-world collections.
	isGCLeader := mg.gcActive.CompareAndSwap(false, true)
	if isGCLeader {
		logging.Debug("Memory pressure: HeapAlloc=%dMB threshold=%dMB, pausing readers",
			ms.HeapAlloc/(1024*1024), mg.threshold/(1024*1024))
		debug.FreeOSMemory()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if isGCLeader {
				mg.gcActive.Store(false)
			}
			return false
		case <-ticker.C:
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc < mg.threshold {
				if isGCLeader {
					mg.gcActive.Store(false)
				}
				return true
			}
			// Only the GC leader triggers collection
			if isGCLeader {
				debug.FreeOSMemory()
			}
		}
	}
}
