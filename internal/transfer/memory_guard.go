package transfer

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johndauphine/dmt/v5/internal/logging"
)

// MemoryGuard throttles readers when the process heap crosses its configured
// threshold. One guard is shared by all jobs in a migration so its GC-leader
// election covers the whole concurrent transfer set (#666). With GOMEMLIMIT
// set from the same budget (#462), it is the second line of defense when live
// data exceeds the static estimates used for pipeline buffer sizing.
type MemoryGuard struct {
	limitBytes uint64 // memory limit from config
	threshold  uint64 // pause readers when HeapAlloc exceeds this (80% of limit)
	gcActive   atomic.Bool
}

// freeOSMemory and forceGC are indirections so the leader election can be
// tested without forcing expensive collections in the test process.
var (
	freeOSMemory = debug.FreeOSMemory
	forceGC      = runtime.GC
)

// Heap sampling: one process-wide goroutine publishes HeapAlloc so readers
// can check memory pressure with an atomic load instead of each calling
// runtime.ReadMemStats (a brief stop-the-world) before every chunk (#462).
// The sampler lives for the remainder of the process once started — fine
// for a CLI, and it avoids per-transfer lifecycle plumbing.
var (
	heapSamplerOnce  sync.Once
	sampledHeapAlloc atomic.Uint64
)

// heapSampleInterval balances staleness against ReadMemStats cost. A stale
// low sample can't stall readers (waitIfNeeded confirms with a fresh read
// before any pause decision); a stale high sample only costs one confirm
// read on the next chunk.
const heapSampleInterval = 250 * time.Millisecond

func startHeapSampler() {
	heapSamplerOnce.Do(func() {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		sampledHeapAlloc.Store(ms.HeapAlloc)
		go func() {
			ticker := time.NewTicker(heapSampleInterval)
			defer ticker.Stop()
			var ms runtime.MemStats
			for range ticker.C {
				runtime.ReadMemStats(&ms)
				sampledHeapAlloc.Store(ms.HeapAlloc)
			}
		}()
	})
}

// NewMemoryGuard creates a guard that pauses readers when heap usage exceeds
// 80% of the configured memory limit. It returns nil if no limit is
// configured, allowing callers with no memory cap to skip this backstop.
func NewMemoryGuard(memoryBudgetMB int64) *MemoryGuard {
	if memoryBudgetMB <= 0 {
		return nil
	}
	limitBytes := uint64(memoryBudgetMB) * 1024 * 1024
	return &MemoryGuard{
		limitBytes: limitBytes,
		threshold:  limitBytes * 80 / 100, // pause at 80% of limit
	}
}

// waitIfNeeded checks heap usage and blocks until memory drops below the
// threshold. Called by readers before producing each chunk. Returns false
// if the context was cancelled while waiting.
//
// The fast path is a single atomic load of the sampled heap value. Only
// when the sample is above threshold does the reader pay for a fresh
// ReadMemStats to confirm before pausing.
func (mg *MemoryGuard) waitIfNeeded(ctx context.Context) bool {
	if mg == nil {
		return true
	}
	startHeapSampler()

	if sampledHeapAlloc.Load() < mg.threshold {
		return true
	}

	// Sample is above threshold — confirm with a fresh read so a stale
	// sample can't pause readers on a heap that GC already shrank.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	sampledHeapAlloc.Store(ms.HeapAlloc)
	if ms.HeapAlloc < mg.threshold {
		return true
	}

	// Memory pressure — pause and let GC + writers catch up. The first
	// goroutine to enter returns freed memory to the OS once; after that
	// the GOMEMLIMIT-paced runtime owns collection — repeatedly forcing
	// full GCs here would slow the very writer drain we're waiting on.
	pauseStart := time.Now()
	isGCLeader := mg.gcActive.CompareAndSwap(false, true)
	if isGCLeader {
		logging.Debug("Memory pressure: HeapAlloc=%dMB threshold=%dMB, pausing readers",
			ms.HeapAlloc/(1024*1024), mg.threshold/(1024*1024))
		freeOSMemory()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// While paused, the leader still triggers a GC on a slow cadence:
	// pressure starts at 80% of the limit, so the heap can sit below
	// GOMEMLIMIT where the runtime feels no urgency to collect — without
	// an occasional GC, chunks freed by draining writers wouldn't show up
	// in HeapAlloc and readers would stay paused on a stale number. Every
	// 4th tick (2s) is frequent enough to observe drain progress without
	// the old behavior of forcing a full collection every 500ms.
	tickCount := 0
	for {
		select {
		case <-ctx.Done():
			if isGCLeader {
				mg.gcActive.Store(false)
			}
			logging.Debug("Memory pressure: reader cancelled after %v pause", time.Since(pauseStart))
			return false
		case <-ticker.C:
			tickCount++
			if isGCLeader && tickCount%4 == 0 {
				forceGC()
				var fresh runtime.MemStats
				runtime.ReadMemStats(&fresh)
				sampledHeapAlloc.Store(fresh.HeapAlloc)
			}
			if heap := sampledHeapAlloc.Load(); heap < mg.threshold {
				if isGCLeader {
					mg.gcActive.Store(false)
				}
				logging.Debug("Memory pressure cleared: HeapAlloc=%dMB, readers paused for %v",
					heap/(1024*1024), time.Since(pauseStart))
				return true
			}
		}
	}
}
