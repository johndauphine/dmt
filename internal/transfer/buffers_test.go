package transfer

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/johndauphine/dmt/v5/internal/config"
	"github.com/johndauphine/dmt/v5/internal/pool"
	"github.com/johndauphine/dmt/v5/internal/source"
	"github.com/johndauphine/dmt/v5/internal/tuning"
)

func perTableCapTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.LoadBytes([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  max_memory_mb: 64
  workers: 4
  chunk_size: 50000
  read_ahead_buffers: 4
  write_ahead_writers: 2
  max_source_connections: 1
  max_target_connections: 1
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	cfg.Migration.RuntimeRepresentativeRowBytes = 100
	cfg.Migration.RuntimeSafetyRowBytes = 4_000
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.FinalizeRuntimeChunkSizeCap()
	return cfg
}

func minimumTestPipelineBuffers(numReaders, numWriters, readAheadBuffers int) pool.PipelineBufferSizes {
	return pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		NumReaders:       numReaders,
		NumWriters:       numWriters,
		ReadAheadBuffers: readAheadBuffers,
	})
}

func expectedCompleteInventoryCap(budgetMB int64, workers, numReaders, numWriters int, buffers pool.PipelineBufferSizes, rowBytes int64) int {
	// Match the externally observable inventory contract without delegating
	// back to the helper under test: two writer slots cover the live rows and
	// encode copy, and one slot covers consumer dispatch between channels.
	liveSlots := buffers.ChunkChanDepth + buffers.JobChanDepth + numReaders + 2*numWriters + 1
	return int((budgetMB * 1024 * 1024) / int64(workers) / int64(liveSlots) / rowBytes)
}

func TestSubtractConnectionOverheadMB(t *testing.T) {
	tests := []struct {
		name              string
		budgetMB          int64
		connectionCount   int64
		wantPipelineMemMB int64
		wantExhausted     bool
	}{
		{name: "normal subtraction", budgetMB: 4_096, connectionCount: 20, wantPipelineMemMB: 3_896},
		{name: "overhead exhausts budget", budgetMB: 100, connectionCount: 16, wantPipelineMemMB: 1, wantExhausted: true},
		{name: "overflowing overhead exhausts budget", budgetMB: 100, connectionCount: math.MaxInt64, wantPipelineMemMB: 1, wantExhausted: true},
		{name: "zero budget", budgetMB: 0, connectionCount: 16, wantPipelineMemMB: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, exhausted := subtractConnectionOverheadMB(tc.budgetMB, tc.connectionCount)
			if got != tc.wantPipelineMemMB || exhausted != tc.wantExhausted {
				t.Errorf("subtractConnectionOverheadMB(%d, %d) = (%d, %v), want (%d, %v)", tc.budgetMB, tc.connectionCount, got, exhausted, tc.wantPipelineMemMB, tc.wantExhausted)
			}
		})
	}
}

func TestPipelineBudgetSaturatedConnectionCountsExhaustReserve(t *testing.T) {
	cfg, err := config.LoadBytes([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  max_memory_mb: 64
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	maxInt := int(^uint(0) >> 1)
	cfg.Migration.MaxSourceConnections = maxInt
	cfg.Migration.MaxTargetConnections = maxInt

	if got := PipelineMemBudgetBytes(cfg); got != 1024*1024 {
		t.Fatalf("PipelineMemBudgetBytes with saturated connection pools = %d, want 1 MiB exhausted reserve", got)
	}
}

func TestPipelineBudgetBytesSaturatesOnOverflow(t *testing.T) {
	if got := pipelineBudgetBytes(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("pipelineBudgetBytes(MaxInt64) = %d, want MaxInt64 saturation", got)
	}
	if got := pipelineBudgetBytes(64); got != 64*1024*1024 {
		t.Fatalf("pipelineBudgetBytes(64) = %d, want 64 MiB", got)
	}
}

func TestTransferBudgetsUseResolvedEnvelopeNotMutatedLegacyCap(t *testing.T) {
	cfg, err := config.LoadBytes([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  max_memory_mb: 64
`))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	envelopeBudgetMB := cfg.AutoConfig().MemoryEnvelope.BudgetMB
	if envelopeBudgetMB != 64 {
		t.Fatalf("test requires the 64 MB user ceiling to bind; resolved budget = %d", envelopeBudgetMB)
	}

	// Simulate a stale downstream field. Once the envelope is resolved,
	// consumers must not reapply Migration.MaxMemoryMB.
	cfg.Migration.MaxMemoryMB = 1
	if got := MemoryGuardLimitMB(cfg); got != envelopeBudgetMB {
		t.Errorf("MemoryGuardLimitMB = %d, want resolved envelope %d", got, envelopeBudgetMB)
	}
	connCount := int64(cfg.Migration.MaxSourceConnections + cfg.Migration.MaxTargetConnections)
	wantPipelineMB, _ := subtractConnectionOverheadMB(envelopeBudgetMB, connCount)
	wantPipelineBytes := wantPipelineMB * 1024 * 1024
	if got := PipelineMemBudgetBytes(cfg); got != wantPipelineBytes {
		t.Errorf("PipelineMemBudgetBytes = %d, want %d from envelope after one overhead subtraction", got, wantPipelineBytes)
	}
}

func TestRuntimeTableTransitionCapUsesEachTableWidth(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	const numReaders, numWriters, readAheadBuffers = 2, 2, 4
	minimums := minimumTestPipelineBuffers(numReaders, numWriters, readAheadBuffers)

	// 64 MiB envelope - 20 MiB connection reserve, modeled across 4 workers.
	const pipelineBudgetMB int64 = 44
	wantNarrow := expectedCompleteInventoryCap(pipelineBudgetMB, 4, numReaders, numWriters, minimums, 100)
	wantWide := expectedCompleteInventoryCap(pipelineBudgetMB, 4, numReaders, numWriters, minimums, 4_000)
	narrow := runtimeTableChunkSizeCap(cfg, 100, 4, numReaders, numWriters, minimums)
	wide := runtimeTableChunkSizeCap(cfg, 4_000, 4, numReaders, numWriters, minimums)
	if narrow != wantNarrow || wide != wantWide {
		t.Fatalf("per-table transition caps narrow/wide = %d/%d, want %d/%d", narrow, wide, wantNarrow, wantWide)
	}
	if narrow <= wide {
		t.Fatalf("narrow table cap %d should exceed wide table cap %d", narrow, wide)
	}
	wantGlobal := int(tuning.SafeChunkSize(64, 4, 4, 2, 100))
	if global := cfg.Migration.RuntimeChunkSizeCap; global != wantGlobal || global <= narrow {
		t.Fatalf("representative global cap = %d, want %d and above transition cap %d", global, wantGlobal, narrow)
	}
}

func TestRuntimeTableChunkSizeCapFallsBackToWidestObservedWidth(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	const numReaders, numWriters, readAheadBuffers = 2, 2, 4
	minimums := minimumTestPipelineBuffers(numReaders, numWriters, readAheadBuffers)
	want := expectedCompleteInventoryCap(44, 4, numReaders, numWriters, minimums, cfg.Migration.RuntimeSafetyRowBytes)
	if got := runtimeTableChunkSizeCap(cfg, 0, 4, numReaders, numWriters, minimums); got != want {
		t.Fatalf("missing table-width cap = %d, want widest-width fallback %d", got, want)
	}

	cfg.Migration.RuntimeSafetyRowBytesKnown = false
	cfg.Migration.TargetHardChunkLimit = 700
	if got := runtimeTableChunkSizeCap(cfg, 0, 4, numReaders, numWriters, minimums); got != 700 {
		t.Fatalf("unobserved missing-width cap = %d, want protocol-only 700", got)
	}
}

func TestRuntimeTableChunkSizeCapSurfacesOneRowMinimumOverBudget(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	minimums := minimumTestPipelineBuffers(2, 2, 4)
	cap, minimumExceeds := runtimeTableChunkSizeCapDetail(
		cfg,
		128*1024*1024,
		4,
		2,
		2,
		minimums,
	)
	if cap != 1 || !minimumExceeds {
		t.Fatalf("one-row fallback = (%d, %v), want (1, true)", cap, minimumExceeds)
	}
}

func TestTunerCallbacksRefusedWriterUpscalePreservesRequestedChunks(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	directCallbacks := newTunerCallbacks(cfg, nil, "wide", 4_000, 4, 2, 2, buffers)
	if got := directCallbacks.chunkSize(); got != 50_000 {
		t.Fatalf("reader chunk without runtime tuner = %d, want requested 50000", got)
	}
	if got := directCallbacks.batchSize(); got != 50_000 {
		t.Fatalf("writer batch without runtime tuner = %d, want requested 50000", got)
	}

	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	tuner.SetTableChunkSize("wide", 40_000)
	tuner.SetTableBatchSize("wide", 45_000)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 2, buffers)
	executionPool := pool.NewWriterPool(context.Background(), pool.WriterPoolConfig{
		NumWriters:    2,
		JobBufferSize: 3,
		WriteFunc:     func(context.Context, int, [][]any) error { return nil },
	})
	executionPool.Start()
	defer executionPool.Wait()

	if got := callbacks.chunkSize(); got != 40_000 {
		t.Fatalf("reader chunk = %d, want requested per-table recovery override 40000", got)
	}
	if got := callbacks.batchSize(); got != 45_000 {
		t.Fatalf("writer batch = %d, want requested per-table recovery override 45000", got)
	}

	moreWriters := 8
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &moreWriters}); err != nil {
		t.Fatalf("Update WAW: %v", err)
	}
	wantAfterGrowth := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 8, buffers)
	if got := callbacks.chunkSize(); got != 40_000 {
		t.Fatalf("reader chunk before actual WAW growth = %d, want requested 40000", got)
	}
	if got := callbacks.batchSize(); got != 45_000 {
		t.Fatalf("writer batch before actual WAW growth = %d, want requested 45000", got)
	}
	applyCalled := false
	applied, observed, prospective, err := callbacks.tryScaleWriters(8, true, func() error {
		applyCalled = true
		return executionPool.ScaleWorkers(8)
	})
	if err != nil {
		t.Fatalf("guarded writer upscale: %v", err)
	}
	if applied || applyCalled || observed != 40_000 || prospective != wantAfterGrowth {
		t.Fatalf("unsafe upscale = applied=%v called=%v observed=%d cap=%d, want deferred with %d/%d",
			applied, applyCalled, observed, prospective, 40_000, wantAfterGrowth)
	}
	if got := executionPool.NumWriters(); got != 2 {
		t.Fatalf("refused upscale changed real writer pool to %d, want 2", got)
	}
	if got := callbacks.chunkSize(); got != 40_000 {
		t.Fatalf("reader chunk after refused WAW growth = %d, want unchanged request 40000", got)
	}
	if got := callbacks.batchSize(); got != 45_000 {
		t.Fatalf("writer batch after refused WAW growth = %d, want unchanged request 45000", got)
	}
	if metrics := tuner.Metrics(); metrics.SafetyProjected {
		t.Fatalf("refused writer growth marked a safety projection: %+v", metrics)
	}

	// Error backoff must not turn into an implicit chunk/batch increase. The
	// writer pool retires busy workers asynchronously, and the fixed channel
	// depths still reflect the earlier high-water inventory.
	fewerWriters := 1
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &fewerWriters}); err != nil {
		t.Fatalf("back off WAW: %v", err)
	}
	relaxedCandidate := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 1, buffers)
	if relaxedCandidate <= wantAfterGrowth {
		t.Fatalf("test setup did not produce a relaxed cap: one-writer=%d high-writer=%d", relaxedCandidate, wantAfterGrowth)
	}
	if got := callbacks.chunkSize(); got != 40_000 {
		t.Fatalf("reader chunk after WAW backoff = %d, want unchanged request 40000 (candidate cap %d)", got, relaxedCandidate)
	}
	if got := callbacks.batchSize(); got != 45_000 {
		t.Fatalf("writer batch after WAW backoff = %d, want unchanged request 45000 (candidate cap %d)", got, relaxedCandidate)
	}
	downscaleCalled := false
	applied, _, _, err = callbacks.tryScaleWriters(1, false, func() error {
		downscaleCalled = true
		return executionPool.ScaleWorkers(1)
	})
	if err != nil || !applied || !downscaleCalled {
		t.Fatalf("safe downscale = applied=%v called=%v err=%v, want applied", applied, downscaleCalled, err)
	}
	if got := executionPool.NumWriters(); got != 1 {
		t.Fatalf("real writer pool after downscale = %d, want 1", got)
	}
}

func TestTunerCallbacksAllowsUpscaleWhenEmittedChunksFitProspectiveInventory(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 2, buffers)
	executionPool := pool.NewWriterPool(context.Background(), pool.WriterPoolConfig{
		NumWriters:    2,
		JobBufferSize: 3,
		WriteFunc:     func(context.Context, int, [][]any) error { return nil },
	})
	executionPool.Start()
	defer executionPool.Wait()
	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("emitted chunk = %d, want 1", got)
	}

	moreWriters := 8
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &moreWriters}); err != nil {
		t.Fatalf("Update WAW: %v", err)
	}
	called := false
	applied, observed, prospective, err := callbacks.tryScaleWriters(8, true, func() error {
		called = true
		return executionPool.ScaleWorkers(8)
	})
	if err != nil || !applied || !called {
		t.Fatalf("fitting upscale = applied=%v called=%v err=%v, want applied", applied, called, err)
	}
	if observed != 1 || prospective < observed {
		t.Fatalf("fitting upscale observed/cap = %d/%d", observed, prospective)
	}
	if got := executionPool.NumWriters(); got != 8 {
		t.Fatalf("real writer pool after allowed upscale = %d, want 8", got)
	}
	tuner.SetTableChunkSize("wide", 50_000)
	if got := callbacks.chunkSize(); got != prospective {
		t.Fatalf("reader chunk after allowed WAW growth = %d, want committed prospective cap %d", got, prospective)
	}
	if metrics := tuner.Metrics(); !metrics.SafetyProjected || metrics.ExecutionChunkMin != 1 || metrics.ExecutionChunkMax != prospective {
		t.Fatalf("allowed transition projection metrics = %+v, want applied with reader range 1..%d", metrics, prospective)
	}
}

func TestTunerCallbacksProtocolLimitBindsWithoutSteadyMemoryProjection(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	cfg.Migration.TargetHardChunkLimit = 1_234
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	direct := newTunerCallbacks(cfg, nil, "wide", 4_000, 4, 2, 2, buffers)
	if got := direct.chunkSize(); got != 1_234 {
		t.Fatalf("direct reader chunk = %d, want target protocol limit 1234", got)
	}
	if got := direct.batchSize(); got != 1_234 {
		t.Fatalf("direct writer batch = %d, want target protocol limit 1234", got)
	}

	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	tuner.SetTableChunkSize("wide", 40_000)
	tuner.SetTableBatchSize("wide", 45_000)
	callbacks := newTunerCallbacks(
		cfg,
		tuner,
		"wide",
		4_000,
		4,
		2,
		2,
		buffers,
	)

	if got := callbacks.chunkSize(); got != 1_234 {
		t.Fatalf("reader chunk = %d, want target protocol limit 1234", got)
	}
	if got := callbacks.batchSize(); got != 1_234 {
		t.Fatalf("writer batch = %d, want target protocol limit 1234", got)
	}
	if metrics := tuner.Metrics(); !metrics.SafetyProjected || metrics.ExecutionChunkMin != 1_234 || metrics.ExecutionChunkMax != 1_234 {
		t.Fatalf("protocol projection metrics = %+v, want applied with reader range 1234..1234", metrics)
	}
}

func TestTunerCallbacksFailedUpscaleLeavesRatchetAndHighWaterUnchanged(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 2, buffers)
	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("initial reader chunk = %d, want 1", got)
	}

	eightWriters := 8
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &eightWriters}); err != nil {
		t.Fatalf("Update WAW 8: %v", err)
	}
	capEight := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 8, buffers)
	if applied, _, prospective, err := callbacks.tryScaleWriters(8, true, nil); applied || err != nil || prospective != capEight {
		t.Fatalf("nil-apply upscale = applied=%v cap=%d err=%v, want no mutation with cap %d", applied, prospective, err, capEight)
	}
	wantErr := errors.New("synthetic upscale failure")
	applied, _, prospective, err := callbacks.tryScaleWriters(8, true, func() error { return wantErr })
	if applied || !errors.Is(err, wantErr) || prospective != capEight {
		t.Fatalf("failed upscale = applied=%v cap=%d err=%v, want failure with cap %d", applied, prospective, err, capEight)
	}
	if metrics := tuner.Metrics(); metrics.SafetyProjected {
		t.Fatalf("failed upscale marked a safety projection: %+v", metrics)
	}

	fourWriters := 4
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &fourWriters}); err != nil {
		t.Fatalf("Update WAW 4: %v", err)
	}
	capFour := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 4, buffers)
	if capFour <= capEight {
		t.Fatalf("test setup cap(4)=%d, want above cap(8)=%d", capFour, capEight)
	}
	applied, _, prospective, err = callbacks.tryScaleWriters(4, true, func() error { return nil })
	if err != nil || !applied || prospective != capFour {
		t.Fatalf("later upscale = applied=%v cap=%d err=%v, want applied with unchanged high-water cap %d", applied, prospective, err, capFour)
	}
}

func TestTunerCallbacksProtocolOnlyEvidenceRefusesWriterUpscale(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	cfg.Migration.RuntimeSafetyRowBytesKnown = false
	cfg.Migration.RuntimeSafetyRowBytes = 0
	cfg.Migration.TargetHardChunkLimit = 700
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	callbacks := newTunerCallbacks(
		cfg,
		tuner,
		"unknown",
		0,
		4,
		2,
		2,
		pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3},
	)
	if got := callbacks.chunkSize(); got != 700 {
		t.Fatalf("protocol-only reader chunk = %d, want 700", got)
	}
	called := false
	applied, _, prospective, err := callbacks.tryScaleWriters(8, true, func() error {
		called = true
		return nil
	})
	if err != nil || applied || called || prospective != 700 {
		t.Fatalf("protocol-only upscale = applied=%v called=%v cap=%d err=%v, want fail-closed at protocol cap 700", applied, called, prospective, err)
	}
}

func TestTunerCallbacksDownscaleReboundRetainsLifetimeWriterCap(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 8,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 8, buffers)
	executionPool := pool.NewWriterPool(context.Background(), pool.WriterPoolConfig{
		NumWriters:    8,
		JobBufferSize: 9,
		WriteFunc:     func(context.Context, int, [][]any) error { return nil },
	})
	executionPool.Start()
	defer executionPool.Wait()

	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("initial reader chunk = %d, want 1", got)
	}
	highWaterCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 8, buffers)
	if highWaterCap <= 1 || highWaterCap >= 50_000 {
		t.Fatalf("test setup lifetime writer cap = %d, want between 1 and 50000", highWaterCap)
	}
	looserReboundCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 4, buffers)
	if looserReboundCap <= highWaterCap {
		t.Fatalf("test setup rebound cap = %d, want above lifetime high-water cap %d", looserReboundCap, highWaterCap)
	}

	oneWriter := 1
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &oneWriter}); err != nil {
		t.Fatalf("Update WAW downscale: %v", err)
	}
	applied, _, transitionCap, err := callbacks.tryScaleWriters(1, false, func() error {
		return executionPool.ScaleWorkers(1)
	})
	if err != nil || !applied || transitionCap != highWaterCap {
		t.Fatalf("downscale = applied=%v cap=%d err=%v, want applied with lifetime cap %d", applied, transitionCap, err, highWaterCap)
	}

	tuner.SetTableChunkSize("wide", 50_000)
	if got := callbacks.chunkSize(); got != highWaterCap {
		t.Fatalf("reader chunk after downscale = %d, want lifetime cap %d", got, highWaterCap)
	}

	fourWriters := 4
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &fourWriters}); err != nil {
		t.Fatalf("Update WAW rebound: %v", err)
	}
	applied, _, transitionCap, err = callbacks.tryScaleWriters(4, true, func() error {
		return executionPool.ScaleWorkers(4)
	})
	if err != nil || !applied || transitionCap != highWaterCap {
		t.Fatalf("rebound = applied=%v cap=%d err=%v, want applied with lifetime cap %d", applied, transitionCap, err, highWaterCap)
	}
	if got := callbacks.chunkSize(); got != highWaterCap {
		t.Fatalf("reader chunk after rebound = %d, want nonrelaxing lifetime cap %d", got, highWaterCap)
	}
}

func TestTunerCallbacksDownscalePublishesTransitionCapBeforeApply(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 8,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 8, buffers)
	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("initial reader chunk = %d, want 1", got)
	}
	highWaterCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 8, buffers)
	if highWaterCap <= 1 || highWaterCap >= 50_000 {
		t.Fatalf("test setup lifetime writer cap = %d, want between 1 and 50000", highWaterCap)
	}

	oneWriter := 1
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &oneWriter}); err != nil {
		t.Fatalf("Update WAW downscale: %v", err)
	}
	tuner.SetTableChunkSize("wide", 50_000)
	applyStarted := make(chan struct{})
	allowApply := make(chan struct{})
	type scaleResult struct {
		applied bool
		cap     int
		err     error
	}
	resultCh := make(chan scaleResult, 1)
	go func() {
		applied, _, transitionCap, err := callbacks.tryScaleWriters(1, false, func() error {
			close(applyStarted)
			<-allowApply
			return nil
		})
		resultCh <- scaleResult{applied: applied, cap: transitionCap, err: err}
	}()

	<-applyStarted
	if got := callbacks.chunkSize(); got != highWaterCap {
		close(allowApply)
		<-resultCh
		t.Fatalf("reader chunk during downscale apply = %d, want pending transition cap %d", got, highWaterCap)
	}
	close(allowApply)
	result := <-resultCh
	if result.err != nil || !result.applied || result.cap != highWaterCap {
		t.Fatalf("downscale result = %+v, want applied with lifetime cap %d", result, highWaterCap)
	}
	if got := callbacks.chunkSize(); got != highWaterCap {
		t.Fatalf("reader chunk after downscale apply = %d, want committed transition cap %d", got, highWaterCap)
	}
}

func TestTunerCallbacksFailedDownscaleClearsPendingTransitionCap(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 8,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 8, buffers)
	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("initial reader chunk = %d, want 1", got)
	}
	highWaterCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 8, buffers)

	oneWriter := 1
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &oneWriter}); err != nil {
		t.Fatalf("Update WAW downscale: %v", err)
	}
	tuner.SetTableChunkSize("wide", 50_000)
	wantErr := errors.New("synthetic downscale failure")
	duringApply := 0
	applied, _, transitionCap, err := callbacks.tryScaleWriters(1, false, func() error {
		duringApply = callbacks.chunkSize()
		return wantErr
	})
	if applied || !errors.Is(err, wantErr) || transitionCap != highWaterCap {
		t.Fatalf("failed downscale = applied=%v cap=%d err=%v, want failure with cap %d", applied, transitionCap, err, highWaterCap)
	}
	if duringApply != highWaterCap {
		t.Fatalf("reader chunk during failed apply = %d, want pending cap %d", duringApply, highWaterCap)
	}
	if got := callbacks.chunkSize(); got != 50_000 {
		t.Fatalf("reader chunk after failed apply = %d, want restored request 50000", got)
	}
}

func TestTunerCallbacksCapRatchetIsConcurrentAndNeverRelaxes(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	buffers := pool.PipelineBufferSizes{ChunkChanDepth: 2, JobChanDepth: 3}
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	tuner.SetTableChunkSize("wide", 1)
	callbacks := newTunerCallbacks(cfg, tuner, "wide", 4_000, 4, 2, 2, buffers)
	if got := callbacks.chunkSize(); got != 1 {
		t.Fatalf("initial fitting chunk = %d, want 1", got)
	}

	highWaterWriters := 8
	if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &highWaterWriters}); err != nil {
		t.Fatalf("establish high-water WAW: %v", err)
	}
	highWaterCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, highWaterWriters, buffers)
	applied, _, _, err := callbacks.tryScaleWriters(highWaterWriters, true, func() error { return nil })
	if err != nil || !applied {
		t.Fatalf("establish high-water WAW: applied=%v err=%v", applied, err)
	}
	tuner.SetTableChunkSize("wide", 50_000)
	if got := callbacks.chunkSize(); got != highWaterCap {
		t.Fatalf("establish ratchet = %d, want %d", got, highWaterCap)
	}

	const goroutines = 16
	const iterations = 50
	errCh := make(chan string, goroutines*iterations*2)
	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				writers := 1
				if (worker+i)%2 == 0 {
					writers = highWaterWriters
				}
				if err := tuner.Update(RuntimeUpdate{WriteAheadWriters: &writers}); err != nil {
					errCh <- err.Error()
					return
				}
				if got := callbacks.chunkSize(); got > highWaterCap {
					errCh <- "reader cap relaxed"
				}
				if got := callbacks.batchSize(); got > highWaterCap {
					errCh <- "writer cap relaxed"
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for errText := range errCh {
		t.Error(errText)
	}
}

func TestRuntimeTableChunkSizeCapSparseSnapshotKeepsPositiveConfigTuple(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	// A sparse/custom snapshot must not erase valid config concurrency with
	// zero values and thereby make the memory cap disappear.
	tuner := NewRuntimeTuner(RuntimeSnapshot{ChunkSize: 50_000})
	tuple := effectiveRuntimeSizingTuple(cfg, tuner)
	if tuple.workers != 4 || tuple.readAheadBuffers != 4 || tuple.writeAheadWriters != 2 {
		t.Fatalf("sparse snapshot erased config tuple: %+v", tuple)
	}
}

func TestCalculatePipelineBuffersUsesRequestedChunkAndTableWidth(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           4,
		ChunkSize:         50_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 2,
	})
	job := Job{Table: source.Table{Name: "wide", EstimatedRowSize: 4_000}}
	want := pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		MemoryBudgetBytes: 44 * 1024 * 1024 / 4,
		ChunkSize:         50_000,
		RowBytes:          4_000,
		NumWriters:        2,
		NumReaders:        2,
		ReadAheadBuffers:  4,
	})
	got := calculatePipelineBuffers(cfg, job, "wide", tuner, 2, 2, 4)
	if got != want {
		t.Fatalf("pipeline buffers = %+v, want %+v from requested chunk 50000", got, want)
	}
	if direct := calculatePipelineBuffers(cfg, job, "wide", nil, 2, 2, 4); direct != want {
		t.Fatalf("direct pipeline buffers = %+v, want %+v from configured chunk 50000", direct, want)
	}
	tuner.SetTableChunkSize("wide", 30_000)
	overrideWant := pool.CalculatePipelineBuffers(pool.PipelineBufferConfig{
		MemoryBudgetBytes: 44 * 1024 * 1024 / 4,
		ChunkSize:         30_000,
		RowBytes:          4_000,
		NumWriters:        2,
		NumReaders:        2,
		ReadAheadBuffers:  4,
	})
	if override := calculatePipelineBuffers(cfg, job, "wide", tuner, 2, 2, 4); override != overrideWant {
		t.Fatalf("override pipeline buffers = %+v, want %+v from requested table chunk 30000", override, overrideWant)
	}
	transitionCap := runtimeTableChunkSizeCap(cfg, 4_000, 4, 2, 2, got)
	if transitionCap <= 0 || transitionCap >= 50_000 {
		t.Fatalf("test setup transition cap = %d, want positive and below requested 50000", transitionCap)
	}
	liveSlots := got.ChunkChanDepth + got.JobChanDepth + 2 + 2*2 + 1
	modeledBytes := int64(4) * int64(liveSlots) * int64(transitionCap) * 4_000
	if modeledBytes > 44*1024*1024 {
		t.Fatalf("complete inventory at transition cap %d consumes %d bytes, exceeds unified pipeline budget", transitionCap, modeledBytes)
	}
}

func TestSteadyPolicyIsNotMutatedByPerTableInventoryProjection(t *testing.T) {
	cfg := perTableCapTestConfig(t)
	cfg.Migration.Workers = 16
	cfg.Migration.ChunkSize = 24_000
	tuner := NewRuntimeTuner(RuntimeSnapshot{
		Workers:           16,
		ChunkSize:         24_000,
		ReadAheadBuffers:  4,
		WriteAheadWriters: 3,
	})
	buffers := minimumTestPipelineBuffers(4, 3, 4)
	modeledFit := runtimeTableChunkSizeCap(cfg, 5_000, 16, 4, 3, buffers)
	if modeledFit <= 0 || modeledFit >= 24_000 {
		t.Fatalf("test setup complete-inventory fit = %d, want positive and below requested 24000", modeledFit)
	}

	callbacks := newTunerCallbacks(cfg, tuner, "wide", 5_000, 16, 4, 3, buffers)
	if got := callbacks.chunkSize(); got != 24_000 {
		t.Fatalf("steady reader chunk = %d, want requested policy 24000 (modeled transition fit %d)", got, modeledFit)
	}
	if got := callbacks.batchSize(); got != 24_000 {
		t.Fatalf("steady writer batch = %d, want requested policy 24000 (modeled transition fit %d)", got, modeledFit)
	}
}
