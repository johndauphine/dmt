package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/pool"
	"github.com/johndauphine/dmt/internal/transfer"
)

const runtimeCapProbeDriverName = "runtime-cap-probe-test"

type runtimeCapProbeDriver struct{ driver.Driver }

func (runtimeCapProbeDriver) Name() string      { return runtimeCapProbeDriverName }
func (runtimeCapProbeDriver) Aliases() []string { return nil }
func (runtimeCapProbeDriver) Defaults() driver.DriverDefaults {
	return driver.DriverDefaults{Port: 9999, WriteAheadWriters: 1}
}
func (runtimeCapProbeDriver) HardChunkLimit(int64) int {
	return 0
}
func (runtimeCapProbeDriver) ProbeTarget(context.Context, *sql.DB) driver.TargetProbe {
	// With the analyzer's conservative 500-byte protocol-only width fallback,
	// 62,500 * 80% / 500 = a 100-row hard cap.
	return driver.TargetProbe{MaxAllowedPacket: 62_500}
}

type runtimeCapTargetPool struct {
	pool.TargetPool
	dbType string
}

func (p runtimeCapTargetPool) DB() *sql.DB    { return nil }
func (p runtimeCapTargetPool) DBType() string { return p.dbType }
func (p runtimeCapTargetPool) MaxConns() int  { return 4 }

type runtimeCapFailureSourcePool struct{ pool.SourcePool }

func (runtimeCapFailureSourcePool) DB() *sql.DB    { return nil }
func (runtimeCapFailureSourcePool) DBType() string { return "runtime-cap-unsupported-source" }
func (runtimeCapFailureSourcePool) MaxConns() int  { return 4 }

func registerRuntimeCapProbeDriver() {
	if !driver.IsRegistered(runtimeCapProbeDriverName) {
		driver.Register(runtimeCapProbeDriver{})
	}
}

func TestApplyTuningManualPreservesProtocolCapAndResetsSafetyEvidence(t *testing.T) {
	registerRuntimeCapProbeDriver()
	cfg := &config.Config{Migration: config.MigrationConfig{
		Tuning:                     "manual",
		ChunkSize:                  1_000,
		RuntimeChunkSizeCap:        777,
		RuntimeSafetyRowBytes:      8_192,
		RuntimeSafetyRowBytesKnown: true,
		RuntimeChunkGrowthAllowed:  true,
		TargetHardChunkLimit:       777,
	}}
	o := &Orchestrator{
		config:     cfg,
		targetPool: runtimeCapTargetPool{dbType: runtimeCapProbeDriverName},
	}

	o.applyTuning(context.Background())

	if cfg.Migration.TargetHardChunkLimit != 100 || cfg.Migration.RuntimeChunkSizeCap != 100 {
		t.Fatalf("manual protocol caps = (target=%d runtime=%d), want 100/100",
			cfg.Migration.TargetHardChunkLimit, cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.RuntimeSafetyRowBytes != 0 || cfg.Migration.RuntimeSafetyRowBytesKnown || cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatalf("manual tuning retained stale width/growth evidence: %+v", cfg.Migration)
	}
}

func TestApplyTuningUnsupportedStatsFinalizesProtocolOnlyCap(t *testing.T) {
	registerRuntimeCapProbeDriver()
	cfg := &config.Config{Migration: config.MigrationConfig{
		ChunkSize:                  1_000,
		RuntimeSafetyRowBytes:      8_192,
		RuntimeSafetyRowBytesKnown: true,
		RuntimeChunkGrowthAllowed:  true,
	}}
	o := &Orchestrator{
		config:     cfg,
		sourcePool: runtimeCapFailureSourcePool{},
		targetPool: runtimeCapTargetPool{dbType: runtimeCapProbeDriverName},
	}

	o.applyTuning(context.Background())

	if cfg.Migration.TargetHardChunkLimit != 100 || cfg.Migration.RuntimeChunkSizeCap != 100 {
		t.Fatalf("formula-only protocol caps = (target=%d runtime=%d), want 100/100",
			cfg.Migration.TargetHardChunkLimit, cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.RuntimeSafetyRowBytesKnown || cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatalf("formula-only analysis authorized memory growth without schema width: %+v", cfg.Migration)
	}
}

func TestRuntimeControllerOptionsCarryRuntimeChunkPolicy(t *testing.T) {
	cfg := &config.Config{Migration: config.MigrationConfig{
		RuntimeChunkSizeCap:       1_200,
		RuntimeChunkGrowthAllowed: true,
	}}
	runner := &TransferRunner{config: cfg}

	opts := runner.runtimeControllerOptions("run-1", nil)
	if opts.MaxChunkSize != 1_200 || !opts.AllowResourceGrowth {
		t.Fatalf("controller cap policy = (cap=%d growth=%v), want (1200,true)",
			opts.MaxChunkSize, opts.AllowResourceGrowth)
	}
	if opts.MinChunkSize != 1_200 {
		t.Fatalf("MinChunkSize = %d, want cap-lowered floor 1200", opts.MinChunkSize)
	}
	if opts.RecomputeMaxChunkSize == nil {
		t.Fatal("controller recompute callback is nil")
	}

	cfg.Migration.RuntimeChunkGrowthAllowed = false
	if got := runner.runtimeControllerOptions("run-2", nil).AllowResourceGrowth; got {
		t.Fatal("protocol-only/degraded config must keep controller growth disabled")
	}
}

func TestClampInitialRuntimeChunkSizeUpdatesAndRecords(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	cfg := &config.Config{
		Migration: config.MigrationConfig{ChunkSize: 1_000, RuntimeChunkSizeCap: 400},
		Source:    config.SourceConfig{ChunkSize: 900},
		Target:    config.TargetConfig{ChunkSize: 800},
	}
	if err := state.CreateRun("run-1", "public", "dbo", cfg, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	recorder := newRuntimeAdjustmentRecorder(state, "run-1")
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1_000, Workers: 4})

	if err := clampInitialRuntimeChunkSize(tuner, cfg, recorder); err != nil {
		t.Fatalf("clampInitialRuntimeChunkSize: %v", err)
	}
	if got := tuner.Snapshot().ChunkSize; got != 400 {
		t.Fatalf("tuner chunk_size = %d, want 400", got)
	}
	if cfg.Migration.ChunkSize != 400 || cfg.Source.ChunkSize != 400 || cfg.Target.ChunkSize != 400 {
		t.Fatalf("config chunk views = migration:%d source:%d target:%d, want all 400",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
	if !recorder.applied() {
		t.Fatal("initial cap clamp must mark the run runtime-adjusted")
	}
	records, err := state.GetRuntimeAdjustments(0)
	if err != nil {
		t.Fatalf("GetRuntimeAdjustments: %v", err)
	}
	if len(records) != 1 || records[0].Action != "chunk_size" || records[0].Adjustments["chunk_size"] != 400 {
		t.Fatalf("initial clamp records = %+v, want one chunk_size=400 adjustment", records)
	}
	if !strings.Contains(records[0].Reasoning, "1000") || !strings.Contains(records[0].Reasoning, "400") {
		t.Fatalf("initial clamp reasoning = %q, want original and capped values", records[0].Reasoning)
	}
}

type failingUpdateRuntimeTuner struct{ transfer.RuntimeTuner }

func (failingUpdateRuntimeTuner) Update(transfer.RuntimeUpdate) error {
	return errors.New("update failed")
}

func TestClampInitialRuntimeChunkSizeFailureDoesNotAdvanceConfigOrRecorder(t *testing.T) {
	state, err := checkpoint.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	cfg := &config.Config{Migration: config.MigrationConfig{ChunkSize: 1_000, RuntimeChunkSizeCap: 400}}
	if err := state.CreateRun("run-1", "public", "dbo", cfg, "", ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	recorder := newRuntimeAdjustmentRecorder(state, "run-1")
	base := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1_000})
	tuner := failingUpdateRuntimeTuner{RuntimeTuner: base}

	err = clampInitialRuntimeChunkSize(tuner, cfg, recorder)
	if err == nil || !strings.Contains(err.Error(), "update failed") {
		t.Fatalf("clamp error = %v, want update failure", err)
	}
	if cfg.Migration.ChunkSize != 1_000 || base.Snapshot().ChunkSize != 1_000 || recorder.applied() {
		t.Fatalf("failed clamp advanced state: cfg=%d tuner=%d applied=%v",
			cfg.Migration.ChunkSize, base.Snapshot().ChunkSize, recorder.applied())
	}
}

func TestClampInitialRuntimeChunkSizeNoCapIsNoop(t *testing.T) {
	cfg := &config.Config{Migration: config.MigrationConfig{ChunkSize: 1_000}}
	tuner := transfer.NewRuntimeTuner(transfer.RuntimeSnapshot{ChunkSize: 1_000})
	if err := clampInitialRuntimeChunkSize(tuner, cfg, nil); err != nil {
		t.Fatal(err)
	}
	if got := tuner.Snapshot().ChunkSize; got != 1_000 {
		t.Fatalf("uncapped tuner chunk_size = %d, want 1000", got)
	}
}
