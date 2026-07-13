package config

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/tuning"
	"gopkg.in/yaml.v3"
)

func runtimeCapTestConfig(budgetMB int64) *Config {
	return &Config{
		Source: SourceConfig{ChunkSize: 50_000},
		Target: TargetConfig{ChunkSize: 50_000},
		Migration: MigrationConfig{
			Workers:           4,
			ChunkSize:         50_000,
			ReadAheadBuffers:  4,
			WriteAheadWriters: 2,
		},
		autoConfig: AutoConfig{MemoryEnvelope: MemoryEnvelope{BudgetMB: budgetMB}},
	}
}

func TestApplyTunerSuggestionsDerivesRuntimeMemoryCapForPostgresAndMSSQL(t *testing.T) {
	for _, targetType := range []string{"postgres", "mssql"} {
		t.Run(targetType, func(t *testing.T) {
			cfg := runtimeCapTestConfig(1_024)
			cfg.Target.Type = targetType
			cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
				Workers:                 4,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				SafetyRowBytes:          8_192,
				SafetyRowBytesKnown:     true,
				ChunkSizeRecommendation: 50_000,
			})

			want := positiveInt64ToInt(tuning.SafeChunkSize(1_024, 4, 4, 2, 8_192))
			if cfg.Migration.RuntimeChunkSizeCap != want || want <= 0 {
				t.Fatalf("runtime cap = %d, want memory cap %d", cfg.Migration.RuntimeChunkSizeCap, want)
			}
			if !cfg.Migration.RuntimeChunkGrowthAllowed {
				t.Fatal("observed-width memory cap should authorize resource growth")
			}
			// Config finalization is derive-only: TransferRunner owns and records
			// the initial atomic clamp before starting the controller.
			if cfg.Migration.ChunkSize != 50_000 || cfg.Source.ChunkSize != 50_000 || cfg.Target.ChunkSize != 50_000 {
				t.Fatalf("cap derivation mutated initial chunks: migration=%d source=%d target=%d",
					cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
			}
		})
	}
}

func TestRuntimeChunkSizeCapForUsesMinimumMemoryAndProtocolCap(t *testing.T) {
	cfg := runtimeCapTestConfig(1_024)
	cfg.Migration.RuntimeSafetyRowBytes = 8_192
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	memoryCap := positiveInt64ToInt(tuning.SafeChunkSize(1_024, 4, 4, 2, 8_192))

	cfg.Migration.TargetHardChunkLimit = memoryCap + 100
	if got := cfg.RuntimeChunkSizeCapFor(4, 4, 2); got != memoryCap {
		t.Fatalf("memory-limited cap = %d, want %d", got, memoryCap)
	}

	cfg.Migration.TargetHardChunkLimit = 100
	if got := cfg.RuntimeChunkSizeCapFor(4, 4, 2); got != 100 {
		t.Fatalf("protocol-limited cap = %d, want 100", got)
	}
}

func TestApplyTunerSuggestionsUsesEffectivePinnedTuple(t *testing.T) {
	cfg := runtimeCapTestConfig(128)
	cfg.Migration.Workers = 8
	cfg.Migration.ReadAheadBuffers = 6
	cfg.Migration.WriteAheadWriters = 4
	cfg.autoConfig.OriginalWorkers = 8
	cfg.autoConfig.OriginalReadAheadBuffers = 6
	cfg.autoConfig.OriginalWriteAheadWriters = 4

	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		Workers:                 2,
		ReadAheadBuffers:        2,
		WriteAheadWriters:       1,
		SafetyRowBytes:          4_096,
		SafetyRowBytesKnown:     true,
		ChunkSizeRecommendation: 50_000,
	})

	if cfg.Migration.Workers != 8 || cfg.Migration.ReadAheadBuffers != 6 || cfg.Migration.WriteAheadWriters != 4 {
		t.Fatalf("pinned tuple was overwritten: %+v", cfg.Migration)
	}
	want := positiveInt64ToInt(tuning.SafeChunkSize(128, 8, 6, 4, 4_096))
	if cfg.Migration.RuntimeChunkSizeCap != want {
		t.Fatalf("runtime cap = %d, want %d from pinned tuple", cfg.Migration.RuntimeChunkSizeCap, want)
	}
}

func TestRuntimeChunkCapUnknownWidthIsProtocolOnlyAndGrowthDisabled(t *testing.T) {
	cfg := runtimeCapTestConfig(1_024)
	cfg.Migration.TargetHardChunkLimit = 750
	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		SafetyRowBytes:      500,
		SafetyRowBytesKnown: false,
	})

	if cfg.Migration.RuntimeSafetyRowBytes != 500 || cfg.Migration.RuntimeSafetyRowBytesKnown {
		t.Fatalf("fallback provenance was not retained: width=%d known=%v",
			cfg.Migration.RuntimeSafetyRowBytes, cfg.Migration.RuntimeSafetyRowBytesKnown)
	}
	if cfg.Migration.RuntimeChunkSizeCap != 750 {
		t.Fatalf("protocol-only cap = %d, want 750", cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatal("protocol-only cap must not authorize resource growth")
	}
	if got := cfg.RuntimeChunkGrowthCapFor(4, 4, 2); got != 0 {
		t.Fatalf("unknown width growth cap = %d, want fail-closed 0", got)
	}
}

func TestRuntimeChunkCapOneRowOverBudgetDisablesGrowth(t *testing.T) {
	var logs bytes.Buffer
	logging.SetOutput(&logs)
	t.Cleanup(func() { logging.SetOutput(os.Stdout) })

	cfg := runtimeCapTestConfig(1)
	cfg.Migration.Workers = 2
	cfg.Migration.ReadAheadBuffers = 1
	cfg.Migration.WriteAheadWriters = 1
	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		SafetyRowBytes:      1024 * 1024,
		SafetyRowBytesKnown: true,
	})

	if cfg.Migration.RuntimeChunkSizeCap != 1 {
		t.Fatalf("one-row minimum-progress cap = %d, want 1", cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatal("one-row over-budget fallback must disable growth")
	}
	if got := cfg.RuntimeChunkGrowthCapFor(2, 1, 1); got != 0 {
		t.Fatalf("one-row over-budget growth cap = %d, want 0", got)
	}
	if !tuning.MemoryEstimateExceedsBudget(1, 2, 1, 1, 1, 1024*1024) {
		t.Fatal("test fixture must remain explicitly over budget at one row")
	}
	if !strings.Contains(logs.String(), "one modeled row still exceeds") ||
		!strings.Contains(logs.String(), "resource growth disabled") {
		t.Fatalf("one-row over-budget warning missing: %q", logs.String())
	}
}

func TestRuntimeChunkGrowthCapForProspectiveWAWCanFailClosed(t *testing.T) {
	cfg := runtimeCapTestConfig(1)
	cfg.Migration.Workers = 1
	cfg.Migration.ReadAheadBuffers = 1
	cfg.Migration.WriteAheadWriters = 1
	cfg.Migration.RuntimeSafetyRowBytes = 400 * 1024
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.FinalizeRuntimeChunkSizeCap()

	if !cfg.Migration.RuntimeChunkGrowthAllowed || cfg.RuntimeChunkGrowthCapFor(1, 1, 1) != 1 {
		t.Fatalf("initial WAW=1 tuple should fit one row: %+v", cfg.Migration)
	}
	if got := cfg.RuntimeChunkGrowthCapFor(1, 1, 2); got != 0 {
		t.Fatalf("prospective WAW=2 one-row-overbudget cap = %d, want 0 to suppress growth", got)
	}
}

func TestResetAndFinalizeRuntimeChunkSafetySupportsDegradedProtocolPath(t *testing.T) {
	cfg := runtimeCapTestConfig(1_024)
	cfg.Migration.RuntimeChunkSizeCap = 500
	cfg.Migration.RuntimeSafetyRowBytes = 8_192
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.Migration.RuntimeChunkGrowthAllowed = true
	cfg.Migration.TargetHardChunkLimit = 250

	cfg.ResetRuntimeChunkSafety()
	if cfg.Migration.RuntimeChunkSizeCap != 0 || cfg.Migration.RuntimeSafetyRowBytes != 0 ||
		cfg.Migration.RuntimeSafetyRowBytesKnown || cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatalf("runtime safety reset left stale metadata: %+v", cfg.Migration)
	}
	if cfg.Migration.TargetHardChunkLimit != 250 {
		t.Fatalf("reset cleared orchestrator-owned protocol cap: %d", cfg.Migration.TargetHardChunkLimit)
	}

	cfg.FinalizeRuntimeChunkSizeCap()
	if cfg.Migration.RuntimeChunkSizeCap != 250 || cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatalf("degraded protocol finalization = cap %d growth %v, want 250/false",
			cfg.Migration.RuntimeChunkSizeCap, cfg.Migration.RuntimeChunkGrowthAllowed)
	}
	if cfg.Migration.ChunkSize != 50_000 || cfg.Source.ChunkSize != 50_000 || cfg.Target.ChunkSize != 50_000 {
		t.Fatalf("degraded finalization consumed the initial clamp delta: migration=%d source=%d target=%d",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
}

func TestRuntimeChunkCapExtremeInputsDoNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cfg := runtimeCapTestConfig(maxMemoryEnvelopeMB)
	cfg.Migration.RuntimeSafetyRowBytes = math.MaxInt64
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.Migration.TargetHardChunkLimit = maxInt

	if got := cfg.RuntimeChunkSizeCapFor(maxInt, maxInt, maxInt); got != 1 {
		t.Fatalf("extreme runtime cap = %d, want conservative one-row minimum", got)
	}
	if got := cfg.RuntimeChunkGrowthCapFor(maxInt, maxInt, maxInt); got != 0 {
		t.Fatalf("extreme over-budget growth cap = %d, want 0", got)
	}
}

func TestRuntimeChunkSafetyMetadataIsNotSerialized(t *testing.T) {
	m := MigrationConfig{
		RuntimeChunkSizeCap:        123,
		RuntimeSafetyRowBytes:      8_192,
		RuntimeSafetyRowBytesKnown: true,
		RuntimeChunkGrowthAllowed:  true,
	}
	jsonData, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	yamlData, err := yaml.Marshal(m)
	if err != nil {
		t.Fatalf("yaml.Marshal: %v", err)
	}
	for _, data := range []string{string(jsonData), string(yamlData)} {
		if strings.Contains(data, "RuntimeChunk") || strings.Contains(data, "RuntimeSafety") ||
			strings.Contains(data, "runtimechunk") || strings.Contains(data, "runtimesafety") {
			t.Fatalf("runtime metadata leaked into serialization: %s", data)
		}
	}
}
