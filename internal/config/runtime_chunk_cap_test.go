package config

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"strconv"
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

func completeRuntimeMemoryProfile(rowBytes int64) tuning.MemoryProfile {
	return tuning.NewMemoryProfile([]tuning.TableMemoryStat{{
		Name:        "large_table",
		RowCount:    math.MaxInt64,
		AvgRowBytes: rowBytes,
	}})
}

func TestApplyTunerSuggestionsDerivesRuntimeMemoryCapForPostgresAndMSSQL(t *testing.T) {
	for _, targetType := range []string{"postgres", "mssql"} {
		t.Run(targetType, func(t *testing.T) {
			cfg := runtimeCapTestConfig(1_024)
			cfg.Target.Type = targetType
			changes := cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
				Workers:                 4,
				ReadAheadBuffers:        4,
				WriteAheadWriters:       2,
				SafetyRowBytes:          8_192,
				SafetyRowBytesKnown:     true,
				RuntimeMemoryProfile:    completeRuntimeMemoryProfile(8_192),
				ChunkSizeRecommendation: 50_000,
			})

			want := positiveInt64ToInt(tuning.SafeChunkSize(1_024, 4, 4, 2, 8_192))
			if cfg.Migration.RuntimeChunkSizeCap != want || want <= 0 {
				t.Fatalf("runtime cap = %d, want memory cap %d", cfg.Migration.RuntimeChunkSizeCap, want)
			}
			if !cfg.Migration.RuntimeChunkGrowthAllowed {
				t.Fatal("observed-width memory cap should authorize resource growth")
			}
			// The cap is materialized before history save and job construction so
			// every configured chunk view already describes the tuple that runs.
			if cfg.Migration.ChunkSize != want || cfg.Source.ChunkSize != want || cfg.Target.ChunkSize != want {
				t.Fatalf("materialized chunk views: migration=%d source=%d target=%d, want all %d",
					cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize, want)
			}
			foundSafetyChange := false
			for _, change := range changes {
				if change.Name == "chunk_size (runtime safety cap)" && change.OldValue == 50_000 && change.NewValue == int64(want) {
					foundSafetyChange = true
				}
			}
			if !foundSafetyChange {
				t.Fatalf("ApplyTunerSuggestions changes = %+v, want runtime safety projection 50000 -> %d", changes, want)
			}
		})
	}
}

func TestRuntimeChunkSizeCapForUsesMinimumMemoryAndProtocolCap(t *testing.T) {
	cfg := runtimeCapTestConfig(1_024)
	cfg.Migration.RuntimeSafetyRowBytes = 8_192
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.Migration.RuntimeMemoryProfile = completeRuntimeMemoryProfile(8_192)
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

func TestRuntimeChunkSizeCapUsesCardinalityAwareProfile(t *testing.T) {
	cfg := runtimeCapTestConfig(1)
	cfg.Migration.Workers = 1
	cfg.Migration.ReadAheadBuffers = 1
	cfg.Migration.WriteAheadWriters = 0
	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		SafetyRowBytes:      36_864,
		SafetyRowBytesKnown: true,
		RuntimeMemoryProfile: tuning.NewMemoryProfile([]tuning.TableMemoryStat{
			{Name: "tiny_lookup", RowCount: 2, AvgRowBytes: 36_864},
			{Name: "large_table", RowCount: 1_000_000, AvgRowBytes: 100},
		}),
	})

	if got, want := cfg.Migration.RuntimeChunkSizeCap, 10_485; got != want {
		t.Fatalf("cardinality-aware runtime cap = %d, want %d", got, want)
	}
	if !cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatal("complete cardinality evidence should authorize fitting resource growth")
	}
	if scalar := tuning.SafeChunkSize(1, 1, 1, 0, 36_864); scalar >= int64(cfg.Migration.RuntimeChunkSizeCap) {
		t.Fatalf("fixture did not separate scalar cap %d from table-aware cap %d", scalar, cfg.Migration.RuntimeChunkSizeCap)
	}
}

func TestIncompleteRuntimeProfileFallsBackButDisablesGrowth(t *testing.T) {
	cfg := runtimeCapTestConfig(1)
	cfg.Migration.Workers = 1
	cfg.Migration.ReadAheadBuffers = 1
	cfg.Migration.WriteAheadWriters = 0
	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		SafetyRowBytes:      2_000,
		SafetyRowBytesKnown: true,
		RuntimeMemoryProfile: tuning.NewMemoryProfile([]tuning.TableMemoryStat{
			{Name: "unknown_cardinality", RowCount: 0, AvgRowBytes: 2_000},
		}),
	})

	if got, want := cfg.Migration.RuntimeChunkSizeCap, 524; got != want {
		t.Fatalf("incomplete-profile scalar fallback cap = %d, want %d", got, want)
	}
	if cfg.Migration.RuntimeChunkGrowthAllowed || cfg.RuntimeChunkGrowthCapFor(1, 1, 0) != 0 {
		t.Fatal("incomplete cardinality evidence authorized runtime growth")
	}
}

func TestApplyTunerSuggestionsUsesEffectivePinnedTuple(t *testing.T) {
	cfg := runtimeCapTestConfig(128)
	cfg.Migration.Workers = 8
	cfg.Migration.ChunkSize = 50_000
	cfg.Migration.ReadAheadBuffers = 6
	cfg.Migration.WriteAheadWriters = 4
	cfg.Migration.ParallelReaders = 3
	cfg.autoConfig.OriginalWorkers = 8
	cfg.autoConfig.OriginalChunkSize = 50_000
	cfg.autoConfig.OriginalReadAheadBuffers = 6
	cfg.autoConfig.OriginalWriteAheadWriters = 4
	cfg.autoConfig.OriginalParallelReaders = 3
	cfg.autoConfig.TunableValueProvenance = map[string]ConfigValueProvenance{
		provenanceMigrationWorkers:           ProvenanceUserConfig,
		provenanceMigrationChunkSize:         ProvenanceUserConfig,
		provenanceMigrationReadAheadBuffers:  ProvenanceUserConfig,
		provenanceMigrationWriteAheadWriters: ProvenanceUserConfig,
		provenanceMigrationParallelReaders:   ProvenanceUserConfig,
	}
	pinned := make(map[string]bool)
	for _, name := range cfg.PinnedTunables() {
		pinned[name] = true
	}
	for _, name := range []string{
		TunableWorkers,
		TunableChunkSize,
		TunableWriteAheadWriters,
		TunableParallelReaders,
		TunableReadAheadBuffers,
	} {
		if !pinned[name] {
			t.Fatalf("PinnedTunables() = %v, missing candidate axis %q", cfg.PinnedTunables(), name)
		}
	}

	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		Workers:                 2,
		ReadAheadBuffers:        2,
		WriteAheadWriters:       1,
		ParallelReaders:         1,
		SafetyRowBytes:          4_096,
		SafetyRowBytesKnown:     true,
		RuntimeMemoryProfile:    completeRuntimeMemoryProfile(4_096),
		ChunkSizeRecommendation: 90_000,
	})

	if cfg.Migration.Workers != 8 || cfg.Migration.ReadAheadBuffers != 6 ||
		cfg.Migration.WriteAheadWriters != 4 || cfg.Migration.ParallelReaders != 3 {
		t.Fatalf("pinned tuple was overwritten: %+v", cfg.Migration)
	}
	want := positiveInt64ToInt(tuning.SafeChunkSize(128, 8, 6, 4, 4_096))
	if cfg.Migration.RuntimeChunkSizeCap != want {
		t.Fatalf("runtime cap = %d, want %d from pinned tuple", cfg.Migration.RuntimeChunkSizeCap, want)
	}
	if cfg.Migration.ChunkSize != want || cfg.Source.ChunkSize != want || cfg.Target.ChunkSize != want {
		t.Fatalf("pinned requested chunk was not safety-projected consistently: migration=%d source=%d target=%d, want %d",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize, want)
	}
	if summary := cfg.TuningProvenanceSummary(); !strings.Contains(summary,
		"chunk_size="+strconv.Itoa(want)+" (requested 50000; safety-capped)") {
		t.Fatalf("pinned chunk provenance did not retain requested/effective identity: %q", summary)
	}
}

func TestMaterializeRuntimeChunkSizeCapIsIdempotentAndNeverRaisesEndpointViews(t *testing.T) {
	cfg := &Config{
		Migration: MigrationConfig{ChunkSize: 1_000, RuntimeChunkSizeCap: 400},
		Source:    SourceConfig{ChunkSize: 250},
		Target:    TargetConfig{ChunkSize: 800},
	}

	before, after := cfg.MaterializeRuntimeChunkSizeCap()
	if before != 1_000 || after != 400 {
		t.Fatalf("materialization delta = %d -> %d, want 1000 -> 400", before, after)
	}
	if cfg.Migration.ChunkSize != 400 || cfg.Source.ChunkSize != 250 || cfg.Target.ChunkSize != 400 {
		t.Fatalf("materialized views = migration:%d source:%d target:%d, want 400/250/400",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}

	before, after = cfg.MaterializeRuntimeChunkSizeCap()
	if before != 400 || after != 400 || cfg.Source.ChunkSize != 250 || cfg.Target.ChunkSize != 400 {
		t.Fatalf("second materialization was not idempotent: delta=%d->%d views=%d/%d/%d",
			before, after, cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
}

func TestBeginRuntimeChunkSizeProjectionRestoresNominalViewsAcrossRuns(t *testing.T) {
	cfg := &Config{
		Migration: MigrationConfig{ChunkSize: 1_000, RuntimeChunkSizeCap: 400},
		Source:    SourceConfig{ChunkSize: 900},
		Target:    TargetConfig{ChunkSize: 800},
	}

	cfg.MaterializeRuntimeChunkSizeCap()
	if cfg.Migration.ChunkSize != 400 || cfg.Source.ChunkSize != 400 || cfg.Target.ChunkSize != 400 {
		t.Fatalf("first projected views = %d/%d/%d, want 400/400/400",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}

	cfg.BeginRuntimeChunkSizeProjection()
	if cfg.Migration.ChunkSize != 1_000 || cfg.Source.ChunkSize != 900 || cfg.Target.ChunkSize != 800 {
		t.Fatalf("restored nominal views = %d/%d/%d, want 1000/900/800",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}

	// A later tuning suggestion becomes the next nominal request, while a
	// looser cap is still materialized consistently for the run.
	cfg.Migration.ChunkSize = 1_200
	cfg.Source.ChunkSize = 1_100
	cfg.Target.ChunkSize = 1_000
	cfg.Migration.RuntimeChunkSizeCap = 700
	cfg.MaterializeRuntimeChunkSizeCap()
	if cfg.Migration.ChunkSize != 700 || cfg.Source.ChunkSize != 700 || cfg.Target.ChunkSize != 700 {
		t.Fatalf("second projected views = %d/%d/%d, want 700/700/700",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}

	cfg.BeginRuntimeChunkSizeProjection()
	if cfg.Migration.ChunkSize != 1_200 || cfg.Source.ChunkSize != 1_100 || cfg.Target.ChunkSize != 1_000 {
		t.Fatalf("restored updated nominal views = %d/%d/%d, want 1200/1100/1000",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
}

func TestRuntimeChunkCapUnknownWidthCanShrinkButGrowthStaysDisabled(t *testing.T) {
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
		t.Fatalf("combined fallback/protocol cap = %d, want 750", cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.RuntimeChunkGrowthAllowed {
		t.Fatal("protocol-only cap must not authorize resource growth")
	}
	if got := cfg.RuntimeChunkGrowthCapFor(4, 4, 2); got != 0 {
		t.Fatalf("unknown width growth cap = %d, want fail-closed 0", got)
	}
}

func TestApplyTunerSuggestionsUnknownWidthMaterializesPinnedChunkShrinkOnly(t *testing.T) {
	cfg := runtimeCapTestConfig(64)
	cfg.Migration.Workers = 12
	cfg.Migration.ReadAheadBuffers = 4
	cfg.Migration.WriteAheadWriters = 8
	cfg.autoConfig.OriginalWorkers = 12
	cfg.autoConfig.OriginalChunkSize = 50_000
	cfg.autoConfig.OriginalReadAheadBuffers = 4
	cfg.autoConfig.OriginalWriteAheadWriters = 8
	cfg.autoConfig.TunableValueProvenance = map[string]ConfigValueProvenance{
		provenanceMigrationWorkers:           ProvenanceUserConfig,
		provenanceMigrationChunkSize:         ProvenanceUserConfig,
		provenanceMigrationReadAheadBuffers:  ProvenanceUserConfig,
		provenanceMigrationWriteAheadWriters: ProvenanceUserConfig,
	}

	cfg.ApplyTunerSuggestions(&driver.SmartConfigSuggestions{
		Workers:                 12,
		ChunkSizeRecommendation: 932,
		ReadAheadBuffers:        4,
		WriteAheadWriters:       8,
		SafetyRowBytes:          500,
		SafetyRowBytesKnown:     false,
	})

	want := positiveInt64ToInt(tuning.SafeChunkSize(64, 12, 4, 8, 500))
	if want != 932 || cfg.Migration.RuntimeChunkSizeCap != want {
		t.Fatalf("fallback cap = %d (config %d), want 932", want, cfg.Migration.RuntimeChunkSizeCap)
	}
	if cfg.Migration.ChunkSize != want || cfg.Source.ChunkSize != want || cfg.Target.ChunkSize != want {
		t.Fatalf("fallback materialized views = migration:%d source:%d target:%d, want all %d",
			cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize, want)
	}
	if cfg.Migration.RuntimeChunkGrowthAllowed || cfg.RuntimeChunkGrowthCapFor(12, 4, 8) != 0 {
		t.Fatal("unobserved fallback shrink authorized runtime growth")
	}
	if summary := cfg.TuningProvenanceSummary(); !strings.Contains(summary,
		"chunk_size=932 (requested 50000; safety-capped)") {
		t.Fatalf("fallback pin provenance = %q", summary)
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
		RuntimeMemoryProfile: tuning.NewMemoryProfile([]tuning.TableMemoryStat{{
			Name: "wide", RowCount: 1, AvgRowBytes: 1024 * 1024,
		}}),
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
	cfg.Migration.RuntimeMemoryProfile = completeRuntimeMemoryProfile(400 * 1024)
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
	cfg.Migration.RuntimeMemoryProfile = completeRuntimeMemoryProfile(8_192)
	cfg.Migration.RuntimeChunkGrowthAllowed = true
	cfg.Migration.TargetHardChunkLimit = 250

	cfg.ResetRuntimeChunkSafety()
	if cfg.Migration.RuntimeChunkSizeCap != 0 || cfg.Migration.RuntimeSafetyRowBytes != 0 ||
		cfg.Migration.RuntimeSafetyRowBytesKnown || cfg.Migration.RuntimeMemoryProfile.Len() != 0 ||
		cfg.Migration.RuntimeChunkGrowthAllowed {
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
	before, after := cfg.MaterializeRuntimeChunkSizeCap()
	if before != 50_000 || after != 250 || cfg.Source.ChunkSize != 250 || cfg.Target.ChunkSize != 250 {
		t.Fatalf("degraded cap materialization = delta %d->%d views=%d/%d/%d, want 50000->250 and all 250",
			before, after, cfg.Migration.ChunkSize, cfg.Source.ChunkSize, cfg.Target.ChunkSize)
	}
}

func TestRuntimeChunkCapExtremeInputsDoNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cfg := runtimeCapTestConfig(maxMemoryEnvelopeMB)
	cfg.Migration.RuntimeSafetyRowBytes = math.MaxInt64
	cfg.Migration.RuntimeSafetyRowBytesKnown = true
	cfg.Migration.RuntimeMemoryProfile = completeRuntimeMemoryProfile(math.MaxInt64)
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
		RuntimeMemoryProfile: tuning.NewMemoryProfile([]tuning.TableMemoryStat{{
			Name: "serialization_sentinel", RowCount: 2, AvgRowBytes: 8_192,
		}}),
		RuntimeChunkGrowthAllowed: true,
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
			strings.Contains(data, "runtimechunk") || strings.Contains(data, "runtimesafety") ||
			strings.Contains(data, "serialization_sentinel") {
			t.Fatalf("runtime metadata leaked into serialization: %s", data)
		}
	}
	baselineJSON, err := json.Marshal(MigrationConfig{})
	if err != nil {
		t.Fatalf("json.Marshal baseline: %v", err)
	}
	baselineYAML, err := yaml.Marshal(MigrationConfig{})
	if err != nil {
		t.Fatalf("yaml.Marshal baseline: %v", err)
	}
	if !bytes.Equal(jsonData, baselineJSON) || !bytes.Equal(yamlData, baselineYAML) {
		t.Fatalf("runtime-only metadata changed serialized identity:\njson=%s\nbaseline=%s\nyaml=%s\nbaseline=%s",
			jsonData, baselineJSON, yamlData, baselineYAML)
	}
}
