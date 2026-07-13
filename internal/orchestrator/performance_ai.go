package orchestrator

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

// BuildPerformanceExplanationPayload converts deterministic smartconfig output
// and persisted runtime history into the redacted AI advisory payload.
func (o *Orchestrator) BuildPerformanceExplanationPayload(suggestions *driver.SmartConfigSuggestions) aicopilot.PerformancePayload {
	if suggestions == nil {
		suggestions = &driver.SmartConfigSuggestions{}
	}
	input := o.performanceAutoTuneInput(suggestions)
	recent, adjustments := o.performanceHistoryForAI(input)
	return aicopilot.BuildPerformancePayload(input, *suggestions, recent, adjustments)
}

// ExplainPerformanceWithAI asks the configured AI provider to explain
// deterministic smartconfig choices. Failures never alter the suggestions.
func (o *Orchestrator) ExplainPerformanceWithAI(ctx context.Context, suggestions *driver.SmartConfigSuggestions) *aicopilot.PerformanceExplanation {
	payload := o.BuildPerformanceExplanationPayload(suggestions)
	mapper := o.aiReviewClient()
	if aicopilot.IsNilTextClient(mapper) {
		return aicopilot.UnavailablePerformanceExplanation("no AI provider configured in secrets", payload)
	}

	explanation, err := aicopilot.GeneratePerformanceExplanation(ctx, mapper, payload)
	if err != nil {
		logging.WarnEvent("AI performance explanation failed",
			"provider", mapper.ProviderName(),
			"model", mapper.Model(),
			"error", logging.Scrub(err.Error()),
		)
		return aicopilot.ErrorPerformanceExplanation(mapper.ProviderName(), mapper.Model(), err, payload)
	}
	logging.InfoEvent("AI performance explanation completed",
		"provider", explanation.Provider,
		"model", explanation.Model,
		"findings", len(explanation.Findings),
	)
	return explanation
}

func (o *Orchestrator) performanceAutoTuneInput(suggestions *driver.SmartConfigSuggestions) driver.AutoTuneInput {
	var input driver.AutoTuneInput
	if o == nil || o.config == nil {
		return input
	}

	auto := o.config.AutoConfig()
	input.CPUCores = auto.CPUCores
	if input.CPUCores <= 0 {
		input.CPUCores = runtime.NumCPU()
	}
	input.AvailableMemoryMB = auto.MemoryEnvelope.AvailableMB
	input.MaxMemoryMB = auto.MemoryEnvelope.BudgetMB
	input.MemoryBudgetMB = auto.MemoryEnvelope.BudgetMB
	input.MemoryGB = int((auto.MemoryEnvelope.CapacityMB + 1023) / 1024)
	input.Platform = driver.DetectPlatform()
	input.DatabaseType = driver.Canonicalize(o.config.Source.Type)
	input.TargetType = driver.Canonicalize(o.config.Target.Type)
	input.TargetMode = o.config.Migration.TargetMode
	input.SourceHost = o.config.Source.Host
	input.SourcePort = o.config.Source.Port
	input.SourcePortless = performanceDriverPortless(input.DatabaseType)
	input.SourceDatabase = o.config.Source.Database
	input.SourceSchema = o.config.Source.Schema
	input.TargetHost = o.config.Target.Host
	input.TargetPort = o.config.Target.Port
	input.TargetPortless = performanceDriverPortless(input.TargetType)
	input.TargetDatabase = o.config.Target.Database
	input.TargetSchema = o.config.Target.Schema

	if suggestions != nil {
		input.TotalTables = suggestions.TotalTables
		input.TotalRows = suggestions.TotalRows
		input.AvgRowBytes = suggestions.AvgRowSizeBytes
		input.RepresentativeRowBytes = suggestions.RepresentativeRowBytes
		input.SafetyRowBytes = suggestions.SafetyRowBytes
		input.SafetyRowBytesKnown = suggestions.SafetyRowBytesKnown
	}
	return input
}

func performanceDriverPortless(dbType string) bool {
	d, err := driver.Get(dbType)
	return err == nil && d.Defaults().Portless
}

func (o *Orchestrator) performanceHistoryForAI(input driver.AutoTuneInput) ([]checkpoint.TuningRecord, []checkpoint.RuntimeAdjustmentRecord) {
	if o == nil || o.state == nil || o.config == nil {
		return nil, nil
	}
	adapter := &stateHistoryAdapter{state: o.state}
	sourceType := driver.Canonicalize(o.config.Source.Type)
	targetType := driver.Canonicalize(o.config.Target.Type)

	rows, err := adapter.GetTuningHistory(0, sourceType, targetType)
	if err != nil {
		logging.DebugEvent("AI performance explanation recent run history unavailable",
			"source_db", sourceType,
			"target_db", targetType,
			"error", logging.Scrub(err.Error()),
		)
		return nil, nil
	}
	recent := make([]checkpoint.TuningRecord, 0, 5)
	for _, row := range rows {
		if row.Timestamp.IsZero() || !samePerformanceWorkload(input, row) || row.FinalThroughput <= 0 {
			continue
		}
		recent = append(recent, row)
		if len(recent) == 5 {
			break
		}
	}
	adjustments, err := adapter.GetRuntimeAdjustments(50)
	if err != nil {
		logging.DebugEvent("AI performance explanation runtime adjustment history unavailable",
			"error", logging.Scrub(err.Error()),
		)
		return recent, nil
	}
	runConfigs := o.performanceRunConfigByID()
	return recent, scopedPerformanceAdjustments(input, adjustments, runConfigs, 5)
}

func scopedPerformanceAdjustments(input driver.AutoTuneInput, adjustments []checkpoint.RuntimeAdjustmentRecord, runConfigs map[string]string, limit int) []checkpoint.RuntimeAdjustmentRecord {
	if limit <= 0 || len(adjustments) == 0 {
		return nil
	}
	scoped := make([]checkpoint.RuntimeAdjustmentRecord, 0, limit)
	for _, adjustment := range adjustments {
		if adjustment.Timestamp.IsZero() || strings.TrimSpace(adjustment.RunID) == "" {
			continue
		}
		if !samePerformanceRunConfig(input, runConfigs[adjustment.RunID]) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(adjustment.Confidence), "deterministic") {
			continue
		}
		scoped = append(scoped, adjustment)
		if len(scoped) == limit {
			break
		}
	}
	return scoped
}

func samePerformanceWorkload(input driver.AutoTuneInput, row checkpoint.TuningRecord) bool {
	return samePerformanceEndpoint(
		input.SourcePortless,
		input.SourceHost, input.SourcePort, input.SourceDatabase, input.SourceSchema,
		row.SourceHost, row.SourcePort, row.SourceDatabase, row.SourceSchema,
	) && samePerformanceEndpoint(
		input.TargetPortless,
		input.TargetHost, input.TargetPort, input.TargetDatabase, input.TargetSchema,
		row.TargetHost, row.TargetPort, row.TargetDatabase, row.TargetSchema,
	)
}

func nonEmptyEqual(a, b string) bool {
	return strings.TrimSpace(a) != "" && strings.TrimSpace(b) != "" && a == b
}

func samePerformanceRunConfig(input driver.AutoTuneInput, rawConfig string) bool {
	if strings.TrimSpace(rawConfig) == "" {
		return false
	}
	var cfg config.Config
	if err := json.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		return false
	}
	return samePerformanceEndpoint(
		input.SourcePortless,
		input.SourceHost, input.SourcePort, input.SourceDatabase, input.SourceSchema,
		cfg.Source.Host, cfg.Source.Port, cfg.Source.Database, cfg.Source.Schema,
	) && samePerformanceEndpoint(
		input.TargetPortless,
		input.TargetHost, input.TargetPort, input.TargetDatabase, input.TargetSchema,
		cfg.Target.Host, cfg.Target.Port, cfg.Target.Database, cfg.Target.Schema,
	)
}

// samePerformanceEndpoint applies the same endpoint identity distinction as
// deterministic tuning. Database/path is always required. Network endpoints
// additionally require exact non-empty host and positive port matches. Schemas
// are optional for engines without a schema concept, but always compare
// exactly. Portless endpoints ignore meaningless host/port values.
func samePerformanceEndpoint(
	portless bool,
	inputHost string,
	inputPort int,
	inputDatabase string,
	inputSchema string,
	storedHost string,
	storedPort int,
	storedDatabase string,
	storedSchema string,
) bool {
	if !nonEmptyEqual(inputDatabase, storedDatabase) {
		return false
	}
	if portless {
		return inputSchema == storedSchema
	}
	return nonEmptyEqual(inputHost, storedHost) &&
		inputPort > 0 && inputPort == storedPort &&
		inputSchema == storedSchema
}

func (o *Orchestrator) performanceRunConfigByID() map[string]string {
	if o == nil || o.state == nil {
		return nil
	}
	runs, err := o.state.GetAllRuns()
	if err != nil {
		logging.DebugEvent("AI performance explanation run config history unavailable",
			"error", logging.Scrub(err.Error()),
		)
		return nil
	}
	out := make(map[string]string, len(runs))
	for _, run := range runs {
		out[run.ID] = run.Config
	}
	return out
}
