package orchestrator

import (
	"context"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/orchestrator/schemaevolution"
	"github.com/johndauphine/dmt/internal/source"
)

// Schema drift detection, contracts, and evolution live in the
// schemaevolution sub-package (#456). The orchestrator drives them through
// these thin wrappers so run/resume/status/health-check call sites keep
// their existing shape, and the engine gets explicit dependencies instead
// of reaching into Orchestrator fields.

// SchemaDriftError and SchemaEvolutionError are re-exported so callers
// (and errors.As call sites) keep matching the orchestrator types.
type (
	SchemaDriftError     = schemaevolution.SchemaDriftError
	SchemaEvolutionError = schemaevolution.SchemaEvolutionError
)

// schemaEvolution returns the run's engine, constructing it on first use.
// Lazy because config/state/pool are populated by the Run/Resume setup
// flows; the instance then persists so contract decisions recorded during
// drift reporting stay readable by later phases.
func (o *Orchestrator) schemaEvolution() *schemaevolution.Engine {
	if o.schemaEvo == nil {
		o.schemaEvo = schemaevolution.New(o.config, o.targetPool, o.state, o.auditEvent, o.aiReviewClient)
	}
	return o.schemaEvo
}

func (o *Orchestrator) reportSchemaDrift(tables []source.Table, allowSchemaEvolution bool) (drift.Report, error) {
	return o.schemaEvolution().ReportDrift(tables, allowSchemaEvolution)
}

func (o *Orchestrator) shouldApplySchemaEvolution(report drift.Report) bool {
	return o.schemaEvolution().ShouldApplyEvolution(report)
}

func (o *Orchestrator) applySchemaEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	return o.schemaEvolution().ApplyEvolution(ctx, report, tables)
}

func (o *Orchestrator) finalizeSchemaContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) {
	o.schemaEvolution().FinalizeContractTableEvolution(ctx, report, tables)
}

func (o *Orchestrator) effectiveTablesForSchemaEvolution(report drift.Report, tables []source.Table) ([]source.Table, error) {
	return o.schemaEvolution().EffectiveTables(report, tables)
}

// ReviewSchemaDriftWithAI generates the advisory AI review of a drift
// report. Exported API preserved from before the #456 extraction.
func (o *Orchestrator) ReviewSchemaDriftWithAI(ctx context.Context, report drift.Report, tables []source.Table, allowSchemaEvolution bool) *aicopilot.SchemaAdvisorReview {
	return o.schemaEvolution().ReviewDriftWithAI(ctx, report, tables, allowSchemaEvolution)
}

func (o *Orchestrator) schemaContractDecisionOutputForRun(runID string) []SchemaContractDecision {
	return o.schemaEvolution().ContractDecisionOutputForRun(runID)
}

func (o *Orchestrator) captureSchemaSnapshotsForReport(runID string, report drift.Report, tables []source.Table) {
	o.schemaEvolution().CaptureSnapshotsForReport(runID, report, tables)
}

func (o *Orchestrator) applySchemaContractTableEvolution(ctx context.Context, report drift.Report, tables []source.Table) error {
	return o.schemaEvolution().ApplyContractTableEvolution(ctx, report, tables)
}

func (o *Orchestrator) schemaSnapshotNamespace() string {
	return o.schemaEvolution().SnapshotNamespace()
}
