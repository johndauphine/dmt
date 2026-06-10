// Package schemaevolution owns schema drift detection, DLT-style schema
// contracts, and opt-in schema evolution (#456). Extracted from the
// orchestrator so the subsystem has explicit dependencies instead of
// reaching into Orchestrator fields; the orchestrator drives it through
// a thin bridge, one call per phase: ReportDrift → contract decisions →
// ApplyEvolution → FinalizeContractTableEvolution.
package schemaevolution

import (
	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/pool"
)

// Engine evaluates schema drift against the configured contract policy and
// applies evolution to the target. One Engine instance lives for the whole
// run: contract decisions recorded while reporting drift are read back
// later by status/triage/health-check surfaces.
type Engine struct {
	cfg        *config.Config
	targetPool pool.TargetPool
	state      checkpoint.StateBackend

	// audit emits one structured audit event; injected so the engine
	// doesn't depend on the orchestrator's auditor lifecycle.
	audit func(typeName string, fields map[string]any)
	// aiClient returns the advisory text client (nil-able — the engine
	// degrades to UnavailableSchemaAdvisorReview). Injected because
	// provider resolution lives with the orchestrator's secrets wiring.
	aiClient func() aicopilot.TextClient

	// contractDecisionRunID + lastContractDecisions cache the decisions
	// recorded for the current run so ContractDecisionOutputForRun can
	// serve them without re-reading the audit file.
	contractDecisionRunID string
	lastContractDecisions []SchemaContractDecision
}

// New wires an Engine. audit and aiClient may be nil only in tests that
// never reach the auditing / advisory paths.
func New(
	cfg *config.Config,
	targetPool pool.TargetPool,
	state checkpoint.StateBackend,
	audit func(typeName string, fields map[string]any),
	aiClient func() aicopilot.TextClient,
) *Engine {
	return &Engine{cfg: cfg, targetPool: targetPool, state: state, audit: audit, aiClient: aiClient}
}

// SetRunID records which run subsequent contract decisions belong to.
// Called when the orchestrator opens the run's auditor.
func (e *Engine) SetRunID(runID string) { e.contractDecisionRunID = runID }

// LastContractDecisions returns a defensive copy of the decisions recorded
// for the current run (empty until ReportDrift has run).
func (e *Engine) LastContractDecisions() []SchemaContractDecision {
	return cloneSchemaContractDecisions(e.lastContractDecisions)
}

// RestoreDecisions rehydrates the per-run decision cache, e.g. when a
// consumer rebuilds run context outside the ReportDrift flow.
func (e *Engine) RestoreDecisions(runID string, decisions []SchemaContractDecision) {
	e.contractDecisionRunID = runID
	e.lastContractDecisions = cloneSchemaContractDecisions(decisions)
}

// auditEvent forwards to the injected audit sink; nil-safe so tests (and
// callers that disable auditing) can leave the sink unset.
func (e *Engine) auditEvent(typeName string, fields map[string]any) {
	if e.audit != nil {
		e.audit(typeName, fields)
	}
}
