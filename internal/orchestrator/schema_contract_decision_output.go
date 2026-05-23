package orchestrator

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/johndauphine/dmt/internal/audit"
	"github.com/johndauphine/dmt/internal/logging"
)

func (o *Orchestrator) schemaContractDecisionOutputForRun(runID string) []SchemaContractDecision {
	if o == nil || o.config == nil || runID == "" {
		return nil
	}

	decisions, err := readSchemaContractDecisionsFromAudit(o.config.Migration.AuditDir, runID)
	if err == nil && len(decisions) > 0 {
		return decisions
	}
	if err != nil && !os.IsNotExist(err) {
		logging.Debug("reading schema contract decisions from audit log: %v", err)
	}
	if o.schemaContractDecisionRunID == runID && len(o.lastSchemaContractDecisions) > 0 {
		return cloneSchemaContractDecisions(o.lastSchemaContractDecisions)
	}
	return nil
}

func readSchemaContractDecisionsFromAudit(auditDir, runID string) ([]SchemaContractDecision, error) {
	path, err := audit.ResolveFilePath(auditDir, runID)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var latest []SchemaContractDecision
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var event struct {
			Type      string                   `json:"type"`
			Decisions []SchemaContractDecision `json:"decisions"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		if event.Type == "schema_contract_decisions" {
			latest = cloneSchemaContractDecisions(event.Decisions)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return latest, nil
}

func cloneSchemaContractDecisions(decisions []SchemaContractDecision) []SchemaContractDecision {
	return append([]SchemaContractDecision(nil), decisions...)
}
