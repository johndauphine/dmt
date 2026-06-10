package orchestrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/johndauphine/dmt/internal/audit"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
	"os"
)

const (
	schemaContractEntityTables   = "tables"
	schemaContractEntityColumns  = "columns"
	schemaContractEntityDataType = "data_type"

	schemaContractActionEvolved        = "evolved"
	schemaContractActionFrozen         = "frozen"
	schemaContractActionDiscardedRow   = "discarded_row"
	schemaContractActionDiscardedValue = "discarded_value"
	schemaContractActionReported       = "reported"
	schemaContractActionBlocked        = "blocked"
)

type schemaContractDecision struct {
	Entity   string `json:"entity"`
	Mode     string `json:"mode"`
	Drift    string `json:"drift_kind"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table"`
	Object   string `json:"object,omitempty"`
	Previous string `json:"previous,omitempty"`
	Current  string `json:"current,omitempty"`
	Action   string `json:"action"`
	Reason   string `json:"reason"`
}

func (o *Orchestrator) schemaContractDecisions(
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
) []schemaContractDecision {
	if o.config == nil || !o.config.Migration.SchemaContractEnabled() || !report.HasChanges() {
		return nil
	}

	decisions := make([]schemaContractDecision, 0, len(report.Changes))
	for _, change := range report.Changes {
		entity := schemaContractEntity(change)
		if entity == "" {
			continue
		}
		mode := o.schemaContractModeForEntity(entity)
		action, reason := o.schemaContractActionAndReason(change, report, entity, mode, tables, allowSchemaEvolution)
		decisions = append(decisions, schemaContractDecision{
			Entity:   entity,
			Mode:     string(mode),
			Drift:    string(change.Kind),
			Schema:   change.Schema,
			Table:    change.TableName,
			Object:   change.ObjectName,
			Previous: change.Previous,
			Current:  change.Current,
			Action:   action,
			Reason:   reason,
		})
	}
	return resolveSchemaContractDecisionPrecedence(decisions)
}

type schemaContractDiscardRowSkip struct {
	entity string
	mode   string
}

func resolveSchemaContractDecisionPrecedence(decisions []schemaContractDecision) []schemaContractDecision {
	skippedTables := make(map[string]schemaContractDiscardRowSkip)
	for _, decision := range decisions {
		if decision.Action != schemaContractActionDiscardedRow {
			continue
		}
		skippedTables[schemaContractDecisionTableKey(decision)] = schemaContractDiscardRowSkip{
			entity: decision.Entity,
			mode:   decision.Mode,
		}
	}
	if len(skippedTables) == 0 {
		return decisions
	}

	resolved := make([]schemaContractDecision, len(decisions))
	copy(resolved, decisions)
	for i := range resolved {
		decision := &resolved[i]
		if decision.Action == schemaContractActionDiscardedRow ||
			decision.Action == schemaContractActionFrozen {
			continue
		}
		skip, ok := skippedTables[schemaContractDecisionTableKey(*decision)]
		if !ok {
			continue
		}
		decision.Action = schemaContractActionReported
		decision.Reason = fmt.Sprintf(
			"%s=%s skips table %s for this run; %s drift is reported without target changes",
			skip.entity,
			skip.mode,
			schemaContractDecisionTableLabel(*decision),
			decision.Entity,
		)
	}
	return resolved
}

func schemaContractDecisionTableKey(decision schemaContractDecision) string {
	return schemaEvolutionTableKey(decision.Schema, decision.Table)
}

func schemaContractDecisionTableLabel(decision schemaContractDecision) string {
	if decision.Schema == "" {
		return decision.Table
	}
	return decision.Schema + "." + decision.Table
}

func (o *Orchestrator) schemaContractModeForEntity(entity string) config.SchemaContractMode {
	switch entity {
	case schemaContractEntityTables:
		return o.config.Migration.SchemaContractTablesMode()
	case schemaContractEntityColumns:
		return o.config.Migration.SchemaContractColumnsMode()
	case schemaContractEntityDataType:
		return o.config.Migration.SchemaContractDataTypeMode()
	default:
		return config.SchemaContractReport
	}
}

func schemaContractEntity(change drift.Change) string {
	switch change.Kind {
	case drift.TableAdded, drift.TableDropped:
		return schemaContractEntityTables
	case drift.AddedColumn, drift.DroppedColumn:
		return schemaContractEntityColumns
	case drift.NullabilityChange, drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
		return schemaContractEntityDataType
	default:
		return ""
	}
}

func (o *Orchestrator) schemaContractActionAndReason(
	change drift.Change,
	report drift.Report,
	entity string,
	mode config.SchemaContractMode,
	tables []source.Table,
	allowSchemaEvolution bool,
) (string, string) {
	switch mode {
	case config.SchemaContractFreeze:
		return schemaContractActionFrozen, fmt.Sprintf("%s=freeze blocks %s before transfer", entity, change.Kind)
	case config.SchemaContractReport:
		return schemaContractActionReported, fmt.Sprintf("%s=report records drift without target schema changes", entity)
	case config.SchemaContractDiscardRow:
		return schemaContractDiscardRowAction(change, entity)
	case config.SchemaContractDiscardValue:
		return o.schemaContractDiscardValueAction(change, entity, tables)
	case "", config.SchemaContractEvolve:
		return o.schemaContractEvolveAction(change, report, entity, tables, allowSchemaEvolution)
	default:
		return schemaContractActionReported, fmt.Sprintf("%s has unrecognized mode %q", entity, mode)
	}
}

func schemaContractDiscardRowAction(change drift.Change, entity string) (string, string) {
	switch entity {
	case schemaContractEntityTables:
		if change.Kind == drift.TableAdded {
			return schemaContractActionDiscardedRow, "tables=discard_row skips newly added source tables for this run"
		}
		return schemaContractActionReported, "tables=discard_row reports dropped source tables and retains target tables"
	case schemaContractEntityColumns:
		if change.Kind == drift.AddedColumn {
			return schemaContractActionDiscardedRow, "columns=discard_row skips tables with newly added source columns"
		}
		return schemaContractActionReported, "columns=discard_row reports dropped source columns and omits them from writes"
	case schemaContractEntityDataType:
		return schemaContractActionDiscardedRow, "data_type=discard_row skips tables with data type/nullability drift"
	default:
		return schemaContractActionReported, "discard_row is not defined for this drift category"
	}
}

func (o *Orchestrator) schemaContractDiscardValueAction(
	change drift.Change,
	entity string,
	tables []source.Table,
) (string, string) {
	switch entity {
	case schemaContractEntityColumns:
		if table, ok := schemaContractDecisionTable(tables, change); ok {
			discardCols := map[string]struct{}{change.ObjectName: {}}
			if err := rejectDiscardedAddedRequiredColumns(
				table,
				discardCols,
				o.config.Migration.DateUpdatedColumns,
			); err != nil {
				return schemaContractActionBlocked, err.Error()
			}
		}
		if change.Kind == drift.AddedColumn {
			return schemaContractActionDiscardedValue, "columns=discard_value omits newly added source columns from the effective plan"
		}
		return schemaContractActionReported, "columns=discard_value reports dropped source columns and retains target columns"
	case schemaContractEntityDataType:
		if table, ok := schemaContractDecisionTable(tables, change); ok {
			discardCols := map[string]struct{}{change.ObjectName: {}}
			if err := rejectDiscardedRequiredDataTypeColumns(
				table,
				discardCols,
				o.config.Migration.DateUpdatedColumns,
			); err != nil {
				return schemaContractActionBlocked, err.Error()
			}
		}
		return schemaContractActionDiscardedValue, "data_type=discard_value omits affected non-required columns from transfer and validation"
	default:
		return schemaContractActionReported, "discard_value is not supported for this entity and is reported only"
	}
}

func rejectDiscardedAddedRequiredColumns(table source.Table, discardCols map[string]struct{}, dateUpdatedColumns []string) error {
	for _, pk := range table.PrimaryKey {
		if stringSetContains(discardCols, pk) {
			return fmt.Errorf(
				"schema evolution cannot discard added primary-key column %s.%s",
				table.FullName(), pk,
			)
		}
	}
	if stringSetContains(discardCols, table.DateColumn) {
		return fmt.Errorf(
			"schema evolution cannot discard added date tracking column %s.%s",
			table.FullName(), table.DateColumn,
		)
	}
	for _, candidate := range dateUpdatedColumns {
		if stringSetContains(discardCols, candidate) && tableHasColumn(table, candidate) {
			return fmt.Errorf(
				"schema evolution cannot discard added date tracking column %s.%s",
				table.FullName(), candidate,
			)
		}
	}
	for _, column := range table.Columns {
		if stringSetContains(discardCols, column.Name) && column.IsIdentity {
			return fmt.Errorf(
				"schema evolution cannot discard added identity column %s.%s",
				table.FullName(), column.Name,
			)
		}
	}
	return nil
}

func schemaContractDecisionTable(tables []source.Table, change drift.Change) (source.Table, bool) {
	for _, table := range tables {
		if table.Schema == change.Schema && table.Name == change.TableName {
			return table, true
		}
	}
	return source.Table{}, false
}

func (o *Orchestrator) schemaContractEvolveAction(
	change drift.Change,
	report drift.Report,
	entity string,
	tables []source.Table,
	allowSchemaEvolution bool,
) (string, string) {
	if !allowSchemaEvolution {
		return schemaContractActionReported, fmt.Sprintf("%s=evolve is reported only in read-only mode", entity)
	}

	switch entity {
	case schemaContractEntityTables:
		if change.Kind == drift.TableAdded {
			if o.config.Migration.TargetMode == "upsert" {
				if table, ok := schemaContractDecisionTable(tables, change); ok && !table.HasPK() {
					return schemaContractActionBlocked, fmt.Sprintf(
						"schema contract cannot evolve added table %s: upsert mode requires a primary key",
						table.FullName(),
					)
				}
			}
			return schemaContractActionEvolved, "tables=evolve creates newly added target tables where needed"
		}
		return schemaContractActionReported, "tables=evolve reports dropped source tables and retains target tables"
	case schemaContractEntityColumns:
		if change.Kind == drift.AddedColumn {
			if o.config.Migration.TargetMode != "upsert" {
				return schemaContractActionEvolved, fmt.Sprintf(
					"columns=evolve accepts newly added source columns through target_mode=%s",
					o.config.Migration.TargetMode,
				)
			}
			if table, column, ok := schemaContractDecisionColumn(tables, change); ok {
				if column.IsIdentity {
					return schemaContractActionBlocked, fmt.Sprintf(
						"schema evolution cannot auto-add identity column %s.%s",
						table.Name, column.Name,
					)
				}
				if tablePrimaryKeyContains(table, column.Name) {
					return schemaContractActionBlocked, fmt.Sprintf(
						"schema evolution cannot auto-add primary-key column %s.%s",
						table.Name, column.Name,
					)
				}
			}
			return schemaContractActionEvolved, "columns=evolve adds compatible source columns where needed"
		}
		return schemaContractActionReported, "columns=evolve reports dropped source columns and omits them from writes"
	case schemaContractEntityDataType:
		if o.config.Migration.TargetMode != "upsert" {
			return schemaContractActionEvolved, fmt.Sprintf(
				"data_type=evolve accepts source type/nullability drift through target_mode=%s",
				o.config.Migration.TargetMode,
			)
		}
		table, column, hasColumn := schemaContractDecisionColumn(tables, change)
		if hasColumnDefaultDrift(report, change) || hasPrimaryKeyDrift(report, change) {
			return schemaContractActionBlocked, "data_type=evolve blocks type/nullability drift when default or primary-key drift is also present"
		}
		if change.Kind == drift.TypeWidened && hasColumnNullabilityDrift(report, change) {
			return schemaContractActionBlocked, "data_type=evolve blocks type widening while nullability drift is also present"
		}
		if isNullabilityRelaxation(change) && hasColumnTypeDrift(report, change) {
			return schemaContractActionBlocked, "data_type=evolve blocks nullability relaxation while type drift is also present"
		}
		if hasColumn {
			if column.IsIdentity {
				return schemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-evolve identity column %s.%s",
					table.Name, column.Name,
				)
			}
			if tablePrimaryKeyContains(table, column.Name) {
				return schemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-evolve primary-key column %s.%s",
					table.Name, column.Name,
				)
			}
			if change.Kind == drift.NullabilityChange &&
				(change.Previous != "NOT NULL" || change.Current != "NULL" || !column.IsNullable) {
				return schemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-tighten nullability for %s.%s (%s -> %s)",
					table.Name, column.Name, change.Previous, change.Current,
				)
			}
		}
		if change.Kind == drift.TypeWidened || isNullabilityRelaxation(change) {
			return schemaContractActionEvolved, "data_type=evolve applies deterministic safe type/nullability changes"
		}
		return schemaContractActionBlocked, "data_type=evolve blocks unsafe narrowed, lossy, or tightening changes before transfer"
	default:
		return schemaContractActionReported, "drift category is outside schema_contract coverage"
	}
}

func schemaContractDecisionColumn(tables []source.Table, change drift.Change) (source.Table, source.Column, bool) {
	for _, table := range tables {
		if table.Schema != change.Schema || table.Name != change.TableName {
			continue
		}
		for _, column := range table.Columns {
			if column.Name == change.ObjectName {
				return table, column, true
			}
		}
		return table, source.Column{}, false
	}
	return source.Table{}, source.Column{}, false
}

func isNullabilityRelaxation(change drift.Change) bool {
	return change.Kind == drift.NullabilityChange &&
		change.Previous == "NOT NULL" &&
		change.Current == "NULL"
}

func (o *Orchestrator) auditSchemaContractDecisions(decisions []schemaContractDecision) {
	if len(decisions) == 0 {
		return
	}
	o.auditEvent("schema_contract_decisions", map[string]any{
		"count":     len(decisions),
		"decisions": schemaContractDecisionAuditPayload(decisions),
	})
}

func schemaContractDecisionAuditPayload(decisions []schemaContractDecision) []any {
	out := make([]any, 0, len(decisions))
	for _, decision := range decisions {
		payload := map[string]any{
			"entity":     decision.Entity,
			"mode":       decision.Mode,
			"drift_kind": decision.Drift,
			"table":      decision.Table,
			"action":     decision.Action,
			"reason":     decision.Reason,
		}
		if decision.Schema != "" {
			payload["schema"] = decision.Schema
		}
		if decision.Object != "" {
			payload["object"] = decision.Object
		}
		if decision.Previous != "" {
			payload["previous"] = decision.Previous
		}
		if decision.Current != "" {
			payload["current"] = decision.Current
		}
		out = append(out, payload)
	}
	return out
}

func formatSchemaContractViolation(decision schemaContractDecision) string {
	target := decision.Table
	if decision.Schema != "" {
		target = decision.Schema + "." + target
	}
	if decision.Entity != schemaContractEntityTables && decision.Object != "" {
		target += "." + decision.Object
	}
	if decision.Reason == "" {
		return fmt.Sprintf("%s=%s blocked %s on %s", decision.Entity, decision.Mode, decision.Drift, target)
	}
	return fmt.Sprintf("%s=%s blocked %s on %s: %s", decision.Entity, decision.Mode, decision.Drift, target, decision.Reason)
}

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
