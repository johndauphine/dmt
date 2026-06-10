package schemaevolution

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
	SchemaContractEntityTables   = "tables"
	SchemaContractEntityColumns  = "columns"
	SchemaContractEntityDataType = "data_type"

	SchemaContractActionEvolved        = "evolved"
	SchemaContractActionFrozen         = "frozen"
	SchemaContractActionDiscardedRow   = "discarded_row"
	SchemaContractActionDiscardedValue = "discarded_value"
	SchemaContractActionReported       = "reported"
	SchemaContractActionBlocked        = "blocked"
)

type SchemaContractDecision struct {
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

func (e *Engine) schemaContractDecisions(
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
) []SchemaContractDecision {
	if e.cfg == nil || !e.cfg.Migration.SchemaContractEnabled() || !report.HasChanges() {
		return nil
	}

	decisions := make([]SchemaContractDecision, 0, len(report.Changes))
	for _, change := range report.Changes {
		entity := schemaContractEntity(change)
		if entity == "" {
			continue
		}
		mode := e.schemaContractModeForEntity(entity)
		action, reason := e.schemaContractActionAndReason(change, report, entity, mode, tables, allowSchemaEvolution)
		decisions = append(decisions, SchemaContractDecision{
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

func resolveSchemaContractDecisionPrecedence(decisions []SchemaContractDecision) []SchemaContractDecision {
	skippedTables := make(map[string]schemaContractDiscardRowSkip)
	for _, decision := range decisions {
		if decision.Action != SchemaContractActionDiscardedRow {
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

	resolved := make([]SchemaContractDecision, len(decisions))
	copy(resolved, decisions)
	for i := range resolved {
		decision := &resolved[i]
		if decision.Action == SchemaContractActionDiscardedRow ||
			decision.Action == SchemaContractActionFrozen {
			continue
		}
		skip, ok := skippedTables[schemaContractDecisionTableKey(*decision)]
		if !ok {
			continue
		}
		decision.Action = SchemaContractActionReported
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

func schemaContractDecisionTableKey(decision SchemaContractDecision) string {
	return schemaEvolutionTableKey(decision.Schema, decision.Table)
}

func schemaContractDecisionTableLabel(decision SchemaContractDecision) string {
	if decision.Schema == "" {
		return decision.Table
	}
	return decision.Schema + "." + decision.Table
}

func (e *Engine) schemaContractModeForEntity(entity string) config.SchemaContractMode {
	switch entity {
	case SchemaContractEntityTables:
		return e.cfg.Migration.SchemaContractTablesMode()
	case SchemaContractEntityColumns:
		return e.cfg.Migration.SchemaContractColumnsMode()
	case SchemaContractEntityDataType:
		return e.cfg.Migration.SchemaContractDataTypeMode()
	default:
		return config.SchemaContractReport
	}
}

func schemaContractEntity(change drift.Change) string {
	switch change.Kind {
	case drift.TableAdded, drift.TableDropped:
		return SchemaContractEntityTables
	case drift.AddedColumn, drift.DroppedColumn:
		return SchemaContractEntityColumns
	case drift.NullabilityChange, drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
		return SchemaContractEntityDataType
	default:
		return ""
	}
}

func (e *Engine) schemaContractActionAndReason(
	change drift.Change,
	report drift.Report,
	entity string,
	mode config.SchemaContractMode,
	tables []source.Table,
	allowSchemaEvolution bool,
) (string, string) {
	switch mode {
	case config.SchemaContractFreeze:
		return SchemaContractActionFrozen, fmt.Sprintf("%s=freeze blocks %s before transfer", entity, change.Kind)
	case config.SchemaContractReport:
		return SchemaContractActionReported, fmt.Sprintf("%s=report records drift without target schema changes", entity)
	case config.SchemaContractDiscardRow:
		return schemaContractDiscardRowAction(change, entity)
	case config.SchemaContractDiscardValue:
		return e.schemaContractDiscardValueAction(change, entity, tables)
	case "", config.SchemaContractEvolve:
		return e.schemaContractEvolveAction(change, report, entity, tables, allowSchemaEvolution)
	default:
		return SchemaContractActionReported, fmt.Sprintf("%s has unrecognized mode %q", entity, mode)
	}
}

func schemaContractDiscardRowAction(change drift.Change, entity string) (string, string) {
	switch entity {
	case SchemaContractEntityTables:
		if change.Kind == drift.TableAdded {
			return SchemaContractActionDiscardedRow, "tables=discard_row skips newly added source tables for this run"
		}
		return SchemaContractActionReported, "tables=discard_row reports dropped source tables and retains target tables"
	case SchemaContractEntityColumns:
		if change.Kind == drift.AddedColumn {
			return SchemaContractActionDiscardedRow, "columns=discard_row skips tables with newly added source columns"
		}
		return SchemaContractActionReported, "columns=discard_row reports dropped source columns and omits them from writes"
	case SchemaContractEntityDataType:
		return SchemaContractActionDiscardedRow, "data_type=discard_row skips tables with data type/nullability drift"
	default:
		return SchemaContractActionReported, "discard_row is not defined for this drift category"
	}
}

func (e *Engine) schemaContractDiscardValueAction(
	change drift.Change,
	entity string,
	tables []source.Table,
) (string, string) {
	switch entity {
	case SchemaContractEntityColumns:
		if table, ok := schemaContractDecisionTable(tables, change); ok {
			discardCols := map[string]struct{}{change.ObjectName: {}}
			if err := rejectDiscardedAddedRequiredColumns(
				table,
				discardCols,
				e.cfg.Migration.DateUpdatedColumns,
			); err != nil {
				return SchemaContractActionBlocked, err.Error()
			}
		}
		if change.Kind == drift.AddedColumn {
			return SchemaContractActionDiscardedValue, "columns=discard_value omits newly added source columns from the effective plan"
		}
		return SchemaContractActionReported, "columns=discard_value reports dropped source columns and retains target columns"
	case SchemaContractEntityDataType:
		if table, ok := schemaContractDecisionTable(tables, change); ok {
			discardCols := map[string]struct{}{change.ObjectName: {}}
			if err := rejectDiscardedRequiredDataTypeColumns(
				table,
				discardCols,
				e.cfg.Migration.DateUpdatedColumns,
			); err != nil {
				return SchemaContractActionBlocked, err.Error()
			}
		}
		return SchemaContractActionDiscardedValue, "data_type=discard_value omits affected non-required columns from transfer and validation"
	default:
		return SchemaContractActionReported, "discard_value is not supported for this entity and is reported only"
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

func (e *Engine) schemaContractEvolveAction(
	change drift.Change,
	report drift.Report,
	entity string,
	tables []source.Table,
	allowSchemaEvolution bool,
) (string, string) {
	if !allowSchemaEvolution {
		return SchemaContractActionReported, fmt.Sprintf("%s=evolve is reported only in read-only mode", entity)
	}

	switch entity {
	case SchemaContractEntityTables:
		if change.Kind == drift.TableAdded {
			if e.cfg.Migration.TargetMode == "upsert" {
				if table, ok := schemaContractDecisionTable(tables, change); ok && !table.HasPK() {
					return SchemaContractActionBlocked, fmt.Sprintf(
						"schema contract cannot evolve added table %s: upsert mode requires a primary key",
						table.FullName(),
					)
				}
			}
			return SchemaContractActionEvolved, "tables=evolve creates newly added target tables where needed"
		}
		return SchemaContractActionReported, "tables=evolve reports dropped source tables and retains target tables"
	case SchemaContractEntityColumns:
		if change.Kind == drift.AddedColumn {
			if e.cfg.Migration.TargetMode != "upsert" {
				return SchemaContractActionEvolved, fmt.Sprintf(
					"columns=evolve accepts newly added source columns through target_mode=%s",
					e.cfg.Migration.TargetMode,
				)
			}
			if table, column, ok := schemaContractDecisionColumn(tables, change); ok {
				if column.IsIdentity {
					return SchemaContractActionBlocked, fmt.Sprintf(
						"schema evolution cannot auto-add identity column %s.%s",
						table.Name, column.Name,
					)
				}
				if tablePrimaryKeyContains(table, column.Name) {
					return SchemaContractActionBlocked, fmt.Sprintf(
						"schema evolution cannot auto-add primary-key column %s.%s",
						table.Name, column.Name,
					)
				}
			}
			return SchemaContractActionEvolved, "columns=evolve adds compatible source columns where needed"
		}
		return SchemaContractActionReported, "columns=evolve reports dropped source columns and omits them from writes"
	case SchemaContractEntityDataType:
		if e.cfg.Migration.TargetMode != "upsert" {
			return SchemaContractActionEvolved, fmt.Sprintf(
				"data_type=evolve accepts source type/nullability drift through target_mode=%s",
				e.cfg.Migration.TargetMode,
			)
		}
		table, column, hasColumn := schemaContractDecisionColumn(tables, change)
		if hasColumnDefaultDrift(report, change) || hasPrimaryKeyDrift(report, change) {
			return SchemaContractActionBlocked, "data_type=evolve blocks type/nullability drift when default or primary-key drift is also present"
		}
		if change.Kind == drift.TypeWidened && hasColumnNullabilityDrift(report, change) {
			return SchemaContractActionBlocked, "data_type=evolve blocks type widening while nullability drift is also present"
		}
		if isNullabilityRelaxation(change) && hasColumnTypeDrift(report, change) {
			return SchemaContractActionBlocked, "data_type=evolve blocks nullability relaxation while type drift is also present"
		}
		if hasColumn {
			if column.IsIdentity {
				return SchemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-evolve identity column %s.%s",
					table.Name, column.Name,
				)
			}
			if tablePrimaryKeyContains(table, column.Name) {
				return SchemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-evolve primary-key column %s.%s",
					table.Name, column.Name,
				)
			}
			if change.Kind == drift.NullabilityChange &&
				(change.Previous != "NOT NULL" || change.Current != "NULL" || !column.IsNullable) {
				return SchemaContractActionBlocked, fmt.Sprintf(
					"schema evolution cannot auto-tighten nullability for %s.%s (%s -> %s)",
					table.Name, column.Name, change.Previous, change.Current,
				)
			}
		}
		if change.Kind == drift.TypeWidened || isNullabilityRelaxation(change) {
			return SchemaContractActionEvolved, "data_type=evolve applies deterministic safe type/nullability changes"
		}
		return SchemaContractActionBlocked, "data_type=evolve blocks unsafe narrowed, lossy, or tightening changes before transfer"
	default:
		return SchemaContractActionReported, "drift category is outside schema_contract coverage"
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

func (e *Engine) auditSchemaContractDecisions(decisions []SchemaContractDecision) {
	if len(decisions) == 0 {
		return
	}
	e.auditEvent("schema_contract_decisions", map[string]any{
		"count":     len(decisions),
		"decisions": schemaContractDecisionAuditPayload(decisions),
	})
}

func schemaContractDecisionAuditPayload(decisions []SchemaContractDecision) []any {
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

func formatSchemaContractViolation(decision SchemaContractDecision) string {
	target := decision.Table
	if decision.Schema != "" {
		target = decision.Schema + "." + target
	}
	if decision.Entity != SchemaContractEntityTables && decision.Object != "" {
		target += "." + decision.Object
	}
	if decision.Reason == "" {
		return fmt.Sprintf("%s=%s blocked %s on %s", decision.Entity, decision.Mode, decision.Drift, target)
	}
	return fmt.Sprintf("%s=%s blocked %s on %s: %s", decision.Entity, decision.Mode, decision.Drift, target, decision.Reason)
}

func (e *Engine) ContractDecisionOutputForRun(runID string) []SchemaContractDecision {
	if e == nil || e.cfg == nil || runID == "" {
		return nil
	}

	decisions, err := readSchemaContractDecisionsFromAudit(e.cfg.Migration.AuditDir, runID)
	if err == nil && len(decisions) > 0 {
		return decisions
	}
	if err != nil && !os.IsNotExist(err) {
		logging.Debug("reading schema contract decisions from audit log: %v", err)
	}
	if e.contractDecisionRunID == runID && len(e.lastContractDecisions) > 0 {
		return cloneSchemaContractDecisions(e.lastContractDecisions)
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
