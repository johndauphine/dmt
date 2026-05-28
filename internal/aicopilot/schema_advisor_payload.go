package aicopilot

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/drift"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

func BuildSchemaAdvisorPayload(
	cfg *config.Config,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
) SchemaAdvisorPayload {
	payload := SchemaAdvisorPayload{
		PromptVersion:        SchemaAdvisorPromptVersion,
		Task:                 "Explain schema drift and recommend advisory schema evolution policy actions. Deterministic policy gates are authoritative.",
		Config:               buildConfigSummary(cfg),
		AllowSchemaEvolution: allowSchemaEvolution,
		Changes:              make([]SchemaDriftAdvisoryChange, 0, len(report.Changes)),
		Redaction: RedactionSummary{
			OmittedFields: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database",
				"target.host", "target.port", "target.user", "target.password", "target.database",
				"ai.api_key", "slack.webhook_url", "column.sample_values",
			},
			ScrubbedText: true,
		},
	}
	if cfg != nil {
		payload.SourceDBType = cfg.Source.Type
		payload.TargetDBType = cfg.Target.Type
	}

	for _, change := range report.Changes {
		advisory := schemaDriftAdvisoryChange(cfg, report, tables, allowSchemaEvolution, change)
		payload.Changes = append(payload.Changes, advisory)
		if !advisory.DeterministicGate.Allowed {
			payload.DeterministicBlockers = append(payload.DeterministicBlockers, schemaAdvisorBlocker(advisory))
		}
	}
	return payload
}

func schemaDriftAdvisoryChange(
	cfg *config.Config,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
	change drift.Change,
) SchemaDriftAdvisoryChange {
	classification, risk, reason := classifySchemaDriftChange(report, change)
	gate := schemaAdvisorPolicyGate(cfg, report, tables, allowSchemaEvolution, change)
	return SchemaDriftAdvisoryChange{
		DriftKind:         string(change.Kind),
		Classification:    classification,
		Risk:              risk,
		Schema:            logging.Scrub(change.Schema),
		Table:             logging.Scrub(change.TableName),
		Column:            logging.Scrub(change.ObjectName),
		Previous:          limitText(logging.Scrub(change.Previous), 300),
		Current:           limitText(logging.Scrub(change.Current), 300),
		Reason:            reason,
		SuggestedPolicy:   suggestedPolicyForGate(gate),
		SuggestedAction:   suggestedActionForGate(gate),
		ManualGuidance:    manualGuidanceForSchemaChange(change, gate),
		DeterministicGate: gate,
	}
}

func classifySchemaDriftChange(report drift.Report, change drift.Change) (string, string, string) {
	switch change.Kind {
	case drift.TableAdded:
		return "additive_table", SchemaRiskMedium, "New source table is additive, but upsert target creation still depends on target mode and primary-key policy."
	case drift.TableDropped:
		return "dropped_table", SchemaRiskMedium, "Source table is no longer present; DMT reports this and retains target tables unless an explicit target mode recreates them."
	case drift.AddedColumn:
		return "additive", SchemaRiskLow, "New source column is additive, but existing target rows and required-column metadata decide whether it can be auto-added or must be discarded/reported."
	case drift.DroppedColumn:
		return "dropped_column", SchemaRiskHigh, "Source column disappeared; target column retention, writes, validation, and downstream consumers need manual review."
	case drift.TypeWidened:
		if hasSchemaAdvisorCompanionRisk(report, change) {
			return "widened", SchemaRiskHigh, "Type widening is combined with nullability, default, or primary-key drift, so it is not a standalone safe widening."
		}
		return "widened", SchemaRiskLow, "Widening preserves values when the target dialect can apply the widening and no companion column drift is present."
	case drift.TypeNarrowed:
		return "narrowing", SchemaRiskBlocked, "Narrowing can truncate or reject existing values and must not be auto-applied without a manual migration."
	case drift.TypeChangedLossy:
		return "lossy_conversion", SchemaRiskBlocked, "The type conversion is potentially lossy and requires a manual migration and validation plan."
	case drift.NullabilityChange:
		if change.Previous == "NOT NULL" && change.Current == "NULL" && !hasSchemaAdvisorCompanionRisk(report, change) {
			return "nullable_relaxation", SchemaRiskLow, "Relaxing NOT NULL to NULL is generally safe when no type, default, or primary-key drift is attached to the column."
		}
		return "nullability_tightening", SchemaRiskBlocked, "Tightening nullability or combining it with other drift can reject rows and needs manual handling."
	case drift.DefaultChange:
		return "default_change", SchemaRiskMedium, "Default changes affect future writes and should be reviewed against target defaults and application expectations."
	case drift.PKChange:
		return "key_constraint_risk", SchemaRiskBlocked, "Primary-key drift changes upsert identity and pagination safety."
	case drift.IndexAdded, drift.IndexDropped:
		return "key_constraint_risk", SchemaRiskMedium, "Index drift can affect performance or uniqueness guarantees, but DMT does not use AI to apply index changes."
	case drift.FKAdded, drift.FKDropped, drift.CheckAdded, drift.CheckDropped:
		return "key_constraint_risk", SchemaRiskHigh, "Constraint drift can change referential or validation behavior and requires deterministic DDL policy review."
	default:
		return "unsupported_dialect_edge", SchemaRiskHigh, "DMT does not have a deterministic auto-apply path for this drift kind."
	}
}

func schemaAdvisorPolicyGate(
	cfg *config.Config,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
	change drift.Change,
) SchemaAdvisorPolicyGate {
	if cfg == nil {
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Reason: "configuration unavailable for deterministic policy evaluation"}
	}
	if cfg.Migration.FailOnSchemaDrift {
		return SchemaAdvisorPolicyGate{
			Allowed: false,
			Action:  "blocked",
			Policy:  "fail_on_schema_drift",
			Reason:  "migration.fail_on_schema_drift is true; transfer aborts before schema evolution",
		}
	}
	if cfg.Migration.SchemaContractEnabled() {
		return schemaContractAdvisorGate(cfg, report, tables, allowSchemaEvolution, change)
	}
	return schemaEvolutionAdvisorGate(cfg, report, tables, allowSchemaEvolution, change)
}

func schemaContractAdvisorGate(
	cfg *config.Config,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
	change drift.Change,
) SchemaAdvisorPolicyGate {
	entity := schemaAdvisorContractEntity(change)
	mode := schemaAdvisorContractMode(cfg, entity)
	if mode == "" {
		mode = config.SchemaContractEvolve
	}
	policy := entity + "=" + string(mode)

	switch mode {
	case config.SchemaContractFreeze:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: policy + " blocks matching schema drift before transfer"}
	case config.SchemaContractReport:
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policy, Reason: policy + " records drift without target schema changes"}
	case config.SchemaContractDiscardRow:
		if entity == "tables" && change.Kind == drift.TableDropped {
			return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policy, Reason: "dropped source tables are reported and target tables are retained"}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "discard_row", Policy: policy, Reason: policy + " skips affected source rows or tables according to the deterministic contract"}
	case config.SchemaContractDiscardValue:
		if blocked := schemaAdvisorDiscardValueBlocker(change, tables, cfg.Migration.DateUpdatedColumns); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "discard_value", Policy: policy, Reason: policy + " omits affected values according to the deterministic contract"}
	case config.SchemaContractEvolve:
		return schemaAdvisorEvolveGate(cfg.Migration.TargetMode, policy, report, tables, allowSchemaEvolution, change)
	default:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: fmt.Sprintf("unrecognized schema contract mode %q", mode)}
	}
}

func schemaEvolutionAdvisorGate(
	cfg *config.Config,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
	change drift.Change,
) SchemaAdvisorPolicyGate {
	if !cfg.Migration.SchemaEvolutionEnabled() {
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: "schema_evolution=disabled", Reason: "schema evolution is disabled; drift is reported only"}
	}
	if !allowSchemaEvolution {
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: "read_only", Reason: "dry-run/read-only mode reports drift without target ALTERs"}
	}
	if cfg.Migration.TargetMode != "upsert" {
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "target_mode_recreate", Policy: "target_mode=" + cfg.Migration.TargetMode, Reason: "target mode handles schema by recreating or replacing target tables instead of applying incremental ALTERs"}
	}

	switch change.Kind {
	case drift.AddedColumn:
		return addedColumnSchemaEvolutionPolicyGate(cfg, change, tables)
	case drift.NullabilityChange:
		return legacySchemaEvolutionPolicyGate(
			"nullability_change",
			cfg.Migration.NullabilityChangeSchemaEvolutionPolicy(),
			func() string { return schemaAdvisorNullabilityBlocker(report, change, tables) },
		)
	case drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
		return legacySchemaEvolutionPolicyGate(
			"type_change",
			cfg.Migration.TypeChangeSchemaEvolutionPolicy(),
			func() string { return schemaAdvisorTypeBlocker(report, change, tables) },
		)
	default:
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: "unsupported", Reason: "DMT has no automatic schema evolution path for this drift kind"}
	}
}

func addedColumnSchemaEvolutionPolicyGate(cfg *config.Config, change drift.Change, tables []source.Table) SchemaAdvisorPolicyGate {
	policy := cfg.Migration.AddedColumnSchemaEvolutionPolicy()
	if policy == "" {
		policy = config.SchemaEvolutionLog
	}
	policyLabel := "added_column=" + string(policy)
	switch policy {
	case config.SchemaEvolutionAuto:
		if blocked := schemaAdvisorAddedColumnBlocker(change, tables); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policyLabel, Reason: policyLabel + " permits this deterministic safe change"}
	case config.SchemaEvolutionLog:
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policyLabel, Reason: policyLabel + " records drift without target schema changes"}
	case config.SchemaEvolutionFail:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: policyLabel + " blocks transfer before schema evolution"}
	case config.SchemaEvolutionDiscard, config.SchemaEvolutionDiscardValue:
		if blocked := schemaAdvisorDiscardValueBlocker(change, tables, cfg.Migration.DateUpdatedColumns); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "discard_value", Policy: policyLabel, Reason: policyLabel + " omits affected values according to deterministic policy"}
	default:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: fmt.Sprintf("unrecognized schema evolution policy %q", policy)}
	}
}

func legacySchemaEvolutionPolicyGate(kind string, policy config.SchemaEvolutionPolicy, blocker func() string) SchemaAdvisorPolicyGate {
	if policy == "" {
		policy = config.SchemaEvolutionLog
	}
	policyLabel := kind + "=" + string(policy)
	switch policy {
	case config.SchemaEvolutionAuto:
		if blocked := blocker(); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policyLabel, Reason: policyLabel + " permits this deterministic safe change"}
	case config.SchemaEvolutionLog:
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policyLabel, Reason: policyLabel + " records drift without target schema changes"}
	case config.SchemaEvolutionFail:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: policyLabel + " blocks transfer before schema evolution"}
	case config.SchemaEvolutionDiscard, config.SchemaEvolutionDiscardValue:
		if blocked := blocker(); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "discard_value", Policy: policyLabel, Reason: policyLabel + " omits affected values according to deterministic policy"}
	default:
		return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policyLabel, Reason: fmt.Sprintf("unrecognized schema evolution policy %q", policy)}
	}
}

func schemaAdvisorEvolveGate(
	targetMode string,
	policy string,
	report drift.Report,
	tables []source.Table,
	allowSchemaEvolution bool,
	change drift.Change,
) SchemaAdvisorPolicyGate {
	if !allowSchemaEvolution {
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policy, Reason: "read-only mode reports drift without target ALTERs"}
	}
	if targetMode != "upsert" && targetMode != "" {
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "target_mode_recreate", Policy: policy, Reason: "target_mode=" + targetMode + " handles schema through target table creation/recreation"}
	}
	switch change.Kind {
	case drift.TableAdded:
		if table, ok := schemaAdvisorSourceTable(tables, change); ok && len(table.PrimaryKey) == 0 {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: "upsert mode cannot evolve an added table without a primary key"}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policy, Reason: "new target table can be created when upsert prerequisites are satisfied"}
	case drift.AddedColumn:
		if blocked := schemaAdvisorAddedColumnBlocker(change, tables); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policy, Reason: "new nullable target column can be added before upsert transfer"}
	case drift.NullabilityChange:
		if blocked := schemaAdvisorNullabilityBlocker(report, change, tables); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policy, Reason: "nullability relaxation can be applied before upsert transfer"}
	case drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
		if blocked := schemaAdvisorTypeBlocker(report, change, tables); blocked != "" {
			return SchemaAdvisorPolicyGate{Allowed: false, Action: "blocked", Policy: policy, Reason: blocked}
		}
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "auto_apply", Policy: policy, Reason: "type widening can be applied before upsert transfer"}
	default:
		return SchemaAdvisorPolicyGate{Allowed: true, Action: "report_only", Policy: policy, Reason: "this drift kind is reported only under evolve mode"}
	}
}

func schemaAdvisorContractEntity(change drift.Change) string {
	switch change.Kind {
	case drift.TableAdded, drift.TableDropped:
		return "tables"
	case drift.AddedColumn, drift.DroppedColumn:
		return "columns"
	case drift.NullabilityChange, drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy:
		return "data_type"
	default:
		return "other"
	}
}

func schemaAdvisorContractMode(cfg *config.Config, entity string) config.SchemaContractMode {
	switch entity {
	case "tables":
		return cfg.Migration.SchemaContractTablesMode()
	case "columns":
		return cfg.Migration.SchemaContractColumnsMode()
	case "data_type":
		return cfg.Migration.SchemaContractDataTypeMode()
	default:
		return config.SchemaContractReport
	}
}

func hasSchemaAdvisorCompanionRisk(report drift.Report, candidate drift.Change) bool {
	for _, change := range report.Changes {
		if change.Schema != candidate.Schema ||
			change.TableName != candidate.TableName ||
			change.ObjectName != candidate.ObjectName {
			if change.Kind == drift.PKChange && change.Schema == candidate.Schema && change.TableName == candidate.TableName {
				return true
			}
			continue
		}
		switch change.Kind {
		case drift.TypeWidened, drift.TypeNarrowed, drift.TypeChangedLossy, drift.NullabilityChange, drift.DefaultChange:
			if change.Kind != candidate.Kind {
				return true
			}
		case drift.PKChange:
			return true
		}
	}
	return false
}

func schemaAdvisorAddedColumnBlocker(change drift.Change, tables []source.Table) string {
	table, column, ok := schemaAdvisorSourceColumn(tables, change)
	if !ok {
		return ""
	}
	if column.IsIdentity {
		return fmt.Sprintf("schema evolution cannot auto-add identity column %s.%s", table.Name, column.Name)
	}
	if schemaAdvisorTablePrimaryKeyContains(table, column.Name) {
		return fmt.Sprintf("schema evolution cannot auto-add primary-key column %s.%s", table.Name, column.Name)
	}
	return ""
}

func schemaAdvisorNullabilityBlocker(report drift.Report, change drift.Change, tables []source.Table) string {
	table, column, ok := schemaAdvisorSourceColumn(tables, change)
	if !ok {
		return ""
	}
	if hasSchemaAdvisorCompanionRisk(report, change) {
		return fmt.Sprintf("schema evolution cannot auto-relax nullability for %s.%s while companion drift is present", table.Name, column.Name)
	}
	if change.Previous != "NOT NULL" || change.Current != "NULL" || !column.IsNullable {
		return fmt.Sprintf("schema evolution cannot auto-tighten nullability for %s.%s (%s -> %s)", table.Name, column.Name, change.Previous, change.Current)
	}
	if column.IsIdentity {
		return fmt.Sprintf("schema evolution cannot auto-relax identity column %s.%s", table.Name, column.Name)
	}
	if schemaAdvisorTablePrimaryKeyContains(table, column.Name) {
		return fmt.Sprintf("schema evolution cannot auto-relax primary-key column %s.%s", table.Name, column.Name)
	}
	return ""
}

func schemaAdvisorTypeBlocker(report drift.Report, change drift.Change, tables []source.Table) string {
	table, column, ok := schemaAdvisorSourceColumn(tables, change)
	if !ok {
		return ""
	}
	if change.Kind != drift.TypeWidened {
		return fmt.Sprintf("schema evolution cannot auto-apply %s for %s.%s (%s -> %s)", change.Kind, table.Name, column.Name, change.Previous, change.Current)
	}
	if hasSchemaAdvisorCompanionRisk(report, change) {
		return fmt.Sprintf("schema evolution cannot auto-widen type for %s.%s while companion drift is present", table.Name, column.Name)
	}
	if column.IsIdentity {
		return fmt.Sprintf("schema evolution cannot auto-widen identity column %s.%s", table.Name, column.Name)
	}
	if schemaAdvisorTablePrimaryKeyContains(table, column.Name) {
		return fmt.Sprintf("schema evolution cannot auto-widen primary-key column %s.%s", table.Name, column.Name)
	}
	return ""
}

func schemaAdvisorDiscardValueBlocker(change drift.Change, tables []source.Table, dateUpdatedColumns []string) string {
	table, column, ok := schemaAdvisorSourceColumn(tables, change)
	if !ok {
		return ""
	}
	if schemaAdvisorTablePrimaryKeyContains(table, column.Name) {
		return fmt.Sprintf("schema evolution cannot discard primary-key column %s.%s", table.Name, column.Name)
	}
	if strings.EqualFold(table.DateColumn, column.Name) {
		return fmt.Sprintf("schema evolution cannot discard date tracking column %s.%s", table.Name, column.Name)
	}
	for _, candidate := range dateUpdatedColumns {
		if strings.EqualFold(candidate, column.Name) {
			return fmt.Sprintf("schema evolution cannot discard date tracking column %s.%s", table.Name, column.Name)
		}
	}
	if column.IsIdentity {
		return fmt.Sprintf("schema evolution cannot discard identity column %s.%s", table.Name, column.Name)
	}
	return ""
}

func schemaAdvisorSourceColumn(tables []source.Table, change drift.Change) (source.Table, source.Column, bool) {
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

func schemaAdvisorSourceTable(tables []source.Table, change drift.Change) (source.Table, bool) {
	for _, table := range tables {
		if table.Schema == change.Schema && table.Name == change.TableName {
			return table, true
		}
	}
	return source.Table{}, false
}

func schemaAdvisorTablePrimaryKeyContains(table source.Table, columnName string) bool {
	for _, pk := range table.PrimaryKey {
		if strings.EqualFold(pk, columnName) {
			return true
		}
	}
	return false
}

func suggestedPolicyForGate(gate SchemaAdvisorPolicyGate) string {
	if gate.Policy != "" {
		return gate.Policy
	}
	switch gate.Action {
	case "auto_apply":
		return "auto"
	case "discard_row", "discard_value", "report_only", "blocked":
		return gate.Action
	default:
		return "manual"
	}
}

func suggestedActionForGate(gate SchemaAdvisorPolicyGate) string {
	switch gate.Action {
	case "auto_apply":
		return "DMT may apply this deterministic safe schema change when the configured policy permits it."
	case "discard_row":
		return "Use the configured discard_row behavior or choose a manual migration if the affected rows must be copied this run."
	case "discard_value":
		return "Use the configured discard_value behavior only if omitting the affected values is acceptable."
	case "report_only":
		return "Review the drift and leave the run in report-only mode until an operator chooses a policy."
	case "target_mode_recreate":
		return "Confirm the target mode behavior, backups, and validation plan before running."
	case "blocked":
		return "Stop and perform a manual migration or change the deterministic policy after review."
	default:
		return "Review the deterministic schema policy before taking action."
	}
}

func manualGuidanceForSchemaChange(change drift.Change, gate SchemaAdvisorPolicyGate) []string {
	switch {
	case gate.Action == "blocked":
		return []string{
			"Prepare an explicit DDL/backfill plan outside DMT before transfer.",
			"Run dmt preflight and dmt run --dry-run again after the schema change.",
			"Validate affected tables before enabling transfer.",
		}
	case change.Kind == drift.TypeNarrowed || change.Kind == drift.TypeChangedLossy:
		return []string{
			"Profile existing values for truncation or conversion failures.",
			"Add a staging column or transform step, then validate row counts and sampled values.",
		}
	case change.Kind == drift.DroppedColumn:
		return []string{
			"Confirm downstream consumers no longer require the target column.",
			"Keep target cleanup as a separate reviewed DDL change.",
		}
	default:
		return nil
	}
}

func schemaAdvisorBlocker(change SchemaDriftAdvisoryChange) string {
	affected := change.Table
	if change.Schema != "" {
		affected = change.Schema + "." + affected
	}
	if change.Column != "" {
		affected += "." + change.Column
	}
	return fmt.Sprintf("%s: %s", affected, change.DeterministicGate.Reason)
}
