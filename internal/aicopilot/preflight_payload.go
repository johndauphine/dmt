package aicopilot

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

func BuildPreflightPayload(cfg *config.Config, health HealthSummary, findings []driver.PreFlightFinding) PreflightPayload {
	payload := PreflightPayload{
		PromptVersion:     PreflightPromptVersion,
		Task:              "Review DMT migration readiness. Deterministic findings are authoritative; AI findings are advisory only.",
		Config:            buildConfigSummary(cfg),
		Health:            scrubHealth(health),
		PreflightFindings: []PreflightFinding{},
		Redaction: RedactionSummary{
			OmittedFields: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database",
				"target.host", "target.port", "target.user", "target.password", "target.database",
				"health.source_error", "health.target_error",
				"ai.api_key", "slack.webhook_url",
			},
			ScrubbedText: true,
		},
	}
	for _, f := range findings {
		pf := PreflightFinding{
			Severity: string(f.Severity),
			Check:    f.Check,
			Side:     string(f.Side),
			Message:  logging.Scrub(f.Message),
			Remedy:   logging.Scrub(f.Remedy),
		}
		payload.PreflightFindings = append(payload.PreflightFindings, pf)
		if f.Severity == driver.SeverityError {
			payload.DeterministicBlockers = append(payload.DeterministicBlockers, fmt.Sprintf("%s/%s: %s", f.Side, f.Check, pf.Message))
		}
	}
	return payload
}

func buildConfigSummary(cfg *config.Config) ConfigSummary {
	if cfg == nil {
		return ConfigSummary{}
	}
	return ConfigSummary{
		Source: endpointSummary(cfg.Source.Type, cfg.Source.Schema, cfg.Source.Auth, cfg.Source.SSLMode, cfg.Source.Encrypt, cfg.Source.TrustServerCert, cfg.Source.PacketSize),
		Target: endpointSummary(cfg.Target.Type, cfg.Target.Schema, cfg.Target.Auth, cfg.Target.SSLMode, cfg.Target.Encrypt, cfg.Target.TrustServerCert, cfg.Target.PacketSize),
		Migration: MigrationSummary{
			TargetMode:             cfg.Migration.TargetMode,
			Workers:                cfg.Migration.Workers,
			ChunkSize:              cfg.Migration.ChunkSize,
			ReadAheadBuffers:       cfg.Migration.ReadAheadBuffers,
			WriteAheadWriters:      cfg.Migration.WriteAheadWriters,
			ParallelReaders:        cfg.Migration.ParallelReaders,
			MaxSourceConnections:   cfg.Migration.MaxSourceConnections,
			MaxTargetConnections:   cfg.Migration.MaxTargetConnections,
			MaxPartitions:          cfg.Migration.MaxPartitions,
			LargeTableThreshold:    cfg.Migration.LargeTableThreshold,
			StrictConsistency:      cfg.Migration.StrictConsistency,
			CreateIndexes:          cfg.Migration.CreateIndexesEnabled(),
			CreateForeignKeys:      cfg.Migration.CreateForeignKeysEnabled(),
			CreateCheckConstraints: cfg.Migration.CreateCheckConstraints,
			AllowPartial:           cfg.Migration.AllowPartial,
			UnmappedTypeAction:     cfg.Migration.UnmappedTypeAction,
			ApproxTypeAction:       cfg.Migration.ApproxTypeAction,
			SchemaContract:         schemaContractSummary(cfg.Migration),
			SchemaEvolution:        schemaEvolutionSummary(cfg.Migration),
			Deletes:                deleteSummary(cfg.Migration),
			Validation:             validationSummary(cfg.Migration.Validation),
		},
	}
}

func endpointSummary(dbType, schema, auth, sslMode string, encrypt *bool, trustServerCert bool, packetSize int) EndpointSummary {
	out := EndpointSummary{
		Type:       dbType,
		Schema:     schema,
		Auth:       auth,
		SSLMode:    sslMode,
		PacketSize: packetSize,
	}
	if encrypt != nil {
		out.Encrypt = fmt.Sprintf("%v", *encrypt)
		if trustServerCert {
			out.Encrypt += " (trust_server_certificate=true)"
		}
	}
	return out
}

func schemaContractSummary(m config.MigrationConfig) SchemaContractSummary {
	if !m.SchemaContractEnabled() {
		return SchemaContractSummary{Enabled: false}
	}
	return SchemaContractSummary{
		Enabled:  true,
		Tables:   string(m.SchemaContractTablesMode()),
		Columns:  string(m.SchemaContractColumnsMode()),
		DataType: string(m.SchemaContractDataTypeMode()),
	}
}

func schemaEvolutionSummary(m config.MigrationConfig) SchemaEvolutionSummary {
	return SchemaEvolutionSummary{
		Enabled:           m.SchemaEvolutionEnabled(),
		AddedColumn:       string(m.AddedColumnSchemaEvolutionPolicy()),
		NullabilityChange: string(m.NullabilityChangeSchemaEvolutionPolicy()),
		TypeChange:        string(m.TypeChangeSchemaEvolutionPolicy()),
	}
}

func deleteSummary(m config.MigrationConfig) DeleteSummary {
	out := DeleteSummary{Mode: string(m.DeleteMode())}
	if m.DeletesEnabled() {
		out.TargetBehavior = string(m.DeleteTargetBehavior())
		out.ReconcileSchedule = string(m.DeleteReconcileSchedule())
		out.ReconcileInterval = m.DeleteReconcileInterval()
		out.BatchSize = m.DeleteReconcileBatchSize()
		out.RequirePrimaryKey = m.DeleteReconcileRequirePrimaryKey()
	}
	return out
}

func validationSummary(v config.ValidationConfig) ValidationSummary {
	mode := strings.TrimSpace(v.Mode)
	if mode == "" {
		mode = "count_only"
	}
	failOnMismatch := true
	if v.FailOnMismatch != nil {
		failOnMismatch = *v.FailOnMismatch
	}
	return ValidationSummary{
		Mode:           mode,
		SampleRows:     v.SampleRows,
		SamplePercent:  v.SampleRowsPercent,
		FailOnMismatch: failOnMismatch,
		Timeout:        v.Timeout,
		MaxParallel:    v.MaxParallel,
	}
}

func scrubHealth(h HealthSummary) HealthSummary {
	if strings.TrimSpace(h.SourceError) != "" {
		h.SourceError = "source connection error details omitted from AI payload"
	}
	if strings.TrimSpace(h.TargetError) != "" {
		h.TargetError = "target connection error details omitted from AI payload"
	}
	return h
}

func deterministicReadiness(payload PreflightPayload) string {
	if !payload.Health.SourceConnected || !payload.Health.TargetConnected || len(payload.DeterministicBlockers) > 0 {
		return ReadinessBlocked
	}
	for _, f := range payload.PreflightFindings {
		if f.Severity == string(driver.SeverityWarn) {
			return ReadinessAttention
		}
	}
	return ReadinessReady
}

func deterministicSummary(payload PreflightPayload) string {
	switch deterministicReadiness(payload) {
	case ReadinessBlocked:
		return "Deterministic preflight found blockers that must be resolved before migration."
	case ReadinessAttention:
		return "Deterministic preflight passed with warnings that should be reviewed before migration."
	case ReadinessReady:
		return "Deterministic preflight did not find blockers."
	default:
		return "Readiness is unknown from the available deterministic preflight data."
	}
}
