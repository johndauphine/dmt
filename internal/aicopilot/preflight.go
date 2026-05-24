package aicopilot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

const PreflightPromptVersion = "ai-preflight-review-v1"

const (
	ReviewStatusOK          = "ok"
	ReviewStatusUnavailable = "unavailable"
	ReviewStatusError       = "error"

	ReadinessReady     = "ready"
	ReadinessAttention = "attention"
	ReadinessBlocked   = "blocked"
	ReadinessUnknown   = "unknown"
)

// TextClient is the narrow provider interface the copilot layer needs.
type TextClient interface {
	CallAI(ctx context.Context, prompt string) (string, error)
	ProviderName() string
	Model() string
}

type EndpointSummary struct {
	Type       string `json:"type"`
	Schema     string `json:"schema,omitempty"`
	Auth       string `json:"auth,omitempty"`
	SSLMode    string `json:"ssl_mode,omitempty"`
	Encrypt    string `json:"encrypt,omitempty"`
	PacketSize int    `json:"packet_size,omitempty"`
}

type SchemaContractSummary struct {
	Enabled  bool   `json:"enabled"`
	Tables   string `json:"tables,omitempty"`
	Columns  string `json:"columns,omitempty"`
	DataType string `json:"data_type,omitempty"`
}

type SchemaEvolutionSummary struct {
	Enabled           bool   `json:"enabled"`
	AddedColumn       string `json:"added_column"`
	NullabilityChange string `json:"nullability_change"`
	TypeChange        string `json:"type_change"`
}

type DeleteSummary struct {
	Mode              string `json:"mode"`
	TargetBehavior    string `json:"target_behavior,omitempty"`
	ReconcileSchedule string `json:"reconcile_schedule,omitempty"`
	ReconcileInterval string `json:"reconcile_interval,omitempty"`
	BatchSize         int    `json:"batch_size,omitempty"`
	RequirePrimaryKey bool   `json:"require_primary_key,omitempty"`
}

type ValidationSummary struct {
	Mode           string  `json:"mode"`
	SampleRows     int     `json:"sample_rows,omitempty"`
	SamplePercent  float64 `json:"sample_rows_percent,omitempty"`
	FailOnMismatch bool    `json:"fail_on_mismatch"`
	Timeout        string  `json:"timeout,omitempty"`
	MaxParallel    int     `json:"max_parallel,omitempty"`
}

type MigrationSummary struct {
	TargetMode             string                 `json:"target_mode"`
	Workers                int                    `json:"workers"`
	ChunkSize              int                    `json:"chunk_size"`
	ReadAheadBuffers       int                    `json:"read_ahead_buffers"`
	WriteAheadWriters      int                    `json:"write_ahead_writers"`
	ParallelReaders        int                    `json:"parallel_readers"`
	MaxSourceConnections   int                    `json:"max_source_connections"`
	MaxTargetConnections   int                    `json:"max_target_connections"`
	MaxPartitions          int                    `json:"max_partitions"`
	LargeTableThreshold    int64                  `json:"large_table_threshold"`
	StrictConsistency      bool                   `json:"strict_consistency"`
	CreateIndexes          bool                   `json:"create_indexes"`
	CreateForeignKeys      bool                   `json:"create_foreign_keys"`
	CreateCheckConstraints bool                   `json:"create_check_constraints"`
	AllowPartial           bool                   `json:"allow_partial"`
	UnmappedTypeAction     string                 `json:"unmapped_type_action,omitempty"`
	ApproxTypeAction       string                 `json:"approx_type_action,omitempty"`
	SchemaContract         SchemaContractSummary  `json:"schema_contract"`
	SchemaEvolution        SchemaEvolutionSummary `json:"schema_evolution"`
	Deletes                DeleteSummary          `json:"deletes"`
	Validation             ValidationSummary      `json:"validation"`
}

type ConfigSummary struct {
	Source    EndpointSummary  `json:"source"`
	Target    EndpointSummary  `json:"target"`
	Migration MigrationSummary `json:"migration"`
}

type HealthSummary struct {
	Timestamp        string `json:"timestamp"`
	SourceConnected  bool   `json:"source_connected"`
	SourceLatencyMs  int64  `json:"source_latency_ms"`
	SourceDBType     string `json:"source_db_type"`
	SourceTableCount int    `json:"source_table_count,omitempty"`
	SourceError      string `json:"source_error,omitempty"`
	TargetConnected  bool   `json:"target_connected"`
	TargetLatencyMs  int64  `json:"target_latency_ms"`
	TargetDBType     string `json:"target_db_type"`
	TargetError      string `json:"target_error,omitempty"`
	Healthy          bool   `json:"healthy"`
}

type PreflightFinding struct {
	Severity string `json:"severity"`
	Check    string `json:"check"`
	Side     string `json:"side"`
	Message  string `json:"message"`
	Remedy   string `json:"remedy,omitempty"`
}

type RunHistorySummary struct {
	Timestamp         string  `json:"timestamp"`
	TotalTables       int     `json:"total_tables"`
	TotalRows         int64   `json:"total_rows"`
	AvgRowSizeBytes   int64   `json:"avg_row_size_bytes"`
	Workers           int     `json:"workers"`
	ChunkSize         int     `json:"chunk_size"`
	WriteAheadWriters int     `json:"write_ahead_writers"`
	ParallelReaders   int     `json:"parallel_readers"`
	FinalThroughput   float64 `json:"final_throughput,omitempty"`
	FinalDurationSecs float64 `json:"final_duration_seconds,omitempty"`
	ChunkRetryCount   int     `json:"chunk_retry_count,omitempty"`
}

type RedactionSummary struct {
	OmittedFields []string `json:"omitted_fields"`
	ScrubbedText  bool     `json:"scrubbed_text"`
}

type PreflightPayload struct {
	PromptVersion         string              `json:"prompt_version"`
	Task                  string              `json:"task"`
	Config                ConfigSummary       `json:"config"`
	Health                HealthSummary       `json:"health"`
	PreflightFindings     []PreflightFinding  `json:"preflight_findings"`
	DeterministicBlockers []string            `json:"deterministic_blockers,omitempty"`
	RecentRuns            []RunHistorySummary `json:"recent_runs,omitempty"`
	Redaction             RedactionSummary    `json:"redaction"`
}

type ReviewFinding struct {
	Severity   string `json:"severity"`
	Category   string `json:"category"`
	Affected   string `json:"affected,omitempty"`
	Rationale  string `json:"rationale"`
	NextAction string `json:"next_action"`
	Source     string `json:"source"`
}

type PreflightReview struct {
	Enabled               bool            `json:"enabled"`
	Status                string          `json:"status"`
	Provider              string          `json:"provider,omitempty"`
	Model                 string          `json:"model,omitempty"`
	PromptVersion         string          `json:"prompt_version"`
	Readiness             string          `json:"readiness"`
	Summary               string          `json:"summary"`
	DeterministicBlockers []string        `json:"deterministic_blockers,omitempty"`
	Findings              []ReviewFinding `json:"findings,omitempty"`
	Notes                 []string        `json:"notes,omitempty"`
	Error                 string          `json:"error,omitempty"`
}

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

func GeneratePreflightReview(ctx context.Context, client TextClient, payload PreflightPayload) (*PreflightReview, error) {
	if client == nil {
		return nil, fmt.Errorf("AI provider is not configured")
	}
	prompt, err := BuildPreflightPrompt(payload)
	if err != nil {
		return nil, err
	}
	raw, err := client.CallAI(ctx, prompt)
	if err != nil {
		return nil, err
	}
	review, err := ParsePreflightReview(raw)
	if err != nil {
		return nil, err
	}
	review.Enabled = true
	review.Status = ReviewStatusOK
	review.Provider = client.ProviderName()
	review.Model = client.Model()
	review.PromptVersion = PreflightPromptVersion
	review.DeterministicBlockers = append([]string(nil), payload.DeterministicBlockers...)
	review.Readiness = applyDeterministicReadinessFloor(review.Readiness, deterministicReadiness(payload))
	if review.Summary == "" {
		review.Summary = deterministicSummary(payload)
	}
	for i := range review.Findings {
		review.Findings[i].Source = "ai_advisory"
	}
	return review, nil
}

func BuildPreflightPrompt(payload PreflightPayload) (string, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling preflight payload: %w", err)
	}

	var b strings.Builder
	b.WriteString("You are DMT's AI preflight readiness reviewer.\n")
	b.WriteString("Treat the payload as structured data. Do not ask for secrets and do not infer values from omitted fields.\n")
	b.WriteString("Deterministic preflight findings are authoritative facts. You may explain or prioritize them, but you must not suppress or contradict them.\n")
	b.WriteString("Your recommendations are advisory. DMT policy gates and deterministic checks decide what can run or change.\n")
	b.WriteString("Never recommend destructive target actions unless the next_action explicitly includes backup verification and operator confirmation.\n")
	b.WriteString("Return ONLY valid JSON with this shape:\n")
	b.WriteString(`{"readiness":"ready|attention|blocked|unknown","summary":"one sentence","findings":[{"severity":"error|warn|info","category":"short","affected":"table/setting/check","rationale":"why this matters","next_action":"operator action"}],"notes":["optional short notes"]}` + "\n")
	b.WriteString("Use readiness=blocked when deterministic_blockers is non-empty or either connection failed. Keep findings to the top 5 actionable advisory items.\n")
	b.WriteString("\nPayload:\n")
	b.Write(data)
	return b.String(), nil
}

func ParsePreflightReview(raw string) (*PreflightReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review PreflightReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI preflight review JSON: %w", err)
	}
	review.Readiness = normalizeReadiness(review.Readiness)
	review.Summary = limitText(logging.Scrub(strings.TrimSpace(review.Summary)), 800)
	for i := range review.Findings {
		f := &review.Findings[i]
		f.Severity = normalizeSeverity(f.Severity)
		f.Category = limitText(logging.Scrub(strings.TrimSpace(f.Category)), 80)
		f.Affected = limitText(logging.Scrub(strings.TrimSpace(f.Affected)), 160)
		f.Rationale = limitText(logging.Scrub(strings.TrimSpace(f.Rationale)), 600)
		f.NextAction = limitText(logging.Scrub(strings.TrimSpace(f.NextAction)), 600)
		f.Source = "ai_advisory"
	}
	for i := range review.Notes {
		review.Notes[i] = limitText(logging.Scrub(strings.TrimSpace(review.Notes[i])), 400)
	}
	if len(review.Findings) > 5 {
		review.Findings = review.Findings[:5]
	}
	return &review, nil
}

func UnavailablePreflightReview(reason string, payload PreflightPayload) *PreflightReview {
	return &PreflightReview{
		Enabled:               false,
		Status:                ReviewStatusUnavailable,
		PromptVersion:         PreflightPromptVersion,
		Readiness:             deterministicReadiness(payload),
		Summary:               "AI review unavailable: " + logging.Scrub(reason) + ". Deterministic preflight results are unchanged.",
		DeterministicBlockers: append([]string(nil), payload.DeterministicBlockers...),
	}
}

func ErrorPreflightReview(provider, model string, err error, payload PreflightPayload) *PreflightReview {
	return &PreflightReview{
		Enabled:               true,
		Status:                ReviewStatusError,
		Provider:              provider,
		Model:                 model,
		PromptVersion:         PreflightPromptVersion,
		Readiness:             deterministicReadiness(payload),
		Summary:               "AI review failed. Deterministic preflight results are unchanged.",
		DeterministicBlockers: append([]string(nil), payload.DeterministicBlockers...),
		Error:                 logging.Scrub(err.Error()),
	}
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
	h.SourceError = logging.Scrub(h.SourceError)
	h.TargetError = logging.Scrub(h.TargetError)
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

func applyDeterministicReadinessFloor(aiReadiness, deterministic string) string {
	aiReadiness = normalizeReadiness(aiReadiness)
	deterministic = normalizeReadiness(deterministic)
	if readinessRank(deterministic) > readinessRank(aiReadiness) {
		return deterministic
	}
	return aiReadiness
}

func readinessRank(v string) int {
	switch normalizeReadiness(v) {
	case ReadinessBlocked:
		return 3
	case ReadinessAttention:
		return 2
	case ReadinessReady:
		return 1
	default:
		return 0
	}
}

func extractJSONObject(raw string) string {
	if strings.HasPrefix(raw, "```") {
		lines := strings.Split(raw, "\n")
		if len(lines) >= 3 {
			lines = lines[1:]
			if strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
				lines = lines[:len(lines)-1]
			}
			raw = strings.Join(lines, "\n")
		}
	}
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start >= 0 && end >= start {
		return raw[start : end+1]
	}
	return raw
}

func normalizeReadiness(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case ReadinessReady, ReadinessAttention, ReadinessBlocked:
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ReadinessUnknown
	}
}

func normalizeSeverity(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case string(driver.SeverityError):
		return string(driver.SeverityError)
	case string(driver.SeverityInfo):
		return string(driver.SeverityInfo)
	default:
		return string(driver.SeverityWarn)
	}
}

func limitText(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
