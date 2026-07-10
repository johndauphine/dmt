package webui

import (
	"time"

	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// The DTOs here give the WebUI API a stable, snake_case JSON shape for
// orchestrator/driver structs that either lack JSON tags or carry fields that
// must not reach the browser. Structs that are already cleanly JSON-tagged
// and secret-free (StatusResult, HealthCheckResult, DryRunResult, the
// aicopilot reviews) are returned directly instead.

// runDTO is one history row. It deliberately omits checkpoint.Run.Config and
// ConfigHash: Config holds the serialized migration config, which can include
// connection secrets, and must never be serialized to the browser.
type runDTO struct {
	ID                 string     `json:"id"`
	Status             string     `json:"status"`
	Resumable          bool       `json:"resumable"`
	ResumabilityReason string     `json:"resumability_reason,omitempty"`
	Phase              string     `json:"phase"`
	StartedAt          time.Time  `json:"started_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	LastHeartbeat      time.Time  `json:"last_heartbeat"`
	SourceSchema       string     `json:"source_schema"`
	TargetSchema       string     `json:"target_schema"`
	ProfileName        string     `json:"profile_name,omitempty"`
	ConfigPath         string     `json:"config_path,omitempty"`
	Error              string     `json:"error,omitempty"`
}

func newRunDTO(r checkpoint.Run) runDTO {
	return runDTO{
		ID:                 r.ID,
		Status:             r.Status,
		Resumable:          r.Resumable,
		ResumabilityReason: r.ResumabilityReason,
		Phase:              r.Phase,
		StartedAt:          r.StartedAt,
		CompletedAt:        r.CompletedAt,
		LastHeartbeat:      r.LastHeartbeat,
		SourceSchema:       r.SourceSchema,
		TargetSchema:       r.TargetSchema,
		ProfileName:        r.ProfileName,
		ConfigPath:         r.ConfigPath,
		Error:              r.Error,
	}
}

func newRunDTOs(runs []checkpoint.Run) []runDTO {
	out := make([]runDTO, len(runs))
	for i, r := range runs {
		out[i] = newRunDTO(r)
	}
	return out
}

// validateDTO is the JSON shape of ValidationRunResult (which is tagless —
// "facts only" internally). The nested deep-validation result is elided for
// now; row-count facts are the operator-facing surface.
type validateDTO struct {
	Mode      string           `json:"mode"`
	Failed    bool             `json:"failed"`
	Error     string           `json:"error,omitempty"`
	StartedAt time.Time        `json:"started_at"`
	EndedAt   time.Time        `json:"ended_at"`
	Rows      []validateRowDTO `json:"rows"`
}

type validateRowDTO struct {
	Table         string `json:"table"`
	SourceCount   int64  `json:"source_count"`
	TargetCount   int64  `json:"target_count"`
	Difference    int64  `json:"difference"`
	CountsKnown   bool   `json:"counts_known"`
	UsedEstimate  bool   `json:"used_estimate"`
	ExactTimedOut bool   `json:"exact_timed_out"`
	TimedOut      bool   `json:"timed_out"`
	Failed        bool   `json:"failed"`
	Error         string `json:"error,omitempty"`
}

func newValidateDTO(r *orchestrator.ValidationRunResult) validateDTO {
	dto := validateDTO{
		Mode:      r.Mode,
		Failed:    r.Failed,
		Error:     r.Error,
		StartedAt: r.StartedAt,
		EndedAt:   r.EndedAt,
		Rows:      make([]validateRowDTO, len(r.Rows)),
	}
	for i, row := range r.Rows {
		dto.Rows[i] = validateRowDTO{
			Table:         row.TableName,
			SourceCount:   row.SourceCount,
			TargetCount:   row.TargetCount,
			Difference:    row.Difference,
			CountsKnown:   row.CountsKnown,
			UsedEstimate:  row.UsedEstimate,
			ExactTimedOut: row.ExactTimedOut,
			TimedOut:      row.TimedOut,
			Failed:        row.Failed,
			Error:         row.Error,
		}
	}
	return dto
}

// analyzeDTO is the JSON shape of driver.SmartConfigSuggestions (tagless;
// rendered as YAML on the CLI). It surfaces the tuning recommendations and
// database statistics an operator reviews before a run.
type analyzeDTO struct {
	ChunkSizeRecommendation int                 `json:"chunk_size_recommendation"`
	Workers                 int                 `json:"workers"`
	ReadAheadBuffers        int                 `json:"read_ahead_buffers"`
	WriteAheadWriters       int                 `json:"write_ahead_writers"`
	ParallelReaders         int                 `json:"parallel_readers"`
	MaxPartitions           int                 `json:"max_partitions"`
	MaxSourceConnections    int                 `json:"max_source_connections"`
	MaxTargetConnections    int                 `json:"max_target_connections"`
	TotalTables             int                 `json:"total_tables"`
	TotalRows               int64               `json:"total_rows"`
	AvgRowSizeBytes         int64               `json:"avg_row_size_bytes"`
	EstimatedMemMB          int64               `json:"estimated_memory_mb"`
	Tier                    string              `json:"tier,omitempty"`
	Reasoning               string              `json:"reasoning,omitempty"`
	Warnings                []string            `json:"warnings,omitempty"`
	ExcludeTables           []string            `json:"exclude_tables,omitempty"`
	DateColumns             map[string][]string `json:"date_columns,omitempty"`
}

func newAnalyzeDTO(s *driver.SmartConfigSuggestions) analyzeDTO {
	return analyzeDTO{
		ChunkSizeRecommendation: s.ChunkSizeRecommendation,
		Workers:                 s.Workers,
		ReadAheadBuffers:        s.ReadAheadBuffers,
		WriteAheadWriters:       s.WriteAheadWriters,
		ParallelReaders:         s.ParallelReaders,
		MaxPartitions:           s.MaxPartitions,
		MaxSourceConnections:    s.MaxSourceConnections,
		MaxTargetConnections:    s.MaxTargetConnections,
		TotalTables:             s.TotalTables,
		TotalRows:               s.TotalRows,
		AvgRowSizeBytes:         s.AvgRowSizeBytes,
		EstimatedMemMB:          s.EstimatedMemMB,
		Tier:                    s.Tier,
		Reasoning:               s.Reasoning,
		Warnings:                s.Warnings,
		ExcludeTables:           s.ExcludeTables,
		DateColumns:             s.DateColumns,
	}
}
