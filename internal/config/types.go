package config

import (
	"fmt"
	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/systemmemory"
	"github.com/johndauphine/dmt/internal/tuning"
	"gopkg.in/yaml.v3"
	"strings"
)

// Type aliases for the shared connection-spec types. Keeping the concrete
// structs in dbconfig lets config loading and driver construction depend on
// the same narrow database connection contract.
type SourceConfig = dbconfig.SourceConfig
type TargetConfig = dbconfig.TargetConfig

// ConfigValueProvenance names the layer that supplied a tunable value.
type ConfigValueProvenance string

const (
	ProvenanceUserConfig     ConfigValueProvenance = "config"
	ProvenanceSecretsDefault ConfigValueProvenance = "secrets default"
	ProvenanceDriverDefault  ConfigValueProvenance = "driver default"
	ProvenanceAutoDefault    ConfigValueProvenance = "auto default"
	ProvenanceSmartConfig    ConfigValueProvenance = "smartconfig"
	ProvenanceRuntimeControl ConfigValueProvenance = "runtime controller"
)

// MemoryEnvelope is the one immutable capacity/budget decision made while
// loading a config. Capacity and availability come from the shared
// host/cgroup-aware detector; BudgetMB applies the automatic reserve and the
// optional migration.max_memory_mb ceiling exactly once.
type MemoryEnvelope struct {
	CapacityMB  int64
	AvailableMB int64
	BudgetMB    int64
	Source      string
}

// AutoConfig tracks which values were auto-configured and why
type AutoConfig struct {
	// System resources detected. AvailableMemoryMB and EffectiveMaxMemoryMB
	// are compatibility projections retained during the memory-envelope epic;
	// new consumers should use MemoryEnvelope directly.
	MemoryEnvelope       MemoryEnvelope
	AvailableMemoryMB    int64
	EffectiveMaxMemoryMB int64
	CPUCores             int
	Platform             string

	// DefaultPolicyReasoning is the load-time tuner's clamp/width audit trail.
	// DefaultPolicyChunkPinned distinguishes an authoritative explicit chunk
	// from the generated chunk described by that policy. Both are runtime-only
	// diagnostics and are intentionally omitted from serialized config.
	DefaultPolicyReasoning   string
	DefaultPolicyChunkPinned bool

	// Raw user-supplied values captured after YAML/template expansion and
	// before secrets, driver defaults, smartconfig, or runtime tuning apply.
	RawUserSource          SourceConfig
	RawUserTarget          TargetConfig
	RawUserMigration       MigrationConfig
	RawUserConfigCaptured  bool
	TunableValueProvenance map[string]ConfigValueProvenance

	// Original values (before auto-tuning)
	OriginalWorkers              int
	OriginalChunkSize            int
	OriginalReadAheadBuffers     int
	OriginalMaxPartitions        int
	OriginalMaxSourceConns       int
	OriginalMaxTargetConns       int
	OriginalWriteAheadWriters    int
	OriginalParallelReaders      int
	OriginalLargeTableThresh     int64
	OriginalSampleSize           int
	OriginalUpsertMergeChunkSize int
	OriginalSourceChunkSize      int
	OriginalTargetChunkSize      int
	OriginalCheckpointFrequency  int
	OriginalMaxRetries           int
}

// Config holds all configuration for the migration tool.
// Note: AI and Slack settings are global-only and loaded from ~/.secrets/dmt-config.yaml
type Config struct {
	Source    SourceConfig    `yaml:"source"`
	Target    TargetConfig    `yaml:"target"`
	Migration MigrationConfig `yaml:"migration"`
	Profile   ProfileConfig   `yaml:"profile,omitempty"`
	AI        *AIConfig       `yaml:"ai,omitempty"`
	Slack     *SlackConfig    `yaml:"slack,omitempty"`

	// AutoConfig stores auto-tuning metadata (not serialized to YAML)
	autoConfig AutoConfig

	// memoryReader is injectable for deterministic config tests. Production
	// loading leaves it nil and resolves one systemmemory.NewReader snapshot.
	memoryReader systemmemory.Reader

	// runtimeChunkProjection preserves the nominal pre-safety chunk views
	// across repeated tuning passes on the same Config. Safety materialization
	// mutates the public compatibility fields, so a fresh pass must restore the
	// prior request before it can derive and apply a new cap.
	runtimeChunkProjection runtimeChunkProjectionState
}

type runtimeChunkProjectionState struct {
	captured  bool
	migration int
	source    int
	target    int
}

// SlackConfig holds Slack notification settings.
type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	Channel    string `yaml:"channel"`
	Username   string `yaml:"username"`
	Enabled    bool   `yaml:"enabled"`
}

// ProfileConfig holds optional profile metadata.
type ProfileConfig struct {
	Name        string `yaml:"name,omitempty"`
	Description string `yaml:"description,omitempty"`
}

// AIConfig holds AI provider configuration.
type AIConfig struct {
	// APIKey is the API key for the AI provider.
	// Supports the same secret patterns as passwords:
	//   ${file:/path/to/key} - read from file (recommended for production)
	//   ${env:VAR_NAME} - read from environment variable
	//   ${VAR_NAME} - legacy env var syntax
	//   literal value - not recommended, use file or env instead
	APIKey string `yaml:"api_key"`

	// Provider specifies which AI provider to use.
	// Valid values: "anthropic", "openai", "gemini", "ollama", "lmstudio"
	// Defaults to "anthropic" if not specified.
	Provider string `yaml:"provider"`

	// Model specifies which model to use (optional).
	// Defaults to smart models for accurate inference:
	//   Anthropic: claude-sonnet-5
	//   OpenAI: gpt-5.5
	//   Gemini: gemini-2.0-flash
	Model string `yaml:"model"`

	// TimeoutSeconds is the API request timeout (default: 30).
	TimeoutSeconds int `yaml:"timeout_seconds"`

	// TypeMapping configures AI-assisted type mapping for unknown types.
	TypeMapping *AITypeMappingConfig `yaml:"type_mapping"`
}

// AITypeMappingConfig contains settings specific to AI type mapping.
type AITypeMappingConfig struct {
	// Enabled turns AI type mapping on/off.
	// Auto-enabled when api_key is configured (unless explicitly set to false).
	Enabled *bool `yaml:"enabled"`

	// CacheFile is the path to the JSON cache file for type mappings.
	// Defaults to ~/.dmt/type-cache.json
	CacheFile string `yaml:"cache_file"`
}

// SchemaEvolutionPolicy controls how DMT handles one schema drift category.
type SchemaEvolutionPolicy string

const (
	SchemaEvolutionAuto         SchemaEvolutionPolicy = "auto"
	SchemaEvolutionLog          SchemaEvolutionPolicy = "log"
	SchemaEvolutionFail         SchemaEvolutionPolicy = "fail"
	SchemaEvolutionDiscard      SchemaEvolutionPolicy = "discard"
	SchemaEvolutionDiscardValue SchemaEvolutionPolicy = "discard_value"
)

// SchemaEvolutionConfig controls opt-in target schema changes after source
// schema drift is detected. A nil config keeps drift reporting read-only.
type SchemaEvolutionConfig struct {
	// AddedColumn controls added source columns. When the section is present
	// but this field is omitted, added columns default to auto-apply.
	AddedColumn SchemaEvolutionPolicy `yaml:"added_column,omitempty" json:"added_column,omitempty"`
	// NullabilityChange controls source nullability drift. Auto only relaxes
	// target columns from NOT NULL to NULL; tightening remains a hard error.
	NullabilityChange SchemaEvolutionPolicy `yaml:"nullability_change,omitempty" json:"nullability_change,omitempty"`
	// TypeChange controls source data type drift. Omitted values stay log-only
	// so widening target ALTERs require an explicit opt-in.
	TypeChange SchemaEvolutionPolicy `yaml:"type_change,omitempty" json:"type_change,omitempty"`
}

const schemaEvolutionDeprecationWarning = `migration.schema_evolution is deprecated; ` +
	`use DLT-style migration.schema_contract settings for tables, columns, ` +
	`and data_type instead. Existing schema_evolution behavior still runs for ` +
	`now, but new configs should not add this legacy section. See issue #403.`

// SchemaContractMode controls how DMT responds when detected schema drift
// violates a schema contract entity. `report` is DMT-specific and preserves
// the legacy report-only behavior without applying target schema changes.
type SchemaContractMode string

const (
	SchemaContractEvolve       SchemaContractMode = "evolve"
	SchemaContractFreeze       SchemaContractMode = "freeze"
	SchemaContractDiscardRow   SchemaContractMode = "discard_row"
	SchemaContractDiscardValue SchemaContractMode = "discard_value"
	SchemaContractReport       SchemaContractMode = "report"
)

// SchemaContractConfig is DMT's DLT-style schema contract surface. A scalar
// YAML value such as `schema_contract: report` expands to all three entities.
// Omitted entities default to evolve when the section is present.
type SchemaContractConfig struct {
	Tables   SchemaContractMode `yaml:"tables,omitempty" json:"tables,omitempty"`
	Columns  SchemaContractMode `yaml:"columns,omitempty" json:"columns,omitempty"`
	DataType SchemaContractMode `yaml:"data_type,omitempty" json:"data_type,omitempty"`
}

// NotifyConfig controls migration completion notifications. Nil fields default
// to true so existing Slack behavior is preserved when a webhook is configured.
type NotifyConfig struct {
	OnSuccess *bool `yaml:"on_success,omitempty" json:"on_success,omitempty"`
	OnFailure *bool `yaml:"on_failure,omitempty" json:"on_failure,omitempty"`
}

// MigrationConfig holds migration behavior settings
type MigrationConfig struct {
	MaxSourceConnections int           `yaml:"max_source_connections"` // Max source database connections
	MaxTargetConnections int           `yaml:"max_target_connections"` // Max target database connections
	ChunkSize            int           `yaml:"chunk_size"`
	MaxPartitions        int           `yaml:"max_partitions"`
	Workers              int           `yaml:"workers"`
	LargeTableThreshold  int64         `yaml:"large_table_threshold"`
	IncludeTables        []string      `yaml:"include_tables"` // Only migrate these tables (glob patterns)
	ExcludeTables        []string      `yaml:"exclude_tables"` // Skip these tables (glob patterns)
	DataDir              string        `yaml:"data_dir"`
	TargetMode           string        `yaml:"target_mode"` // "drop_recreate" (default) or "upsert"
	Deletes              *DeleteConfig `yaml:"deletes,omitempty" json:"deletes,omitempty"`
	StrictConsistency    bool          `yaml:"strict_consistency"` // Pin source reads to stable snapshots; scope controls table vs migration lifetime (#640, #663)
	// StrictConsistencyScope controls whether strict snapshots are created per
	// table (the backwards-compatible default) or once for the transfer phase.
	// PostgreSQL imports one exported snapshot; SQL Server reads from a
	// server-side database snapshot shared by every table/reader.
	StrictConsistencyScope string `yaml:"strict_consistency_scope" json:"strict_consistency_scope,omitempty"`
	CreateIndexes          *bool  `yaml:"create_indexes,omitempty"`      // Create non-PK indexes (default: true)
	CreateForeignKeys      *bool  `yaml:"create_foreign_keys,omitempty"` // Create foreign key constraints (default: true)
	CreateCheckConstraints bool   `yaml:"create_check_constraints"`      // Create CHECK constraints
	// FailOnSchemaDrift turns the #305 read-only drift report into a hard
	// pre-transfer gate. It is exit policy, not data-plane behavior, so it
	// must stay out of the resume config hash.
	FailOnSchemaDrift bool `yaml:"fail_on_schema_drift" json:"-"`
	// SchemaEvolution applies compatible source drift to the target before
	// transfer. Omit the section to keep drift reporting read-only.
	SchemaEvolution *SchemaEvolutionConfig `yaml:"schema_evolution,omitempty" json:"schema_evolution,omitempty"`
	// SchemaContract is the DLT-style replacement for schema_evolution. It
	// supports tables/columns/data_type entities plus DMT's report mode.
	SchemaContract   *SchemaContractConfig `yaml:"schema_contract,omitempty" json:"schema_contract,omitempty"`
	SampleValidation bool                  `yaml:"sample_validation"` // (legacy) Enable PK-existence sample validation; superseded by validation.mode (#226)
	SampleSize       int                   `yaml:"sample_size"`       // (legacy) Number of rows to sample for validation; superseded by validation.sample_rows (#226)
	// Notify controls non-data-plane completion alerts. It is omitted from
	// the resume config hash because changing alert policy should not make a
	// resumable migration look incompatible with its original run config.
	Notify NotifyConfig `yaml:"notify,omitempty" json:"-"`
	// AllowPartial controls the exit-code contract when one or more
	// tables fail to transfer. Default (false) returns a
	// PartialMigrationError so unattended automation (Airflow, k8s
	// jobs, CI) sees a non-zero exit code. Set to true to preserve
	// the pre-#248 behavior where partial migrations exited 0 with a
	// "partial" status in JSON output and a warning in the log.
	//
	// `json:"-"` is load-bearing: the resume config hash
	// (computeConfigHash) is computed via json.Marshal of the
	// sanitized config; including a new field with a non-omitted zero
	// value would falsely flag an unchanged YAML as "config changed"
	// across the #248 upgrade (Codex review on PR closing this issue).
	// Exit-code policy doesn't affect data transfer, so it has no
	// business participating in the hash.
	AllowPartial bool `yaml:"allow_partial" json:"-"`

	// SkipPreflight is a comma-separated list of preflight check names to
	// skip, e.g. "privileges.create_table,disk.estimate" or "all" to
	// disable preflight entirely (#228). Mirrors the --skip-preflight CLI
	// flag; populated by the CLI before the orchestrator runs preflight.
	// `json:"-"` for the same hash-stability reason as AllowPartial:
	// skipping a check is policy, not a data-changing setting.
	SkipPreflight []string `yaml:"skip_preflight" json:"-"`

	// ConfirmBackup must be set to true for drop_recreate runs against a
	// target schema that already contains non-empty tables (#228). It's a
	// hard guard against accidentally dropping data. Mirrors the
	// --confirm-backup CLI flag.
	ConfirmBackup bool `yaml:"confirm_backup" json:"-"`

	// AuditDir overrides the audit-log directory (#235). Default is
	// $HOME/.dmt/audit. Files written here are append-only during the
	// run, chmod 0444 after. `json:"-"` matches the other CLI-only
	// fields — the audit-dir choice doesn't affect the migration's
	// data plane, so it shouldn't break resume on config-hash mismatch.
	AuditDir string `yaml:"audit_dir" json:"-"`

	// AuditTamperEvident enables hash-chained audit events (#235).
	// Each event carries seq/prev_hash/hash so retroactive modification
	// is detectable. Off by default; opt in for high-compliance
	// scenarios (financial/healthcare/government).
	AuditTamperEvident bool `yaml:"audit_tamper_evident" json:"-"`

	// NoAudit disables the audit log entirely (#235). Default false
	// (audit enabled). Use only when the operator has another
	// compliance mechanism in place.
	NoAudit bool `yaml:"no_audit" json:"-"`

	// Validation configures the cross-DB validation passes that run
	// after data transfer completes (#226). Zero value preserves the
	// pre-#226 behavior (row-count validation only). See the Validation
	// struct's field docs for the full mode taxonomy.
	Validation           ValidationConfig `yaml:"validation"`
	ReadAheadBuffers     int              `yaml:"read_ahead_buffers"`      // Number of chunks to read ahead (default=4)
	WriteAheadWriters    int              `yaml:"write_ahead_writers"`     // Number of parallel writers per job (default=2)
	ParallelReaders      int              `yaml:"parallel_readers"`        // Number of parallel readers per job (default=2)
	UpsertMergeChunkSize int              `yaml:"upsert_merge_chunk_size"` // Chunk size for upsert UPDATE+INSERT (default=5000, auto-tuned)
	MaxMemoryMB          int64            `yaml:"max_memory_mb"`           // Optional ceiling on the resolved automatic memory budget
	// Restartability settings
	CheckpointFrequency  int `yaml:"checkpoint_frequency"`   // Save progress every N chunks (default=20)
	MaxRetries           int `yaml:"max_retries"`            // Retry failed tables N times (default=3)
	HistoryRetentionDays int `yaml:"history_retention_days"` // Keep run history for N days (default=30)
	// Date-based incremental sync (upsert mode only)
	DateUpdatedColumns []string `yaml:"date_updated_columns"` // Column names to check for last-modified date (tries each in order)
	// Real-time parameter adjustment via the rule-based runtime
	// controller (#172 of the AI-optional epic).
	//
	// Two parallel field sets exist during the #211 deprecation cycle:
	//
	//   - RuntimeTuning / RuntimeTuningInterval (new, canonical) —
	//     the current names. Post-#172 the controller is deterministic
	//     and rule-based; no AI dependency, no LLM round-trip.
	//   - AIAdjust / AIAdjustInterval (deprecated) — preserved for
	//     backward compat. normalizeRuntimeTuningFields migrates these
	//     into the new fields at config-load time and emits a one-time
	//     WARN log. Slated for removal in a future release.
	//
	// Both fields use *bool so the parser can distinguish "user explicitly
	// set false" from "field unset" (which inherits the secrets default).
	// A plain bool would silently revert false → true at the auto-enable
	// step (issue #149).
	//
	// JSON tags use the legacy names (AIAdjust / AIAdjustInterval) so
	// the resume config hash stays stable across the rename. Pre-#211
	// runs had `"AIAdjust": <bool>` and `"AIAdjustInterval": "<dur>"`
	// in their stored hash JSON; with the tags below, post-#211 emits
	// exactly the same wire shape from the renamed Go fields. The
	// legacy AIAdjust / AIAdjustInterval Go fields below get `json:"-"`
	// so they don't double-write (normalizeRuntimeTuningFields clears
	// them anyway, but the tag belt-and-suspenders prevents accidental
	// JSON collisions if normalize is ever skipped).
	// Tuning is the coarse pre-run tuner switch (#461): "auto" (default,
	// also the meaning of empty) derives parameters from system stats,
	// driver profiles, and run history; "manual" disables derivation so
	// configured values and formula defaults rule. Distinct from
	// RuntimeTuning, which controls the mid-run rule-based controller.
	Tuning string `yaml:"tuning,omitempty" json:"Tuning,omitempty"`

	RuntimeTuning         *bool  `yaml:"runtime_tuning,omitempty" json:"AIAdjust"`
	RuntimeTuningInterval string `yaml:"runtime_tuning_interval,omitempty" json:"AIAdjustInterval"`

	// Legacy alias for RuntimeTuning. Renamed in #211; still parsed so
	// existing user configs keep working. normalizeRuntimeTuningFields
	// migrates the value into RuntimeTuning at config-load time and
	// clears this field; downstream code reads RuntimeTuning only.
	// Slated for removal in a future release.
	AIAdjust *bool `yaml:"ai_adjust,omitempty" json:"-"`
	// Legacy alias for RuntimeTuningInterval. See AIAdjust above.
	AIAdjustInterval string `yaml:"ai_adjust_interval,omitempty" json:"-"`

	// TargetHardChunkLimit is the runtime-discovered hard cap on chunk_size
	// from target-side probes (today: MySQL @@max_allowed_packet, see
	// #166). Populated by the orchestrator after the smartconfig analyzer
	// runs; never serialized — it's a derived value, not user config.
	// `json:"-"` is load-bearing: the resume config hash is computed via
	// json.Marshal, and including this runtime-probed value would falsely
	// flag an unchanged YAML as "config changed" across resumes (Codex
	// review on #166).
	TargetHardChunkLimit int `yaml:"-" json:"-"`

	// Runtime chunk-safety metadata is derived from the immutable memory
	// envelope plus the current run's smartconfig analysis (#709). It is kept
	// out of YAML and JSON because it is neither user configuration nor stable
	// resume identity; failed/manual analysis must start from the zero-value,
	// growth-disabled state instead of reusing an earlier run's evidence.
	RuntimeChunkSizeCap        int                  `yaml:"-" json:"-"`
	RuntimeSafetyRowBytes      int64                `yaml:"-" json:"-"`
	RuntimeSafetyRowBytesKnown bool                 `yaml:"-" json:"-"`
	RuntimeMemoryProfile       tuning.MemoryProfile `yaml:"-" json:"-"`
	RuntimeChunkGrowthAllowed  bool                 `yaml:"-" json:"-"`

	// Explore forces an exploration probe on this run instead of the
	// tuner's argmax pick (PR2 #179 wires the actual exploration policy;
	// PR1 #175 just plumbs the flag through CLI → config → orchestrator).
	// Settable via YAML (`explore: true`) or CLI (`--explore`).
	Explore bool `yaml:"explore,omitempty"`

	// ExploreMode controls the steady-state ε-perturbation probability
	// the tuner uses to keep training data refreshing after the planned-
	// grid phase ends. Recognized values: "off" (0), "low" (0.10),
	// "balanced" (0.15, also the default for empty), "high" (0.25). PR2
	// #179 reads this; unknown values fall back to "balanced".
	ExploreMode string `yaml:"explore_mode,omitempty"`

	// UnmappedTypeAction controls what the type-mapper FallbackChain
	// does for vendor-specific column types (PG inet/cidr/macaddr,
	// MSSQL hierarchyid, etc.) when no AI fallback is configured.
	// Valid values:
	//   "fail"               — emit empty SQL type so DDL emission
	//                          fails visibly (the safe default)
	//   "skip"               — emit empty (same effect today; future
	//                          writer changes can interpret as skip)
	//   "conservative-text"  — emit the target's most-permissive text
	//                          type (NVARCHAR(MAX) on MSSQL,
	//                          LONGTEXT on MySQL, TEXT on PG); lossy
	//                          but lets the migration progress
	// Defaults to "fail" via applyDefaults; safer than silently
	// degrading without a user opt-in. Issue #170.
	UnmappedTypeAction string `yaml:"unmapped_type_action,omitempty"`

	// ApproxTypeAction controls what the type-mapper FallbackChain does
	// for columns that map deterministically but with IsApproximate=true
	// (known-lossy mappings — e.g. PG INTERVAL → MSSQL NVARCHAR(255),
	// PG ENUM → VARCHAR(255), JSONB → MySQL JSON). Valid values:
	//   "deterministic"  — keep the deterministic mapping; emit an INFO
	//                      log naming the affected columns
	//   "ai_fallback"    — route any table containing approx columns
	//                      through the AI fallback (only fires when AI
	//                      is configured; ignored otherwise). Costs an
	//                      AI call per affected table.
	// Default depends on AI availability at runtime (issue #209): when
	// AI is configured, defaults to "ai_fallback" (consistent with how
	// Raw / table-DDL-error / finalization-error / error-diagnosis paths
	// already default-on when AI is available — configuring AI is an
	// implicit opt-in). When AI is NOT configured, defaults to
	// "deterministic" (no AI to route to). Users who want
	// "deterministic" with AI configured can set the field explicitly.
	// Issues #197, #209.
	ApproxTypeAction string `yaml:"approx_type_action,omitempty"`
}

// CreateIndexesEnabled returns the effective create_indexes setting.
func (m MigrationConfig) CreateIndexesEnabled() bool {
	return boolPtrDefault(m.CreateIndexes, true)
}

// CreateForeignKeysEnabled returns the effective create_foreign_keys setting.
func (m MigrationConfig) CreateForeignKeysEnabled() bool {
	return boolPtrDefault(m.CreateForeignKeys, true)
}

// SchemaContractEnabled returns true when the operator configured the
// DLT-style schema contract surface.
func (m MigrationConfig) SchemaContractEnabled() bool {
	return m.SchemaContract != nil
}

// SchemaEvolutionEnabled returns true when the operator opted into either the
// legacy schema evolution surface or the schema contract replacement.
func (m MigrationConfig) SchemaEvolutionEnabled() bool {
	return m.SchemaEvolution != nil || m.SchemaContract != nil
}

// SchemaEvolutionDeprecationWarning returns the user-facing warning for
// configs that still opt into the legacy schema evolution section.
func (m MigrationConfig) SchemaEvolutionDeprecationWarning() string {
	if m.SchemaEvolution == nil {
		return ""
	}
	return schemaEvolutionDeprecationWarning
}

// SchemaContractTablesMode returns the effective tables contract mode.
func (m MigrationConfig) SchemaContractTablesMode() SchemaContractMode {
	if m.SchemaContract == nil || m.SchemaContract.Tables == "" {
		return SchemaContractEvolve
	}
	return m.SchemaContract.Tables
}

// SchemaContractColumnsMode returns the effective columns contract mode.
func (m MigrationConfig) SchemaContractColumnsMode() SchemaContractMode {
	if m.SchemaContract == nil || m.SchemaContract.Columns == "" {
		return SchemaContractEvolve
	}
	return m.SchemaContract.Columns
}

// SchemaContractDataTypeMode returns the effective data_type contract mode.
func (m MigrationConfig) SchemaContractDataTypeMode() SchemaContractMode {
	if m.SchemaContract == nil || m.SchemaContract.DataType == "" {
		return SchemaContractEvolve
	}
	return m.SchemaContract.DataType
}

// AddedColumnSchemaEvolutionPolicy returns the effective added-column policy.
// The absent section preserves read-only drift reporting. Once the section is
// present, omitted added_column defaults to auto.
func (m MigrationConfig) AddedColumnSchemaEvolutionPolicy() SchemaEvolutionPolicy {
	if m.SchemaContract != nil {
		return schemaContractColumnsPolicy(m.SchemaContractColumnsMode())
	}
	if m.SchemaEvolution == nil {
		return SchemaEvolutionLog
	}
	if m.SchemaEvolution.AddedColumn == "" {
		return SchemaEvolutionAuto
	}
	if m.SchemaEvolution.AddedColumn == SchemaEvolutionDiscard {
		return SchemaEvolutionDiscardValue
	}
	return m.SchemaEvolution.AddedColumn
}

// NullabilityChangeSchemaEvolutionPolicy returns the effective nullability
// policy. The absent section preserves read-only drift reporting. Once the
// section is present, omitted nullability_change defaults to auto.
func (m MigrationConfig) NullabilityChangeSchemaEvolutionPolicy() SchemaEvolutionPolicy {
	if m.SchemaContract != nil {
		return schemaContractDataTypePolicy(m.SchemaContractDataTypeMode())
	}
	if m.SchemaEvolution == nil {
		return SchemaEvolutionLog
	}
	if m.SchemaEvolution.NullabilityChange == "" {
		return SchemaEvolutionAuto
	}
	return m.SchemaEvolution.NullabilityChange
}

// TypeChangeSchemaEvolutionPolicy returns the effective type-change policy.
// Type evolution can rewrite target storage, so it stays log-only unless the
// operator explicitly opts into migration.schema_evolution.type_change.
func (m MigrationConfig) TypeChangeSchemaEvolutionPolicy() SchemaEvolutionPolicy {
	if m.SchemaContract != nil {
		return schemaContractDataTypePolicy(m.SchemaContractDataTypeMode())
	}
	if m.SchemaEvolution == nil || m.SchemaEvolution.TypeChange == "" {
		return SchemaEvolutionLog
	}
	return m.SchemaEvolution.TypeChange
}

func schemaContractColumnsPolicy(mode SchemaContractMode) SchemaEvolutionPolicy {
	switch mode {
	case SchemaContractFreeze:
		return SchemaEvolutionFail
	case SchemaContractDiscardRow:
		return SchemaEvolutionLog
	case SchemaContractDiscardValue:
		return SchemaEvolutionDiscardValue
	case SchemaContractReport:
		return SchemaEvolutionLog
	case "", SchemaContractEvolve:
		return SchemaEvolutionAuto
	default:
		return SchemaEvolutionFail
	}
}

func schemaContractDataTypePolicy(mode SchemaContractMode) SchemaEvolutionPolicy {
	switch mode {
	case SchemaContractFreeze:
		return SchemaEvolutionFail
	case SchemaContractDiscardRow, SchemaContractDiscardValue:
		return SchemaEvolutionLog
	case SchemaContractReport:
		return SchemaEvolutionLog
	case "", SchemaContractEvolve:
		return SchemaEvolutionAuto
	default:
		return SchemaEvolutionFail
	}
}

// NotifyOnSuccess reports whether successful completion notifications should
// be sent when a notification provider is configured.
func (m MigrationConfig) NotifyOnSuccess() bool {
	if m.Notify.OnSuccess == nil {
		return true
	}
	return *m.Notify.OnSuccess
}

// NotifyOnFailure reports whether failed or partial-run notifications should
// be sent when a notification provider is configured.
func (m MigrationConfig) NotifyOnFailure() bool {
	if m.Notify.OnFailure == nil {
		return true
	}
	return *m.Notify.OnFailure
}

// NotifyOnStart reports whether the existing migration-start notification
// should be sent. Start has no separate knob; it follows whether any
// completion notification is enabled.
func (m MigrationConfig) NotifyOnStart() bool {
	return m.NotifyOnSuccess() || m.NotifyOnFailure()
}

func boolPtrDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// ValidationConfig configures the post-transfer validation passes
// added in #226. The default zero value preserves pre-#226 behavior
// (row-count validation only); higher modes layer on additional checks
// and cost more wall time in proportion to table size.
//
// Modes (inclusive — each one runs everything below it):
//
//	count_only   row-count only (legacy default)
//	null_parity  + per-column NULL count parity (one query/table)
//	sample       + N random rows fetched + canonicalized + compared
//	full         RESERVED for a future iteration; rejected at runtime
type ValidationConfig struct {
	// Mode selects the validation passes. Empty string means
	// count_only (preserves pre-#226 default). Set to
	// "null_parity" or "sample"; "full" is reserved.
	Mode string `yaml:"mode,omitempty"`

	// SampleRows is the cap on Pass B's row sample. Zero means
	// the validation package's default (1000).
	SampleRows int `yaml:"sample_rows,omitempty"`

	// SampleRowsPercent sets an additional cap as a percent of the
	// table's row count: actual sample = min(SampleRows,
	// rows * SampleRowsPercent/100). Zero disables this cap.
	SampleRowsPercent float64 `yaml:"sample_rows_percent,omitempty"`

	// HashColumns is reserved for a future row-hash pass; not
	// consumed by this PR's null_parity / sample passes. Kept on
	// the config surface so existing YAML doesn't break if it
	// lands later.
	HashColumns []string `yaml:"hash_columns,omitempty"`

	// FailOnMismatch controls whether a failing pass causes the
	// orchestrator to exit non-zero. Default true. Set false for
	// log-only runs (useful in initial roll-outs to gather signal
	// without blocking).
	FailOnMismatch *bool `yaml:"fail_on_mismatch,omitempty"`

	// FailOnTimeout controls whether a row-count validation timeout
	// is treated as a failure or a warning (#253). Default true:
	// timeouts fail the run, matching the "fail loud, fail early"
	// stance used by #248's exit-code policy. Set to false to
	// restore pre-#253 behavior where timeouts logged a warning
	// and the run could still be reported successful.
	//
	// `json:"-"` so the field doesn't perturb the resume config
	// hash (same load-bearing rationale as MigrationConfig.AllowPartial).
	FailOnTimeout *bool `yaml:"fail_on_timeout,omitempty" json:"-"`

	// FailOnEstimateMismatch controls whether a count disagreement
	// observed via the estimated-counts fallback (after exact COUNT
	// timed out on one or both sides) is treated as a failure or a
	// warning (#253). Default true. Set to false to log-only.
	// Same `json:"-"` rationale as FailOnTimeout.
	FailOnEstimateMismatch *bool `yaml:"fail_on_estimate_mismatch,omitempty" json:"-"`

	// Timeout caps per-table validation runtime as a Go-format
	// duration string (e.g. "30s", "5m", "1h"). Empty means no cap
	// — the surrounding context's deadline is the only bound.
	// Stored as string rather than time.Duration so YAML "1h"
	// parses; the orchestrator runs it through time.ParseDuration
	// at use time (Codex review on #226).
	Timeout string `yaml:"timeout,omitempty"`

	// MaxParallel bounds how many tables validate concurrently.
	// Zero uses the validation package's default (4). Each
	// parallel table consumes ~one DB connection per side per
	// pass; bounded fan-out prevents pool exhaustion on schemas
	// with 100+ tables (Codex review on #226).
	MaxParallel int `yaml:"max_parallel,omitempty"`
}

// LoadOptions controls configuration loading behavior.
type LoadOptions struct {
	SuppressWarnings bool
}

// Sanitized returns a copy of the config with sensitive fields redacted
func (c *Config) Sanitized() *Config {
	sanitized := *c // shallow copy

	// Redact source credentials
	sanitized.Source.Password = "[REDACTED]"

	// Redact target credentials
	sanitized.Target.Password = "[REDACTED]"

	if c.AI != nil {
		ai := *c.AI
		ai.APIKey = "[REDACTED]"
		sanitized.AI = &ai
	}

	if c.Slack != nil {
		slack := *c.Slack
		slack.WebhookURL = "[REDACTED]"
		sanitized.Slack = &slack
	}

	return &sanitized
}

// SanitizedYAML returns the config as YAML with sensitive fields redacted
func (c *Config) SanitizedYAML() string {
	sanitized := c.Sanitized()
	data, err := yaml.Marshal(sanitized)
	if err != nil {
		return fmt.Sprintf("error marshaling config: %v", err)
	}
	return string(data)
}

// UnmarshalYAML accepts DLT's two schema_contract forms:
//
//	schema_contract: report
//	schema_contract:
//	  columns: discard_value
//	  data_type: freeze
func (c *SchemaContractConfig) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!null" || strings.TrimSpace(node.Value) == "" {
			return nil
		}
		mode := SchemaContractMode(strings.TrimSpace(node.Value))
		c.Tables = mode
		c.Columns = mode
		c.DataType = mode
		return nil
	case yaml.MappingNode:
		type plain SchemaContractConfig
		var decoded plain
		if err := node.Decode(&decoded); err != nil {
			return err
		}
		*c = SchemaContractConfig(decoded)
		return nil
	default:
		return fmt.Errorf("migration.schema_contract must be a mode string or mapping")
	}
}
