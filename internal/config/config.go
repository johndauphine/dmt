package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/johndauphine/dmt/internal/dbconfig"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
	"gopkg.in/yaml.v3"

	// Import driver packages to trigger init() registration before validation
	_ "github.com/johndauphine/dmt/internal/driver/mssql"
	_ "github.com/johndauphine/dmt/internal/driver/mysql"
	_ "github.com/johndauphine/dmt/internal/driver/postgres"
	_ "github.com/johndauphine/dmt/internal/driver/sqlite"
)

// Type aliases for database configuration types.
// These are defined in dbconfig package to break circular imports with driver package.
type SourceConfig = dbconfig.SourceConfig
type TargetConfig = dbconfig.TargetConfig

// canonicalDriverName returns the canonical driver name for a given type.
// For example, "pg" -> "postgres", "sqlserver" -> "mssql".
// Uses the driver registry for lookup.
func canonicalDriverName(dbType string) string {
	if d, err := driver.Get(dbType); err == nil {
		return d.Name()
	}
	return dbType
}

// isValidDriverType returns true if the type is a valid driver type or alias.
// Uses the driver registry for validation.
func isValidDriverType(dbType string) bool {
	return driver.IsRegistered(dbType)
}

// availableDriverTypes returns a list of supported driver types.
// Uses the driver registry to get available drivers.
func availableDriverTypes() []string {
	return driver.Available()
}

// expandTilde expands ~ or ~/ at the start of a path to the user's home directory
func expandTilde(path string) string {
	if path == "" {
		return path
	}
	if path == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	if strings.HasPrefix(path, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, path[2:])
	}
	return path
}

// Template patterns for secret expansion:
// - filePattern: ${file:/path/to/file} - any path characters allowed
// - envPattern: ${env:VAR_NAME} - valid env var names only [A-Za-z_][A-Za-z0-9_]*
// - legacyEnvPattern: ${VAR_NAME} - legacy shorthand for ${env:VAR_NAME}
var filePattern = regexp.MustCompile(`^\$\{file:(.+)\}$`)
var envPattern = regexp.MustCompile(`^\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}$`)
var legacyEnvPattern = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

// expandTemplateValue expands template patterns in a string value.
// Supported patterns:
//   - ${file:/path/to/file} - reads value from file (trimmed of whitespace)
//   - ${env:VAR_NAME} - reads value from environment variable (explicit)
//   - ${VAR_NAME} - reads value from environment variable (legacy shorthand)
//   - Any other value is returned as-is (cleartext password)
//
// Returns the expanded value and any error encountered.
func expandTemplateValue(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	// Check for ${file:...} pattern
	if matches := filePattern.FindStringSubmatch(value); matches != nil {
		filePath := expandTilde(matches[1])
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("reading secret from file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	// Check for ${env:VAR_NAME} pattern (explicit, restricted to valid env var names)
	if matches := envPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - allows optional env vars
		// but may cause silent auth failures if variable name is misspelled.
		return os.Getenv(matches[1]), nil
	}

	// Check for legacy ${VAR_NAME} pattern (shorthand for ${env:VAR_NAME})
	if matches := legacyEnvPattern.FindStringSubmatch(value); matches != nil {
		// Return empty string if env var not set - matches ${env:VAR} behavior.
		// This allows optional env vars but may cause silent auth failures
		// if the variable name is misspelled. Use explicit ${env:VAR} for clarity.
		return os.Getenv(matches[1]), nil
	}

	// Not a template pattern - return as-is (cleartext)
	return value, nil
}

// AutoConfig tracks which values were auto-configured and why
type AutoConfig struct {
	// System resources detected
	AvailableMemoryMB    int64
	EffectiveMaxMemoryMB int64 // After applying user limit and 70% cap
	CPUCores             int

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

	// Target memory used for calculations
	TargetMemoryMB int64
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
	//   Anthropic: claude-haiku-4-5-20251001
	//   OpenAI: gpt-4o
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

// MigrationConfig holds migration behavior settings
type MigrationConfig struct {
	MaxSourceConnections   int      `yaml:"max_source_connections"` // Max source database connections
	MaxTargetConnections   int      `yaml:"max_target_connections"` // Max target database connections
	ChunkSize              int      `yaml:"chunk_size"`
	MaxPartitions          int      `yaml:"max_partitions"`
	Workers                int      `yaml:"workers"`
	LargeTableThreshold    int64    `yaml:"large_table_threshold"`
	IncludeTables          []string `yaml:"include_tables"` // Only migrate these tables (glob patterns)
	ExcludeTables          []string `yaml:"exclude_tables"` // Skip these tables (glob patterns)
	DataDir                string   `yaml:"data_dir"`
	TargetMode             string   `yaml:"target_mode"`              // "drop_recreate" (default) or "upsert"
	StrictConsistency      bool     `yaml:"strict_consistency"`       // Use table locks instead of NOLOCK
	CreateIndexes          bool     `yaml:"create_indexes"`           // Create non-PK indexes
	CreateForeignKeys      bool     `yaml:"create_foreign_keys"`      // Create foreign key constraints
	CreateCheckConstraints bool     `yaml:"create_check_constraints"` // Create CHECK constraints
	SampleValidation       bool     `yaml:"sample_validation"`        // (legacy) Enable PK-existence sample validation; superseded by validation.mode (#226)
	SampleSize             int      `yaml:"sample_size"`              // (legacy) Number of rows to sample for validation; superseded by validation.sample_rows (#226)
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
	AllowPartial           bool     `yaml:"allow_partial" json:"-"`

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
	// pre-#226 behavior (row-count parity only). See the Validation
	// struct's field docs for the full mode taxonomy.
	Validation ValidationConfig `yaml:"validation"`
	ReadAheadBuffers       int      `yaml:"read_ahead_buffers"`       // Number of chunks to read ahead (default=8)
	WriteAheadWriters      int      `yaml:"write_ahead_writers"`      // Number of parallel writers per job (default=2)
	ParallelReaders        int      `yaml:"parallel_readers"`         // Number of parallel readers per job (default=2)
	UpsertMergeChunkSize   int      `yaml:"upsert_merge_chunk_size"`  // Chunk size for upsert UPDATE+INSERT (default=5000, auto-tuned)
	MaxMemoryMB            int64    `yaml:"max_memory_mb"`            // Max memory to use (default=70% of available, hard cap at 70%)
	// Restartability settings
	CheckpointFrequency  int `yaml:"checkpoint_frequency"`   // Save progress every N chunks (default=10)
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

// ValidationConfig configures the post-transfer validation passes
// added in #226. The default zero value preserves pre-#226 behavior
// (row-count parity only); higher modes layer on additional checks
// and cost more wall time in proportion to table size.
//
// Modes (inclusive — each one runs everything below it):
//
//	count_only   row-count parity only (legacy default)
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

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	return LoadWithOptions(path, LoadOptions{})
}

// LoadWithOptions reads configuration from a YAML file with options.
func LoadWithOptions(path string, opts LoadOptions) (*Config, error) {
	// Check file permissions before reading (warns if insecure)
	if warning := checkFilePermissions(path); warning != "" && !opts.SuppressWarnings {
		fmt.Fprint(os.Stderr, warning)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	return LoadBytes(data)
}

// expandYAMLTemplates expands ${file:path} and ${env:VAR} templates in YAML string.
// Also supports legacy ${VAR} syntax for backward compatibility.
// This runs before YAML parsing to allow templates in any field.
//
// Security note: File paths are not restricted - users should only use trusted paths
// like /run/secrets/ for Docker secrets. Avoid user-controlled paths.
func expandYAMLTemplates(yamlStr string) (string, error) {
	// Pattern to match ${file:path}, ${env:VAR}, or ${VAR} in YAML
	// - file: allows any path characters (user responsibility to use safe paths)
	// - env: restricted to valid env var names [A-Za-z_][A-Za-z0-9_]*
	// - legacy ${VAR}: also restricted to valid env var names
	pattern := regexp.MustCompile(`\$\{file:([^}]+)\}|\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

	var firstErr error
	result := pattern.ReplaceAllStringFunc(yamlStr, func(match string) string {
		// If we've already encountered an error, leave subsequent matches unchanged
		if firstErr != nil {
			return match
		}

		expanded, err := expandTemplateValue(match)
		if err != nil {
			firstErr = err
			return match // Keep original on error
		}
		return expanded
	})

	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}

// Expand resolves a single ${env:...}, ${file:...}, or legacy ${VAR}
// template in s. Non-template strings are returned unchanged. Pair with
// LoadRaw: callers who need a resolved single field (e.g. just the
// source-side password for a connection test) can scope expansion to
// one value at a time instead of rolling up the whole config.
func Expand(s string) (string, error) {
	return expandTemplateValue(s)
}

// LoadRaw reads a config YAML for *editing* — no secret-template expansion,
// no defaults application, no validation. Placeholders like ${env:DB_PASSWORD}
// and ${file:/run/secrets/x} survive as literal strings so the setup wizard
// can re-marshal them back to disk without exposing resolved secrets.
//
// This is intentionally narrow: the migration runtime must always use Load /
// LoadWithOptions so that templates resolve before connection attempts.
//
// Known limitations (each tracked as a follow-up):
//   - Templated non-string scalars (e.g. `source.port: ${env:DB_PORT}` or
//     `migration.workers: ${env:DMT_WORKERS}`) fail YAML unmarshal here
//     because the int/bool field cannot hold the placeholder string. The
//     wizard's edit path then falls back to fresh-setup mode for such
//     configs. Fixing requires yaml.Node-level expansion of non-string
//     fields while preserving placeholders for string secrets.
//   - YAML omission of an optional bool (e.g. `create_indexes`) is
//     indistinguishable from explicit `false` in the resulting Config.
//     The wizard's EditMode currently treats both as `false`, which can
//     flip the documented `y` default to `n` on round-trip. The fix
//     mirrors the AIAdjust precedent: change the field to *bool so nil
//     means "absent".
func LoadRaw(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return &cfg, nil
}

// LoadBytes reads configuration from YAML bytes.
func LoadBytes(data []byte) (*Config, error) {
	// Expand templates (${file:path}, ${env:VAR}, and legacy ${VAR} syntax)
	expanded, err := expandYAMLTemplates(string(data))
	if err != nil {
		return nil, fmt.Errorf("expanding templates: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Apply defaults
	if err := cfg.applyDefaults(); err != nil {
		return nil, fmt.Errorf("applying defaults: %w", err)
	}

	// Validate
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// DefaultDataDir returns the default data directory for state storage.
func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".dmt")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

// applyGlobalDefaults loads migration defaults from the global secrets file
// and applies them as base values (only if not already set in the config).
func (c *Config) applyGlobalDefaults() {
	secretsCfg, err := secrets.Load()
	if err != nil {
		// Secrets file is optional - continue without global defaults
		return
	}

	defaults := secretsCfg.GetMigrationDefaults()
	if defaults == nil {
		return
	}

	// Performance settings - only apply if not set in migration config
	if c.Migration.Workers == 0 && defaults.Workers > 0 {
		c.Migration.Workers = defaults.Workers
	}
	if c.Migration.MaxSourceConnections == 0 && defaults.MaxSourceConnections > 0 {
		c.Migration.MaxSourceConnections = defaults.MaxSourceConnections
	}
	if c.Migration.MaxTargetConnections == 0 && defaults.MaxTargetConnections > 0 {
		c.Migration.MaxTargetConnections = defaults.MaxTargetConnections
	}
	if c.Migration.MaxMemoryMB == 0 && defaults.MaxMemoryMB > 0 {
		c.Migration.MaxMemoryMB = defaults.MaxMemoryMB
	}
	if c.Migration.ReadAheadBuffers == 0 && defaults.ReadAheadBuffers > 0 {
		c.Migration.ReadAheadBuffers = defaults.ReadAheadBuffers
	}
	if c.Migration.WriteAheadWriters == 0 && defaults.WriteAheadWriters > 0 {
		c.Migration.WriteAheadWriters = defaults.WriteAheadWriters
	}
	if c.Migration.ParallelReaders == 0 && defaults.ParallelReaders > 0 {
		c.Migration.ParallelReaders = defaults.ParallelReaders
	}

	// Boolean settings - apply from global defaults when explicitly set (non-nil pointer)
	// and the migration config value is false.
	//
	// Limitation: Go's bool defaults to false, so we cannot distinguish between
	// "user didn't set this" and "user explicitly set to false". This means:
	//   - Global true  + migration unset/false → true  (global wins)
	//   - Global true  + migration true        → true  (both agree)
	//   - Global false + migration unset/false → false (both agree)
	//   - Global false + migration true        → true  (migration wins)
	//
	// In practice: you CAN override a global "false" to "true" per-migration,
	// but you CANNOT override a global "true" to "false" per-migration.
	if defaults.CreateIndexes != nil && !c.Migration.CreateIndexes {
		c.Migration.CreateIndexes = *defaults.CreateIndexes
	}
	if defaults.CreateForeignKeys != nil && !c.Migration.CreateForeignKeys {
		c.Migration.CreateForeignKeys = *defaults.CreateForeignKeys
	}
	if defaults.CreateCheckConstraints != nil && !c.Migration.CreateCheckConstraints {
		c.Migration.CreateCheckConstraints = *defaults.CreateCheckConstraints
	}
	if defaults.StrictConsistency != nil && !c.Migration.StrictConsistency {
		c.Migration.StrictConsistency = *defaults.StrictConsistency
	}
	if defaults.SampleValidation != nil && !c.Migration.SampleValidation {
		c.Migration.SampleValidation = *defaults.SampleValidation
	}
	if c.Migration.SampleSize == 0 && defaults.SampleSize > 0 {
		c.Migration.SampleSize = defaults.SampleSize
	}

	// Runtime tuning: inherit from secrets only if NEITHER the new
	// `runtime_tuning` field nor the deprecated `ai_adjust` alias is
	// set per-migration. Both layers use *bool so we can correctly
	// distinguish unset (nil) from explicit-false (issue #149).
	// Without this, the auto-enable site downstream fills nil → &true
	// and silently overrides any per-migration `runtime_tuning: false`.
	//
	// Checking BOTH per-migration names matters during the #211
	// deprecation cycle: a config that still uses `ai_adjust: false`
	// must keep that user-intent through the inherit step so
	// normalizeRuntimeTuningFields (which runs after) can copy it
	// into RuntimeTuning unchanged. Without this two-name guard, a
	// secrets file's auto-applied `runtime_tuning: true` default
	// would silently flip the per-migration `ai_adjust: false`.
	//
	// On the secrets side, prefer the new field; fall through to the
	// legacy field if the secrets file is still using the old name.
	// The secrets-layer legacy field is migrated silently here to
	// avoid one warning per per-migration config that inherits from
	// the same secrets file.
	if c.Migration.RuntimeTuning == nil && c.Migration.AIAdjust == nil {
		switch {
		case defaults.RuntimeTuning != nil:
			v := *defaults.RuntimeTuning
			c.Migration.RuntimeTuning = &v
		case defaults.AIAdjust != nil:
			v := *defaults.AIAdjust
			c.Migration.RuntimeTuning = &v
			// The acceptance criteria for #211 say "emit a WARN per
			// migration when set" — set anywhere, including the secrets
			// layer. Without this warning, a user with
			// `migration_defaults.ai_adjust: false` in their global secrets
			// file sees no signal that they need to rename before v6.0.0
			// removes the legacy field.
			logging.Warn(`field "migration_defaults.ai_adjust" in secrets file is deprecated; ` +
				`rename to "migration_defaults.runtime_tuning". Will be removed in v6.0.0.`)
		}
	}
	if c.Migration.RuntimeTuningInterval == "" && c.Migration.AIAdjustInterval == "" {
		switch {
		case defaults.RuntimeTuningInterval != "":
			c.Migration.RuntimeTuningInterval = defaults.RuntimeTuningInterval
		case defaults.AIAdjustInterval != "":
			c.Migration.RuntimeTuningInterval = defaults.AIAdjustInterval
			logging.Warn(`field "migration_defaults.ai_adjust_interval" in secrets file is deprecated; ` +
				`rename to "migration_defaults.runtime_tuning_interval". Will be removed in v6.0.0.`)
		}
	}

	// Checkpoint and recovery
	if c.Migration.CheckpointFrequency == 0 && defaults.CheckpointFrequency > 0 {
		c.Migration.CheckpointFrequency = defaults.CheckpointFrequency
	}
	if c.Migration.MaxRetries == 0 && defaults.MaxRetries > 0 {
		c.Migration.MaxRetries = defaults.MaxRetries
	}
	if c.Migration.HistoryRetentionDays == 0 && defaults.HistoryRetentionDays > 0 {
		c.Migration.HistoryRetentionDays = defaults.HistoryRetentionDays
	}

	// Data directory
	if c.Migration.DataDir == "" && defaults.DataDir != "" {
		c.Migration.DataDir = defaults.DataDir
	}
}

// normalizeRuntimeTuningFields handles the #211 ai_adjust →
// runtime_tuning rename deprecation cycle.
//
// Semantics (per the issue's acceptance criteria):
//
//   - If runtime_tuning is set, it wins (canonical). If ai_adjust is
//     also set with a conflicting value, emit a WARN naming both
//     fields; the new field's value is used.
//   - If only ai_adjust is set (legacy configs), copy it into
//     runtime_tuning and emit a one-time deprecation WARN.
//   - If both are set with the same value, emit the deprecation WARN
//     for the legacy field only (the user already migrated the
//     intent; they just left the old field around).
//   - If neither is set, leave both nil — downstream auto-enable
//     handles that case.
//
// Same rules apply to the interval pair.
//
// After this function returns, all downstream code should read
// RuntimeTuning / RuntimeTuningInterval exclusively; the AIAdjust*
// fields are cleared so a later reader can't accidentally pick them
// up and bypass the rename.
func (c *Config) normalizeRuntimeTuningFields() {
	// WARN messages follow the VERSIONING.md model — same wording the
	// policy doc uses to describe a v5-to-v6 deprecation cycle, naming
	// the target removal version so operators can plan migrations.
	const deprecatedBoolMsg = `field "ai_adjust" is deprecated; rename to "runtime_tuning". Will be removed in v6.0.0.`
	const deprecatedIntervalMsg = `field "ai_adjust_interval" is deprecated; rename to "runtime_tuning_interval". Will be removed in v6.0.0.`

	// Boolean enable knob.
	switch {
	case c.Migration.RuntimeTuning != nil && c.Migration.AIAdjust != nil:
		if *c.Migration.AIAdjust != *c.Migration.RuntimeTuning {
			logging.Warn(`field "ai_adjust"=%t conflicts with "runtime_tuning"=%t; `+
				`using "runtime_tuning". Remove "ai_adjust" from your config to silence this warning; `+
				`it will be removed in v6.0.0.`,
				*c.Migration.AIAdjust, *c.Migration.RuntimeTuning)
		} else {
			logging.Warn(deprecatedBoolMsg)
		}
	case c.Migration.RuntimeTuning == nil && c.Migration.AIAdjust != nil:
		v := *c.Migration.AIAdjust
		c.Migration.RuntimeTuning = &v
		logging.Warn(deprecatedBoolMsg)
	}
	c.Migration.AIAdjust = nil

	// Interval.
	switch {
	case c.Migration.RuntimeTuningInterval != "" && c.Migration.AIAdjustInterval != "":
		if c.Migration.AIAdjustInterval != c.Migration.RuntimeTuningInterval {
			logging.Warn(`field "ai_adjust_interval"=%q conflicts with `+
				`"runtime_tuning_interval"=%q; using "runtime_tuning_interval". `+
				`Remove "ai_adjust_interval" from your config to silence this warning; `+
				`it will be removed in v6.0.0.`,
				c.Migration.AIAdjustInterval, c.Migration.RuntimeTuningInterval)
		} else {
			logging.Warn(deprecatedIntervalMsg)
		}
	case c.Migration.RuntimeTuningInterval == "" && c.Migration.AIAdjustInterval != "":
		c.Migration.RuntimeTuningInterval = c.Migration.AIAdjustInterval
		logging.Warn(deprecatedIntervalMsg)
	}
	c.Migration.AIAdjustInterval = ""
}

func (c *Config) applyDefaults() error {
	// Apply global defaults from secrets file first
	c.applyGlobalDefaults()

	// #211: resolve the rename deprecation cycle before any downstream
	// code reads runtime-tuning fields. After this call, the canonical
	// RuntimeTuning / RuntimeTuningInterval fields carry the user's
	// intent regardless of which name the YAML used; AIAdjust* are
	// drained.
	c.normalizeRuntimeTuningFields()

	// Capture original values before auto-tuning
	c.autoConfig.OriginalWorkers = c.Migration.Workers
	c.autoConfig.OriginalChunkSize = c.Migration.ChunkSize
	c.autoConfig.OriginalReadAheadBuffers = c.Migration.ReadAheadBuffers
	c.autoConfig.OriginalMaxPartitions = c.Migration.MaxPartitions
	c.autoConfig.OriginalMaxSourceConns = c.Migration.MaxSourceConnections
	c.autoConfig.OriginalMaxTargetConns = c.Migration.MaxTargetConnections
	c.autoConfig.OriginalWriteAheadWriters = c.Migration.WriteAheadWriters
	c.autoConfig.OriginalParallelReaders = c.Migration.ParallelReaders
	c.autoConfig.OriginalLargeTableThresh = c.Migration.LargeTableThreshold
	c.autoConfig.OriginalSampleSize = c.Migration.SampleSize
	c.autoConfig.OriginalUpsertMergeChunkSize = c.Migration.UpsertMergeChunkSize
	c.autoConfig.OriginalSourceChunkSize = c.Source.ChunkSize
	c.autoConfig.OriginalTargetChunkSize = c.Target.ChunkSize
	c.autoConfig.OriginalCheckpointFrequency = c.Migration.CheckpointFrequency
	c.autoConfig.OriginalMaxRetries = c.Migration.MaxRetries

	// Detect system resources (only if not already set, for testing)
	if c.autoConfig.CPUCores == 0 {
		c.autoConfig.CPUCores = runtime.NumCPU()
	}
	availMem, err := getAvailableMemoryMB()
	if err != nil {
		// If user set max_memory_mb, we don't need system detection
		if c.Migration.MaxMemoryMB > 0 {
			availMem = c.Migration.MaxMemoryMB
		} else {
			return fmt.Errorf("detecting available memory: %w (set max_memory_mb in config to override)", err)
		}
	}
	c.autoConfig.AvailableMemoryMB = availMem

	// Calculate target memory for auto-tuning (50% of limit)
	// If user specified max_memory_mb, use that as the base
	// Otherwise use available memory
	baseMemoryMB := c.autoConfig.AvailableMemoryMB
	if c.Migration.MaxMemoryMB > 0 && c.Migration.MaxMemoryMB < baseMemoryMB {
		baseMemoryMB = c.Migration.MaxMemoryMB
	}
	c.autoConfig.TargetMemoryMB = baseMemoryMB / 2

	// Source defaults - use driver registry for pluggable defaults
	if c.Source.Type == "" {
		c.Source.Type = "mssql" // Default source is SQL Server for backward compat
	}
	if sourceDriver, err := driver.Get(c.Source.Type); err == nil {
		defaults := sourceDriver.Defaults()
		if c.Source.Port == 0 && defaults.Port > 0 {
			c.Source.Port = defaults.Port
		}
		if c.Source.Schema == "" && defaults.Schema != "" {
			c.Source.Schema = defaults.Schema
		}
		if c.Source.SSLMode == "" && defaults.SSLMode != "" {
			c.Source.SSLMode = defaults.SSLMode
		}
		if c.Source.Encrypt == nil {
			c.Source.Encrypt = &defaults.Encrypt
		}
		if c.Source.PacketSize == 0 && defaults.PacketSize > 0 {
			c.Source.PacketSize = defaults.PacketSize
		}
	}

	// Target defaults - use driver registry for pluggable defaults
	if c.Target.Type == "" {
		c.Target.Type = "postgres" // Default target is PostgreSQL for backward compat
	}
	if targetDriver, err := driver.Get(c.Target.Type); err == nil {
		defaults := targetDriver.Defaults()
		if c.Target.Port == 0 && defaults.Port > 0 {
			c.Target.Port = defaults.Port
		}
		if c.Target.Schema == "" && defaults.Schema != "" {
			c.Target.Schema = defaults.Schema
		}
		// MySQL uses database name as schema (no separate schema concept)
		if c.Target.Type == "mysql" && c.Target.Schema == "" && c.Target.Database != "" {
			c.Target.Schema = c.Target.Database
		}
		if c.Target.SSLMode == "" && defaults.SSLMode != "" {
			c.Target.SSLMode = defaults.SSLMode
		}
		if c.Target.Encrypt == nil {
			c.Target.Encrypt = &defaults.Encrypt
		}
		if c.Target.PacketSize == 0 && defaults.PacketSize > 0 {
			c.Target.PacketSize = defaults.PacketSize
		}
	}

	// Set default max connections for source and target
	if c.Migration.MaxSourceConnections == 0 {
		c.Migration.MaxSourceConnections = 12
	}
	if c.Migration.MaxTargetConnections == 0 {
		c.Migration.MaxTargetConnections = 12
	}
	// Auto-detect CPU cores for workers
	// Formula: (cores - 2), clamped to 4-12 for optimal performance
	// This aligns with Rust implementation for consistent behavior
	if c.Migration.Workers == 0 {
		cores := runtime.NumCPU()
		c.Migration.Workers = cores - 2
		if c.Migration.Workers < 4 {
			c.Migration.Workers = 4
		}
		if c.Migration.Workers > 12 {
			c.Migration.Workers = 12 // Cap at 12 workers (diminishing returns beyond)
		}
	}
	if c.Migration.MaxPartitions == 0 {
		c.Migration.MaxPartitions = c.Migration.Workers // Match workers
	}
	// Auto-tune chunk size based on available RAM
	// Formula: 75K base + 25K per 8GB RAM, clamped to 50K-200K
	// This matches the Rust implementation for consistent behavior
	targetMemoryMB := c.autoConfig.TargetMemoryMB
	if c.Migration.ChunkSize == 0 {
		ramGB := float64(c.autoConfig.AvailableMemoryMB) / 1024.0
		chunkSize := 75000 + int(ramGB*25000.0/8.0)
		if chunkSize < 50000 {
			chunkSize = 50000
		}
		if chunkSize > 200000 {
			chunkSize = 200000
		}
		c.Migration.ChunkSize = chunkSize
	}
	if c.Migration.LargeTableThreshold == 0 {
		c.Migration.LargeTableThreshold = 5000000
	}
	if c.Migration.DataDir == "" {
		home, _ := os.UserHomeDir()
		c.Migration.DataDir = filepath.Join(home, ".dmt")
	} else {
		c.Migration.DataDir = expandTilde(c.Migration.DataDir)
	}
	if c.Migration.TargetMode == "" {
		c.Migration.TargetMode = "drop_recreate" // Default: drop and recreate tables
	}
	if c.Migration.UnmappedTypeAction == "" {
		// Default: fail visibly when an unmapped type appears and no AI
		// fallback is configured. Users opt into degraded modes
		// ("conservative-text", "skip") explicitly. Issue #170.
		c.Migration.UnmappedTypeAction = "fail"
	}
	// ApproxTypeAction default is intentionally NOT set here — issue
	// #209. NewFallbackChain in internal/driver fills it in based on
	// whether AI is available at runtime: ai_fallback when AI is
	// configured (implicit opt-in, consistent with how Raw / table-
	// DDL-error / finalization-error / error-diagnosis already work),
	// deterministic when AI isn't configured. Users who want the
	// pre-#209 behavior can set approx_type_action: deterministic
	// explicitly in their YAML.
	if c.Migration.RuntimeTuning == nil {
		// Default-enable the rule-based runtime controller (#172).
		// Belt-and-suspenders fallback in case the secrets layer
		// didn't populate it (e.g., no secrets file at all). The
		// controller has no AI dependency post-#172, so default-on is
		// the right behavior even in no-AI environments (Codex review
		// on PR #195).
		v := true
		c.Migration.RuntimeTuning = &v
	}
	if c.Migration.SampleSize == 0 {
		c.Migration.SampleSize = 100 // Default sample size for validation
	}
	// Auto-tune parallel writers based on target driver defaults
	if c.Migration.WriteAheadWriters == 0 {
		if targetDriver, err := driver.Get(c.Target.Type); err == nil {
			defaults := targetDriver.Defaults()
			if defaults.ScaleWritersWithCores {
				// Scale with CPU cores (e.g., PostgreSQL COPY handles parallelism well)
				cores := c.autoConfig.CPUCores
				writers := cores / 4
				if writers < defaults.WriteAheadWriters {
					writers = defaults.WriteAheadWriters
				}
				c.Migration.WriteAheadWriters = writers
			} else {
				// Use fixed value (e.g., MSSQL TABLOCK serializes writes)
				c.Migration.WriteAheadWriters = defaults.WriteAheadWriters
			}
		} else {
			// Fallback for unknown drivers - log warning as this may indicate a config issue
			logging.Warn("Unknown target driver type '%s', using fallback WriteAheadWriters=2", c.Target.Type)
			c.Migration.WriteAheadWriters = 2
		}
	}
	// Auto-tune parallel readers based on CPU cores
	if c.Migration.ParallelReaders == 0 {
		cores := c.autoConfig.CPUCores
		readers := cores / 4
		if readers < 2 {
			readers = 2
		}
		c.Migration.ParallelReaders = readers
	}
	// MSSQLRowsPerBatch defaults to chunk_size (set after chunk_size is finalized)
	// This is applied later since it depends on ChunkSize being set
	if c.Migration.ReadAheadBuffers == 0 {
		// Scale buffers: enough to keep writers fed, but within memory limits
		// Formula: targetMemoryMB / workers / (chunkSize * 500 bytes avg)
		bytesPerChunk := int64(c.Migration.ChunkSize) * 500 // ~500 bytes per row average
		buffersPerWorker := (targetMemoryMB * 1024 * 1024) / int64(c.Migration.Workers) / bytesPerChunk
		c.Migration.ReadAheadBuffers = int(buffersPerWorker)
		if c.Migration.ReadAheadBuffers < 4 {
			c.Migration.ReadAheadBuffers = 4
		}
		if c.Migration.ReadAheadBuffers > 32 {
			c.Migration.ReadAheadBuffers = 32 // Cap to avoid excessive memory
		}
	}

	// Calculate effective memory limit for Go GC soft limit
	// Hard cap at 70% of available memory to leave room for OS and other processes
	hardCapMB := c.autoConfig.AvailableMemoryMB * 70 / 100
	effectiveMaxMB := hardCapMB
	if c.Migration.MaxMemoryMB > 0 {
		// User specified a limit - use it, but enforce hard cap
		if c.Migration.MaxMemoryMB < hardCapMB {
			effectiveMaxMB = c.Migration.MaxMemoryMB
		}
	}
	c.autoConfig.EffectiveMaxMemoryMB = effectiveMaxMB

	// Auto-size connection pools based on workers, readers, and writers
	// Each worker needs: parallel_readers source connections + write_ahead_writers target connections
	// Add 4 connections for headroom (orchestrator, health checks, etc.)
	requiredSourceConns := c.Migration.Workers*c.Migration.ParallelReaders + 4
	requiredTargetConns := c.Migration.Workers*c.Migration.WriteAheadWriters + 4
	if c.Migration.MaxSourceConnections < requiredSourceConns {
		c.Migration.MaxSourceConnections = requiredSourceConns
	}
	if c.Migration.MaxTargetConnections < requiredTargetConns {
		c.Migration.MaxTargetConnections = requiredTargetConns
	}

	// Default source chunk_size to migration chunk_size if not specified
	// This is the batch size for reading from the source database
	if c.Source.ChunkSize == 0 {
		c.Source.ChunkSize = c.Migration.ChunkSize
	}

	// Default target chunk_size to migration chunk_size if not specified
	// This is the batch size for writing to the target database
	if c.Target.ChunkSize == 0 {
		c.Target.ChunkSize = c.Migration.ChunkSize
	}

	// Auto-tune UpsertMergeChunkSize for upsert mode
	// This controls UPDATE+INSERT chunk size for MSSQL target
	// Smaller chunks reduce SQL Server memory pressure during merge operations
	if c.Migration.UpsertMergeChunkSize == 0 {
		if c.Migration.TargetMode == "upsert" {
			// For upsert mode, use smaller chunks based on available memory
			// Base: 5000 rows, scale up with memory (max 20000)
			memoryFactor := targetMemoryMB / 1024 // Scale factor per GB
			if memoryFactor < 1 {
				memoryFactor = 1
			}
			c.Migration.UpsertMergeChunkSize = int(5000 * memoryFactor)
			if c.Migration.UpsertMergeChunkSize > 20000 {
				c.Migration.UpsertMergeChunkSize = 20000
			}
			if c.Migration.UpsertMergeChunkSize < 2000 {
				c.Migration.UpsertMergeChunkSize = 2000
			}
		} else {
			// Not in upsert mode - set a sensible default anyway
			c.Migration.UpsertMergeChunkSize = 10000
		}
	}

	// Restartability defaults
	if c.Migration.CheckpointFrequency == 0 {
		c.Migration.CheckpointFrequency = 10 // Save progress every 10 chunks
	}
	if c.Migration.MaxRetries == 0 {
		c.Migration.MaxRetries = 3 // Retry failed tables 3 times
	}
	if c.Migration.HistoryRetentionDays == 0 {
		c.Migration.HistoryRetentionDays = 30 // Keep run history for 30 days
	}

	// AI features: load from secrets if not configured in config file
	// This allows AI to be configured globally in secrets without needing ai: section in each config
	if c.AI == nil {
		// No AI section in config - check if secrets has AI configuration
		secretsCfg, err := secrets.Load()
		if err == nil && secretsCfg.AI.DefaultProvider != "" {
			provider, provErr := secretsCfg.GetProvider(secretsCfg.AI.DefaultProvider)
			if provErr == nil && provider.APIKey != "" {
				c.AI = &AIConfig{
					Provider: secretsCfg.AI.DefaultProvider,
					APIKey:   provider.APIKey,
					Model:    provider.Model,
				}
			}
		}
	} else if c.AI.Provider != "" && c.AI.APIKey == "" {
		// AI section exists with provider but no API key - load key from secrets
		secretsCfg, err := secrets.Load()
		if err != nil {
			// Distinguish between "secrets file not found" (acceptable) and other errors (should be reported)
			if _, ok := err.(*secrets.SecretsNotFoundError); !ok {
				logging.Warn("failed to load secrets configuration for AI provider: %v", err)
			}
		} else {
			// Get the provider's API key from secrets
			provider, err := secretsCfg.GetProvider(c.AI.Provider)
			if err == nil && provider.APIKey != "" {
				c.AI.APIKey = provider.APIKey
			}
		}
	}

	// AI features: apply defaults when api_key is configured
	if c.AI != nil && c.AI.APIKey != "" {
		// Default provider to anthropic if not specified
		if c.AI.Provider == "" {
			c.AI.Provider = "anthropic"
		}
		// Normalize provider to lowercase for case-insensitive matching
		if c.AI.Provider != "" {
			c.AI.Provider = driver.NormalizeAIProvider(c.AI.Provider)
		}

		// Type mapping: auto-enable if not explicitly disabled
		if c.AI.TypeMapping == nil {
			c.AI.TypeMapping = &AITypeMappingConfig{}
		}
		if c.AI.TypeMapping.Enabled == nil {
			enabled := true
			c.AI.TypeMapping.Enabled = &enabled
		}

		// Runtime tuning: auto-enable when AI is configured, but only if the user
		// didn't explicitly set it. nil means "unset by user" — fill in true.
		// Pre-#149 this was a `bool` field with `if !c.Migration.AIAdjust { ...
		// = true }` which clobbered any explicit `ai_adjust: false`.
		if c.Migration.RuntimeTuning == nil {
			enabled := true
			c.Migration.RuntimeTuning = &enabled
		}
		if c.Migration.RuntimeTuningInterval == "" {
			c.Migration.RuntimeTuningInterval = "30s"
		}
	}

	// Slack notification: auto-enable when webhook URL is configured
	// Load webhook from secrets if not provided in config
	if c.Slack == nil || c.Slack.WebhookURL == "" {
		secretsCfg, err := secrets.Load()
		if err != nil {
			// Distinguish between "secrets file not found" (acceptable) and other errors (should be reported)
			if _, ok := err.(*secrets.SecretsNotFoundError); !ok {
				logging.Warn("failed to load secrets configuration for Slack webhook: %v", err)
			}
		} else if secretsCfg.Notifications.Slack.WebhookURL != "" {
			if c.Slack == nil {
				c.Slack = &SlackConfig{}
			}
			c.Slack.WebhookURL = secretsCfg.Notifications.Slack.WebhookURL
		}
	}
	// Auto-enable Slack notifications when webhook URL is available
	if c.Slack != nil && c.Slack.WebhookURL != "" && !c.Slack.Enabled {
		c.Slack.Enabled = true
	}

	return nil
}

// TableRowSize holds row size info for a table
type TableRowSize struct {
	Name             string
	RowCount         int64
	EstimatedRowSize int64
}

// RefineSettingsForRowSizes reports actual table row sizes for informational purposes.
// Previously this function would reduce chunk_size/workers based on memory estimates,
// but this caused performance regressions. The Go GC soft limit handles memory pressure.
// Returns false (no adjustments made) and a description of row sizes.
func (c *Config) RefineSettingsForRowSizes(tables []TableRowSize) (adjusted bool, changes string) {
	if len(tables) == 0 {
		return false, ""
	}

	// Calculate weighted average row size based on row counts (for informational purposes)
	var totalRows int64
	var weightedSum int64
	var maxRowSize int64
	var maxRowSizeTable string

	for _, t := range tables {
		if t.EstimatedRowSize > 0 {
			totalRows += t.RowCount
			weightedSum += t.RowCount * t.EstimatedRowSize
			if t.EstimatedRowSize > maxRowSize {
				maxRowSize = t.EstimatedRowSize
				maxRowSizeTable = t.Name
			}
		}
	}

	if totalRows == 0 || weightedSum == 0 {
		return false, ""
	}

	weightedAvgRowSize := weightedSum / totalRows

	// No adjustments made - just report row sizes for visibility
	return false, fmt.Sprintf("Row sizes: weighted avg %s, max %s in %s",
		FormatMemorySize(weightedAvgRowSize), FormatMemorySize(maxRowSize), maxRowSizeTable)
}

func (c *Config) validate() error {
	// Validate source
	if c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if !isValidDriverType(c.Source.Type) {
		return fmt.Errorf("source.type '%s' is not a valid driver type (supported: %v)", c.Source.Type, availableDriverTypes())
	}
	// Kerberos auth is descoped pending a verifiable test environment
	// (#251). The DSN-building functions in this file (SourceDSN /
	// TargetDSN) emit correct Kerberos DSNs, but the runtime drivers
	// use Dialect.BuildDSN(..., cfg.DSNOptions()) instead, and
	// DSNOptions doesn't carry auth/keytab/realm/SPN — so an
	// `auth: kerberos` config silently falls back to password auth.
	// Reject it at load time rather than letting that happen.
	if strings.EqualFold(c.Source.Auth, "kerberos") {
		return fmt.Errorf("source.auth: kerberos is not currently supported; tracking re-enable in #251")
	}

	// Validate target
	if c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if !isValidDriverType(c.Target.Type) {
		return fmt.Errorf("target.type '%s' is not a valid driver type (supported: %v)", c.Target.Type, availableDriverTypes())
	}
	// See source.auth note above — same descope applies to target. (#251)
	if strings.EqualFold(c.Target.Auth, "kerberos") {
		return fmt.Errorf("target.auth: kerberos is not currently supported; tracking re-enable in #251")
	}

	// Same-engine migration validation: prevent migration to the exact same database
	// Compare canonical driver names to handle aliases (e.g., "mssql" == "sqlserver")
	if canonicalDriverName(c.Source.Type) == canonicalDriverName(c.Target.Type) {
		// Use case-insensitive comparison for hostnames (RFC 1035)
		sameHost := strings.EqualFold(c.Source.Host, c.Target.Host)
		samePort := c.Source.Port == c.Target.Port
		sameDB := c.Source.Database == c.Target.Database
		if sameHost && samePort && sameDB {
			return fmt.Errorf("source and target cannot be the same database (%s:%d/%s)",
				c.Source.Host, c.Source.Port, c.Source.Database)
		}
	}

	// Validate migration settings
	if c.Migration.TargetMode != "drop_recreate" && c.Migration.TargetMode != "upsert" {
		return fmt.Errorf("migration.target_mode must be 'drop_recreate' or 'upsert'")
	}
	switch c.Migration.UnmappedTypeAction {
	case "", "fail", "skip", "conservative-text":
		// Valid. Empty is allowed because applyDefaults sets it to
		// "fail" before validate runs in the normal load path; tests
		// that construct a Config directly and call validate may
		// leave it empty and that's OK.
	default:
		// Non-empty value that doesn't match is a user typo.
		// Fail loudly rather than silently misbehave (Copilot review
		// on PR #192 — without this, a typo'd action would default
		// to empty SQLType in the chain's handleUnmapped and produce
		// invalid DDL with no clear cause).
		return fmt.Errorf("migration.unmapped_type_action must be 'fail', 'skip', or 'conservative-text'; got %q",
			c.Migration.UnmappedTypeAction)
	}

	switch c.Migration.ApproxTypeAction {
	case "", "deterministic", "ai_fallback":
		// Valid. Empty intentionally falls through to NewFallbackChain
		// (#209) which picks ai_fallback when AI is configured,
		// deterministic when AI isn't — config doesn't know about AI
		// availability, the chain does. Explicit values override that
		// runtime decision. Issues #197, #209.
	default:
		return fmt.Errorf("migration.approx_type_action must be 'deterministic' or 'ai_fallback'; got %q",
			c.Migration.ApproxTypeAction)
	}

	// Note: AI configuration is validated in the secrets package when loaded from ~/.secrets/dmt-config.yaml

	return nil
}

// SourceDSN returns the source database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) SourceDSN() string {
	// Use driver registry to get canonical name (e.g., "pg" -> "postgres")
	driverName := canonicalDriverName(c.Source.Type)
	switch driverName {
	case "postgres":
		return c.buildPostgresDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, c.Source.SSLMode,
			c.Source.Auth, c.Source.GSSEncMode)
	case "mssql":
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		return c.buildMSSQLDSN(c.Source.Host, c.Source.Port, c.Source.Database,
			c.Source.User, c.Source.Password, encrypt, c.Source.TrustServerCert,
			c.Source.PacketSize, c.Source.Auth, c.Source.Krb5Conf, c.Source.Keytab, c.Source.Realm, c.Source.SPN)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// TargetDSN returns the target database connection string.
// Uses driver registry to determine the correct DSN builder.
func (c *Config) TargetDSN() string {
	// Use driver registry to get canonical name (e.g., "sqlserver" -> "mssql")
	driverName := canonicalDriverName(c.Target.Type)
	switch driverName {
	case "mssql":
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		return c.buildMSSQLDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, encrypt, c.Target.TrustServerCert,
			c.Target.PacketSize, c.Target.Auth, c.Target.Krb5Conf, c.Target.Keytab, c.Target.Realm, c.Target.SPN)
	case "postgres":
		return c.buildPostgresDSN(c.Target.Host, c.Target.Port, c.Target.Database,
			c.Target.User, c.Target.Password, c.Target.SSLMode,
			c.Target.Auth, c.Target.GSSEncMode)
	default:
		// Unknown driver type - should have been caught in validation
		// Return empty string to trigger connection error
		return ""
	}
}

// buildMSSQLDSN builds an MSSQL connection string with optional Kerberos auth
func (c *Config) buildMSSQLDSN(host string, port int, database, user, password string, encrypt bool,
	trustServerCert bool, packetSize int, auth, krb5Conf, keytab, realm, spn string) string {

	encryptStr := "disable"
	if encrypt {
		encryptStr = "true"
	}
	trustCert := "false"
	if trustServerCert {
		trustCert = "true"
	}

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	encodedDB := url.QueryEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos authentication
	if auth == "kerberos" {
		dsn := fmt.Sprintf("sqlserver://%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s&authenticator=krb5",
			host, port, encodedDB, encryptStr, trustCert)

		// Add packet size for better throughput (default 4KB is too small)
		if packetSize > 0 {
			dsn += fmt.Sprintf("&packet+size=%d", packetSize)
		}

		// Optional Kerberos parameters
		if krb5Conf != "" {
			dsn += "&krb5-configfile=" + url.QueryEscape(krb5Conf)
		}
		if keytab != "" {
			dsn += "&krb5-keytabfile=" + url.QueryEscape(keytab)
		}
		if realm != "" {
			dsn += "&krb5-realm=" + url.QueryEscape(realm)
		}
		if spn != "" {
			dsn += "&ServerSPN=" + url.QueryEscape(spn)
		}
		// If user specified, use it as the principal
		if user != "" {
			dsn += "&krb5-username=" + url.QueryEscape(user)
		}
		return dsn
	}

	// Password authentication (default)
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=%s&TrustServerCertificate=%s",
		encodedUser, encodedPass, host, port, encodedDB, encryptStr, trustCert)

	// Add packet size for better throughput (default 4KB is too small)
	if packetSize > 0 {
		dsn += fmt.Sprintf("&packet+size=%d", packetSize)
	}

	return dsn
}

// buildPostgresDSN builds a PostgreSQL connection string with optional Kerberos auth
func (c *Config) buildPostgresDSN(host string, port int, database, user, password, sslMode,
	auth, gssEncMode string) string {

	// URL-encode values that may contain special characters
	// Use QueryEscape for user/password to encode @ and : which are reserved in userinfo
	// Use PathEscape for database since it's in the URL path
	encodedDB := url.PathEscape(database)
	encodedUser := url.QueryEscape(user)
	encodedPass := url.QueryEscape(password)

	// Kerberos/GSSAPI authentication
	if auth == "kerberos" {
		gssEnc := "prefer"
		if gssEncMode != "" {
			gssEnc = gssEncMode
		}
		// For Kerberos, we don't include password in the DSN
		if user != "" {
			return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&gssencmode=%s",
				encodedUser, host, port, encodedDB, sslMode, gssEnc)
		}
		return fmt.Sprintf("postgres://%s:%d/%s?sslmode=%s&gssencmode=%s",
			host, port, encodedDB, sslMode, gssEnc)
	}

	// Password authentication (default)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		encodedUser, encodedPass, host, port, encodedDB, sslMode)
}

// Sanitized returns a copy of the config with sensitive fields redacted
func (c *Config) Sanitized() *Config {
	sanitized := *c // shallow copy

	// Redact source credentials
	sanitized.Source.Password = "[REDACTED]"

	// Redact target credentials
	sanitized.Target.Password = "[REDACTED]"

	// Note: AI and Slack credentials are in global secrets file, not migration config

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

// formatAutoValue formats a value with auto-tuning explanation if applicable
func formatAutoValue(current, original int, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatAutoValue64 formats an int64 value with auto-tuning explanation if applicable
func formatAutoValue64(current, original int64, explanation string) string {
	if original == 0 {
		return fmt.Sprintf("%d (auto: %s)", current, explanation)
	}
	return fmt.Sprintf("%d", current)
}

// formatMemorySize formats bytes as a human-readable size
func formatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// EstimateMemoryUsage calculates expected memory usage given actual row sizes.
// avgRowSize should be the weighted average row size across all tables.
// Returns estimated bytes.
func (c *Config) EstimateMemoryUsage(avgRowSize int64) int64 {
	if avgRowSize <= 0 {
		avgRowSize = 500
	}
	// Formula: workers * (readers * buffers + writers * buffers) * chunk_size * avg_row_size
	// Simplified: workers * total_buffers * chunk_size * avg_row_size
	// Each worker has read-ahead buffers + pending writes
	totalBuffers := int64(c.Migration.ReadAheadBuffers) * 2 // read + write queues
	return int64(c.Migration.Workers) * totalBuffers * int64(c.Migration.ChunkSize) * avgRowSize
}

// FormatMemoryEstimate returns a human-readable memory estimate string.
func (c *Config) FormatMemoryEstimate(avgRowSize int64) string {
	mem := c.EstimateMemoryUsage(avgRowSize)
	return fmt.Sprintf("~%s (%d workers * %d buffers * %d chunk * %d bytes/row)",
		formatMemorySize(mem),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers*2,
		c.Migration.ChunkSize,
		avgRowSize)
}

// FormatMemorySize exports the formatMemorySize function for use by other packages.
func FormatMemorySize(bytes int64) string {
	return formatMemorySize(bytes)
}

// AutoConfig returns the auto-configuration metadata.
func (c *Config) AutoConfig() AutoConfig {
	return c.autoConfig
}

// ParamChange records a parameter change from formula to AI value.
type ParamChange struct {
	Name     string
	OldValue int64
	NewValue int64
}

// ApplyAISuggestions overrides formula-computed defaults with AI recommendations.
// Only parameters the user didn't explicitly set (Original* == 0) are overridden.
// Returns a list of parameters that were changed.
func (c *Config) ApplyAISuggestions(s *driver.SmartConfigSuggestions) []ParamChange {
	ac := c.autoConfig
	var changes []ParamChange

	// Helper to conditionally apply a suggestion
	apply := func(name string, original int, current *int, suggested int) {
		if original == 0 && suggested > 0 && suggested != *current {
			changes = append(changes, ParamChange{name, int64(*current), int64(suggested)})
			*current = suggested
		}
	}
	applyInt64 := func(name string, original int64, current *int64, suggested int64) {
		if original == 0 && suggested > 0 && suggested != *current {
			changes = append(changes, ParamChange{name, *current, suggested})
			*current = suggested
		}
	}

	// Core tunable parameters
	apply("workers", ac.OriginalWorkers, &c.Migration.Workers, s.Workers)
	apply("chunk_size", ac.OriginalChunkSize, &c.Migration.ChunkSize, s.ChunkSizeRecommendation)
	apply("read_ahead_buffers", ac.OriginalReadAheadBuffers, &c.Migration.ReadAheadBuffers, s.ReadAheadBuffers)
	apply("write_ahead_writers", ac.OriginalWriteAheadWriters, &c.Migration.WriteAheadWriters, s.WriteAheadWriters)
	apply("parallel_readers", ac.OriginalParallelReaders, &c.Migration.ParallelReaders, s.ParallelReaders)
	apply("max_partitions", ac.OriginalMaxPartitions, &c.Migration.MaxPartitions, s.MaxPartitions)
	applyInt64("large_table_threshold", ac.OriginalLargeTableThresh, &c.Migration.LargeTableThreshold, s.LargeTableThreshold)
	apply("upsert_merge_chunk_size", ac.OriginalUpsertMergeChunkSize, &c.Migration.UpsertMergeChunkSize, s.UpsertMergeChunkSize)
	apply("checkpoint_frequency", ac.OriginalCheckpointFrequency, &c.Migration.CheckpointFrequency, s.CheckpointFrequency)
	apply("max_retries", ac.OriginalMaxRetries, &c.Migration.MaxRetries, s.MaxRetries)

	// Re-derive dependent values after core parameter changes
	if len(changes) > 0 {
		// Connection pools (only if user didn't specify)
		if ac.OriginalMaxSourceConns == 0 {
			required := c.Migration.Workers*c.Migration.ParallelReaders + 4
			if required != c.Migration.MaxSourceConnections {
				c.Migration.MaxSourceConnections = required
			}
		}
		if ac.OriginalMaxTargetConns == 0 {
			required := c.Migration.Workers*c.Migration.WriteAheadWriters + 4
			if required != c.Migration.MaxTargetConnections {
				c.Migration.MaxTargetConnections = required
			}
		}
		// Source/Target chunk sizes default to migration chunk_size
		if ac.OriginalSourceChunkSize == 0 {
			c.Source.ChunkSize = c.Migration.ChunkSize
		}
		if ac.OriginalTargetChunkSize == 0 {
			c.Target.ChunkSize = c.Migration.ChunkSize
		}
	}

	return changes
}

// DebugDump returns a comprehensive configuration dump with auto-tuning explanations
func (c *Config) DebugDump() string {
	var b strings.Builder
	ac := c.autoConfig

	b.WriteString("\n=== Configuration ===\n\n")

	// System Resources
	b.WriteString("System Resources:\n")
	b.WriteString(fmt.Sprintf("  Available Memory: %d MB\n", ac.AvailableMemoryMB))
	if c.Migration.MaxMemoryMB > 0 {
		b.WriteString(fmt.Sprintf("  Max Memory Limit: %d MB (user configured)\n", c.Migration.MaxMemoryMB))
	}
	b.WriteString(fmt.Sprintf("  Effective Max Memory: %d MB (hard cap 70%%)\n", ac.EffectiveMaxMemoryMB))
	b.WriteString(fmt.Sprintf("  CPU Cores: %d\n", ac.CPUCores))

	// Source Database
	b.WriteString(fmt.Sprintf("\nSource (%s):\n", c.Source.Type))
	b.WriteString(fmt.Sprintf("  Host: %s\n", c.Source.Host))
	b.WriteString(fmt.Sprintf("  Port: %d\n", c.Source.Port))
	b.WriteString(fmt.Sprintf("  Database: %s\n", c.Source.Database))
	b.WriteString(fmt.Sprintf("  Schema: %s\n", c.Source.Schema))
	b.WriteString(fmt.Sprintf("  User: %s\n", c.Source.User))
	b.WriteString("  Password: [REDACTED]\n")
	if canonicalDriverName(c.Source.Type) == "mssql" {
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		b.WriteString(fmt.Sprintf("  Encrypt: %v\n", encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Source.TrustServerCert))
		b.WriteString(fmt.Sprintf("  PacketSize: %d\n", c.Source.PacketSize))
	} else {
		b.WriteString(fmt.Sprintf("  SSLMode: %s\n", c.Source.SSLMode))
	}
	auth := c.Source.Auth
	if auth == "" {
		auth = "password"
	}
	b.WriteString(fmt.Sprintf("  Auth: %s\n", auth))
	if c.Source.Auth == "kerberos" {
		if c.Source.Krb5Conf != "" {
			b.WriteString(fmt.Sprintf("  Krb5Conf: %s\n", c.Source.Krb5Conf))
		}
		if c.Source.Realm != "" {
			b.WriteString(fmt.Sprintf("  Realm: %s\n", c.Source.Realm))
		}
	}

	// Target Database
	b.WriteString(fmt.Sprintf("\nTarget (%s):\n", c.Target.Type))
	b.WriteString(fmt.Sprintf("  Host: %s\n", c.Target.Host))
	b.WriteString(fmt.Sprintf("  Port: %d\n", c.Target.Port))
	b.WriteString(fmt.Sprintf("  Database: %s\n", c.Target.Database))
	b.WriteString(fmt.Sprintf("  Schema: %s\n", c.Target.Schema))
	b.WriteString(fmt.Sprintf("  User: %s\n", c.Target.User))
	b.WriteString("  Password: [REDACTED]\n")
	if canonicalDriverName(c.Target.Type) == "mssql" {
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		b.WriteString(fmt.Sprintf("  Encrypt: %v\n", encrypt))
		b.WriteString(fmt.Sprintf("  TrustServerCert: %v\n", c.Target.TrustServerCert))
		b.WriteString(fmt.Sprintf("  PacketSize: %d\n", c.Target.PacketSize))
	} else {
		b.WriteString(fmt.Sprintf("  SSLMode: %s\n", c.Target.SSLMode))
	}
	auth = c.Target.Auth
	if auth == "" {
		auth = "password"
	}
	b.WriteString(fmt.Sprintf("  Auth: %s\n", auth))
	if c.Target.Auth == "kerberos" {
		if c.Target.Krb5Conf != "" {
			b.WriteString(fmt.Sprintf("  Krb5Conf: %s\n", c.Target.Krb5Conf))
		}
		if c.Target.Realm != "" {
			b.WriteString(fmt.Sprintf("  Realm: %s\n", c.Target.Realm))
		}
	}

	// Migration Settings
	b.WriteString("\nMigration Settings:\n")

	// Workers
	workersExpl := fmt.Sprintf("(cores-2) clamped 4-12, %d cores", ac.CPUCores)
	b.WriteString(fmt.Sprintf("  Workers: %s\n", formatAutoValue(c.Migration.Workers, ac.OriginalWorkers, workersExpl)))

	// ChunkSize
	ramGB := float64(ac.AvailableMemoryMB) / 1024.0
	chunkExpl := fmt.Sprintf("75K + %.1fGB*3.1K", ramGB)
	b.WriteString(fmt.Sprintf("  ChunkSize: %s\n", formatAutoValue(c.Migration.ChunkSize, ac.OriginalChunkSize, chunkExpl)))

	// ReadAheadBuffers
	buffersExpl := fmt.Sprintf("memory/%d workers/chunk bytes", c.Migration.Workers)
	b.WriteString(fmt.Sprintf("  ReadAheadBuffers: %s\n", formatAutoValue(c.Migration.ReadAheadBuffers, ac.OriginalReadAheadBuffers, buffersExpl)))

	// MaxPartitions
	partitionsExpl := "matches workers"
	b.WriteString(fmt.Sprintf("  MaxPartitions: %s\n", formatAutoValue(c.Migration.MaxPartitions, ac.OriginalMaxPartitions, partitionsExpl)))

	// Connection pools
	sourceConnsExpl := fmt.Sprintf("%d workers * %d readers + 4", c.Migration.Workers, c.Migration.ParallelReaders)
	b.WriteString(fmt.Sprintf("  MaxSourceConnections: %s\n", formatAutoValue(c.Migration.MaxSourceConnections, ac.OriginalMaxSourceConns, sourceConnsExpl)))

	targetConnsExpl := fmt.Sprintf("%d workers * %d writers + 4", c.Migration.Workers, c.Migration.WriteAheadWriters)
	b.WriteString(fmt.Sprintf("  MaxTargetConnections: %s\n", formatAutoValue(c.Migration.MaxTargetConnections, ac.OriginalMaxTargetConns, targetConnsExpl)))

	// WriteAheadWriters - use driver defaults for explanation
	var writersExpl string
	if targetDriver, err := driver.Get(c.Target.Type); err == nil {
		defaults := targetDriver.Defaults()
		if defaults.ScaleWritersWithCores {
			writersExpl = fmt.Sprintf("driver default scaled with cores (%d cores)", ac.CPUCores)
		} else {
			writersExpl = fmt.Sprintf("driver default fixed at %d", defaults.WriteAheadWriters)
		}
	} else {
		writersExpl = "fallback default"
	}
	b.WriteString(fmt.Sprintf("  WriteAheadWriters: %s\n", formatAutoValue(c.Migration.WriteAheadWriters, ac.OriginalWriteAheadWriters, writersExpl)))

	// ParallelReaders
	readersExpl := fmt.Sprintf("cores/4 clamped 2-4, %d cores", ac.CPUCores)
	b.WriteString(fmt.Sprintf("  ParallelReaders: %s\n", formatAutoValue(c.Migration.ParallelReaders, ac.OriginalParallelReaders, readersExpl)))

	// LargeTableThreshold
	b.WriteString(fmt.Sprintf("  LargeTableThreshold: %s\n", formatAutoValue64(c.Migration.LargeTableThreshold, ac.OriginalLargeTableThresh, "default 5M")))

	// Source/Target ChunkSize
	sourceChunkExpl := "defaults to migration chunk_size"
	b.WriteString(fmt.Sprintf("  Source.ChunkSize: %s\n", formatAutoValue(c.Source.ChunkSize, ac.OriginalSourceChunkSize, sourceChunkExpl)))
	targetChunkExpl := "defaults to migration chunk_size"
	b.WriteString(fmt.Sprintf("  Target.ChunkSize: %s\n", formatAutoValue(c.Target.ChunkSize, ac.OriginalTargetChunkSize, targetChunkExpl)))

	// Other settings
	b.WriteString(fmt.Sprintf("  TargetMode: %s\n", c.Migration.TargetMode))

	// UpsertMergeChunkSize - only show in upsert mode
	if c.Migration.TargetMode == "upsert" {
		upsertExpl := "auto: memory-scaled 5K-20K"
		b.WriteString(fmt.Sprintf("  UpsertMergeChunkSize: %s\n", formatAutoValue(c.Migration.UpsertMergeChunkSize, ac.OriginalUpsertMergeChunkSize, upsertExpl)))
		// DateUpdatedColumns - only show in upsert mode if configured
		if len(c.Migration.DateUpdatedColumns) > 0 {
			b.WriteString(fmt.Sprintf("  DateUpdatedColumns: %v\n", c.Migration.DateUpdatedColumns))
		}
	}
	b.WriteString(fmt.Sprintf("  StrictConsistency: %v\n", c.Migration.StrictConsistency))
	b.WriteString(fmt.Sprintf("  CreateIndexes: %v\n", c.Migration.CreateIndexes))
	b.WriteString(fmt.Sprintf("  CreateForeignKeys: %v\n", c.Migration.CreateForeignKeys))
	b.WriteString(fmt.Sprintf("  CreateCheckConstraints: %v\n", c.Migration.CreateCheckConstraints))
	b.WriteString(fmt.Sprintf("  SampleValidation: %v\n", c.Migration.SampleValidation))
	b.WriteString(fmt.Sprintf("  SampleSize: %s\n", formatAutoValue(c.Migration.SampleSize, ac.OriginalSampleSize, "default 100")))
	b.WriteString(fmt.Sprintf("  DataDir: %s\n", c.Migration.DataDir))

	// Restartability Settings
	b.WriteString("\nRestartability:\n")
	b.WriteString(fmt.Sprintf("  CheckpointFrequency: %d chunks\n", c.Migration.CheckpointFrequency))
	b.WriteString(fmt.Sprintf("  MaxRetries: %d\n", c.Migration.MaxRetries))
	b.WriteString(fmt.Sprintf("  HistoryRetentionDays: %d\n", c.Migration.HistoryRetentionDays))

	// Table Filters
	b.WriteString("\nTable Filters:\n")
	if len(c.Migration.IncludeTables) > 0 {
		b.WriteString(fmt.Sprintf("  IncludeTables: %v\n", c.Migration.IncludeTables))
	} else {
		b.WriteString("  IncludeTables: [all]\n")
	}
	if len(c.Migration.ExcludeTables) > 0 {
		b.WriteString(fmt.Sprintf("  ExcludeTables: %v\n", c.Migration.ExcludeTables))
	} else {
		b.WriteString("  ExcludeTables: [none]\n")
	}

	// Memory Estimate (conservative estimate, actual may vary based on row content)
	b.WriteString("\nMemory Estimate:\n")
	bytesPerRow := int64(500) // conservative default - actual sizes queried during schema extraction
	bufferMemory := int64(c.Migration.Workers) * int64(c.Migration.ReadAheadBuffers) * int64(c.Migration.ChunkSize) * bytesPerRow
	b.WriteString(fmt.Sprintf("  Buffer Memory: ~%s (%d workers * %d buffers * %d rows * %d bytes/row)\n",
		formatMemorySize(bufferMemory),
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.ChunkSize,
		bytesPerRow))
	b.WriteString("  Note: Actual memory depends on row sizes. Tables with large text columns use more.\n")

	// Profile (if set)
	if c.Profile.Name != "" || c.Profile.Description != "" {
		b.WriteString("\nProfile:\n")
		if c.Profile.Name != "" {
			b.WriteString(fmt.Sprintf("  Name: %s\n", c.Profile.Name))
		}
		if c.Profile.Description != "" {
			b.WriteString(fmt.Sprintf("  Description: %s\n", c.Profile.Description))
		}
	}

	// Notifications and AI Features (loaded from global secrets)
	b.WriteString("\nNotifications:\n")
	secretsCfg, secretsErr := secrets.Load()
	if secretsErr == nil && secretsCfg.Notifications.Slack.WebhookURL != "" {
		b.WriteString("  Slack: enabled\n")
		b.WriteString("  WebhookURL: [REDACTED]\n")
	} else {
		b.WriteString("  Slack: disabled\n")
	}

	// AI Features (from global secrets)
	b.WriteString("\nAI Features:\n")
	if secretsErr == nil {
		provider, providerName, err := secretsCfg.GetDefaultProvider()
		// Check for valid provider: API-key-based (Anthropic, OpenAI) or local with BaseURL (Ollama, LMStudio)
		if err == nil && provider != nil && (provider.APIKey != "" || provider.BaseURL != "") {
			b.WriteString(fmt.Sprintf("  Provider: %s\n", providerName))
			if provider.APIKey != "" {
				b.WriteString("  APIKey: [REDACTED]\n")
			}
			if provider.BaseURL != "" {
				b.WriteString(fmt.Sprintf("  BaseURL: %s\n", provider.BaseURL))
			}
			if provider.Model != "" {
				b.WriteString(fmt.Sprintf("  Model: %s\n", provider.Model))
			} else {
				b.WriteString(fmt.Sprintf("  Model: %s (default)\n", provider.GetEffectiveModel(providerName)))
			}
			// AI features status - check each feature separately.
			// Type mapping always works (deterministic since #170);
			// the AI mapper is now optional fallback for Raw types only.
			if driver.GetAIMapper() != nil {
				b.WriteString("  Type Mapping: deterministic + AI fallback for Raw types\n")
			} else {
				b.WriteString("  Type Mapping: deterministic only (no AI fallback)\n")
			}
			b.WriteString("  Error Diagnosis: deterministic catalog (no AI)\n")
			// Runtime tuning settings from migration_defaults (#211).
			// Prefer the new name; fall back to the deprecated AIAdjust
			// field so a secrets file using the legacy name still renders
			// correctly during the deprecation cycle.
			defaults := secretsCfg.GetMigrationDefaults()
			enabled := defaults.RuntimeTuning
			if enabled == nil {
				enabled = defaults.AIAdjust
			}
			interval := defaults.RuntimeTuningInterval
			if interval == "" {
				interval = defaults.AIAdjustInterval
			}
			if enabled != nil && *enabled {
				if interval == "" {
					interval = "5s"
				}
				fmt.Fprintf(&b, "  Runtime Tuning: enabled (interval: %s)\n", interval)
			} else {
				b.WriteString("  Runtime Tuning: disabled\n")
			}
		} else {
			b.WriteString("  Disabled (no provider configured in ~/.secrets/dmt-config.yaml)\n")
		}
	} else {
		b.WriteString("  Disabled (no secrets file)\n")
	}

	return b.String()
}
