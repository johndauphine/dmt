package config

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
	"github.com/johndauphine/dmt/internal/tuning"
)

// ParamChange records a parameter change from formula to AI value.
type ParamChange struct {
	Name     string
	OldValue int64
	NewValue int64
}

// ApplyTunerSuggestions overrides formula-computed defaults with AI recommendations.
// Only parameters the user didn't explicitly set (Original* == 0) are overridden.
// Returns a list of parameters that were changed.
func (c *Config) ApplyTunerSuggestions(s *driver.SmartConfigSuggestions) []ParamChange {
	ac := c.autoConfig
	var changes []ParamChange

	// Helper to conditionally apply a suggestion
	apply := func(name string, provenanceName string, original int, current *int, suggested int) {
		if c.smartConfigCanOverride(provenanceName, original == 0) && suggested > 0 && suggested != *current {
			changes = append(changes, ParamChange{name, int64(*current), int64(suggested)})
			*current = suggested
			c.setTunableProvenance(provenanceName, ProvenanceSmartConfig)
		}
	}
	applyInt64 := func(name string, provenanceName string, original int64, current *int64, suggested int64) {
		if c.smartConfigCanOverride(provenanceName, original == 0) && suggested > 0 && suggested != *current {
			changes = append(changes, ParamChange{name, *current, suggested})
			*current = suggested
			c.setTunableProvenance(provenanceName, ProvenanceSmartConfig)
		}
	}

	// Core tunable parameters
	apply("workers", provenanceMigrationWorkers, ac.OriginalWorkers, &c.Migration.Workers, s.Workers)
	apply("chunk_size", provenanceMigrationChunkSize, ac.OriginalChunkSize, &c.Migration.ChunkSize, s.ChunkSizeRecommendation)
	apply("read_ahead_buffers", provenanceMigrationReadAheadBuffers, ac.OriginalReadAheadBuffers, &c.Migration.ReadAheadBuffers, s.ReadAheadBuffers)
	apply("write_ahead_writers", provenanceMigrationWriteAheadWriters, ac.OriginalWriteAheadWriters, &c.Migration.WriteAheadWriters, s.WriteAheadWriters)
	apply("parallel_readers", provenanceMigrationParallelReaders, ac.OriginalParallelReaders, &c.Migration.ParallelReaders, s.ParallelReaders)
	apply("max_partitions", provenanceMigrationMaxPartitions, ac.OriginalMaxPartitions, &c.Migration.MaxPartitions, s.MaxPartitions)
	applyInt64("large_table_threshold", provenanceMigrationLargeTableThreshold, ac.OriginalLargeTableThresh, &c.Migration.LargeTableThreshold, s.LargeTableThreshold)
	apply("upsert_merge_chunk_size", provenanceMigrationUpsertMergeChunkSize, ac.OriginalUpsertMergeChunkSize, &c.Migration.UpsertMergeChunkSize, s.UpsertMergeChunkSize)
	apply("checkpoint_frequency", provenanceMigrationCheckpointFrequency, ac.OriginalCheckpointFrequency, &c.Migration.CheckpointFrequency, s.CheckpointFrequency)
	apply("max_retries", provenanceMigrationMaxRetries, ac.OriginalMaxRetries, &c.Migration.MaxRetries, s.MaxRetries)

	// Generated pools always track the effective tuple, including when all core
	// suggestions are no-ops. Suggestions for PR/WAW may be pinned, so derive
	// from config after the gates above rather than from suggestion pool fields.
	derivedSourceConns, derivedTargetConns := tuning.ConnectionPoolSizes(
		c.Migration.Workers,
		c.Migration.ParallelReaders,
		c.Migration.WriteAheadWriters,
	)
	if c.smartConfigCanOverride(provenanceMigrationMaxSourceConns, ac.OriginalMaxSourceConns == 0) &&
		derivedSourceConns != c.Migration.MaxSourceConnections {
		c.Migration.MaxSourceConnections = derivedSourceConns
		c.setTunableProvenance(provenanceMigrationMaxSourceConns, ProvenanceSmartConfig)
	}
	if c.smartConfigCanOverride(provenanceMigrationMaxTargetConns, ac.OriginalMaxTargetConns == 0) &&
		derivedTargetConns != c.Migration.MaxTargetConnections {
		c.Migration.MaxTargetConnections = derivedTargetConns
		c.setTunableProvenance(provenanceMigrationMaxTargetConns, ProvenanceSmartConfig)
	}

	// Re-derive non-pool dependent values only after a core parameter change.
	// Chunk propagation deliberately retains its prior conditional semantics.
	if len(changes) > 0 {
		// Source/Target chunk sizes default to migration chunk_size
		if c.smartConfigCanOverride(provenanceSourceChunkSize, ac.OriginalSourceChunkSize == 0) {
			c.Source.ChunkSize = c.Migration.ChunkSize
			c.setTunableProvenance(provenanceSourceChunkSize, ProvenanceSmartConfig)
		}
		if c.smartConfigCanOverride(provenanceTargetChunkSize, ac.OriginalTargetChunkSize == 0) {
			c.Target.ChunkSize = c.Migration.ChunkSize
			c.setTunableProvenance(provenanceTargetChunkSize, ProvenanceSmartConfig)
		}
	}

	// Retain current-run width evidence only after the effective tuple is known:
	// the apply gates above preserve user/secrets pins, so these values are the
	// workers and buffers that will actually run. Unknown/fallback width remains
	// explicit and cannot authorize resource growth (#709).
	c.Migration.RuntimeSafetyRowBytes = s.SafetyRowBytes
	c.Migration.RuntimeSafetyRowBytesKnown = s.SafetyRowBytesKnown
	c.FinalizeRuntimeChunkSizeCap()

	return changes
}

// DebugDump returns a comprehensive configuration dump with auto-tuning explanations
func (c *Config) DebugDump() string {
	var b strings.Builder
	ac := c.autoConfig

	b.WriteString("\n=== Configuration ===\n\n")

	// System Resources
	b.WriteString("System Resources:\n")
	fmt.Fprintf(&b, "  Memory Capacity: %d MB\n", ac.MemoryEnvelope.CapacityMB)
	fmt.Fprintf(&b, "  Memory Available: %d MB\n", ac.MemoryEnvelope.AvailableMB)
	fmt.Fprintf(&b, "  Memory Budget: %d MB\n", ac.MemoryEnvelope.BudgetMB)
	fmt.Fprintf(&b, "  Memory Source: %s\n", ac.MemoryEnvelope.Source)
	if c.Migration.MaxMemoryMB > 0 {
		fmt.Fprintf(&b, "  Max Memory Limit: %d MB (user ceiling)\n", c.Migration.MaxMemoryMB)
	}
	fmt.Fprintf(&b, "  CPU Cores: %d\n", ac.CPUCores)
	fmt.Fprintf(&b, "  Platform: %s\n", ac.Platform)

	// Source Database
	fmt.Fprintf(&b, "\nSource (%s):\n", c.Source.Type)
	fmt.Fprintf(&b, "  Host: %s\n", c.Source.Host)
	fmt.Fprintf(&b, "  Port: %d\n", c.Source.Port)
	fmt.Fprintf(&b, "  Database: %s\n", c.Source.Database)
	fmt.Fprintf(&b, "  Schema: %s\n", c.Source.Schema)
	fmt.Fprintf(&b, "  User: %s\n", c.Source.User)
	b.WriteString("  Password: [REDACTED]\n")
	if canonicalDriverName(c.Source.Type) == "mssql" {
		encrypt := c.Source.Encrypt != nil && *c.Source.Encrypt
		fmt.Fprintf(&b, "  Encrypt: %v\n", encrypt)
		fmt.Fprintf(&b, "  TrustServerCert: %v\n", c.Source.TrustServerCert)
		fmt.Fprintf(&b, "  PacketSize: %d\n", c.Source.PacketSize)
	} else {
		fmt.Fprintf(&b, "  SSLMode: %s\n", c.Source.SSLMode)
	}
	auth := c.Source.Auth
	if auth == "" {
		auth = "password"
	}
	fmt.Fprintf(&b, "  Auth: %s\n", auth)
	if c.Source.Auth == "kerberos" {
		if c.Source.Krb5Conf != "" {
			fmt.Fprintf(&b, "  Krb5Conf: %s\n", c.Source.Krb5Conf)
		}
		if c.Source.Realm != "" {
			fmt.Fprintf(&b, "  Realm: %s\n", c.Source.Realm)
		}
	}

	// Target Database
	fmt.Fprintf(&b, "\nTarget (%s):\n", c.Target.Type)
	fmt.Fprintf(&b, "  Host: %s\n", c.Target.Host)
	fmt.Fprintf(&b, "  Port: %d\n", c.Target.Port)
	fmt.Fprintf(&b, "  Database: %s\n", c.Target.Database)
	fmt.Fprintf(&b, "  Schema: %s\n", c.Target.Schema)
	fmt.Fprintf(&b, "  User: %s\n", c.Target.User)
	b.WriteString("  Password: [REDACTED]\n")
	if canonicalDriverName(c.Target.Type) == "mssql" {
		encrypt := c.Target.Encrypt != nil && *c.Target.Encrypt
		fmt.Fprintf(&b, "  Encrypt: %v\n", encrypt)
		fmt.Fprintf(&b, "  TrustServerCert: %v\n", c.Target.TrustServerCert)
		fmt.Fprintf(&b, "  PacketSize: %d\n", c.Target.PacketSize)
	} else {
		fmt.Fprintf(&b, "  SSLMode: %s\n", c.Target.SSLMode)
	}
	auth = c.Target.Auth
	if auth == "" {
		auth = "password"
	}
	fmt.Fprintf(&b, "  Auth: %s\n", auth)
	if c.Target.Auth == "kerberos" {
		if c.Target.Krb5Conf != "" {
			fmt.Fprintf(&b, "  Krb5Conf: %s\n", c.Target.Krb5Conf)
		}
		if c.Target.Realm != "" {
			fmt.Fprintf(&b, "  Realm: %s\n", c.Target.Realm)
		}
	}

	// Migration Settings
	b.WriteString("\nMigration Settings:\n")

	const defaultPolicyExpl = "canonical no-history tuning policy"

	// Workers
	fmt.Fprintf(&b, "  Workers: %s\n", c.formatTunableValue(c.Migration.Workers, ac.OriginalWorkers, provenanceMigrationWorkers, defaultPolicyExpl))

	// ChunkSize
	fmt.Fprintf(&b, "  ChunkSize: %s\n", c.formatTunableValue(c.Migration.ChunkSize, ac.OriginalChunkSize, provenanceMigrationChunkSize, defaultPolicyExpl))

	// ReadAheadBuffers
	fmt.Fprintf(&b, "  ReadAheadBuffers: %s\n", c.formatTunableValue(c.Migration.ReadAheadBuffers, ac.OriginalReadAheadBuffers, provenanceMigrationReadAheadBuffers, defaultPolicyExpl))

	// MaxPartitions
	fmt.Fprintf(&b, "  MaxPartitions: %s\n", c.formatTunableValue(c.Migration.MaxPartitions, ac.OriginalMaxPartitions, provenanceMigrationMaxPartitions, defaultPolicyExpl))

	// Connection pools
	const poolPolicyExpl = "shared connection-pool policy from effective concurrency"
	fmt.Fprintf(&b, "  MaxSourceConnections: %s\n", c.formatTunableValue(c.Migration.MaxSourceConnections, ac.OriginalMaxSourceConns, provenanceMigrationMaxSourceConns, poolPolicyExpl))
	fmt.Fprintf(&b, "  MaxTargetConnections: %s\n", c.formatTunableValue(c.Migration.MaxTargetConnections, ac.OriginalMaxTargetConns, provenanceMigrationMaxTargetConns, poolPolicyExpl))

	// WriteAheadWriters
	fmt.Fprintf(&b, "  WriteAheadWriters: %s\n", c.formatTunableValue(c.Migration.WriteAheadWriters, ac.OriginalWriteAheadWriters, provenanceMigrationWriteAheadWriters, defaultPolicyExpl))

	// ParallelReaders
	fmt.Fprintf(&b, "  ParallelReaders: %s\n", c.formatTunableValue(c.Migration.ParallelReaders, ac.OriginalParallelReaders, provenanceMigrationParallelReaders, defaultPolicyExpl))

	// LargeTableThreshold
	fmt.Fprintf(&b, "  LargeTableThreshold: %s\n", c.formatTunableValue64(c.Migration.LargeTableThreshold, ac.OriginalLargeTableThresh, provenanceMigrationLargeTableThreshold, defaultPolicyExpl))

	// Source/Target ChunkSize
	sourceChunkExpl := "defaults to migration chunk_size"
	fmt.Fprintf(&b, "  Source.ChunkSize: %s\n", c.formatTunableValue(c.Source.ChunkSize, ac.OriginalSourceChunkSize, provenanceSourceChunkSize, sourceChunkExpl))
	targetChunkExpl := "defaults to migration chunk_size"
	fmt.Fprintf(&b, "  Target.ChunkSize: %s\n", c.formatTunableValue(c.Target.ChunkSize, ac.OriginalTargetChunkSize, provenanceTargetChunkSize, targetChunkExpl))

	// Other settings
	fmt.Fprintf(&b, "  TargetMode: %s\n", c.Migration.TargetMode)

	// UpsertMergeChunkSize - only show in upsert mode
	if c.Migration.TargetMode == "upsert" {
		fmt.Fprintf(&b, "  UpsertMergeChunkSize: %s\n", c.formatTunableValue(c.Migration.UpsertMergeChunkSize, ac.OriginalUpsertMergeChunkSize, provenanceMigrationUpsertMergeChunkSize, defaultPolicyExpl))
		// DateUpdatedColumns - only show in upsert mode if configured
		if len(c.Migration.DateUpdatedColumns) > 0 {
			fmt.Fprintf(&b, "  DateUpdatedColumns: %v\n", c.Migration.DateUpdatedColumns)
		}
	}
	fmt.Fprintf(&b, "  StrictConsistency: %v\n", c.Migration.StrictConsistency)
	fmt.Fprintf(&b, "  CreateIndexes: %v\n", c.Migration.CreateIndexesEnabled())
	fmt.Fprintf(&b, "  CreateForeignKeys: %v\n", c.Migration.CreateForeignKeysEnabled())
	fmt.Fprintf(&b, "  CreateCheckConstraints: %v\n", c.Migration.CreateCheckConstraints)
	fmt.Fprintf(&b, "  FailOnSchemaDrift: %v\n", c.Migration.FailOnSchemaDrift)
	fmt.Fprintf(&b, "  SampleValidation: %v\n", c.Migration.SampleValidation)
	fmt.Fprintf(&b, "  SampleSize: %s\n", c.formatTunableValue(c.Migration.SampleSize, ac.OriginalSampleSize, provenanceMigrationSampleSize, "default 100"))
	fmt.Fprintf(&b, "  DataDir: %s\n", c.Migration.DataDir)

	// Restartability Settings
	b.WriteString("\nRestartability:\n")
	fmt.Fprintf(&b, "  CheckpointFrequency: %s chunks\n", c.formatTunableValue(c.Migration.CheckpointFrequency, ac.OriginalCheckpointFrequency, provenanceMigrationCheckpointFrequency, defaultPolicyExpl))
	fmt.Fprintf(&b, "  MaxRetries: %s\n", c.formatTunableValue(c.Migration.MaxRetries, ac.OriginalMaxRetries, provenanceMigrationMaxRetries, defaultPolicyExpl))
	fmt.Fprintf(&b, "  HistoryRetentionDays: %d\n", c.Migration.HistoryRetentionDays)

	// Table Filters
	b.WriteString("\nTable Filters:\n")
	if len(c.Migration.IncludeTables) > 0 {
		fmt.Fprintf(&b, "  IncludeTables: %v\n", c.Migration.IncludeTables)
	} else {
		b.WriteString("  IncludeTables: [all]\n")
	}
	if len(c.Migration.ExcludeTables) > 0 {
		fmt.Fprintf(&b, "  ExcludeTables: %v\n", c.Migration.ExcludeTables)
	} else {
		b.WriteString("  ExcludeTables: [none]\n")
	}

	// Memory Estimate (formula-only fallback; actual may vary by row content).
	// Use the tuner's complete model so diagnostics include writer buffers and
	// cannot disagree with the clamp that produced an automatic chunk.
	b.WriteString("\nMemory Estimate:\n")
	estimatedMemMB := tuning.EstimatedMemMB(
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.WriteAheadWriters,
		c.Migration.ChunkSize,
		loadTimeFallbackRowBytes,
	)
	overBudget := tuning.MemoryEstimateExceedsBudget(
		ac.MemoryEnvelope.BudgetMB,
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.WriteAheadWriters,
		c.Migration.ChunkSize,
		loadTimeFallbackRowBytes,
	)
	fmt.Fprintf(&b, "  Modeled Working Set: ~%d MB (%d workers * (%d read-ahead + %d write-ahead) * %d rows * %d bytes/row)\n",
		estimatedMemMB,
		c.Migration.Workers,
		c.Migration.ReadAheadBuffers,
		c.Migration.WriteAheadWriters,
		c.Migration.ChunkSize,
		loadTimeFallbackRowBytes,
	)
	if ac.MemoryEnvelope.BudgetMB > 0 {
		if overBudget {
			suffix := ""
			if ac.DefaultPolicyChunkPinned {
				suffix = "; explicit chunk_size preserved"
			}
			fmt.Fprintf(&b, "  Budget Status: exceeds %d MB budget%s\n", ac.MemoryEnvelope.BudgetMB, suffix)
		} else {
			fmt.Fprintf(&b, "  Budget Status: within %d MB budget\n", ac.MemoryEnvelope.BudgetMB)
		}
	}
	b.WriteString("  Width Source: unobserved 500-byte fallback estimate; schema-aware tuning uses the widest observed table-average model.\n")
	if ac.DefaultPolicyReasoning != "" && !ac.DefaultPolicyChunkPinned {
		fmt.Fprintf(&b, "  Load-time Policy Reasoning: %s\n", ac.DefaultPolicyReasoning)
	}

	// Profile (if set)
	if c.Profile.Name != "" || c.Profile.Description != "" {
		b.WriteString("\nProfile:\n")
		if c.Profile.Name != "" {
			fmt.Fprintf(&b, "  Name: %s\n", c.Profile.Name)
		}
		if c.Profile.Description != "" {
			fmt.Fprintf(&b, "  Description: %s\n", c.Profile.Description)
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
			fmt.Fprintf(&b, "  Provider: %s\n", providerName)
			if provider.APIKey != "" {
				b.WriteString("  APIKey: [REDACTED]\n")
			}
			if provider.BaseURL != "" {
				fmt.Fprintf(&b, "  BaseURL: %s\n", provider.BaseURL)
			}
			if provider.Model != "" {
				fmt.Fprintf(&b, "  Model: %s\n", provider.Model)
			} else {
				fmt.Fprintf(&b, "  Model: %s (default)\n", provider.GetEffectiveModel(providerName))
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

func (c *Config) formatTunableValue(current, original int, provenanceName string, explanation string) string {
	source := c.tunableProvenance(provenanceName)
	if source == ProvenanceSmartConfig {
		return fmt.Sprintf("%d (source: %s)", current, source)
	}
	if original == 0 {
		if source == "" {
			source = ProvenanceAutoDefault
		}
		return fmt.Sprintf("%d (auto: %s; source: %s)", current, explanation, source)
	}
	if source != "" {
		return fmt.Sprintf("%d (source: %s)", current, source)
	}
	return fmt.Sprintf("%d", current)
}

func (c *Config) formatTunableValue64(current, original int64, provenanceName string, explanation string) string {
	source := c.tunableProvenance(provenanceName)
	if source == ProvenanceSmartConfig {
		return fmt.Sprintf("%d (source: %s)", current, source)
	}
	if original == 0 {
		if source == "" {
			source = ProvenanceAutoDefault
		}
		return fmt.Sprintf("%d (auto: %s; source: %s)", current, explanation, source)
	}
	if source != "" {
		return fmt.Sprintf("%d (source: %s)", current, source)
	}
	return fmt.Sprintf("%d", current)
}
