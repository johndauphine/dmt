package config

import (
	"fmt"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/secrets"
	"strings"
)

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
	fmt.Fprintf(&b, "  Available Memory: %d MB\n", ac.AvailableMemoryMB)
	if c.Migration.MaxMemoryMB > 0 {
		fmt.Fprintf(&b, "  Max Memory Limit: %d MB (user configured)\n", c.Migration.MaxMemoryMB)
	}
	fmt.Fprintf(&b, "  Effective Max Memory: %d MB (hard cap 70%%)\n", ac.EffectiveMaxMemoryMB)
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
