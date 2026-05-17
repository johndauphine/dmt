package config

import (
	"fmt"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
	"os"
	"path/filepath"
	"runtime"
)

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
	if c.Migration.CreateIndexes == nil {
		v := true
		c.Migration.CreateIndexes = &v
	}
	if c.Migration.CreateForeignKeys == nil {
		v := true
		c.Migration.CreateForeignKeys = &v
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
