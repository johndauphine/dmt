package config

import (
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/secrets"
)

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
