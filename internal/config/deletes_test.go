package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func testDeleteConfig(mode DeleteMode) *Config {
	return &Config{
		Source: SourceConfig{
			Type: "postgres", Host: "src", Port: 5432, Database: "d",
			User: "u", Password: "p",
		},
		Target: TargetConfig{
			Type: "mssql", Host: "tgt", Port: 1433, Database: "d",
			User: "u", Password: "p",
		},
		Migration: MigrationConfig{
			TargetMode: "upsert",
			Deletes: &DeleteConfig{
				Mode: mode,
			},
		},
	}
}

func TestDeleteConfigEffectiveDefaults(t *testing.T) {
	disabled := MigrationConfig{}
	if disabled.DeletesEnabled() {
		t.Fatal("DeletesEnabled() = true, want false")
	}
	if got := disabled.DeleteMode(); got != DeleteModeOff {
		t.Fatalf("DeleteMode() = %q, want %q", got, DeleteModeOff)
	}
	if got := disabled.DeleteTargetBehavior(); got != DeleteTargetBehaviorHard {
		t.Fatalf("DeleteTargetBehavior() = %q, want %q", got, DeleteTargetBehaviorHard)
	}
	if got := disabled.DeleteReconcileSchedule(); got != DeleteReconcileScheduleInterval {
		t.Fatalf("DeleteReconcileSchedule() = %q, want %q", got, DeleteReconcileScheduleInterval)
	}
	if got := disabled.DeleteReconcileInterval(); got != defaultDeleteReconcileInterval {
		t.Fatalf("DeleteReconcileInterval() = %q, want %q", got, defaultDeleteReconcileInterval)
	}
	if got := disabled.DeleteReconcileBatchSize(); got != defaultDeleteReconcileBatchSize {
		t.Fatalf("DeleteReconcileBatchSize() = %d, want %d", got, defaultDeleteReconcileBatchSize)
	}
	if !disabled.DeleteReconcileRequirePrimaryKey() {
		t.Fatal("DeleteReconcileRequirePrimaryKey() = false, want true")
	}

	enabled := MigrationConfig{Deletes: &DeleteConfig{Mode: DeleteModeReconcile}}
	if !enabled.DeletesEnabled() {
		t.Fatal("DeletesEnabled() = false, want true")
	}
}

func TestApplyDefaultsDeleteReconcile(t *testing.T) {
	cfg := testDeleteConfig(DeleteModeReconcile)
	cfg.Migration.applyDeleteDefaults()

	if got := cfg.Migration.Deletes.TargetBehavior; got != DeleteTargetBehaviorHard {
		t.Fatalf("target_behavior = %q, want %q", got, DeleteTargetBehaviorHard)
	}
	if got := cfg.Migration.Deletes.Reconcile.Schedule; got != DeleteReconcileScheduleInterval {
		t.Fatalf("reconcile.schedule = %q, want %q", got, DeleteReconcileScheduleInterval)
	}
	if got := cfg.Migration.Deletes.Reconcile.Interval; got != defaultDeleteReconcileInterval {
		t.Fatalf("reconcile.interval = %q, want %q", got, defaultDeleteReconcileInterval)
	}
	if got := cfg.Migration.Deletes.Reconcile.BatchSize; got != defaultDeleteReconcileBatchSize {
		t.Fatalf("reconcile.batch_size = %d, want %d", got, defaultDeleteReconcileBatchSize)
	}
	if cfg.Migration.Deletes.Reconcile.RequirePrimaryKey == nil {
		t.Fatal("reconcile.require_primary_key is nil, want default true")
	}
	if !*cfg.Migration.Deletes.Reconcile.RequirePrimaryKey {
		t.Fatal("reconcile.require_primary_key = false, want true")
	}
}

func TestValidateDeleteConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{
			name: "absent section",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes = nil
			},
		},
		{
			name: "off",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes = &DeleteConfig{Mode: DeleteModeOff}
			},
		},
		{
			name: "reconcile minimal",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes = &DeleteConfig{Mode: DeleteModeReconcile}
			},
		},
		{
			name: "reconcile explicit supported settings",
			edit: func(cfg *Config) {
				requirePrimaryKey := true
				cfg.Migration.Deletes = &DeleteConfig{
					Mode:           DeleteModeReconcile,
					TargetBehavior: DeleteTargetBehaviorHard,
					Reconcile: DeleteReconcileConfig{
						Schedule:          DeleteReconcileScheduleInterval,
						Interval:          "24h",
						BatchSize:         500,
						RequirePrimaryKey: &requirePrimaryKey,
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testDeleteConfig(DeleteModeReconcile)
			tt.edit(cfg)
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate returned error: %v", err)
			}
		})
	}
}

func TestValidateDeleteConfigRejectsUnsupportedSettings(t *testing.T) {
	requirePrimaryKeyFalse := false
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{
			name: "unknown mode",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Mode = "tombstone"
			},
			want: "migration.deletes.mode",
		},
		{
			name: "reconcile with drop_recreate",
			edit: func(cfg *Config) {
				cfg.Migration.TargetMode = "drop_recreate"
			},
			want: "requires migration.target_mode: upsert",
		},
		{
			name: "soft target behavior",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.TargetBehavior = "soft"
			},
			want: "migration.deletes.target_behavior",
		},
		{
			name: "manual schedule",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.Schedule = "manual"
			},
			want: "migration.deletes.reconcile.schedule",
		},
		{
			name: "every_n_runs",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.EveryNRuns = 7
			},
			want: "every_n_runs is not supported yet",
		},
		{
			name: "invalid interval",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.Interval = "weekly"
			},
			want: "must be a Go duration",
		},
		{
			name: "negative interval",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.Interval = "-1h"
			},
			want: "interval must be positive",
		},
		{
			name: "negative batch size",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.BatchSize = -1
			},
			want: "batch_size must not be negative",
		},
		{
			name: "primary-key requirement disabled",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Reconcile.RequirePrimaryKey = &requirePrimaryKeyFalse
			},
			want: "require_primary_key=false is not supported yet",
		},
		{
			name: "primary-key requirement disabled while off",
			edit: func(cfg *Config) {
				cfg.Migration.Deletes.Mode = DeleteModeOff
				cfg.Migration.Deletes.Reconcile.RequirePrimaryKey = &requirePrimaryKeyFalse
			},
			want: "require_primary_key=false is not supported yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testDeleteConfig(DeleteModeReconcile)
			tt.edit(cfg)
			err := cfg.validate()
			if err == nil {
				t.Fatal("validate returned nil error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate error = %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestLoadBytesDeleteConfigDefaults(t *testing.T) {
	cfg, err := LoadBytes([]byte(`
source:
  type: postgres
  host: src
  database: source_db
target:
  type: mssql
  host: tgt
  database: target_db
migration:
  target_mode: upsert
  max_memory_mb: 1024
  deletes:
    mode: reconcile
`))
	if err != nil {
		t.Fatalf("LoadBytes returned error: %v", err)
	}

	if cfg.Migration.DeleteMode() != DeleteModeReconcile {
		t.Fatalf("DeleteMode() = %q, want %q", cfg.Migration.DeleteMode(), DeleteModeReconcile)
	}
	if cfg.Migration.DeleteReconcileBatchSize() != defaultDeleteReconcileBatchSize {
		t.Fatalf("DeleteReconcileBatchSize() = %d, want %d",
			cfg.Migration.DeleteReconcileBatchSize(), defaultDeleteReconcileBatchSize)
	}
	if !cfg.Migration.DeleteReconcileRequirePrimaryKey() {
		t.Fatal("DeleteReconcileRequirePrimaryKey() = false, want true")
	}
}

func TestDeleteConfigResumeHashOmittedWhenAbsent(t *testing.T) {
	cfg := &Config{}

	data, err := json.Marshal(cfg.Sanitized().Migration)
	if err != nil {
		t.Fatalf("marshal migration: %v", err)
	}
	if strings.Contains(string(data), "deletes") || strings.Contains(string(data), "Deletes") {
		t.Fatalf("absent delete config leaked into resume hash JSON: %s", data)
	}
}

func TestDeleteConfigResumeHashIncludedWhenPresent(t *testing.T) {
	cfg := &Config{
		Migration: MigrationConfig{
			Deletes: &DeleteConfig{
				Mode: DeleteModeReconcile,
			},
		},
	}

	data, err := json.Marshal(cfg.Sanitized().Migration)
	if err != nil {
		t.Fatalf("marshal migration: %v", err)
	}
	if !strings.Contains(string(data), `"deletes"`) {
		t.Fatalf("delete config missing from resume hash JSON: %s", data)
	}
}
