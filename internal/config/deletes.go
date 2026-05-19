package config

import (
	"fmt"
	"time"
)

const (
	DeleteModeOff       DeleteMode = "off"
	DeleteModeReconcile DeleteMode = "reconcile"

	DeleteTargetBehaviorHard DeleteTargetBehavior = "hard"

	DeleteReconcileScheduleInterval DeleteReconcileSchedule = "interval"
)

const (
	defaultDeleteReconcileInterval  = "168h"
	defaultDeleteReconcileBatchSize = 10000
)

// DeleteMode controls how upsert mode propagates source-side deletes.
type DeleteMode string

// DeleteTargetBehavior controls how DMT mutates target rows that no longer
// exist on the source.
type DeleteTargetBehavior string

// DeleteReconcileSchedule controls when key-set reconciliation is due.
type DeleteReconcileSchedule string

// DeleteConfig configures opt-in delete propagation for upsert mode. The first
// implementation slice only validates `mode: off` and `mode: reconcile`; other
// modes remain documented design options until implemented.
type DeleteConfig struct {
	Mode           DeleteMode            `yaml:"mode,omitempty" json:"mode,omitempty"`
	TargetBehavior DeleteTargetBehavior  `yaml:"target_behavior,omitempty" json:"target_behavior,omitempty"`
	Reconcile      DeleteReconcileConfig `yaml:"reconcile,omitempty" json:"reconcile,omitempty"`
}

// DeleteReconcileConfig configures periodic key-set reconciliation.
type DeleteReconcileConfig struct {
	Schedule          DeleteReconcileSchedule `yaml:"schedule,omitempty" json:"schedule,omitempty"`
	EveryNRuns        int                     `yaml:"every_n_runs,omitempty" json:"every_n_runs,omitempty"`
	Interval          string                  `yaml:"interval,omitempty" json:"interval,omitempty"`
	BatchSize         int                     `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	RequirePrimaryKey *bool                   `yaml:"require_primary_key,omitempty" json:"require_primary_key,omitempty"`
}

// DeletesEnabled reports whether delete propagation is enabled.
func (m MigrationConfig) DeletesEnabled() bool {
	return m.DeleteMode() != DeleteModeOff
}

// DeleteMode returns the effective delete mode. An omitted section preserves
// existing behavior: no delete propagation.
func (m MigrationConfig) DeleteMode() DeleteMode {
	if m.Deletes == nil || m.Deletes.Mode == "" {
		return DeleteModeOff
	}
	return m.Deletes.Mode
}

// DeleteTargetBehavior returns the effective target behavior for propagated
// deletes. The first implementation supports hard deletes only.
func (m MigrationConfig) DeleteTargetBehavior() DeleteTargetBehavior {
	if m.Deletes == nil || m.Deletes.TargetBehavior == "" {
		return DeleteTargetBehaviorHard
	}
	return m.Deletes.TargetBehavior
}

// DeleteReconcileSchedule returns the effective reconciliation schedule.
func (m MigrationConfig) DeleteReconcileSchedule() DeleteReconcileSchedule {
	if m.Deletes == nil || m.Deletes.Reconcile.Schedule == "" {
		return DeleteReconcileScheduleInterval
	}
	return m.Deletes.Reconcile.Schedule
}

// DeleteReconcileInterval returns the effective reconciliation interval string.
func (m MigrationConfig) DeleteReconcileInterval() string {
	if m.Deletes == nil || m.Deletes.Reconcile.Interval == "" {
		return defaultDeleteReconcileInterval
	}
	return m.Deletes.Reconcile.Interval
}

// DeleteReconcileBatchSize returns the effective delete batch size.
func (m MigrationConfig) DeleteReconcileBatchSize() int {
	if m.Deletes == nil || m.Deletes.Reconcile.BatchSize == 0 {
		return defaultDeleteReconcileBatchSize
	}
	return m.Deletes.Reconcile.BatchSize
}

// DeleteReconcileRequirePrimaryKey reports whether reconciliation requires a
// primary key or stable key. The first implementation requires primary keys.
func (m MigrationConfig) DeleteReconcileRequirePrimaryKey() bool {
	if m.Deletes == nil {
		return true
	}
	return boolPtrDefault(m.Deletes.Reconcile.RequirePrimaryKey, true)
}

func (m *MigrationConfig) applyDeleteDefaults() {
	if m.Deletes == nil {
		return
	}
	if m.Deletes.Mode == "" {
		m.Deletes.Mode = DeleteModeOff
	}
	if m.Deletes.Mode != DeleteModeReconcile {
		return
	}
	if m.Deletes.TargetBehavior == "" {
		m.Deletes.TargetBehavior = DeleteTargetBehaviorHard
	}
	if m.Deletes.Reconcile.Schedule == "" {
		m.Deletes.Reconcile.Schedule = DeleteReconcileScheduleInterval
	}
	if m.Deletes.Reconcile.Interval == "" {
		m.Deletes.Reconcile.Interval = defaultDeleteReconcileInterval
	}
	if m.Deletes.Reconcile.BatchSize == 0 {
		m.Deletes.Reconcile.BatchSize = defaultDeleteReconcileBatchSize
	}
	if m.Deletes.Reconcile.RequirePrimaryKey == nil {
		v := true
		m.Deletes.Reconcile.RequirePrimaryKey = &v
	}
}

func (c *Config) validateDeletes() error {
	if c.Migration.Deletes == nil {
		return nil
	}

	switch c.Migration.DeleteMode() {
	case DeleteModeOff:
		return c.validateDeleteShape()
	case DeleteModeReconcile:
		if c.Migration.TargetMode != "upsert" {
			return fmt.Errorf("migration.deletes.mode: reconcile requires migration.target_mode: upsert")
		}
		if err := c.validateDeleteShape(); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("migration.deletes.mode must be 'off' or 'reconcile'; got %q",
			c.Migration.Deletes.Mode)
	}
}

func (c *Config) validateDeleteShape() error {
	switch c.Migration.DeleteTargetBehavior() {
	case DeleteTargetBehaviorHard:
		// Valid. Soft-delete target behavior is a later slice.
	default:
		return fmt.Errorf("migration.deletes.target_behavior must be 'hard'; got %q",
			c.Migration.Deletes.TargetBehavior)
	}

	switch c.Migration.DeleteReconcileSchedule() {
	case DeleteReconcileScheduleInterval:
		// Valid. Other schedules are documented design options, not implemented.
	default:
		return fmt.Errorf("migration.deletes.reconcile.schedule must be 'interval'; got %q",
			c.Migration.Deletes.Reconcile.Schedule)
	}

	if c.Migration.Deletes.Reconcile.EveryNRuns != 0 {
		return fmt.Errorf("migration.deletes.reconcile.every_n_runs is not supported yet")
	}
	if c.Migration.DeleteReconcileBatchSize() < 0 {
		return fmt.Errorf("migration.deletes.reconcile.batch_size must not be negative")
	}
	interval := c.Migration.DeleteReconcileInterval()
	duration, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("migration.deletes.reconcile.interval must be a Go duration: %w", err)
	}
	if duration <= 0 {
		return fmt.Errorf("migration.deletes.reconcile.interval must be positive")
	}
	if !c.Migration.DeleteReconcileRequirePrimaryKey() {
		return fmt.Errorf("migration.deletes.reconcile.require_primary_key=false is not supported yet")
	}

	return nil
}
