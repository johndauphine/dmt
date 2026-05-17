package orchestrator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/driver/dbtuning"
	"github.com/johndauphine/dmt/internal/logging"
)

// addDatabaseTuningRecommendations adds source and target database
// tuning recommendations from the deterministic catalog (#172).
// Previously this required an AI mapper; the catalog is now hardcoded
// per driver and no AI is consulted, so the call always runs.
func (o *Orchestrator) addDatabaseTuningRecommendations(ctx context.Context, suggestions *driver.SmartConfigSuggestions) {
	// Schema statistics for recommendations
	stats := dbtuning.SchemaStatistics{
		TotalTables:     suggestions.TotalTables,
		TotalRows:       suggestions.TotalRows,
		AvgRowSizeBytes: suggestions.AvgRowSizeBytes,
		EstimatedMemMB:  suggestions.EstimatedMemMB,
	}

	// HostMemoryMB describes the DB *server's* RAM, not dmt's host.
	// Only pass dmt's host RAM when the DB connection is local;
	// otherwise rules that size memory from it would emit recommendations
	// against the wrong machine (Codex review on #172).
	dmtHost := dbtuning.DetectSystemInfo()
	sourceSys := dbtuning.SystemInfo{CPUCores: dmtHost.CPUCores}
	targetSys := dbtuning.SystemInfo{CPUCores: dmtHost.CPUCores}
	if isLocalDBHost(o.config.Source.Host) {
		sourceSys.HostMemoryMB = dmtHost.HostMemoryMB
	}
	if isLocalDBHost(o.config.Target.Host) {
		targetSys.HostMemoryMB = dmtHost.HostMemoryMB
	}

	// Run source and target analysis concurrently for better performance
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Analyze source database tuning (concurrent)
	if o.sourcePool != nil && o.sourcePool.DB() != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Independent timeout for source analysis
			sourceCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			logging.Debug("Analyzing source database configuration...")
			sourceTuning, err := dbtuning.Analyze(
				sourceCtx,
				o.sourcePool.DB(),
				o.sourcePool.DBType(),
				"source",
				stats,
				sourceSys,
			)

			mu.Lock()
			if err != nil {
				logging.Warn("Failed to analyze source database tuning: %v", err)
				// Set fallback tuning so output is consistent
				suggestions.SourceTuning = &dbtuning.DatabaseTuning{
					DatabaseType:    o.sourcePool.DBType(),
					Role:            "source",
					TuningPotential: "unknown",
					EstimatedImpact: fmt.Sprintf("Analysis failed: %v", err),
				}
			} else {
				suggestions.SourceTuning = sourceTuning
				if sourceTuning.TuningPotential != "unknown" {
					logging.Info("Source tuning: %s potential (%s)", sourceTuning.TuningPotential, sourceTuning.EstimatedImpact)
				}
			}
			mu.Unlock()
		}()
	} else {
		// No source pool available
		suggestions.SourceTuning = &dbtuning.DatabaseTuning{
			DatabaseType:    "unknown",
			Role:            "source",
			TuningPotential: "unknown",
			EstimatedImpact: "Source database not available for analysis",
		}
	}

	// Analyze target database tuning using AI-driven approach (concurrent)
	if o.targetPool != nil && o.targetPool.DB() != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Independent timeout for target analysis
			targetCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			logging.Debug("Analyzing target database configuration...")
			targetTuning, err := dbtuning.Analyze(
				targetCtx,
				o.targetPool.DB(),
				o.targetPool.DBType(),
				"target",
				stats,
				targetSys,
			)

			mu.Lock()
			if err != nil {
				logging.Warn("Failed to analyze target database tuning: %v", err)
				// Set fallback tuning so output is consistent
				suggestions.TargetTuning = &dbtuning.DatabaseTuning{
					DatabaseType:    o.targetPool.DBType(),
					Role:            "target",
					TuningPotential: "unknown",
					EstimatedImpact: fmt.Sprintf("Analysis failed: %v", err),
				}
			} else {
				suggestions.TargetTuning = targetTuning
				if targetTuning.TuningPotential != "unknown" {
					logging.Info("Target tuning: %s potential (%s)", targetTuning.TuningPotential, targetTuning.EstimatedImpact)
				}
			}
			mu.Unlock()
		}()
	} else {
		// No target pool available
		suggestions.TargetTuning = &dbtuning.DatabaseTuning{
			DatabaseType:    "unknown",
			Role:            "target",
			TuningPotential: "unknown",
			EstimatedImpact: "Target database not available for analysis",
		}
	}

	// Wait for all analyses to complete before returning
	wg.Wait()
}

// isLocalDBHost reports whether the given config host string points at
// the same machine dmt is running on. Used to decide whether dmt's
// host RAM (gopsutil) is a meaningful proxy for the DB server's RAM
// for sizing recommendations. Conservative — when in doubt, returns
// false (resulting in no RAM-based recommendation rather than a
// confidently wrong one).
func isLocalDBHost(host string) bool {
	if host == "" {
		// Empty host typically means "connect via socket / pipe", which
		// requires the DB to be local. Treat as local.
		return true
	}
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	if h, err := os.Hostname(); err == nil && strings.EqualFold(host, h) {
		return true
	}
	return false
}
