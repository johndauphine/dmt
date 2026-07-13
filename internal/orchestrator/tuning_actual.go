package orchestrator

import (
	"github.com/johndauphine/dmt/internal/checkpoint"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/logging"
)

type connectionPoolLimit interface {
	MaxConns() int
}

type tuningHistorySaver interface {
	SaveTuningWithActualParams(driver.ActualParams) int64
}

// resizeConnectionPool applies a post-analysis limit when the driver supports
// live resizing and always returns the limit the pool reports it will run.
func resizeConnectionPool(role string, pool connectionPoolLimit, desired int) int {
	if pool == nil {
		return 0
	}
	before := pool.MaxConns()

	if resizer, ok := pool.(driver.ConnectionPoolResizer); ok {
		accepted := resizer.ResizeConnectionPool(desired)
		actual := pool.MaxConns()
		if accepted != actual {
			logging.Warn("%s connection pool resize reported inconsistent limits (accepted=%d live=%d); using live limit", role, accepted, actual)
		}
		if actual != desired {
			logging.Warn("%s connection pool applied an engine constraint (configured=%d live=%d)", role, desired, actual)
		}
		if actual != before {
			logging.Info("%s connection pool resized: %d -> %d", role, before, actual)
		}
		return actual
	}

	actual := pool.MaxConns()
	if actual != desired {
		logging.Warn("%s connection pool does not support live resizing (configured=%d live=%d); preserving live limit", role, desired, actual)
	}
	return actual
}

func (o *Orchestrator) applyLiveConnectionPoolLimits() (source, target int) {
	if o == nil || o.config == nil {
		return 0, 0
	}
	if o.sourcePool != nil {
		source = resizeConnectionPool("source", o.sourcePool, o.config.Migration.MaxSourceConnections)
	}
	if o.targetPool != nil {
		target = resizeConnectionPool("target", o.targetPool, o.config.Migration.MaxTargetConnections)
	}
	return source, target
}

// saveTuningWithLivePools keeps the ordering contract explicit: apply the
// post-analysis limits to the already-open pools, then persist the limits those
// pools report, never the unverified config recommendation (#701).
func (o *Orchestrator) saveTuningWithLivePools(saver tuningHistorySaver, tuning checkpoint.TuningRecord) int64 {
	if o == nil || o.config == nil || saver == nil {
		return 0
	}
	sourceMax, targetMax := o.applyLiveConnectionPoolLimits()
	return saver.SaveTuningWithActualParams(actualTuningParams(o.config, sourceMax, targetMax, tuning))
}

func actualTuningParams(cfg *config.Config, sourceMax, targetMax int, tuning checkpoint.TuningRecord) driver.ActualParams {
	return driver.ActualParams{
		Workers:                 cfg.Migration.Workers,
		ChunkSize:               cfg.Migration.ChunkSize,
		ReadAheadBuffers:        cfg.Migration.ReadAheadBuffers,
		WriteAheadWriters:       cfg.Migration.WriteAheadWriters,
		ParallelReaders:         cfg.Migration.ParallelReaders,
		MaxPartitions:           cfg.Migration.MaxPartitions,
		MaxSourceConnections:    sourceMax,
		MaxTargetConnections:    targetMax,
		TargetSharedBuffersMB:   tuning.TargetSharedBuffersMB,
		TargetSyncCommit:        tuning.TargetSyncCommit,
		TargetFsync:             tuning.TargetFsync,
		TargetFullPageWrites:    tuning.TargetFullPageWrites,
		TargetMaxWALSizeMB:      tuning.TargetMaxWALSizeMB,
		TargetWALLevel:          tuning.TargetWALLevel,
		SourceMaxServerMemoryMB: tuning.SourceMaxServerMemoryMB,
	}
}
