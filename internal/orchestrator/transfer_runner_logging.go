package orchestrator

import (
	"time"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/source"
)

// logPoolStats logs connection pool statistics.
func (r *TransferRunner) logPoolStats() {
	if !logging.IsDebug() {
		return
	}

	logging.Debug("\nConnection Pool Usage:")
	logging.Debug("  Source %s", r.sourcePool.PoolStats())
	logging.Debug("  Target %s", r.targetPool.PoolStats())
}

// logTransferProfile logs per-table transfer statistics.
func (r *TransferRunner) logTransferProfile(tables []source.Table, statsMap map[string]*tableStats) {
	if !logging.IsDebug() {
		return
	}

	logging.Debug("\nTransfer Profile (per table):")
	logging.Debug("------------------------------")

	var totalQuery, totalScan, totalWrite time.Duration
	for _, t := range tables {
		ts := statsMap[t.Name]
		if ts.stats.Rows > 0 {
			logging.Debug("%-25s %s", t.Name, ts.stats.String())
			totalQuery += ts.stats.QueryTime
			totalScan += ts.stats.ScanTime
			totalWrite += ts.stats.WriteTime
		}
	}

	totalTime := totalQuery + totalScan + totalWrite
	if totalTime > 0 {
		logging.Debug("------------------------------")
		logging.Debug("%-25s query=%.1fs (%.0f%%), scan=%.1fs (%.0f%%), write=%.1fs (%.0f%%)",
			"TOTAL",
			totalQuery.Seconds(), float64(totalQuery)/float64(totalTime)*100,
			totalScan.Seconds(), float64(totalScan)/float64(totalTime)*100,
			totalWrite.Seconds(), float64(totalWrite)/float64(totalTime)*100)
	}
}
