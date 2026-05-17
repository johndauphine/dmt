package orchestrator

import "fmt"

func (o *Orchestrator) clearResumeProgress(runID, taskKey string, taskID int64, tableName string) error {
	if err := o.state.ClearPartitionTransferProgress(runID, taskKey); err != nil {
		return fmt.Errorf("clearing partition progress for %s: %w", tableName, err)
	}
	if err := o.state.ClearTransferProgress(taskID); err != nil {
		return fmt.Errorf("clearing transfer progress for %s: %w", tableName, err)
	}
	return nil
}

func (o *Orchestrator) expectedResumeRows(
	runID, taskKey string,
	isPartitioned bool,
	tableLastPK any,
	tableRowsDone int64,
) (int64, bool, error) {
	expectedRows := tableRowsDone
	hasProgress := tableLastPK != nil

	if !isPartitioned {
		return expectedRows, hasProgress, nil
	}

	summary, err := o.state.GetPartitionTransferProgressSummary(runID, taskKey)
	if err != nil {
		return 0, false, fmt.Errorf("getting partition progress for %s: %w", taskKey, err)
	}
	if !summary.HasProgress() {
		return expectedRows, hasProgress, nil
	}
	if !hasProgress || summary.RowsDone > expectedRows {
		expectedRows = summary.RowsDone
	}
	return expectedRows, true, nil
}
