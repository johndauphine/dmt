package checkpoint

import (
	"sort"
	"time"
)

func (fs *FileState) SaveFallbackEvent(runID, surface, fingerprint string) error {
	if runID == "" || surface == "" {
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.state.FallbackEvents == nil {
		fs.state.FallbackEvents = map[string]map[string]map[string]fallbackEventState{}
	}
	bySurface, ok := fs.state.FallbackEvents[runID]
	if !ok {
		bySurface = map[string]map[string]fallbackEventState{}
		fs.state.FallbackEvents[runID] = bySurface
	}
	byFP, ok := bySurface[surface]
	if !ok {
		byFP = map[string]fallbackEventState{}
		bySurface[surface] = byFP
	}
	now := time.Now().UTC()
	rec, ok := byFP[fingerprint]
	if !ok {
		rec = fallbackEventState{FirstSeen: now}
	}
	rec.Count++
	rec.LastSeen = now
	byFP[fingerprint] = rec
	return fs.save()
}

// GetFallbackEventsByRun returns the persisted fallback events for a
// run, sorted by (surface, fingerprint) for deterministic status
// rendering. Empty slice (not nil) when nothing has been recorded.
func (fs *FileState) GetFallbackEventsByRun(runID string) ([]FallbackEventRecord, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	out := make([]FallbackEventRecord, 0)
	bySurface, ok := fs.state.FallbackEvents[runID]
	if !ok {
		return out, nil
	}
	surfaces := make([]string, 0, len(bySurface))
	for s := range bySurface {
		surfaces = append(surfaces, s)
	}
	sort.Strings(surfaces)
	for _, surface := range surfaces {
		byFP := bySurface[surface]
		fps := make([]string, 0, len(byFP))
		for fp := range byFP {
			fps = append(fps, fp)
		}
		sort.Strings(fps)
		for _, fp := range fps {
			rec := byFP[fp]
			out = append(out, FallbackEventRecord{
				RunID:       runID,
				Surface:     surface,
				Fingerprint: fp,
				Count:       rec.Count,
				FirstSeen:   rec.FirstSeen,
				LastSeen:    rec.LastSeen,
			})
		}
	}
	return out, nil
}
