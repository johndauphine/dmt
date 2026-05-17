package checkpoint

import "time"

// SaveFallbackEvent records one AI fallback event for a run (#176).
// Bumps the count for (run_id, surface, fingerprint) under UPSERT
// semantics; first_seen is preserved across calls, last_seen advances.
// Surface is one of observability.Surface* values; fingerprint may be
// empty for call sites that don't carry one (the row still aggregates
// the count under the surface).
//
// Called from the observability package's FallbackSink registration
// (orchestrator wires this in Run/Resume). Cheap by design — one
// UPSERT per fallback; a pathological migration's distinct-fingerprint
// count is bounded by the source schema's vocabulary, not the row
// count, so this stays well under the IO budget of the migration
// itself.
func (s *State) SaveFallbackEvent(runID, surface, fingerprint string) error {
	if runID == "" || surface == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`
		INSERT INTO fallback_events (run_id, surface, fingerprint, count, first_seen, last_seen)
		VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(run_id, surface, fingerprint) DO UPDATE SET
			count = count + 1,
			last_seen = excluded.last_seen
	`, runID, surface, fingerprint, now, now)
	return err
}

// GetFallbackEventsByRun returns every fallback row for a run in
// surface-then-fingerprint order so the status renderer's per-surface
// totals are deterministic. Returns an empty slice (never nil) when
// the run has no events so callers don't need a nil guard.
func (s *State) GetFallbackEventsByRun(runID string) ([]FallbackEventRecord, error) {
	rows, err := s.db.Query(`
		SELECT run_id, surface, fingerprint, count, first_seen, last_seen
		FROM fallback_events
		WHERE run_id = ?
		ORDER BY surface ASC, fingerprint ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FallbackEventRecord, 0)
	for rows.Next() {
		var rec FallbackEventRecord
		var firstStr, lastStr string
		if err := rows.Scan(&rec.RunID, &rec.Surface, &rec.Fingerprint, &rec.Count, &firstStr, &lastStr); err != nil {
			return nil, err
		}
		rec.FirstSeen, _ = time.Parse(time.RFC3339Nano, firstStr)
		rec.LastSeen, _ = time.Parse(time.RFC3339Nano, lastStr)
		out = append(out, rec)
	}
	return out, rows.Err()
}
