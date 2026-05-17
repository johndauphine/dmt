package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// computeHash returns sha256(prev_hash || canonicalJSON(event without
// hash)). The verifier replays this calculation across the file to
// detect retroactive edits.
func computeHash(ev Event, scrubbed map[string]any) (string, error) {
	// Construct the flat event WITHOUT the hash field — the verifier
	// must be able to compute the same hash by stripping the on-disk
	// hash and re-running this function.
	flat := make(map[string]any, len(scrubbed)+7)
	for k, v := range scrubbed {
		flat[k] = v
	}
	flat["ts"] = ev.Timestamp
	flat["type"] = ev.Type
	flat["run_id"] = ev.RunID
	flat["seq"] = ev.Seq
	flat["prev_hash"] = ev.PrevHash

	body, err := canonicalJSON(flat)
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	io.WriteString(sum, ev.PrevHash)
	sum.Write(body)
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}
