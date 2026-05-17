package audit

import (
	"encoding/json"
	"sort"
	"strings"
)

// marshalLine encodes the event + its (already-scrubbed) fields as
// one canonical JSON object. Field keys are sorted for stable output
// (matters for hash chaining and golden-file tests).
func marshalLine(ev Event, scrubbed map[string]any) ([]byte, error) {
	// Build a single map combining the typed Event fields with the
	// arbitrary Fields. The typed fields win on key collision (so a
	// caller that puts "type" in Fields can't override the event's
	// own Type field).
	flat := make(map[string]any, len(scrubbed)+8)
	for k, v := range scrubbed {
		flat[k] = v
	}
	flat["ts"] = ev.Timestamp
	flat["type"] = ev.Type
	flat["run_id"] = ev.RunID
	if ev.Seq > 0 {
		flat["seq"] = ev.Seq
		flat["prev_hash"] = ev.PrevHash
		flat["hash"] = ev.Hash
	}
	return canonicalJSON(flat)
}

// canonicalJSON encodes a map with sorted keys and no extraneous
// whitespace. Required for hash chaining to be reproducible AND for
// the documented `jq + sha256sum` verifier to produce matching hashes
// — jq does not HTML-escape `<>&`, so we must not either. We use a
// json.Encoder with SetEscapeHTML(false) and trim its trailing newline
// (Encoder appends one) to match jq's `-c` output byte-for-byte.
// (Copilot review on #235.)
func canonicalJSON(m map[string]any) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf []byte
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		kb, err := marshalNoEscape(k)
		if err != nil {
			return nil, err
		}
		buf = append(buf, kb...)
		buf = append(buf, ':')
		vb, err := canonicalValue(m[k])
		if err != nil {
			return nil, err
		}
		buf = append(buf, vb...)
	}
	buf = append(buf, '}')
	return buf, nil
}

// canonicalValue recursively encodes a value. Maps go through
// canonicalJSON so nested objects also have sorted keys; everything
// else uses marshalNoEscape so HTML-special characters survive
// unchanged for the documented verifier to match.
func canonicalValue(v any) ([]byte, error) {
	switch typed := v.(type) {
	case map[string]any:
		return canonicalJSON(typed)
	case []any:
		var buf []byte
		buf = append(buf, '[')
		for i, item := range typed {
			if i > 0 {
				buf = append(buf, ',')
			}
			b, err := canonicalValue(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, b...)
		}
		buf = append(buf, ']')
		return buf, nil
	default:
		return marshalNoEscape(v)
	}
}

// marshalNoEscape returns the canonical-JSON encoding of v with HTML
// escaping disabled. encoding/json's default behavior is to escape
// `<`, `>`, and `&` as `<` / `>` / `&` for safety in
// HTML contexts; that's the wrong choice for an audit log designed
// to be replayed through `jq -cS`, which does no such escaping. The
// hash verifier and the bytes-on-disk both go through this function
// so they stay aligned.
func marshalNoEscape(v any) ([]byte, error) {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := sb.String()
	// Encoder always appends a trailing newline; strip it so the
	// caller can splice the bytes into a larger structure cleanly.
	return []byte(strings.TrimSuffix(out, "\n")), nil
}
