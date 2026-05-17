package audit

import (
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

// secretKeyNames is the set of field keys whose values are redacted
// regardless of value content. `logging.Scrub` catches secret patterns
// inside strings (DSNs, Bearer headers, etc.), but a structured event
// like `{source: {password: "hunter2"}}` carries the secret as the
// VALUE of a key named "password" — Scrub's regex doesn't see that
// because there's no `password=` prefix in the string itself. We redact
// by key name as a second line of defense.
//
// Match is case-insensitive on the trimmed key.
var secretKeyNames = map[string]struct{}{
	"password": {}, "passwd": {}, "pwd": {},
	"api_key": {}, "apikey": {}, "api-key": {},
	"secret": {}, "token": {},
	"webhook_url": {}, "webhook-url": {},
	"authorization": {},
}

// scrubFields walks the caller-supplied Fields map and returns a copy
// where:
//   - string values are passed through logging.Scrub (catches DSNs,
//     Bearer tokens, sk- API keys, Slack webhook URLs in free text)
//   - values whose KEY name matches secretKeyNames are replaced with
//     the redacted token regardless of value content (catches the
//     structured `{source: {password: "..."}}` case)
//   - nested maps recurse so secrets at any depth are caught
//
// Keys themselves are left intact — only values get redacted. Scrubbing
// keys would mangle the event schema and make audit replay harder.
func scrubFields(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if _, secret := secretKeyNames[strings.ToLower(strings.TrimSpace(k))]; secret {
			out[k] = logging.RedactedToken
			continue
		}
		switch typed := v.(type) {
		case string:
			out[k] = logging.Scrub(typed)
		case map[string]any:
			out[k] = scrubFields(typed)
		default:
			out[k] = v
		}
	}
	return out
}
