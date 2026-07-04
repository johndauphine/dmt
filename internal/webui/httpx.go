package webui

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/johndauphine/dmt/internal/exitcodes"
	"github.com/johndauphine/dmt/internal/logging"
)

// maxRequestBody caps decoded request bodies. Command requests are small
// (origins + option flags); a generous 1 MiB ceiling still blocks abuse.
const maxRequestBody = 1 << 20

// originReq is the config/profile/state origin shared by every command
// request, mirroring how the CLI and TUI resolve which config a command acts
// on (@config / --profile / --state-file). Endpoint-specific option structs
// embed it.
type originReq struct {
	Config    string `json:"config"`
	Profile   string `json:"profile"`
	StateFile string `json:"state_file"`
}

// decodeJSON reads a JSON request body into v. An empty body is allowed (v
// keeps its zero values) so a bare POST uses the server's default config.
// Returns false and writes an error response when the body is malformed.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && err != io.EOF {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// writeAPIError maps a domain error to an HTTP status + stable error code,
// reusing the CLI's exit-code classification so the WebUI, CLI, and automation
// agree on what kind of failure occurred. The message is scrubbed of secrets
// before it reaches the browser — this is the API's single error egress, so
// any DSN/credential that slipped into an upstream error is redacted here.
func writeAPIError(w http.ResponseWriter, err error) {
	status, code := http.StatusInternalServerError, "internal_error"
	switch exitcodes.FromError(err) {
	case exitcodes.ConfigError:
		status, code = http.StatusBadRequest, "config_error"
	case exitcodes.ValidationError:
		status, code = http.StatusUnprocessableEntity, "validation_error"
	case exitcodes.ConnectionError:
		status, code = http.StatusBadGateway, "connection_error"
	case exitcodes.IOError:
		status, code = http.StatusBadRequest, "io_error"
	case exitcodes.StateError:
		status, code = http.StatusConflict, "state_error"
	case exitcodes.Cancelled:
		status, code = http.StatusRequestTimeout, "cancelled"
	}
	// Feature-limitation errors (e.g. analyze on an engine smartconfig
	// doesn't support) are the operator's input, not a server fault — surface
	// them as 422 rather than a scary 500.
	if status == http.StatusInternalServerError && isUnsupportedFeature(err) {
		status, code = http.StatusUnprocessableEntity, "unsupported"
	}
	writeError(w, status, code, logging.Scrub(err.Error()))
}

// isUnsupportedFeature reports whether err describes a capability the engine
// or command does not implement (vs. a genuine internal fault).
func isUnsupportedFeature(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "unsupported") ||
		strings.Contains(m, "not supported") ||
		strings.Contains(m, "not implemented")
}
