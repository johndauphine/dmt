package webui

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// originFromQuery reads the config/profile/state origin from GET query
// parameters (the POST handlers read it from the JSON body instead).
func originFromQuery(r *http.Request) originReq {
	q := r.URL.Query()
	return originReq{
		Config:    q.Get("config"),
		Profile:   q.Get("profile"),
		StateFile: q.Get("state_file"),
	}
}

// handleStatus reports the current/last run. "No active migration" is a normal
// idle state, not an error, so it returns 200 with active:false.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	cfg, _, stateFile, err := s.resolveConfig(originFromQuery(r))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewDiagnosticsWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	result, err := orch.GetStatusResult()
	if err != nil {
		if strings.Contains(err.Error(), "no active migration") {
			writeJSON(w, http.StatusOK, map[string]any{"active": false})
			return
		}
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"active": true, "run": result})
}

// handleStatusByID returns the full result for one run id.
func (s *Server) handleStatusByID(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("runID")
	cfg, _, stateFile, err := s.resolveConfig(originFromQuery(r))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewDiagnosticsWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	result, err := orch.GetRunResult(runID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// Scrub even this benign message so writeAPIError stays the single
			// vetted egress and no future error string leaks here (#603).
			writeError(w, http.StatusNotFound, "not_found", logging.Scrub(err.Error()))
			return
		}
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// historyStatuses is the set of run statuses the runs table actually persists
// (CompleteRun writes success/partial/failed; the initial insert is running).
// The history filter accepts only these; anything else is rejected so a typo
// can't masquerade as an empty result.
var historyStatuses = map[string]bool{
	"running": true, "success": true, "partial": true, "failed": true,
}

const (
	historyDefaultLimit = 20
	historyMaxLimit     = 100
)

// handleHistory lists persisted runs (most recent first), paginated. Query
// params: limit (default 20, max 100), offset (default 0), status (optional
// filter). The response carries the page plus the total match count so the
// client can render pagination.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	if status != "" && !historyStatuses[status] {
		writeError(w, http.StatusBadRequest, "invalid_status", "unknown status filter")
		return
	}
	limit := clampQueryInt(q.Get("limit"), historyDefaultLimit, 1, historyMaxLimit)
	offset := clampQueryInt(q.Get("offset"), 0, 0, -1)

	cfg, _, stateFile, err := s.resolveConfig(originFromQuery(r))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewDiagnosticsWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	runs, total, err := orch.GetRunsPage(status, limit, offset)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"runs":   newRunDTOs(runs),
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// clampQueryInt parses a query int, falling back to def on empty/invalid input,
// then clamps it to [min, max]. A negative max means no upper bound.
func clampQueryInt(raw string, def, min, max int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		n = min
	}
	if max >= 0 && n > max {
		n = max
	}
	return n
}
