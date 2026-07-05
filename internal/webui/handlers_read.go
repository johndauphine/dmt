package webui

import (
	"net/http"
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

// handleHistory lists all persisted runs (most recent first).
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
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

	runs, err := orch.GetAllRuns()
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": newRunDTOs(runs)})
}
