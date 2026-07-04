package webui

import (
	"context"
	"net/http"
	"time"

	"github.com/johndauphine/dmt/internal/aicopilot"
	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/driver"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// advisoryTimeout bounds a single DB-touching advisory call.
const advisoryTimeout = 90 * time.Second

// handleValidate runs row-count (and configured deep) validation, returning
// the deterministic facts even when validation fails.
func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, _, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(r.Context(), advisoryTimeout)
	defer cancel()

	// ValidateDetailed always returns a non-nil result. When it also returns
	// an error but produced per-table row results, validation actually ran —
	// a count mismatch is a legitimate 200 outcome the UI renders (result
	// carries failed=true). An error with no rows (source unreachable, bad
	// validation.mode) is an operational failure and gets an error status.
	result, err := orch.ValidateDetailed(ctx)
	if err != nil && len(result.Rows) == 0 {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newValidateDTO(result))
}

// handleDiagnose builds deterministic failure facts for a run, with opt-in AI
// triage. State-only, like the CLI's diagnose.
func (s *Server) handleDiagnose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
		RunID    string `json:"run_id"`
		AITriage bool   `json:"ai_triage"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, _, stateFile, err := s.resolveConfig(req.originReq)
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

	ctx, cancel := context.WithTimeout(r.Context(), advisoryTimeout)
	defer cancel()

	review, err := orch.DiagnoseRun(ctx, req.RunID, req.AITriage)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, review)
}

// handlePreflight runs connectivity + preflight checks, with opt-in AI review.
func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
		AIReview bool `json:"ai_review"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, _, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(r.Context(), advisoryTimeout)
	defer cancel()

	result, err := orch.HealthCheck(ctx)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if req.AIReview {
		result.AIPreflightReview = orch.ReviewPreflightWithAI(ctx, result)
	}
	writeJSON(w, http.StatusOK, result)
}

// handleConfigCheck previews the migration plan (dry run).
func (s *Server) handleConfigCheck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, _, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(r.Context(), advisoryTimeout)
	defer cancel()

	result, err := orch.DryRun(ctx)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleAnalyze runs deterministic smartconfig analysis. Like the CLI, it
// falls back to source-only when the target can't be reached.
func (s *Server) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, _, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: stateFile})
	if err != nil {
		// Target may be unavailable; source-only analysis still yields useful
		// tuning advice (mirrors cmd/migrate/analyze.go).
		orch, err = orchestrator.NewWithOptions(cfg, orchestrator.Options{StateFile: stateFile, SourceOnly: true})
		if err != nil {
			writeAPIError(w, err)
			return
		}
	}
	defer orch.Close()

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	suggestions, err := orch.AnalyzeConfig(ctx, sourceSchema(cfg))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newAnalyzeDTO(suggestions))
}

// handleAIConfigReview generates an advisory config review + runbook. It
// degrades to a structured "unavailable"/"error" review when no AI provider is
// configured or the call fails, never a 5xx — the review is always advisory.
func (s *Server) handleAIConfigReview(w http.ResponseWriter, r *http.Request) {
	var req struct {
		originReq
		Request string `json:"request"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, path, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}

	payload := aicopilot.BuildConfigReviewPayload(cfg, aicopilot.ConfigReviewOptions{
		OperatorRequest: req.Request,
		ConfigPath:      path,
		StateFile:       stateFile,
	})

	client := driver.GetAIMapper()
	var review *aicopilot.ConfigReview
	if aicopilot.IsNilTextClient(client) {
		review = aicopilot.UnavailableConfigReview("no AI provider configured in secrets", payload)
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), advisoryTimeout)
		defer cancel()
		generated, genErr := aicopilot.GenerateConfigReview(ctx, client, payload)
		if genErr != nil {
			review = aicopilot.ErrorConfigReview(client.ProviderName(), client.Model(), genErr, payload)
		} else {
			review = generated
		}
	}
	writeJSON(w, http.StatusOK, review)
}

// sourceSchema mirrors the CLI's default source-schema selection for analyze:
// the configured schema, else the engine default (public for Postgres, dbo
// otherwise).
func sourceSchema(cfg *config.Config) string {
	if cfg.Source.Schema != "" {
		return cfg.Source.Schema
	}
	if cfg.Source.Type == "postgres" {
		return "public"
	}
	return "dbo"
}
