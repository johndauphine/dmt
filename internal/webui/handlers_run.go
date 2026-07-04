package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/johndauphine/dmt/internal/config"
	"github.com/johndauphine/dmt/internal/logging"
	"github.com/johndauphine/dmt/internal/orchestrator"
)

// progressInterval is how often the orchestrator's tracker emits a progress
// update into the event hub.
const progressInterval = 1 * time.Second

// runRequest is the POST /api/run body, mirroring the CLI `run` flags the TUI
// exposes.
type runRequest struct {
	originReq
	DryRun          bool   `json:"dry_run"`
	AISchemaAdvisor bool   `json:"ai_schema_advisor"`
	SourceSchema    string `json:"source_schema"`
	TargetSchema    string `json:"target_schema"`
	Workers         int    `json:"workers"`
	SkipPreflight   string `json:"skip_preflight"`
	ConfirmBackup   bool   `json:"confirm_backup"`
}

// resumeRequest is the POST /api/resume body.
type resumeRequest struct {
	originReq
	Force         bool   `json:"force"`
	SkipPreflight string `json:"skip_preflight"`
}

// handleRun starts a migration (or previews it when dry_run is set). A real run
// is asynchronous: it returns 202 with the run id and streams progress over
// /api/events. Only one migration runs at a time (409 otherwise).
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, path, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	applyRunOverrides(cfg, req)

	if req.DryRun {
		s.serveDryRun(w, r, cfg, stateFile, req.AISchemaAdvisor)
		return
	}
	s.startMigration(w, cfg, path, kindRun, orchestrator.Options{
		StateFile:             stateFile,
		EnableAISchemaAdvisor: req.AISchemaAdvisor,
	})
}

// handleResume resumes an interrupted migration.
func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	var req resumeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cfg, path, stateFile, err := s.resolveConfig(req.originReq)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if req.SkipPreflight != "" {
		cfg.Migration.SkipPreflight = []string{req.SkipPreflight}
	}
	s.startMigration(w, cfg, path, kindResume, orchestrator.Options{
		StateFile:   stateFile,
		ForceResume: req.Force,
	})
}

// applyRunOverrides layers per-request run flags onto the loaded config, same
// as cmd/migrate/run.go's IsSet overrides.
func applyRunOverrides(cfg *config.Config, req runRequest) {
	if req.SourceSchema != "" {
		cfg.Source.Schema = req.SourceSchema
	}
	if req.TargetSchema != "" {
		cfg.Target.Schema = req.TargetSchema
	}
	if req.Workers > 0 {
		cfg.Migration.Workers = req.Workers
	}
	if req.SkipPreflight != "" {
		cfg.Migration.SkipPreflight = []string{req.SkipPreflight}
	}
	if req.ConfirmBackup {
		cfg.Migration.ConfirmBackup = true
	}
}

// serveDryRun previews the plan synchronously (like config/check). Dry runs
// don't touch the single-flight guard or the event stream.
func (s *Server) serveDryRun(w http.ResponseWriter, r *http.Request, cfg *config.Config, stateFile string, aiAdvisor bool) {
	orch, err := orchestrator.NewWithOptions(cfg, orchestrator.Options{
		StateFile:             stateFile,
		EnableAISchemaAdvisor: aiAdvisor,
	})
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

// startMigration acquires the single-flight guard, wires progress into the hub,
// and launches the migration goroutine. The goroutine owns the orchestrator's
// lifetime (it Closes it), so this function must not defer Close.
func (s *Server) startMigration(w http.ResponseWriter, cfg *config.Config, configPath string, kind runKind, opts orchestrator.Options) {
	opts.RunID = shortRunID()
	orch, err := orchestrator.NewWithOptions(cfg, opts)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	orch.SetRunContext("", configPath)
	orch.SetProgressReporter(s.hub, progressInterval)

	ctx, cancel := context.WithCancel(context.Background())
	st, ok := s.runs.start(kind, opts.RunID, cancel)
	if !ok {
		cancel()
		orch.Close()
		writeError(w, http.StatusConflict, "run_in_progress", "a migration is already running")
		return
	}
	s.hub.setLive(true)
	s.hub.publish(event{Type: eventStarted, Run: &st})
	s.runWg.Add(1)
	go s.execute(ctx, cancel, orch, kind)
	writeJSON(w, http.StatusAccepted, st)
}

// execute runs the migration to completion and emits the terminal event. It is
// the sole owner of orch after startMigration hands off.
func (s *Server) execute(ctx context.Context, cancel context.CancelFunc, orch *orchestrator.Orchestrator, kind runKind) {
	defer s.runWg.Done()
	defer cancel()
	defer orch.Close()

	var runErr error
	switch kind {
	case kindResume:
		runErr = orch.Resume(ctx)
	default:
		runErr = orch.Run(ctx)
	}

	status, evType := "completed", eventDone
	var errMsg string
	if runErr != nil {
		if ctx.Err() == context.Canceled {
			status, evType = "cancelled", eventCancelled
		} else {
			status, evType = "failed", eventFailed
		}
		errMsg = logging.Scrub(runErr.Error())
	}
	// Record terminal state while still holding the single-flight slot, stop
	// relaying progress, publish the terminal event, then release the slot.
	// Ordering matters: releasing the slot first would let a successor run
	// start and have its progress suppressed by this run's setLive(false).
	st := s.runs.complete(status, errMsg)
	s.hub.setLive(false)
	s.hub.publish(event{Type: evType, Run: &st})
	s.runs.clear()
}

// handleRunCancel cancels the active migration.
func (s *Server) handleRunCancel(w http.ResponseWriter, r *http.Request) {
	if s.runs.cancel() {
		writeJSON(w, http.StatusOK, map[string]any{"status": "cancelling"})
		return
	}
	writeError(w, http.StatusConflict, "no_active_run", "no migration is running")
}

// handleRunState reports the current or last migration's state.
func (s *Server) handleRunState(w http.ResponseWriter, r *http.Request) {
	st := s.runs.state()
	if st == nil {
		writeJSON(w, http.StatusOK, map[string]any{"active": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"active": st.Status == "running", "run": st})
}

// handleEvents streams progress + lifecycle events to the browser over SSE.
// EventSource can't set headers, so this endpoint relies on the session cookie
// for auth (established at login). A late joiner is synced with the current run
// state and last progress snapshot before live events begin.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "no_stream", "response streaming unsupported")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // don't let a reverse proxy buffer the stream

	id, ch := s.hub.subscribe()
	defer s.hub.unsubscribe(id)

	if st := s.runs.state(); st != nil {
		writeSSE(w, event{Type: runStateEventType(st), Run: st})
	}
	if snap := s.hub.snapshot(); snap != nil {
		writeSSE(w, *snap)
	}
	flusher.Flush()

	keepalive := time.NewTicker(25 * time.Second)
	defer keepalive.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			writeSSE(w, e)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// runStateEventType maps a run status to the event type used when replaying
// the current state to a newly connected client.
func runStateEventType(st *runState) eventType {
	switch st.Status {
	case "running":
		return eventStarted
	case "failed":
		return eventFailed
	case "cancelled":
		return eventCancelled
	default:
		return eventDone
	}
}

// writeSSE encodes one event in the text/event-stream framing.
func writeSSE(w io.Writer, e event) {
	if e.Time == "" {
		e.Time = time.Now().UTC().Format(time.RFC3339)
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, data)
}

// shortRunID mints an 8-char run id, matching the orchestrator's own scheme so
// the id we return is the id the run uses.
func shortRunID() string { return uuid.New().String()[:8] }
