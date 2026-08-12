package webui

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/johndauphine/dmt/v5/internal/setup"
)

// setupSession wraps the shared, UI-agnostic setup.State (internal/setup) that
// the TUI also drives. The WebUI is a thin server-side driver over it: it
// answers the current prompt, applies input via Process, and runs auto-action
// steps (secrets/config writes, connection tests) synchronously. One setup
// flow at a time is sufficient for this single-operator tool.
type setupSession struct {
	mu      sync.Mutex
	state   *setup.State
	lastErr string
}

// setupPromptDTO is the current setup question (or completion) for the client.
type setupPromptDTO struct {
	Done          bool     `json:"done"`
	Step          int      `json:"step"`
	Text          string   `json:"text,omitempty"`
	Default       string   `json:"default,omitempty"`
	Choices       []string `json:"choices,omitempty"`
	IsMasked      bool     `json:"is_masked,omitempty"`
	SectionHeader string   `json:"section_header,omitempty"`
	Error         string   `json:"error,omitempty"`
	ConfigPath    string   `json:"config_path,omitempty"`
}

// handleSetupStart begins a fresh guided setup and returns the first prompt.
func (s *Server) handleSetupStart(w http.ResponseWriter, r *http.Request) {
	st := setup.NewState()
	st.SlackWebhook = setup.LoadExistingSlackWebhook()
	st.SlackWebhookOriginal = st.SlackWebhook
	sess := &setupSession{state: st}
	sess.pump()

	s.setupMu.Lock()
	s.setup = sess
	s.setupMu.Unlock()

	writeJSON(w, http.StatusOK, sess.prompt())
}

// handleSetupPrompt returns the current prompt without advancing.
func (s *Server) handleSetupPrompt(w http.ResponseWriter, r *http.Request) {
	sess := s.currentSetup()
	if sess == nil {
		writeError(w, http.StatusConflict, "no_setup", "no setup in progress; POST /api/setup/start first")
		return
	}
	writeJSON(w, http.StatusOK, sess.prompt())
}

// handleSetupInput applies the operator's answer and advances through any
// auto-action steps, returning the next prompt (or completion).
func (s *Server) handleSetupInput(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Input string `json:"input"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	sess := s.currentSetup()
	if sess == nil {
		writeError(w, http.StatusConflict, "no_setup", "no setup in progress; POST /api/setup/start first")
		return
	}
	sess.input(req.Input)
	writeJSON(w, http.StatusOK, sess.prompt())
}

func (s *Server) currentSetup() *setupSession {
	s.setupMu.Lock()
	defer s.setupMu.Unlock()
	return s.setup
}

// input applies one user answer, then pumps auto steps.
func (sess *setupSession) input(in string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.state.CurrentStep == setup.StepDone {
		return
	}
	if errMsg := sess.state.Process(in); errMsg != "" {
		sess.lastErr = errMsg
		return
	}
	sess.lastErr = ""
	sess.pumpLocked()
}

// pump runs auto steps (caller holds no lock).
func (sess *setupSession) pump() {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.pumpLocked()
}

// pumpLocked advances through IsAutoAction steps until a user prompt or done.
// Auto steps that write files can fail; on failure it records the error and
// stops so the client can surface it (and the state stays on the step).
func (sess *setupSession) pumpLocked() {
	st := sess.state
	for {
		if st.CurrentStep == setup.StepDone {
			return
		}
		info := st.Prompt()
		if !info.IsAutoAction {
			return
		}
		before := st.CurrentStep
		switch st.CurrentStep {
		case setup.StepCheckSecrets:
			st.Process(setup.CheckExistingSecrets())
		case setup.StepWriteSecrets:
			if err := st.WriteSecretsFile(); err != nil {
				sess.lastErr = st.Process(err.Error())
				return
			}
			st.Process("")
		case setup.StepWriteSlackSecret:
			if err := st.WriteSlackSecret(); err != nil {
				sess.lastErr = st.Process(err.Error())
				return
			}
			st.Process("")
		case setup.StepSourceConnTest:
			st.Process(connInput(runConnTest(st, true)))
		case setup.StepTargetConnTest:
			st.Process(connInput(runConnTest(st, false)))
		case setup.StepWriteConfig:
			if err := st.WriteConfigFile(); err != nil {
				sess.lastErr = st.Process(err.Error())
				return
			}
			st.Process("")
		default:
			st.Process("")
		}
		// Backstop: every current auto step's Process advances the step (or a
		// failure path returned above). If a future auto step is ever added
		// without a case here and doesn't advance, stop rather than spin.
		if st.CurrentStep == before {
			return
		}
	}
}

// prompt snapshots the current prompt for the client.
func (sess *setupSession) prompt() setupPromptDTO {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st := sess.state
	if st.CurrentStep == setup.StepDone {
		return setupPromptDTO{Done: true, Step: int(st.CurrentStep), ConfigPath: st.ConfigPath}
	}
	info := st.Prompt()
	return setupPromptDTO{
		Step:          int(st.CurrentStep),
		Text:          info.Text,
		Default:       info.Default,
		Choices:       info.Choices,
		IsMasked:      info.IsMasked,
		SectionHeader: info.SectionHeader,
		Error:         sess.lastErr,
		ConfigPath:    st.ConfigPath,
	}
}

// connInput maps a connection-test result to the input setup.Process expects:
// "" for success, the error string for failure.
func connInput(res *setup.ConnTestResult) string {
	if res.Connected {
		return ""
	}
	return res.Error
}

// runConnTest performs a synchronous connection test for the source or target
// side, resolving only that side's secret placeholders (as the TUI does).
func runConnTest(st *setup.State, source bool) *setup.ConnTestResult {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if source {
		c := st.SourceConnConfig()
		return setup.TestConnection(ctx, c.Source.Type, c.Source.Host, c.Source.Port,
			c.Source.Database, c.Source.User, c.Source.Password, c.Source.DSNOptions())
	}
	c := st.TargetConnConfig()
	return setup.TestConnection(ctx, c.Target.Type, c.Target.Host, c.Target.Port,
		c.Target.Database, c.Target.User, c.Target.Password, c.Target.DSNOptions())
}
