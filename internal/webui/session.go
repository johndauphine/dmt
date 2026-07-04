package webui

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
)

// sessionKey describes one server-side default an operator can set once and
// have applied to subsequent commands, mirroring the TUI's /session keys
// (internal/tui/session.go). The catalog is duplicated here because the TUI's
// is package-private; #583 tracks consolidating them.
type sessionKey struct {
	name string
	desc string
	// validate checks a value and applies any immediate side effect (e.g.
	// verbosity flips the log level now). Returns an error for a bad value.
	validate func(string) error
}

var webSessionKeys = []sessionKey{
	{name: "config", desc: "Default config file path"},
	{name: "profile", desc: "Default saved profile (overrides config)"},
	{name: "state-file", desc: "YAML state-file override"},
	{name: "verbosity", desc: "Log level (debug|info|warn|error)", validate: func(v string) error {
		lvl, err := logging.ParseLevel(v)
		if err != nil {
			return err
		}
		logging.SetLevel(lvl)
		return nil
	}},
	{name: "log-format", desc: "Log format (text|json)", validate: func(v string) error {
		if v != "text" && v != "json" {
			return fmt.Errorf("log-format must be text or json")
		}
		logging.SetFormat(v)
		return nil
	}},
	{name: "metrics-addr", desc: "Prometheus /metrics listen address", validate: func(v string) error {
		if _, _, err := net.SplitHostPort(v); err != nil {
			return fmt.Errorf("metrics-addr must be host:port")
		}
		return nil
	}},
	{name: "otel-endpoint", desc: "OTLP trace exporter endpoint", validate: func(v string) error {
		u, err := url.Parse(v)
		if err != nil || u.Host == "" {
			return fmt.Errorf("otel-endpoint must be a URL with a host")
		}
		return nil
	}},
	{name: "audit-dir", desc: "Audit log directory"},
	{name: "audit-tamper-evident", desc: "Hash-chain the audit log (true|false)", validate: validateBool},
	{name: "no-audit", desc: "Disable the audit log (true|false)", validate: validateBool},
}

func validateBool(v string) error {
	if v != "true" && v != "false" {
		return fmt.Errorf("value must be true or false")
	}
	return nil
}

func lookupSessionKey(name string) (sessionKey, bool) {
	for _, k := range webSessionKeys {
		if k.name == name {
			return k, true
		}
	}
	return sessionKey{}, false
}

// sessionDefaults holds validated per-server session values. Distinct from the
// auth sessionStore (browser login sessions).
type sessionDefaults struct {
	mu sync.Mutex
	m  map[string]string
}

func newSessionDefaults() *sessionDefaults {
	return &sessionDefaults{m: make(map[string]string)}
}

// set validates and stores a value (applying any side effect), or clears the
// key when value is empty. Note: verbosity/log-format validators apply a
// process-global logging change immediately (matching the TUI's /session
// semantics); clearing the key does not revert that change.
func (d *sessionDefaults) set(name, value string) error {
	key, ok := lookupSessionKey(name)
	if !ok {
		return fmt.Errorf("unknown session key %q", name)
	}
	if value == "" {
		d.mu.Lock()
		delete(d.m, name)
		d.mu.Unlock()
		return nil
	}
	if key.validate != nil {
		if err := key.validate(value); err != nil {
			return err
		}
	}
	d.mu.Lock()
	d.m[name] = value
	d.mu.Unlock()
	return nil
}

func (d *sessionDefaults) get(name string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.m[name]
}

func (d *sessionDefaults) clear(name string) {
	d.mu.Lock()
	delete(d.m, name)
	d.mu.Unlock()
}

// snapshot returns a copy of all set values.
func (d *sessionDefaults) snapshot() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]string, len(d.m))
	for k, v := range d.m {
		out[k] = v
	}
	return out
}

// --- HTTP handlers ---

// sessionKeyInfo describes a key + its current value for GET /api/session.
type sessionKeyInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Value       string `json:"value,omitempty"`
}

func (s *Server) handleSessionGet(w http.ResponseWriter, r *http.Request) {
	set := s.sessionDefaults.snapshot()
	out := make([]sessionKeyInfo, 0, len(webSessionKeys))
	for _, k := range webSessionKeys {
		out = append(out, sessionKeyInfo{Name: k.name, Description: k.desc, Value: set[k.name]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *Server) handleSessionSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.sessionDefaults.set(req.Key, req.Value); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_session_value", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleSessionClear(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if _, ok := lookupSessionKey(key); !ok {
		writeError(w, http.StatusNotFound, "unknown_key", fmt.Sprintf("unknown session key %q", key))
		return
	}
	s.sessionDefaults.clear(key)
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}
