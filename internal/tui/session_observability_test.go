package tui

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

// #445: bad observability/audit values must be rejected at set time,
// not minutes into a run.
func TestSessionKeyValidation(t *testing.T) {
	m := &Model{}

	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "log-format", "xml"})); !strings.Contains(out, "invalid log format") {
		t.Fatalf("log-format output: %q", out)
	}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "no-audit", "yes"})); !strings.Contains(out, "(true|false)") {
		t.Fatalf("no-audit output: %q", out)
	}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "audit-tamper-evident", "true"})); !strings.Contains(out, "audit-tamper-evident = true") {
		t.Fatalf("set output: %q", out)
	}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "metrics-addr", ":9090"})); !strings.Contains(out, "metrics-addr = :9090") {
		t.Fatalf("metrics-addr output: %q", out)
	}
}

// #445: only keys the operator actually set override the loaded config,
// mirroring the CLI's IsSet gate — an unset bool key must not stomp a
// config-file true.
func TestApplyAuditSessionSettings(t *testing.T) {
	m := &Model{session: map[string]string{
		"audit-dir": "/var/log/dmt",
		"no-audit":  "false",
	}}
	settings := m.captureRunSessionSettings()

	cfg := &config.Config{}
	cfg.Migration.AuditTamperEvident = true
	cfg.Migration.NoAudit = true
	applyAuditSessionSettings(cfg, settings)

	if cfg.Migration.AuditDir != "/var/log/dmt" {
		t.Errorf("AuditDir = %q", cfg.Migration.AuditDir)
	}
	if !cfg.Migration.AuditTamperEvident {
		t.Error("unset audit-tamper-evident must not override config value")
	}
	if cfg.Migration.NoAudit {
		t.Error("explicit no-audit=false must override config true")
	}
}
