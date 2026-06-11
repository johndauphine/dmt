package tui

import (
	"strings"
	"testing"
)

// #444: normalized parsing — same behavior for every slash command.
func TestParseSlashArgs(t *testing.T) {
	spec := argSpec{
		command: "/test",
		bools:   map[string]string{"-d": "detailed", "--detailed": "detailed"},
		strs:    map[string]string{"--profile": "profile", "--run": "run"},
	}
	t.Run("positional config with @ stripped", func(t *testing.T) {
		pa, err := parseSlashArgs(spec, []string{"/test", "@my.yaml", "-d"})
		if err != nil {
			t.Fatal(err)
		}
		if pa.positionals[0] != "my.yaml" || !pa.bools["detailed"] {
			t.Fatalf("got %+v", pa)
		}
	})
	t.Run("flag value and = syntax", func(t *testing.T) {
		pa, err := parseSlashArgs(spec, []string{"/test", "--profile", "prod", "--run=abc"})
		if err != nil {
			t.Fatal(err)
		}
		if pa.strs["profile"] != "prod" || pa.strs["run"] != "abc" {
			t.Fatalf("got %+v", pa)
		}
	})
	t.Run("unknown flag errors consistently", func(t *testing.T) {
		_, err := parseSlashArgs(spec, []string{"/test", "--bogus"})
		if err == nil || !strings.Contains(err.Error(), "/test: unknown flag --bogus") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing value errors consistently", func(t *testing.T) {
		_, err := parseSlashArgs(spec, []string{"/test", "--profile"})
		if err == nil || !strings.Contains(err.Error(), "requires a value") {
			t.Fatalf("err = %v", err)
		}
	})
}

// #444: explicit args win; session defaults fill gaps; config.yaml last.
func TestResolveOriginSessionFallback(t *testing.T) {
	m := &Model{}
	pa, _ := parseSlashArgs(argSpec{command: "/x", strs: originFlags()}, []string{"/x"})
	if cf, pn := m.resolveOrigin(pa); cf != "config.yaml" || pn != "" {
		t.Fatalf("default: %q %q", cf, pn)
	}
	m.session = map[string]string{"config": "team.yaml", "profile": "prod"}
	if cf, pn := m.resolveOrigin(pa); cf != "team.yaml" || pn != "prod" {
		t.Fatalf("session: %q %q", cf, pn)
	}
	pa2, _ := parseSlashArgs(argSpec{command: "/x", strs: originFlags()}, []string{"/x", "@mine.yaml"})
	if cf, _ := m.resolveOrigin(pa2); cf != "mine.yaml" {
		t.Fatalf("explicit positional must beat session: %q", cf)
	}
}

func TestHandleSessionCommand(t *testing.T) {
	m := &Model{}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "config", "x.yaml"})); !strings.Contains(out, "config = x.yaml") {
		t.Fatalf("set output: %q", out)
	}
	if m.sessionGet("config") != "x.yaml" {
		t.Fatal("value not stored")
	}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "bogus", "v"})); !strings.Contains(out, "unknown key") {
		t.Fatalf("unknown key output: %q", out)
	}
	if out := runCmd(t, m.handleSessionCommand([]string{"/session", "clear", "config"})); !strings.Contains(out, "cleared") {
		t.Fatalf("clear output: %q", out)
	}
	if m.sessionGet("config") != "" {
		t.Fatal("clear did not unset")
	}
}
