package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// templatedConfigYAML builds a minimal valid config with the given source
// user/password expressions (which may contain ${...} templates), plus a
// fixed target so tests can assert the target is never injected/overridden.
func templatedConfigYAML(srcUser, srcPassword string) []byte {
	return []byte(`
source:
  type: mssql
  host: src.example
  database: source
  user: ` + srcUser + `
  password: ` + srcPassword + `
target:
  type: postgres
  host: target.example
  database: target
  user: user
  password: pass
migration:
  target_mode: drop_recreate
`)
}

func TestLoadBytesSecretWithHashNotTruncated(t *testing.T) {
	// Regression for #552: raw text substitution turned
	//   password: ${env:DMT_PW}   with DMT_PW="secret #2024"
	// into `password: secret #2024`, and YAML treated " #2024" as a comment,
	// so the migration authenticated with "secret". Per-scalar expansion keeps
	// the full value.
	t.Setenv("DMT_TEST_PW_HASH", "secret #2024")

	cfg, err := LoadBytes(templatedConfigYAML("user", "${env:DMT_TEST_PW_HASH}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Source.Password != "secret #2024" {
		t.Fatalf("Source.Password = %q, want %q (comment truncation regression)", cfg.Source.Password, "secret #2024")
	}
}

func TestLoadBytesSecretWithNewlineCannotInjectStructure(t *testing.T) {
	// Regression for #552: a secret containing newlines used to be spliced
	// verbatim into the YAML text, letting the value inject or override
	// top-level keys (here, the target connection). Per-scalar expansion keeps
	// it a single string value and leaves the target untouched.
	t.Setenv("DMT_TEST_PW_INJECT", "pw\ntarget:\n  host: attacker.example\n  database: evil")

	cfg, err := LoadBytes(templatedConfigYAML("user", "${env:DMT_TEST_PW_INJECT}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Target.Host != "target.example" {
		t.Fatalf("Target.Host = %q, want %q — secret injected config structure", cfg.Target.Host, "target.example")
	}
	if cfg.Target.Database != "target" {
		t.Fatalf("Target.Database = %q, want %q — secret injected config structure", cfg.Target.Database, "target")
	}
	if !strings.Contains(cfg.Source.Password, "attacker.example") || !strings.Contains(cfg.Source.Password, "\n") {
		t.Fatalf("Source.Password did not retain the literal multi-line secret: %q", cfg.Source.Password)
	}
}

func TestLoadBytesSecretLookingLikeYAMLKeyStaysScalar(t *testing.T) {
	// A secret that looks like a "key: value" mapping must remain a scalar.
	t.Setenv("DMT_TEST_PW_COLON", "host: evil.example")

	cfg, err := LoadBytes(templatedConfigYAML("user", "${env:DMT_TEST_PW_COLON}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Source.Password != "host: evil.example" {
		t.Fatalf("Source.Password = %q, want %q", cfg.Source.Password, "host: evil.example")
	}
}

func TestLoadBytesFileSecretWithInteriorNewlinesStaysScalar(t *testing.T) {
	// Regression for #552: a ${file:} secret with interior newlines (PEM key,
	// JSON blob) — which TrimSpace does not strip — used to be spliced verbatim
	// into the document and could inject structure. It must survive as a single
	// literal scalar and leave the rest of the config intact.
	secret := "-----BEGIN KEY-----\ntarget:\n  host: attacker.example\nline3\n-----END KEY-----"
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "pem")
	if err := os.WriteFile(secretFile, []byte(secret), 0600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}

	cfg, err := LoadBytes(templatedConfigYAML("user", "${file:"+secretFile+"}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Target.Host != "target.example" {
		t.Fatalf("Target.Host = %q, want %q — file secret injected structure", cfg.Target.Host, "target.example")
	}
	// TrimSpace only strips leading/trailing whitespace, so the interior
	// newlines and every line are preserved verbatim.
	if cfg.Source.Password != secret {
		t.Fatalf("Source.Password did not retain the literal multi-line file secret:\n got: %q\nwant: %q", cfg.Source.Password, secret)
	}
}

func TestLoadBytesNumericLookingSecretStaysString(t *testing.T) {
	// A numeric-looking secret expanded into a string field must remain a
	// string (leading zero preserved), not be coerced to an int by the tag.
	t.Setenv("DMT_TEST_PW_NUMERIC", "0123")

	cfg, err := LoadBytes(templatedConfigYAML("user", "${env:DMT_TEST_PW_NUMERIC}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Source.Password != "0123" {
		t.Fatalf("Source.Password = %q, want %q (numeric secret must stay a string)", cfg.Source.Password, "0123")
	}
}

func TestLoadBytesEmbeddedAndMultipleTemplatesStillExpand(t *testing.T) {
	// Composite/embedded and multiple templates within one scalar must still
	// resolve (the previous behavior), even though expansion is now per-scalar.
	t.Setenv("DMT_TEST_USER", "alice")
	t.Setenv("DMT_TEST_DOMAIN", "corp")
	t.Setenv("DMT_TEST_PW_PART", "sw0rd")

	cfg, err := LoadBytes(templatedConfigYAML("${env:DMT_TEST_USER}@${env:DMT_TEST_DOMAIN}", "pa-${env:DMT_TEST_PW_PART}"))
	if err != nil {
		t.Fatalf("LoadBytes: %v", err)
	}
	if cfg.Source.User != "alice@corp" {
		t.Fatalf("Source.User = %q, want %q (embedded/multi-template regression)", cfg.Source.User, "alice@corp")
	}
	if cfg.Source.Password != "pa-sw0rd" {
		t.Fatalf("Source.Password = %q, want %q (embedded template regression)", cfg.Source.Password, "pa-sw0rd")
	}
}
