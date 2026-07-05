package setup

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmt/internal/config"
)

func TestHappyPath(t *testing.T) {
	s := NewState()

	// Phase 1: Check secrets - no existing AI
	if p := s.Prompt(); !p.IsAutoAction {
		t.Fatal("StepCheckSecrets should be auto")
	}
	if err := s.Process("no_ai"); err != "" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Configure AI? No
	if s.CurrentStep != StepConfigureAI {
		t.Fatalf("expected StepConfigureAI, got %d", s.CurrentStep)
	}
	if err := s.Process("n"); err != "" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Phase 1b: Slack — skip
	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("expected StepSlackWebhook, got %d", s.CurrentStep)
	}
	s.Process("") // skip Slack
	if s.CurrentStep != StepSourceType {
		t.Fatalf("expected StepSourceType, got %d", s.CurrentStep)
	}

	// Source: type, host, port, db, user, pass, schema, ssl
	s.Process("postgres")
	s.Process("db.example.com")
	s.Process("5432")
	s.Process("mydb")
	s.Process("myuser")
	s.Process("mypass")
	s.Process("public")
	s.Process("disable")

	// Connection test - success
	if s.CurrentStep != StepSourceConnTest {
		t.Fatalf("expected StepSourceConnTest, got %d", s.CurrentStep)
	}
	s.Process("") // connected
	if !s.SourceConnOK {
		t.Fatal("expected SourceConnOK")
	}

	// Target
	if s.CurrentStep != StepTargetType {
		t.Fatalf("expected StepTargetType, got %d", s.CurrentStep)
	}
	s.Process("mssql")
	s.Process("target.example.com")
	s.Process("1433")
	s.Process("targetdb")
	s.Process("sa")
	s.Process("targetpass")
	s.Process("dbo")
	s.Process("y") // trust cert

	// Connection test - success
	if s.CurrentStep != StepTargetConnTest {
		t.Fatalf("expected StepTargetConnTest, got %d", s.CurrentStep)
	}
	s.Process("") // connected
	if !s.TargetConnOK {
		t.Fatal("expected TargetConnOK")
	}

	// Migration settings
	if s.CurrentStep != StepTargetMode {
		t.Fatalf("expected StepTargetMode, got %d", s.CurrentStep)
	}
	s.Process("drop_recreate")
	s.Process("y") // create indexes
	s.Process("y") // create FKs
	s.Process("4") // workers

	// Config path
	if s.CurrentStep != StepConfigPath {
		t.Fatalf("expected StepConfigPath, got %d", s.CurrentStep)
	}
	s.Process("test-config.yaml")

	// Write config
	if s.CurrentStep != StepWriteConfig {
		t.Fatalf("expected StepWriteConfig, got %d", s.CurrentStep)
	}
	s.Process("") // success

	// Smartconfig analysis is deterministic (#443): offered whenever
	// the source connection works, even with no AI configured.
	if s.CurrentStep != StepRunAnalysis {
		t.Fatalf("expected StepRunAnalysis, got %d", s.CurrentStep)
	}
	s.Process("n")
	if s.CurrentStep != StepDone {
		t.Fatalf("expected StepDone, got %d", s.CurrentStep)
	}

	// Verify config
	if s.Config.Source.Type != "postgres" {
		t.Fatalf("expected postgres, got %s", s.Config.Source.Type)
	}
	if s.Config.Source.Host != "db.example.com" {
		t.Fatalf("expected db.example.com, got %s", s.Config.Source.Host)
	}
	if s.Config.Source.Database != "mydb" {
		t.Fatalf("expected mydb, got %s", s.Config.Source.Database)
	}
	if s.Config.Target.Type != "mssql" {
		t.Fatalf("expected mssql, got %s", s.Config.Target.Type)
	}
	if s.Config.Target.TrustServerCert != true {
		t.Fatal("expected TrustServerCert true")
	}
	if s.Config.Migration.Workers != 4 {
		t.Fatalf("expected 4 workers, got %d", s.Config.Migration.Workers)
	}
	if s.Config.Migration.TargetMode != "drop_recreate" {
		t.Fatalf("expected drop_recreate, got %s", s.Config.Migration.TargetMode)
	}
	if !s.Config.Migration.CreateIndexesEnabled() {
		t.Fatal("expected CreateIndexes true")
	}
}

func TestSecretsExistWithValidAI(t *testing.T) {
	s := NewState()
	s.Process("has_ai")

	// Phase 1b Slack interposes between Phase 1 AI and Phase 2 source.
	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("expected StepSlackWebhook, got %d", s.CurrentStep)
	}
	if !s.AIConfigured {
		t.Fatal("expected AIConfigured")
	}
}

func TestSecretsExistNoAI(t *testing.T) {
	s := NewState()
	s.Process("no_ai")

	if s.CurrentStep != StepConfigureAI {
		t.Fatalf("expected StepConfigureAI, got %d", s.CurrentStep)
	}
}

func TestAINoSkips(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n") // don't configure AI

	// Skipping AI now lands on Slack, not directly on source.
	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("expected StepSlackWebhook, got %d", s.CurrentStep)
	}
	if s.AIConfigured {
		t.Fatal("expected AIConfigured to be false")
	}
}

// Per #174, an empty input at StepConfigureAI defaults to "n" (skip AI)
// — the friction-free path for a fresh install on the AI-optional
// architecture. Pre-#174 the default was "y" which prompted for an API
// key the user might not have.
func TestConfigureAI_EmptyInputDefaultsToSkip(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("") // empty input — should default to skip

	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("empty input at StepConfigureAI should default to skip-AI; got step %d", s.CurrentStep)
	}
	if s.AIConfigured {
		t.Fatal("AIConfigured should be false after skipping with default")
	}
}

func TestConfigureAI_DefaultPromptIsSkip(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	if s.CurrentStep != StepConfigureAI {
		t.Fatalf("expected StepConfigureAI, got %d", s.CurrentStep)
	}
	p := s.Prompt()
	if p.Default != "n" {
		t.Errorf("StepConfigureAI prompt default should be \"n\" per #174; got %q", p.Default)
	}
}

func TestInvalidInput(t *testing.T) {
	s := NewState()
	s.Process("no_ai")

	// Invalid y/n
	if err := s.Process("maybe"); err == "" {
		t.Fatal("expected error for invalid input")
	}
	if s.CurrentStep != StepConfigureAI {
		t.Fatal("step should not advance on error")
	}

	// Valid input advances
	if err := s.Process("y"); err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
	if s.CurrentStep != StepAIProvider {
		t.Fatalf("expected StepAIProvider, got %d", s.CurrentStep)
	}
}

func TestInvalidProvider(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("y")

	// Invalid provider
	if err := s.Process("nonexistent"); err == "" {
		t.Fatal("expected error for invalid provider")
	}
	if s.CurrentStep != StepAIProvider {
		t.Fatal("step should not advance on invalid provider")
	}
}

func TestCloudProviderRequiresKey(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("y")
	s.Process("anthropic")

	// Empty API key
	if err := s.Process(""); err == "" {
		t.Fatal("expected error for empty API key")
	}
	if s.CurrentStep != StepAIKey {
		t.Fatal("step should not advance on empty API key")
	}

	// Valid key
	s.Process("sk-test-key")
	if s.CurrentStep != StepWriteSecrets {
		t.Fatalf("expected StepWriteSecrets, got %d", s.CurrentStep)
	}
	if s.AIKey != "sk-test-key" {
		t.Fatalf("expected sk-test-key, got %s", s.AIKey)
	}
}

func TestLocalProviderDefaultURL(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("y")
	s.Process("ollama")

	// Accept default URL
	s.Process("")
	if s.CurrentStep != StepWriteSecrets {
		t.Fatalf("expected StepWriteSecrets, got %d", s.CurrentStep)
	}
	if s.AIKey != "http://localhost:11434" {
		t.Fatalf("expected default ollama URL, got %s", s.AIKey)
	}
	if !s.AIConfigured {
		t.Fatal("expected AIConfigured")
	}
}

func TestConnectionFailRetryEditSkip(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Fill in source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")

	// Connection failed
	if s.CurrentStep != StepSourceConnTest {
		t.Fatalf("expected StepSourceConnTest, got %d", s.CurrentStep)
	}
	s.Process("connection refused")

	if s.CurrentStep != StepSourceConnResult {
		t.Fatalf("expected StepSourceConnResult, got %d", s.CurrentStep)
	}

	// Test retry
	s.Process("r")
	if s.CurrentStep != StepSourceConnTest {
		t.Fatalf("expected StepSourceConnTest after retry, got %d", s.CurrentStep)
	}

	// Fail again
	s.Process("connection refused")

	// Test edit
	s.Process("e")
	if s.CurrentStep != StepSourceType {
		t.Fatalf("expected StepSourceType after edit, got %d", s.CurrentStep)
	}

	// Fill in again
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")

	// Fail again
	s.Process("connection refused")

	// Test skip
	s.Process("s")
	if s.CurrentStep != StepTargetType {
		t.Fatalf("expected StepTargetType after skip, got %d", s.CurrentStep)
	}
	if s.SourceConnOK {
		t.Fatal("expected SourceConnOK to be false after skip")
	}
}

func TestTargetConnectionFailRetry(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Fill in source
	s.Process("mssql")
	s.Process("localhost")
	s.Process("1433")
	s.Process("srcdb")
	s.Process("sa")
	s.Process("pass")
	s.Process("dbo")
	s.Process("n") // don't trust cert
	s.Process("")  // source conn success

	// Fill in target
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("tgtdb")
	s.Process("postgres")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")

	// Target connection failed
	s.Process("connection timeout")

	if s.CurrentStep != StepTargetConnResult {
		t.Fatalf("expected StepTargetConnResult, got %d", s.CurrentStep)
	}

	// Skip
	s.Process("s")
	if s.CurrentStep != StepTargetMode {
		t.Fatalf("expected StepTargetMode after skip, got %d", s.CurrentStep)
	}
}

func TestInvalidConnResultChoice(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")

	// Connection failed
	s.Process("error")

	// Invalid choice
	if err := s.Process("x"); err == "" {
		t.Fatal("expected error for invalid choice")
	}
	if s.CurrentStep != StepSourceConnResult {
		t.Fatal("step should not advance on invalid choice")
	}
}

func TestAIConfiguredShowsAnalysis(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("y")      // configure AI
	s.Process("ollama") // local provider
	s.Process("")       // default URL
	s.Process("")       // write secrets success
	s.Process("")       // skip Slack

	// Source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn success

	// Target
	s.Process("mssql")
	s.Process("localhost")
	s.Process("1433")
	s.Process("targetdb")
	s.Process("sa")
	s.Process("pass")
	s.Process("dbo")
	s.Process("y")
	s.Process("") // target conn success

	// Migration settings
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")
	s.Process("4")

	// Config path
	s.Process("test.yaml")

	// Write config
	s.Process("")

	// AI configured + source conn OK -> should show analysis prompt
	if s.CurrentStep != StepRunAnalysis {
		t.Fatalf("expected StepRunAnalysis, got %d", s.CurrentStep)
	}

	s.Process("n")
	if s.CurrentStep != StepDone {
		t.Fatalf("expected StepDone, got %d", s.CurrentStep)
	}
	if s.RunAnalysis {
		t.Fatal("expected RunAnalysis false")
	}
}

func TestAnalysisYes(t *testing.T) {
	s := NewState()
	s.Process("has_ai") // existing AI
	s.Process("")       // skip Slack

	// Source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn success

	// Target
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("targetdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // target conn success

	// Settings
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")
	s.Process("4")
	s.Process("out.yaml")
	s.Process("") // write success

	if s.CurrentStep != StepRunAnalysis {
		t.Fatalf("expected StepRunAnalysis, got %d", s.CurrentStep)
	}

	s.Process("y")
	if s.CurrentStep != StepDone {
		t.Fatalf("expected StepDone, got %d", s.CurrentStep)
	}
	if !s.RunAnalysis {
		t.Fatal("expected RunAnalysis true")
	}
}

func TestAnalysisOfferedWithoutAI(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n") // no AI
	s.Process("")  // skip Slack

	// Source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // success

	// Target
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("targetdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // success

	// Settings
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")
	s.Process("4")
	s.Process("out.yaml")
	s.Process("") // write success

	// No AI still offers the deterministic smartconfig analysis (#443)
	// because the source connection succeeded.
	if s.CurrentStep != StepRunAnalysis {
		t.Fatalf("expected StepRunAnalysis (deterministic, AI-free), got %d", s.CurrentStep)
	}
	s.Process("y")
	if s.CurrentStep != StepDone || !s.RunAnalysis {
		t.Fatalf("expected StepDone with RunAnalysis, got step %d analysis %v", s.CurrentStep, s.RunAnalysis)
	}
}

func TestNoAnalysisWhenSourceConnFailed(t *testing.T) {
	s := NewState()
	s.Process("has_ai")
	s.Process("") // skip Slack

	// Source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("conn refused") // fail
	s.Process("s")            // skip

	// Target
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("targetdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // success

	// Settings
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")
	s.Process("4")
	s.Process("out.yaml")
	s.Process("") // write success

	// AI configured but source failed -> no analysis
	if s.CurrentStep != StepDone {
		t.Fatalf("expected StepDone (source failed), got %d", s.CurrentStep)
	}
}

func TestDefaultValues(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Accept all defaults
	s.Process("") // source type -> mssql
	s.Process("") // host -> localhost
	s.Process("") // port -> 1433

	if s.Config.Source.Type != "mssql" {
		t.Fatalf("expected mssql default, got %s", s.Config.Source.Type)
	}
	if s.Config.Source.Host != "localhost" {
		t.Fatalf("expected localhost default, got %s", s.Config.Source.Host)
	}
	if s.Config.Source.Port != 1433 {
		t.Fatalf("expected 1433 default, got %d", s.Config.Source.Port)
	}
}

func TestInvalidPort(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack
	s.Process("postgres")
	s.Process("localhost")

	// Non-numeric port
	if err := s.Process("abc"); err == "" {
		t.Fatal("expected error for invalid port")
	}
	if s.CurrentStep != StepSourcePort {
		t.Fatal("step should not advance on invalid port")
	}

	// Port out of range
	if err := s.Process("99999"); err == "" {
		t.Fatal("expected error for port out of range")
	}

	// Negative port
	if err := s.Process("-1"); err == "" {
		t.Fatal("expected error for negative port")
	}

	// Valid port
	if err := s.Process("5432"); err != "" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestRequiredDatabase(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")

	// Empty database name
	if err := s.Process(""); err == "" {
		t.Fatal("expected error for empty database name")
	}
	if s.CurrentStep != StepSourceDB {
		t.Fatal("step should not advance on empty database")
	}
}

func TestInvalidWorkers(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Fast-forward to workers
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("db2")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // target conn
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")

	if s.CurrentStep != StepWorkers {
		t.Fatalf("expected StepWorkers, got %d", s.CurrentStep)
	}

	// Invalid workers
	if err := s.Process("abc"); err == "" {
		t.Fatal("expected error for invalid workers")
	}
	if err := s.Process("0"); err == "" {
		t.Fatal("expected error for zero workers")
	}
	if err := s.Process("-1"); err == "" {
		t.Fatal("expected error for negative workers")
	}
}

func TestInvalidTargetMode(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Fast-forward to target mode
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("db2")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // target conn

	if s.CurrentStep != StepTargetMode {
		t.Fatalf("expected StepTargetMode, got %d", s.CurrentStep)
	}

	if err := s.Process("invalid_mode"); err == "" {
		t.Fatal("expected error for invalid target mode")
	}
}

func TestUpsertPromptsForDateColumns(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("db2")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // target conn

	if s.CurrentStep != StepTargetMode {
		t.Fatalf("expected StepTargetMode, got %d", s.CurrentStep)
	}
	s.Process("upsert")
	if s.CurrentStep != StepDateColumns {
		t.Fatalf("upsert should route to StepDateColumns, got %d", s.CurrentStep)
	}

	s.Process("LastActivityDate, LastEditDate ,CreationDate")
	want := []string{"LastActivityDate", "LastEditDate", "CreationDate"}
	if !reflect.DeepEqual(s.Config.Migration.DateUpdatedColumns, want) {
		t.Fatalf("DateUpdatedColumns mismatch: got %v want %v",
			s.Config.Migration.DateUpdatedColumns, want)
	}
	if s.CurrentStep != StepCreateIndexes {
		t.Fatalf("after StepDateColumns expected StepCreateIndexes, got %d", s.CurrentStep)
	}
}

func TestDropRecreateSkipsDateColumns(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // source conn
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5433")
	s.Process("db2")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // target conn

	s.Process("drop_recreate")
	if s.CurrentStep != StepCreateIndexes {
		t.Fatalf("drop_recreate should bypass StepDateColumns, got %d", s.CurrentStep)
	}
}

func TestTargetModeDefaultHonorsExistingConfig(t *testing.T) {
	s := NewState()
	s.Config.Migration.TargetMode = "upsert"
	s.CurrentStep = StepTargetMode

	if got := s.Prompt().Default; got != "upsert" {
		t.Fatalf("StepTargetMode default should reflect pre-loaded config, got %q", got)
	}

	// Hitting Enter should keep "upsert" rather than silently reverting.
	s.Process("")
	if s.Config.Migration.TargetMode != "upsert" {
		t.Fatalf("empty input should preserve upsert, got %q", s.Config.Migration.TargetMode)
	}
	if s.CurrentStep != StepDateColumns {
		t.Fatalf("expected routing to StepDateColumns, got %d", s.CurrentStep)
	}
}

func TestEditOrNewKeepsLoadedConfig(t *testing.T) {
	s := NewState()
	s.Config = config.Config{}
	s.Config.Source.Host = "prod-db.example.com"
	s.Config.Migration.TargetMode = "upsert"
	s.ConfigPath = "config.yaml"
	s.CurrentStep = StepEditOrNew

	s.Process("e")
	if s.CurrentStep != StepCheckSecrets {
		t.Fatalf("StepEditOrNew(e) should advance to StepCheckSecrets, got %d", s.CurrentStep)
	}
	if s.Config.Source.Host != "prod-db.example.com" {
		t.Fatal("editing should preserve loaded source host")
	}
	if s.Config.Migration.TargetMode != "upsert" {
		t.Fatal("editing should preserve loaded target_mode")
	}
}

func TestEditOrNewResetsOnFresh(t *testing.T) {
	s := NewState()
	s.Config.Source.Host = "prod-db.example.com"
	s.Config.Migration.TargetMode = "upsert"
	s.ConfigPath = "config.yaml"
	s.CurrentStep = StepEditOrNew

	s.Process("n")
	if s.CurrentStep != StepCheckSecrets {
		t.Fatalf("StepEditOrNew(n) should advance to StepCheckSecrets, got %d", s.CurrentStep)
	}
	if s.Config.Source.Host != "" {
		t.Fatal("new should discard loaded source host")
	}
	if s.ConfigPath != "config.yaml" {
		t.Fatalf("new should preserve ConfigPath, got %q", s.ConfigPath)
	}
}

func TestDateColumnsDashClearsList(t *testing.T) {
	s := NewState()
	s.Config.Migration.DateUpdatedColumns = []string{"LastActivityDate"}
	s.CurrentStep = StepDateColumns

	s.Process("-")
	if len(s.Config.Migration.DateUpdatedColumns) != 0 {
		t.Fatalf("'-' should clear DateUpdatedColumns, got %v",
			s.Config.Migration.DateUpdatedColumns)
	}
	if s.CurrentStep != StepCreateIndexes {
		t.Fatalf("expected StepCreateIndexes after clear, got %d", s.CurrentStep)
	}
}

func TestDateColumnsBlankPreserves(t *testing.T) {
	s := NewState()
	s.Config.Migration.DateUpdatedColumns = []string{"LastActivityDate", "CreationDate"}
	s.CurrentStep = StepDateColumns

	s.Process("")
	want := []string{"LastActivityDate", "CreationDate"}
	if !reflect.DeepEqual(s.Config.Migration.DateUpdatedColumns, want) {
		t.Fatalf("blank input should preserve existing list, got %v",
			s.Config.Migration.DateUpdatedColumns)
	}
}

func TestEditModePreservesBoolFalseDefaults(t *testing.T) {
	// User had `create_indexes: false` in their YAML — editing should
	// show "n" as default and Enter should preserve false.
	s := NewState()
	s.EditMode = true
	createIndexes := false
	createForeignKeys := false
	s.Config.Migration.CreateIndexes = &createIndexes
	s.Config.Migration.CreateForeignKeys = &createForeignKeys
	s.CurrentStep = StepCreateIndexes

	if got := s.Prompt().Default; got != "n" {
		t.Fatalf("EditMode + loaded false: expected default 'n', got %q", got)
	}
	s.Process("")
	if s.Config.Migration.CreateIndexesEnabled() {
		t.Fatal("Enter on 'n' default should preserve CreateIndexes=false")
	}
	if got := s.Prompt().Default; got != "n" {
		t.Fatalf("EditMode + loaded FKs false: expected default 'n', got %q", got)
	}
	s.Process("")
	if s.Config.Migration.CreateForeignKeysEnabled() {
		t.Fatal("Enter on 'n' default should preserve CreateForeignKeys=false")
	}
}

func TestEditModeOmittedBoolDefaultsToFreshDefault(t *testing.T) {
	s := NewState()
	s.EditMode = true
	s.CurrentStep = StepCreateIndexes

	if got := s.Prompt().Default; got != "y" {
		t.Fatalf("EditMode + omitted create_indexes: expected default 'y', got %q", got)
	}
	s.Process("")
	if !s.Config.Migration.CreateIndexesEnabled() {
		t.Fatal("Enter on omitted create_indexes should materialize true")
	}
	if got := s.Prompt().Default; got != "y" {
		t.Fatalf("EditMode + omitted create_foreign_keys: expected default 'y', got %q", got)
	}
	s.Process("")
	if !s.Config.Migration.CreateForeignKeysEnabled() {
		t.Fatal("Enter on omitted create_foreign_keys should materialize true")
	}
}

func TestFreshModeBoolDefaults(t *testing.T) {
	// No config loaded — bool prompts should default to "y" (dmt's fresh
	// default) even though the pointer is nil.
	s := NewState()
	s.CurrentStep = StepCreateIndexes
	if got := s.Prompt().Default; got != "y" {
		t.Fatalf("fresh mode: expected default 'y', got %q", got)
	}
}

func TestEditModeWorkersPreserved(t *testing.T) {
	s := NewState()
	s.EditMode = true
	s.Config.Migration.Workers = 16
	s.CurrentStep = StepWorkers

	if got := s.Prompt().Default; got != "16" {
		t.Fatalf("loaded Workers should drive default, got %q", got)
	}
	s.Process("")
	if s.Config.Migration.Workers != 16 {
		t.Fatalf("blank input should preserve Workers=16, got %d", s.Config.Migration.Workers)
	}
}

func TestEditModeWorkersZeroOmissionPreserved(t *testing.T) {
	// Config omitted `workers:` — Enter must leave it 0 so runtime
	// auto-tuning still kicks in.
	s := NewState()
	s.EditMode = true
	s.Config.Migration.Workers = 0
	s.CurrentStep = StepWorkers

	info := s.Prompt()
	if info.Default != "" {
		t.Fatalf("EditMode + Workers=0: prompt should not show a concrete default, got %q", info.Default)
	}
	s.Process("")
	if s.Config.Migration.Workers != 0 {
		t.Fatalf("blank input must preserve Workers=0 in EditMode, got %d", s.Config.Migration.Workers)
	}
}

func TestFreshModeWorkersGetsNumCPUDefault(t *testing.T) {
	s := NewState()
	s.CurrentStep = StepWorkers
	if info := s.Prompt(); info.Default == "" {
		t.Fatal("fresh setup should show a concrete NumCPU-capped default")
	}
	s.Process("")
	if s.Config.Migration.Workers <= 0 {
		t.Fatalf("fresh setup blank input should materialize NumCPU default, got %d", s.Config.Migration.Workers)
	}
}

func TestSourceConnConfigResolvesPlaceholders(t *testing.T) {
	t.Setenv("DMT_TEST_PASSWORD", "s3cret")
	s := NewState()
	s.Config.Source.Password = "${env:DMT_TEST_PASSWORD}"

	conn := s.SourceConnConfig()
	if conn.Source.Password != "s3cret" {
		t.Fatalf("SourceConnConfig should resolve env placeholder, got %q", conn.Source.Password)
	}
	// Raw state must remain untouched so a re-save preserves the placeholder.
	if s.Config.Source.Password != "${env:DMT_TEST_PASSWORD}" {
		t.Fatalf("SourceConnConfig must not mutate s.Config, got %q", s.Config.Source.Password)
	}
}

func TestSourceConnConfigIgnoresTargetExpansionFailure(t *testing.T) {
	t.Setenv("DMT_SRC_PASS", "good")
	s := NewState()
	s.Config.Source.Password = "${env:DMT_SRC_PASS}"
	// Target points at a file that doesn't exist; previously this would
	// poison the whole expansion and leave source unresolved.
	s.Config.Target.Password = "${file:/tmp/dmt-test-does-not-exist-zzz}"

	conn := s.SourceConnConfig()
	if conn.Source.Password != "good" {
		t.Fatalf("missing target file should not break source resolution, got %q", conn.Source.Password)
	}
}

func TestPerFieldExpansionFailureLeavesPlaceholder(t *testing.T) {
	s := NewState()
	s.Config.Source.Password = "${file:/tmp/dmt-test-does-not-exist-zzz}"

	conn := s.SourceConnConfig()
	// On failure, the placeholder survives so the eventual auth error
	// mentions the actual missing credential rather than silent emptyness.
	if conn.Source.Password != "${file:/tmp/dmt-test-does-not-exist-zzz}" {
		t.Fatalf("failed expansion should leave placeholder, got %q", conn.Source.Password)
	}
}

func TestEditOrNewRejectsBadInput(t *testing.T) {
	s := NewState()
	s.ConfigPath = "config.yaml"
	s.CurrentStep = StepEditOrNew

	if errMsg := s.Process("maybe"); errMsg == "" {
		t.Fatal("bad input should produce a validation error")
	}
	if s.CurrentStep != StepEditOrNew {
		t.Fatal("step should not advance on bad input")
	}
}

func TestMSSQLSSLDefaults(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// MSSQL source
	s.Process("mssql")
	s.Process("localhost")
	s.Process("1433")
	s.Process("db")
	s.Process("sa")
	s.Process("pass")
	s.Process("dbo")

	// SSL prompt for MSSQL should offer the TLS modes (require/trust/disable).
	info := s.Prompt()
	if !strings.Contains(info.Text, "TLS:") {
		t.Fatalf("expected TLS-mode prompt, got %s", info.Text)
	}

	// "trust" → encrypt on, trust the self-signed cert.
	s.Process("trust")
	if s.Config.Source.Encrypt == nil || !*s.Config.Source.Encrypt || !s.Config.Source.TrustServerCert {
		t.Fatalf("trust: want encrypt=true trust=true, got encrypt=%v trust=%v", s.Config.Source.Encrypt, s.Config.Source.TrustServerCert)
	}
}

func TestMSSQLTLSModes(t *testing.T) {
	cases := []struct {
		in        string
		wantEnc   *bool
		wantTrust bool
	}{
		{"disable", boolp(false), false},
		{"trust", boolp(true), true},
		{"require", boolp(true), false},
		{"y", boolp(true), true},        // legacy alias → trust
		{"n", boolp(true), false},       // legacy "don't trust cert" → require (still encrypted)
		{"", nil, false},                // empty → keep current
		{"  TRUST ", boolp(true), true}, // case + whitespace tolerant
		{"off", boolp(false), false},    // alias → disable
		{"verify", boolp(true), false},  // alias → require
		{"xyz", boolp(true), false},     // unknown input → safe default (require)
	}
	for _, c := range cases {
		enc, trust := parseMSSQLTLS(c.in)
		if (enc == nil) != (c.wantEnc == nil) || (enc != nil && *enc != *c.wantEnc) {
			t.Errorf("parseMSSQLTLS(%q) encrypt = %v, want %v", c.in, enc, c.wantEnc)
		}
		if trust != c.wantTrust {
			t.Errorf("parseMSSQLTLS(%q) trust = %v, want %v", c.in, trust, c.wantTrust)
		}
	}
}

func boolp(b bool) *bool { return &b }

// TestMSSQLSSLPromptDefault: the shown default must reflect the loaded config
// (EditMode), so pressing Enter — which keeps the current value — matches it.
func TestMSSQLSSLPromptDefault(t *testing.T) {
	s := NewState()
	cases := []struct {
		trust   bool
		encrypt *bool
		want    string
	}{
		{false, nil, "require"},          // fresh config
		{true, nil, "trust"},             // trust the cert
		{false, boolp(false), "disable"}, // loaded encrypt:false
		{true, boolp(true), "trust"},     // encrypt on + trust
	}
	for _, c := range cases {
		if got := s.sslPrompt("mssql", "", c.trust, c.encrypt).Default; got != c.want {
			t.Errorf("sslPrompt(trust=%v encrypt=%v) default = %q, want %q", c.trust, c.encrypt, got, c.want)
		}
	}
}

func TestPostgresSSLDefaults(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("db")
	s.Process("user")
	s.Process("pass")
	s.Process("public")

	// Accept default SSL mode
	s.Process("")
	if s.Config.Source.SSLMode != "prefer" {
		t.Fatalf("expected prefer default ssl, got %s", s.Config.Source.SSLMode)
	}
}

func TestSlackWebhookBlankSkipsSave(t *testing.T) {
	s := NewState()
	s.CurrentStep = StepSlackWebhook
	// Blank input means "keep existing" — should bypass StepWriteSlackSecret
	// entirely and proceed to source. No I/O attempted.
	s.Process("")
	if s.CurrentStep != StepSourceType {
		t.Fatalf("blank Slack input should skip directly to StepSourceType, got %d", s.CurrentStep)
	}
}

func TestSlackWebhookDashClears(t *testing.T) {
	s := NewState()
	s.SlackWebhook = "https://hooks.example.invalid/services/T/B/X"
	s.CurrentStep = StepSlackWebhook
	s.Process("-")
	if s.SlackWebhook != "" {
		t.Fatalf("'-' should clear SlackWebhook, got %q", s.SlackWebhook)
	}
	if s.CurrentStep != StepWriteSlackSecret {
		t.Fatalf("'-' should route through StepWriteSlackSecret to persist clear, got %d", s.CurrentStep)
	}
}

func TestSlackWebhookURLSets(t *testing.T) {
	s := NewState()
	s.CurrentStep = StepSlackWebhook
	s.Process("https://hooks.example.invalid/services/T/B/X")
	if s.SlackWebhook != "https://hooks.example.invalid/services/T/B/X" {
		t.Fatalf("URL should be stored in SlackWebhook, got %q", s.SlackWebhook)
	}
	if s.CurrentStep != StepWriteSlackSecret {
		t.Fatalf("URL should route through StepWriteSlackSecret, got %d", s.CurrentStep)
	}
}

func TestSlackWebhookPromptDoesNotLeakSecret(t *testing.T) {
	// Regression: webhook URLs are credentials (logging/scrub.go redacts
	// them). The prompt must NOT put the raw URL in Default — that would
	// echo in the CLI "[default]" rendering and TUI scrollback. The
	// prompt must also be marked masked so the TUI's input-echo path
	// (handleSetupStep) shows ****** instead of the pasted URL.
	s := NewState()
	secret := "https://hooks.example.invalid/services/T01ABCDEFGH/B01ZZZZZZZZ/XXXXXXXXXXXXXXXXXXXXXXXX"
	s.SlackWebhook = secret
	s.SlackWebhookOriginal = secret
	s.CurrentStep = StepSlackWebhook

	info := s.Prompt()
	if info.Default != "" {
		t.Fatalf("Default must be empty to avoid leaking webhook URL, got %q", info.Default)
	}
	if strings.Contains(info.Text, secret) {
		t.Fatalf("prompt Text must not echo the webhook URL, got %q", info.Text)
	}
	if !info.IsMasked {
		t.Fatal("IsMasked must be true so TUI does not echo pasted URL into scrollback")
	}
	// Sanity: the prompt should at least indicate that something is configured
	// so the user knows Enter means "keep current" rather than "skip".
	if !strings.Contains(strings.ToLower(info.Text), "currently") &&
		!strings.Contains(strings.ToLower(info.Text), "keep") {
		t.Fatalf("prompt Text should hint that a value is configured, got %q", info.Text)
	}
}

func TestSlackPromptHintUsesOriginalNotStaged(t *testing.T) {
	// The "currently configured" hint must reflect what's on disk, not
	// what's been staged in memory. Otherwise a failed write would leave
	// an unsaved URL in SlackWebhook and the prompt would incorrectly
	// claim it's configured.
	s := NewState()
	s.SlackWebhook = "https://hooks.example.invalid/services/T/B/staged"
	s.SlackWebhookOriginal = "" // nothing on disk
	s.CurrentStep = StepSlackWebhook

	info := s.Prompt()
	if strings.Contains(strings.ToLower(info.Text), "currently") {
		t.Fatalf("prompt should NOT claim 'currently configured' when only staged in memory, got %q", info.Text)
	}
}

func TestSlackWriteFailureRevertsStaged(t *testing.T) {
	// On write failure the in-memory staged URL must revert to the
	// loaded value so the next prompt accurately reflects disk state
	// and the user has to re-paste to retry (rather than silently
	// skipping the save by pressing Enter).
	s := NewState()
	s.SlackWebhookOriginal = "https://hooks.example.invalid/services/T/B/original"
	s.SlackWebhook = "https://hooks.example.invalid/services/T/B/staged" // user just pasted this
	s.CurrentStep = StepWriteSlackSecret

	errMsg := s.Process("permission denied")
	if errMsg == "" {
		t.Fatal("expected error message on write failure")
	}
	if s.SlackWebhook != s.SlackWebhookOriginal {
		t.Fatalf("write failure should revert SlackWebhook to Original, got %q want %q",
			s.SlackWebhook, s.SlackWebhookOriginal)
	}
	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("expected to return to StepSlackWebhook, got %d", s.CurrentStep)
	}
}

func TestSlackWriteSuccessUpdatesOriginal(t *testing.T) {
	// On successful write, Original should advance to match SlackWebhook
	// so a subsequent Enter-to-keep doesn't redundantly re-trigger the
	// write.
	s := NewState()
	s.SlackWebhookOriginal = "https://hooks.example.invalid/services/T/B/old"
	s.SlackWebhook = "https://hooks.example.invalid/services/T/B/new"
	s.CurrentStep = StepWriteSlackSecret

	s.Process("") // success
	if s.SlackWebhookOriginal != s.SlackWebhook {
		t.Fatalf("Original should match SlackWebhook after successful write, got original=%q webhook=%q",
			s.SlackWebhookOriginal, s.SlackWebhook)
	}
}

func TestSlackWebhookBlankInEditModePreservesSecret(t *testing.T) {
	// Companion to the redaction test: even though Default is empty,
	// hitting Enter must preserve s.SlackWebhook rather than wiping it.
	s := NewState()
	secret := "https://hooks.example.invalid/services/T/B/X"
	s.SlackWebhook = secret
	s.CurrentStep = StepSlackWebhook
	s.Process("")
	if s.SlackWebhook != secret {
		t.Fatalf("blank input should preserve SlackWebhook, got %q", s.SlackWebhook)
	}
	if s.CurrentStep != StepSourceType {
		t.Fatalf("blank should skip directly to StepSourceType (no rewrite), got %d", s.CurrentStep)
	}
}

func TestSlackWriteFailureGoesBackToPrompt(t *testing.T) {
	s := NewState()
	s.CurrentStep = StepWriteSlackSecret
	errMsg := s.Process("permission denied")
	if errMsg == "" {
		t.Fatal("expected error message on write failure")
	}
	if s.CurrentStep != StepSlackWebhook {
		t.Fatalf("expected to return to StepSlackWebhook on failure, got %d", s.CurrentStep)
	}
}

func TestWriteSecretsFailureGoesBackToProvider(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("y")
	s.Process("anthropic")
	s.Process("sk-test-key")

	if s.CurrentStep != StepWriteSecrets {
		t.Fatalf("expected StepWriteSecrets, got %d", s.CurrentStep)
	}

	// Simulate write failure
	errMsg := s.Process("permission denied")
	if errMsg == "" {
		t.Fatal("expected error message on write failure")
	}
	if s.CurrentStep != StepAIProvider {
		t.Fatalf("expected StepAIProvider after write failure, got %d", s.CurrentStep)
	}
}

func TestWriteConfigFailureGoesBackToConfigPath(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")
	s.Process("") // skip Slack

	// Fill source
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("testdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // conn test success

	// Fill target
	s.Process("postgres")
	s.Process("localhost")
	s.Process("5432")
	s.Process("targetdb")
	s.Process("user")
	s.Process("pass")
	s.Process("public")
	s.Process("disable")
	s.Process("") // conn test success

	// Migration settings
	s.Process("drop_recreate")
	s.Process("y")
	s.Process("y")
	s.Process("4")
	s.Process("config.yaml") // config path

	if s.CurrentStep != StepWriteConfig {
		t.Fatalf("expected StepWriteConfig, got %d", s.CurrentStep)
	}

	// Simulate write failure
	errMsg := s.Process("read-only filesystem")
	if errMsg == "" {
		t.Fatal("expected error message on write failure")
	}
	if s.CurrentStep != StepConfigPath {
		t.Fatalf("expected StepConfigPath after write failure, got %d", s.CurrentStep)
	}
}

func TestConfigPath(t *testing.T) {
	s := NewState()
	if s.ConfigPath != "config.yaml" {
		t.Fatalf("expected default config.yaml, got %s", s.ConfigPath)
	}

	s.ConfigPath = "custom.yaml"
	if s.ConfigPath != "custom.yaml" {
		t.Fatal("expected custom.yaml")
	}
}
