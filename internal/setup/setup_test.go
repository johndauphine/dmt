package setup

import (
	"testing"
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

	// Should jump to source type
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

	// No AI configured, so should skip to done
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
	if s.Config.Migration.CreateIndexes != true {
		t.Fatal("expected CreateIndexes true")
	}
}

func TestSecretsExistWithValidAI(t *testing.T) {
	s := NewState()
	s.Process("has_ai")

	if s.CurrentStep != StepSourceType {
		t.Fatalf("expected StepSourceType, got %d", s.CurrentStep)
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

	if s.CurrentStep != StepSourceType {
		t.Fatalf("expected StepSourceType, got %d", s.CurrentStep)
	}
	if s.AIConfigured {
		t.Fatal("expected AIConfigured to be false")
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

func TestNoAnalysisWhenAINotConfigured(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n") // no AI

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

	// No AI -> straight to done, skip analysis
	if s.CurrentStep != StepDone {
		t.Fatalf("expected StepDone (no analysis), got %d", s.CurrentStep)
	}
}

func TestNoAnalysisWhenSourceConnFailed(t *testing.T) {
	s := NewState()
	s.Process("has_ai")

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

func TestMSSQLSSLDefaults(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")

	// MSSQL source
	s.Process("mssql")
	s.Process("localhost")
	s.Process("1433")
	s.Process("db")
	s.Process("sa")
	s.Process("pass")
	s.Process("dbo")

	// SSL prompt for MSSQL should ask about TrustServerCert
	info := s.Prompt()
	if info.Text != "Trust server certificate? (y/n)" {
		t.Fatalf("expected trust cert prompt, got %s", info.Text)
	}

	s.Process("y")
	if !s.Config.Source.TrustServerCert {
		t.Fatal("expected TrustServerCert true")
	}
}

func TestPostgresSSLDefaults(t *testing.T) {
	s := NewState()
	s.Process("no_ai")
	s.Process("n")

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
