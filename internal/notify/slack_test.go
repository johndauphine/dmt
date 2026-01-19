package notify

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/johndauphine/dmt/internal/secrets"
)

func TestNew(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		n := New(nil)
		if n == nil {
			t.Fatal("expected notifier, got nil")
		}
		if n.IsEnabled() {
			t.Error("expected notifier to be disabled with nil config")
		}
	})

	t.Run("valid config", func(t *testing.T) {
		cfg := &SlackConfig{
			Enabled:    true,
			WebhookURL: "https://hooks.slack.com/test",
			Channel:    "#test",
			Username:   "test-bot",
		}
		n := New(cfg)
		if n == nil {
			t.Fatal("expected notifier, got nil")
		}
		if !n.IsEnabled() {
			t.Error("expected notifier to be enabled")
		}
	})
}

func TestIsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   *SlackConfig
		expected bool
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: false,
		},
		{
			name:     "disabled explicitly",
			config:   &SlackConfig{Enabled: false, WebhookURL: "https://test"},
			expected: false,
		},
		{
			name:     "enabled but no webhook",
			config:   &SlackConfig{Enabled: true, WebhookURL: ""},
			expected: false,
		},
		{
			name:     "enabled with webhook",
			config:   &SlackConfig{Enabled: true, WebhookURL: "https://hooks.slack.com/test"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New(tt.config)
			if got := n.IsEnabled(); got != tt.expected {
				t.Errorf("IsEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMigrationStarted(t *testing.T) {
	t.Run("disabled notifier returns nil", func(t *testing.T) {
		n := New(nil)
		err := n.MigrationStarted("run-123", "source-db", "target-db", 10)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("sends correct payload", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{
			Enabled:    true,
			WebhookURL: server.URL,
			Channel:    "#migrations",
			Username:   "migrate-bot",
		}
		n := New(cfg)

		err := n.MigrationStarted("run-123", "source-db", "target-db", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if receivedMsg.Channel != "#migrations" {
			t.Errorf("channel = %q, want %q", receivedMsg.Channel, "#migrations")
		}
		if receivedMsg.Username != "migrate-bot" {
			t.Errorf("username = %q, want %q", receivedMsg.Username, "migrate-bot")
		}
		if len(receivedMsg.Attachments) != 1 {
			t.Fatalf("expected 1 attachment, got %d", len(receivedMsg.Attachments))
		}
		if receivedMsg.Attachments[0].Title != "Migration Started" {
			t.Errorf("title = %q, want %q", receivedMsg.Attachments[0].Title, "Migration Started")
		}
	})
}

func TestMigrationCompleted(t *testing.T) {
	t.Run("disabled notifier returns nil", func(t *testing.T) {
		n := New(nil)
		err := n.MigrationCompleted("run-123", time.Now(), 5*time.Minute, 10, 1000000, 50000)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("sends correct payload", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{
			Enabled:    true,
			WebhookURL: server.URL,
		}
		n := New(cfg)

		startTime := time.Date(2026, 1, 12, 10, 0, 0, 0, time.UTC)
		err := n.MigrationCompleted("run-456", startTime, 5*time.Minute, 10, 1000000, 50000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if receivedMsg.IconEmoji != ":white_check_mark:" {
			t.Errorf("icon = %q, want %q", receivedMsg.IconEmoji, ":white_check_mark:")
		}
		if len(receivedMsg.Attachments) != 1 {
			t.Fatalf("expected 1 attachment, got %d", len(receivedMsg.Attachments))
		}
		if receivedMsg.Attachments[0].Color != "#36a64f" {
			t.Errorf("color = %q, want green (#36a64f)", receivedMsg.Attachments[0].Color)
		}
	})
}

func TestMigrationFailed(t *testing.T) {
	t.Run("disabled notifier returns nil", func(t *testing.T) {
		n := New(nil)
		err := n.MigrationFailed("run-123", errors.New("test error"), 5*time.Minute)
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("nil error handled", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.MigrationFailed("run-123", nil, 5*time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check that "Unknown error" is used for nil error
		found := false
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Error" && field.Value == "Unknown error" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected 'Unknown error' field for nil error")
		}
	})

	t.Run("long error truncated", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		// Create an error message longer than 500 characters
		longError := make([]byte, 600)
		for i := range longError {
			longError[i] = 'a'
		}
		err := n.MigrationFailed("run-123", errors.New(string(longError)), 5*time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check that error was truncated
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Error" {
				if len(field.Value) > 510 { // 500 + "..."
					t.Errorf("error message not truncated: len=%d", len(field.Value))
				}
				if field.Value[len(field.Value)-3:] != "..." {
					t.Error("truncated error should end with '...'")
				}
				break
			}
		}
	})

	t.Run("sends correct payload", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.MigrationFailed("run-789", errors.New("connection timeout"), 2*time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if receivedMsg.IconEmoji != ":x:" {
			t.Errorf("icon = %q, want %q", receivedMsg.IconEmoji, ":x:")
		}
		if receivedMsg.Attachments[0].Color != "#dc3545" {
			t.Errorf("color = %q, want red (#dc3545)", receivedMsg.Attachments[0].Color)
		}
		if receivedMsg.Attachments[0].Title != "Migration Failed" {
			t.Errorf("title = %q, want %q", receivedMsg.Attachments[0].Title, "Migration Failed")
		}
	})
}

func TestMigrationCompletedWithErrors(t *testing.T) {
	t.Run("disabled notifier returns nil", func(t *testing.T) {
		n := New(nil)
		err := n.MigrationCompletedWithErrors("run-123", time.Now(), 5*time.Minute, 8, 2, 1000000, 50000, []string{"table1", "table2"})
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("few failures listed", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.MigrationCompletedWithErrors("run-123", time.Now(), 5*time.Minute, 8, 2, 1000000, 50000, []string{"users", "posts"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check that failures are listed
		found := false
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Failed Tables" {
				if field.Value != "Failed tables: users, posts" {
					t.Errorf("unexpected failure summary: %q", field.Value)
				}
				found = true
				break
			}
		}
		if !found {
			t.Error("expected 'Failed Tables' field")
		}
	})

	t.Run("many failures truncated", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		failures := []string{"table1", "table2", "table3", "table4", "table5", "table6", "table7"}
		err := n.MigrationCompletedWithErrors("run-123", time.Now(), 5*time.Minute, 3, 7, 500000, 25000, failures)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check that "and X more" is shown
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Failed Tables" {
				expected := "Failed tables: table1, table2, table3... and 4 more"
				if field.Value != expected {
					t.Errorf("failure summary = %q, want %q", field.Value, expected)
				}
				break
			}
		}
	})

	t.Run("sends correct payload", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.MigrationCompletedWithErrors("run-123", time.Now(), 5*time.Minute, 8, 2, 1000000, 50000, []string{"table1"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if receivedMsg.IconEmoji != ":warning:" {
			t.Errorf("icon = %q, want %q", receivedMsg.IconEmoji, ":warning:")
		}
		if receivedMsg.Attachments[0].Color != "#ffc107" {
			t.Errorf("color = %q, want yellow (#ffc107)", receivedMsg.Attachments[0].Color)
		}
	})
}

func TestTableTransferFailed(t *testing.T) {
	t.Run("disabled notifier returns nil", func(t *testing.T) {
		n := New(nil)
		err := n.TableTransferFailed("run-123", "users", errors.New("test"))
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("nil error handled", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.TableTransferFailed("run-123", "users", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		found := false
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Error" && field.Value == "Unknown error" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected 'Unknown error' for nil error")
		}
	})

	t.Run("sends correct payload", func(t *testing.T) {
		var receivedMsg SlackMessage
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedMsg)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.TableTransferFailed("run-123", "orders", errors.New("duplicate key"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if receivedMsg.Attachments[0].Title != "Table Transfer Failed" {
			t.Errorf("title = %q, want %q", receivedMsg.Attachments[0].Title, "Table Transfer Failed")
		}

		// Check table name is in fields
		found := false
		for _, field := range receivedMsg.Attachments[0].Fields {
			if field.Title == "Table" && field.Value == "orders" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected table name in fields")
		}
	})
}

func TestSend(t *testing.T) {
	t.Run("HTTP error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		cfg := &SlackConfig{Enabled: true, WebhookURL: server.URL}
		n := New(cfg)

		err := n.MigrationStarted("run-123", "src", "tgt", 5)
		if err == nil {
			t.Error("expected error for non-200 response")
		}
	})

	t.Run("connection error", func(t *testing.T) {
		cfg := &SlackConfig{Enabled: true, WebhookURL: "http://localhost:99999"}
		n := New(cfg)

		err := n.MigrationStarted("run-123", "src", "tgt", 5)
		if err == nil {
			t.Error("expected error for connection failure")
		}
	})
}

func TestGetUsername(t *testing.T) {
	t.Run("custom username", func(t *testing.T) {
		cfg := &SlackConfig{Username: "custom-bot"}
		n := New(cfg)
		if got := n.getUsername(); got != "custom-bot" {
			t.Errorf("getUsername() = %q, want %q", got, "custom-bot")
		}
	})

	t.Run("default username", func(t *testing.T) {
		cfg := &SlackConfig{}
		n := New(cfg)
		if got := n.getUsername(); got != "dmt" {
			t.Errorf("getUsername() = %q, want %q", got, "dmt")
		}
	})
}

func TestNewFromSecrets(t *testing.T) {
	t.Run("missing secrets file returns disabled notifier", func(t *testing.T) {
		secrets.Reset() // Clear cached secrets
		// Point to non-existent secrets file
		t.Setenv("DMT_SECRETS_FILE", "/nonexistent/path/to/secrets.yaml")

		n := NewFromSecrets()
		if n == nil {
			t.Fatal("expected notifier, got nil")
		}
		if n.IsEnabled() {
			t.Error("expected notifier to be disabled when secrets file missing")
		}
	})

	t.Run("secrets without webhook returns disabled notifier", func(t *testing.T) {
		secrets.Reset() // Clear cached secrets
		// Create temp secrets file without webhook
		tmpDir := t.TempDir()
		secretsPath := tmpDir + "/secrets.yaml"
		secretsContent := `
ai:
  default_provider: claude
  providers:
    claude:
      api_key: "test-key"
notifications:
  slack:
    webhook_url: ""
`
		if err := writeTestFile(t, secretsPath, secretsContent); err != nil {
			t.Fatalf("failed to write secrets file: %v", err)
		}
		t.Setenv("DMT_SECRETS_FILE", secretsPath)

		n := NewFromSecrets()
		if n == nil {
			t.Fatal("expected notifier, got nil")
		}
		if n.IsEnabled() {
			t.Error("expected notifier to be disabled when webhook URL empty")
		}
	})

	t.Run("secrets with webhook returns enabled notifier", func(t *testing.T) {
		secrets.Reset() // Clear cached secrets
		// Create temp secrets file with webhook
		tmpDir := t.TempDir()
		secretsPath := tmpDir + "/secrets.yaml"
		secretsContent := `
ai:
  default_provider: claude
  providers:
    claude:
      api_key: "test-key"
notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/TEST"
`
		if err := writeTestFile(t, secretsPath, secretsContent); err != nil {
			t.Fatalf("failed to write secrets file: %v", err)
		}
		t.Setenv("DMT_SECRETS_FILE", secretsPath)

		n := NewFromSecrets()
		if n == nil {
			t.Fatal("expected notifier, got nil")
		}
		if !n.IsEnabled() {
			t.Error("expected notifier to be enabled when webhook URL configured")
		}
	})
}

func writeTestFile(t *testing.T, path, content string) error {
	t.Helper()
	return os.WriteFile(path, []byte(content), 0600)
}

func TestFormatNumberWithCommas(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{12, "12"},
		{123, "123"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{1000000000, "1,000,000,000"},
		{-1234, "-1,234"},
		{-1234567, "-1,234,567"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := formatNumberWithCommas(tt.input)
			if got != tt.expected {
				t.Errorf("formatNumberWithCommas(%d) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{0, "0s"},
		{1 * time.Second, "1s"},
		{30 * time.Second, "30s"},
		{59 * time.Second, "59s"},
		{60 * time.Second, "1m 0s"},
		{61 * time.Second, "1m 1s"},
		{5*time.Minute + 30*time.Second, "5m 30s"},
		{59*time.Minute + 59*time.Second, "59m 59s"},
		{60 * time.Minute, "1h 0m 0s"},
		{1*time.Hour + 30*time.Minute + 45*time.Second, "1h 30m 45s"},
		{25*time.Hour + 5*time.Minute + 10*time.Second, "25h 5m 10s"},
		// Test rounding
		{1*time.Second + 500*time.Millisecond, "2s"},
		{1*time.Second + 499*time.Millisecond, "1s"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := formatDuration(tt.input)
			if got != tt.expected {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
