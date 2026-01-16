package dbtuning

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestValidateQuery tests SQL query validation for security
func TestValidateQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Safe queries
		{
			name:    "simple select",
			query:   "SELECT * FROM pg_settings",
			wantErr: false,
		},
		{
			name:    "select with where",
			query:   "SELECT name, setting FROM pg_settings WHERE category = 'Resource Usage'",
			wantErr: false,
		},
		{
			name:    "show variables",
			query:   "SHOW VARIABLES LIKE 'max_connections'",
			wantErr: false,
		},
		{
			name:    "exec sp_configure",
			query:   "EXEC sp_configure",
			wantErr: false,
		},
		{
			name:    "describe table",
			query:   "DESCRIBE information_schema.tables",
			wantErr: false,
		},
		{
			name:    "explain query",
			query:   "EXPLAIN SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "lowercase select",
			query:   "select * from pg_stat_database",
			wantErr: false,
		},

		// Dangerous queries
		{
			name:    "drop table",
			query:   "DROP TABLE users",
			wantErr: true,
		},
		{
			name:    "delete from",
			query:   "DELETE FROM users WHERE id = 1",
			wantErr: true,
		},
		{
			name:    "update set",
			query:   "UPDATE users SET name = 'hacker'",
			wantErr: true,
		},
		{
			name:    "insert into",
			query:   "INSERT INTO users (name) VALUES ('hacker')",
			wantErr: true,
		},
		{
			name:    "alter table",
			query:   "ALTER TABLE users ADD COLUMN hacked BOOLEAN",
			wantErr: true,
		},
		{
			name:    "truncate table",
			query:   "TRUNCATE TABLE users",
			wantErr: true,
		},
		{
			name:    "create table",
			query:   "CREATE TABLE hackers (id INT)",
			wantErr: true,
		},
		{
			name:    "grant permissions",
			query:   "GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'",
			wantErr: true,
		},
		{
			name:    "revoke permissions",
			query:   "REVOKE ALL PRIVILEGES ON *.* FROM 'user'@'%'",
			wantErr: true,
		},
		{
			name:    "rename table",
			query:   "RENAME TABLE users TO victims",
			wantErr: true,
		},
		{
			name:    "replace into",
			query:   "REPLACE INTO users (id, name) VALUES (1, 'hacker')",
			wantErr: true,
		},
		{
			name:    "invalid prefix",
			query:   "VACUUM ANALYZE users",
			wantErr: true,
		},
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			query:   "   ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQuery(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestExecuteConfigQueries tests query execution with validation
func TestExecuteConfigQueries(t *testing.T) {
	// Create in-memory SQLite database for testing
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE test_config (name TEXT, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO test_config (name, value) VALUES ('max_connections', '100'), ('buffer_size', '8MB')`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create mock AI mapper
	mockMapper := &mockAIQuerier{
		queryFunc: func(ctx context.Context, prompt string) (string, error) {
			return "test response", nil
		},
	}

	analyzer := &AITuningAnalyzer{
		aiMapper: mockMapper,
	}

	tests := []struct {
		name        string
		queries     []string
		wantErr     bool
		wantResults int // number of successful queries
	}{
		{
			name: "safe queries",
			queries: []string{
				"SELECT * FROM test_config",
				"SELECT name, value FROM test_config WHERE name = 'max_connections'",
			},
			wantErr:     false,
			wantResults: 2,
		},
		{
			name: "mixed safe and unsafe",
			queries: []string{
				"SELECT * FROM test_config",
				"DROP TABLE test_config", // should be blocked
			},
			wantErr:     false,
			wantResults: 1, // only first query succeeds
		},
		{
			name: "all unsafe queries",
			queries: []string{
				"DELETE FROM test_config",
				"UPDATE test_config SET value = 'hacked'",
			},
			wantErr:     false,
			wantResults: 0, // all queries blocked
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			config, err := analyzer.executeConfigQueries(ctx, db, tt.queries)

			if (err != nil) != tt.wantErr {
				t.Errorf("executeConfigQueries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Count successful queries (those with map value containing "rows" key)
			successCount := 0
			for _, value := range config {
				if m, ok := value.(map[string]interface{}); ok {
					if _, hasRows := m["rows"]; hasRows {
						successCount++
					}
				}
			}

			if successCount != tt.wantResults {
				t.Errorf("executeConfigQueries() got %d successful queries, want %d", successCount, tt.wantResults)
			}
		})
	}
}

// mockAIQuerier is a test implementation of AIQuerier
type mockAIQuerier struct {
	queryFunc func(ctx context.Context, prompt string) (string, error)
}

func (m *mockAIQuerier) Query(ctx context.Context, prompt string) (string, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, prompt)
	}
	return "", nil
}

// TestGenerateInterrogationSQL tests SQL query generation parsing
func TestGenerateInterrogationSQL(t *testing.T) {
	tests := []struct {
		name         string
		aiResponse   string
		wantQueries  int
		wantErr      bool
		expectedSQL  []string
	}{
		{
			name: "json array format",
			aiResponse: `["SELECT * FROM pg_settings", "SHOW VARIABLES"]`,
			wantQueries: 2,
			wantErr: false,
			expectedSQL: []string{"SELECT * FROM pg_settings", "SHOW VARIABLES"},
		},
		{
			name: "json array with markdown fences",
			aiResponse: "```json\n[\"SELECT name, setting FROM pg_settings\", \"SHOW STATUS\"]\n```",
			wantQueries: 2,
			wantErr: false,
			expectedSQL: []string{"SELECT name, setting FROM pg_settings", "SHOW STATUS"},
		},
		{
			name: "plain text fallback",
			aiResponse: `SELECT * FROM pg_settings
SHOW VARIABLES
SELECT @@version`,
			wantQueries: 3,
			wantErr: false,
			expectedSQL: []string{"SELECT * FROM pg_settings", "SHOW VARIABLES", "SELECT @@version"},
		},
		{
			name: "plain text with comments",
			aiResponse: `# PostgreSQL queries
SELECT * FROM pg_settings
# MySQL queries
SHOW VARIABLES`,
			wantQueries: 2,
			wantErr: false,
			expectedSQL: []string{"SELECT * FROM pg_settings", "SHOW VARIABLES"},
		},
		{
			name: "empty response",
			aiResponse: "",
			wantQueries: 0,
			wantErr: true,
		},
		{
			name: "no valid SQL",
			aiResponse: "This is just explanatory text with no SQL queries",
			wantQueries: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMapper := &mockAIQuerier{
				queryFunc: func(ctx context.Context, prompt string) (string, error) {
					return tt.aiResponse, nil
				},
			}

			analyzer := &AITuningAnalyzer{
				aiMapper: mockMapper,
			}

			ctx := context.Background()
			queries, err := analyzer.generateInterrogationSQL(ctx, "postgres", "source")

			if (err != nil) != tt.wantErr {
				t.Errorf("generateInterrogationSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(queries) != tt.wantQueries {
				t.Errorf("generateInterrogationSQL() got %d queries, want %d", len(queries), tt.wantQueries)
			}

			// Verify expected SQL queries if provided
			if len(tt.expectedSQL) > 0 {
				for i, expected := range tt.expectedSQL {
					if i >= len(queries) {
						t.Errorf("Missing expected query %d: %s", i, expected)
						continue
					}
					if queries[i] != expected {
						t.Errorf("Query %d = %q, want %q", i, queries[i], expected)
					}
				}
			}
		})
	}
}

// TestSchemaStatistics tests the SchemaStatistics struct
func TestSchemaStatistics(t *testing.T) {
	stats := SchemaStatistics{
		TotalTables:     10,
		TotalRows:       1000000,
		AvgRowSizeBytes: 256,
		EstimatedMemMB:  1024,
	}

	if stats.TotalTables != 10 {
		t.Errorf("TotalTables = %d, want 10", stats.TotalTables)
	}
	if stats.TotalRows != 1000000 {
		t.Errorf("TotalRows = %d, want 1000000", stats.TotalRows)
	}
	if stats.AvgRowSizeBytes != 256 {
		t.Errorf("AvgRowSizeBytes = %d, want 256", stats.AvgRowSizeBytes)
	}
	if stats.EstimatedMemMB != 1024 {
		t.Errorf("EstimatedMemMB = %d, want 1024", stats.EstimatedMemMB)
	}
}

// TestTuningRecommendation tests the TuningRecommendation struct
func TestTuningRecommendation(t *testing.T) {
	rec := TuningRecommendation{
		Parameter:        "max_connections",
		CurrentValue:     100,
		RecommendedValue: 200,
		Impact:           "high",
		Reason:           "Increase connection pool for better throughput",
		Priority:         1,
		CanApplyRuntime:  true,
		SQLCommand:       "SET GLOBAL max_connections = 200",
		RequiresRestart:  false,
		ConfigFile:       "max_connections = 200",
	}

	if rec.Parameter != "max_connections" {
		t.Errorf("Parameter = %s, want max_connections", rec.Parameter)
	}
	if rec.Priority != 1 {
		t.Errorf("Priority = %d, want 1", rec.Priority)
	}
	if !rec.CanApplyRuntime {
		t.Error("CanApplyRuntime should be true")
	}
	if rec.RequiresRestart {
		t.Error("RequiresRestart should be false")
	}
}
