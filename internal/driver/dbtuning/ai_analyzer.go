package dbtuning

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

// AIQuerier represents anything that can query AI.
type AIQuerier interface {
	Query(ctx context.Context, prompt string) (string, error)
}

// AITuningAnalyzer uses AI to analyze database configuration and generate recommendations.
type AITuningAnalyzer struct {
	aiMapper AIQuerier
}

// NewAITuningAnalyzer creates a new AI-driven tuning analyzer.
func NewAITuningAnalyzer(aiMapper AIQuerier) *AITuningAnalyzer {
	return &AITuningAnalyzer{
		aiMapper: aiMapper,
	}
}

// AnalyzeDatabase analyzes database configuration and generates tuning recommendations.
func (a *AITuningAnalyzer) AnalyzeDatabase(
	ctx context.Context,
	db *sql.DB,
	dbType string,
	role string,
	stats SchemaStatistics,
) (*DatabaseTuning, error) {
	if a.aiMapper == nil {
		return &DatabaseTuning{
			DatabaseType:    dbType,
			Role:            role,
			TuningPotential: "unknown",
			EstimatedImpact: "AI not configured - set ai.api_key to enable database tuning recommendations",
		}, nil
	}

	// Step 1: Use AI to generate SQL queries to interrogate database configuration
	logging.Debug("Step 1: Requesting AI to generate interrogation SQL for %s %s database", role, dbType)
	sqlQueries, err := a.generateInterrogationSQL(ctx, dbType, role)
	if err != nil {
		return nil, fmt.Errorf("generating interrogation SQL: %w", err)
	}
	logging.Debug("AI generated %d SQL queries", len(sqlQueries))

	// Step 2: Execute the AI-generated SQL queries
	logging.Debug("Step 2: Executing AI-generated SQL queries")
	configData, err := a.executeConfigQueries(ctx, db, sqlQueries)
	if err != nil {
		return nil, fmt.Errorf("executing config queries: %w", err)
	}
	logging.Debug("Collected configuration data from %d queries", len(configData))

	// Step 3: Use AI to analyze configuration and generate recommendations
	logging.Debug("Step 3: Requesting AI to analyze configuration and generate recommendations")
	recommendations, potential, impact, err := a.generateRecommendations(ctx, dbType, role, stats, configData)
	if err != nil {
		return nil, fmt.Errorf("generating recommendations: %w", err)
	}
	logging.Debug("AI generated %d recommendations (potential: %s)", len(recommendations), potential)

	return &DatabaseTuning{
		DatabaseType:    dbType,
		Role:            role,
		CurrentSettings: configData,
		Recommendations: recommendations,
		TuningPotential: potential,
		EstimatedImpact: impact,
	}, nil
}

// generateInterrogationSQL uses AI to generate SQL queries for database configuration.
func (a *AITuningAnalyzer) generateInterrogationSQL(ctx context.Context, dbType string, role string) ([]string, error) {
	prompt := fmt.Sprintf(`Generate SQL queries to retrieve database configuration settings for %s optimization.

Database Type: %s
Role: %s (source or target in a database migration tool)

Requirements:
1. Generate SQL queries that retrieve ALL relevant configuration parameters
2. For MySQL: Use SHOW VARIABLES, SHOW STATUS, SHOW GLOBAL VARIABLES
3. For PostgreSQL: Query pg_settings, pg_stat_database
4. For MSSQL: Use sp_configure, sys.dm_os_sys_info DMVs
5. For Oracle: Query V$PARAMETER, V$SYSTEM_PARAMETER

Focus on settings relevant for %s database in migration workloads:
- Buffer pools and caches
- I/O settings
- Connection limits
- Write/flush behavior
- Parallel processing
- Memory settings

IMPORTANT: Return ONLY a JSON array of SQL query strings, no other text.

Output format:
["SQL query 1", "SQL query 2", ...]

Example:
["SELECT name, setting FROM pg_settings WHERE category = 'Resource Usage'", "SELECT * FROM pg_stat_database"]`, role, dbType, role, role)

	logging.Debug("Sending AI prompt for SQL generation (%d bytes)", len(prompt))
	response, err := a.aiMapper.Query(ctx, prompt)
	if err != nil {
		logging.Debug("AI query failed: %v", err)
		return nil, fmt.Errorf("AI query failed: %w", err)
	}
	logging.Debug("AI response received for SQL generation (%d bytes)", len(response))

	// Strip markdown code fences if present
	cleanResponse := response
	if strings.Contains(response, "```json") {
		cleanResponse = strings.ReplaceAll(response, "```json", "")
		cleanResponse = strings.ReplaceAll(cleanResponse, "```", "")
	}
	cleanResponse = strings.TrimSpace(cleanResponse)

	// Parse JSON array of SQL queries
	var queries []string

	// Try to extract JSON from response
	jsonStart := strings.Index(cleanResponse, "[")
	jsonEnd := strings.LastIndex(cleanResponse, "]")
	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonStr := cleanResponse[jsonStart : jsonEnd+1]
		if err := json.Unmarshal([]byte(jsonStr), &queries); err != nil {
			logging.Debug("Failed to parse JSON array, length: %d", len(jsonStr))
			return nil, fmt.Errorf("parsing AI response: %w", err)
		}
	} else {
		// Fallback: Try to extract SQL queries from plain text (one per line)
		logging.Debug("No JSON array found, trying plain text extraction")
		lines := strings.Split(cleanResponse, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			// Skip empty lines and comments
			if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
				continue
			}
			// Look for SQL-like statements
			upperLine := strings.ToUpper(line)
			if strings.HasPrefix(upperLine, "SELECT") ||
			   strings.HasPrefix(upperLine, "SHOW") ||
			   strings.HasPrefix(upperLine, "EXEC") {
				queries = append(queries, line)
			}
		}

		if len(queries) == 0 {
			logging.Debug("No SQL queries found in response, length: %d", len(cleanResponse))
			return nil, fmt.Errorf("AI response did not contain SQL queries")
		}
		logging.Debug("Extracted %d SQL queries from plain text", len(queries))
	}

	logging.Debug("Parsed %d SQL queries from AI response:", len(queries))
	for i, q := range queries {
		logging.Debug("  Query %d: %s", i+1, q)
	}

	return queries, nil
}

// executeConfigQueries executes SQL queries and collects configuration data.
// Limits each query to maxRowsPerQuery to prevent token overflow.
func (a *AITuningAnalyzer) executeConfigQueries(ctx context.Context, db *sql.DB, queries []string) (map[string]interface{}, error) {
	const maxRowsPerQuery = 15 // Balance between config detail and prompt size
	config := make(map[string]interface{})

	for i, query := range queries {
		queryKey := fmt.Sprintf("query_%d", i+1)

		logging.Debug("Executing query %d: %s", i+1, query)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			// Log error but continue with other queries
			logging.Debug("Query %d failed: %v", i+1, err)
			config[queryKey+"_error"] = err.Error()
			continue
		}
		defer rows.Close() // Ensure resources are always released

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			continue
		}

		// Collect rows up to limit
		var results []map[string]interface{}
		rowCount := 0
		truncated := false

		for rows.Next() {
			rowCount++

			// Stop collecting after limit, but continue counting
			if rowCount > maxRowsPerQuery {
				truncated = true
				continue
			}

			// Create a slice of interface{} to hold each column value
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			// Convert to map
			row := make(map[string]interface{})
			for i, col := range columns {
				val := values[i]
				// Convert []byte to string
				if b, ok := val.([]byte); ok {
					row[col] = string(b)
				} else {
					row[col] = val
				}
			}
			results = append(results, row)
		}

		// Store results with metadata about truncation
		queryResult := map[string]interface{}{
			"rows":      results,
			"row_count": len(results),
		}
		if truncated {
			queryResult["truncated"] = true
			queryResult["total_rows"] = rowCount
			logging.Debug("Query %d returned %d rows (truncated from %d total)", i+1, len(results), rowCount)
		} else {
			queryResult["truncated"] = false
			logging.Debug("Query %d returned %d rows", i+1, len(results))
		}

		config[queryKey] = queryResult
	}

	logging.Debug("Configuration data collection complete (%d successful queries)", len(config))
	return config, nil
}

// generateRecommendations uses AI to analyze configuration and generate recommendations.
func (a *AITuningAnalyzer) generateRecommendations(
	ctx context.Context,
	dbType string,
	role string,
	stats SchemaStatistics,
	configData map[string]interface{},
) ([]TuningRecommendation, string, string, error) {
	// Serialize config data to JSON for AI
	configJSON, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return nil, "", "", fmt.Errorf("marshaling config: %w", err)
	}

	prompt := fmt.Sprintf(`Analyze database configuration and provide tuning recommendations for migration workload.

Database Type: %s
Role: %s database
Schema Statistics:
- Total Tables: %d
- Total Rows: %d
- Average Row Size: %d bytes
- Estimated Memory: %d MB

Current Configuration:
%s

Note: Each query result includes metadata:
- "rows": array of configuration data (limited to first 15 rows for token efficiency)
- "row_count": number of rows returned
- "truncated": true if more rows exist
- "total_rows": total rows available (when truncated)
Large result sets are sampled; focus on critical parameters only.

Task: Analyze this configuration for a %s database in a migration tool (dmt).

Migration Workload Characteristics:
- Source databases perform sequential full table scans for data extraction
- Target databases perform high-volume bulk inserts for data loading
- This %s database role: optimize for its specific workload characteristics
- No user queries during migration
- Can tolerate 1-second data loss on crash
- Optimize for throughput over ACID guarantees

Generate tuning recommendations in the following JSON format (LIMIT TO TOP 5 RECOMMENDATIONS):
{
  "recommendations": [
    {
      "parameter": "parameter_name",
      "current_value": "current value",
      "recommended_value": "recommended value",
      "impact": "high|medium|low",
      "reason": "concise 1-2 sentence explanation",
      "priority": 1|2|3,
      "can_apply_runtime": true|false,
      "sql_command": "SET GLOBAL param = value;" or "",
      "requires_restart": true|false,
      "config_file": "config snippet" or ""
    }
  ],
  "tuning_potential": "high|medium|low|none",
  "estimated_impact": "brief description"
}

IMPORTANT:
- Limit to TOP 5 most impactful recommendations only
- Keep "reason" field to 1-2 sentences max
- Keep "estimated_impact" brief (under 50 words)
- Focus on: buffer pools, write optimization, I/O capacity, connections, checkpoints

Return ONLY the JSON object, no other text.`, dbType, role, stats.TotalTables, stats.TotalRows, stats.AvgRowSizeBytes, stats.EstimatedMemMB, string(configJSON), role, role, role)

	logging.Debug("Sending AI prompt for recommendations (%d bytes)", len(prompt))
	response, err := a.aiMapper.Query(ctx, prompt)
	if err != nil {
		logging.Debug("AI query failed: %v", err)
		return nil, "", "", fmt.Errorf("AI query failed: %w", err)
	}
	logging.Debug("AI response received for recommendations (%d bytes)", len(response))

	// Parse JSON response
	type AIRecommendation struct {
		Parameter        string      `json:"parameter"`
		CurrentValue     interface{} `json:"current_value"`
		RecommendedValue interface{} `json:"recommended_value"`
		Impact           string      `json:"impact"`
		Reason           string      `json:"reason"`
		Priority         int         `json:"priority"`
		CanApplyRuntime  bool        `json:"can_apply_runtime"`
		SQLCommand       string      `json:"sql_command"`
		RequiresRestart  bool        `json:"requires_restart"`
		ConfigFile       string      `json:"config_file"`
	}

	type AIResponse struct {
		Recommendations  []AIRecommendation `json:"recommendations"`
		TuningPotential  string             `json:"tuning_potential"`
		EstimatedImpact  string             `json:"estimated_impact"`
	}

	// Strip markdown code fences if present
	cleanResponse := response
	if strings.Contains(response, "```json") {
		cleanResponse = strings.ReplaceAll(response, "```json", "")
		cleanResponse = strings.ReplaceAll(cleanResponse, "```", "")
	}
	cleanResponse = strings.TrimSpace(cleanResponse)

	// Extract JSON from response
	jsonStart := strings.Index(cleanResponse, "{")
	jsonEnd := strings.LastIndex(cleanResponse, "}")
	if jsonStart < 0 || jsonEnd <= jsonStart {
		return nil, "", "", fmt.Errorf("AI response did not contain JSON object: %s", cleanResponse)
	}

	jsonStr := cleanResponse[jsonStart : jsonEnd+1]

	// Try to parse JSON - if it fails due to truncation, try to fix it
	var aiResp AIResponse
	err = json.Unmarshal([]byte(jsonStr), &aiResp)
	if err != nil {
		// If parsing failed, it might be truncated - try to close the JSON properly
		logging.Debug("JSON parse failed, attempting to fix truncated response: %v", err)

		// Try adding missing closing brackets
		fixedJSON := jsonStr
		if !strings.HasSuffix(strings.TrimSpace(fixedJSON), "}") {
			// Add closing braces for: current object, recommendations array, root object
			fixedJSON = strings.TrimRight(fixedJSON, "\n\t ") + "]}}"
		}

		err = json.Unmarshal([]byte(fixedJSON), &aiResp)
		if err != nil {
			// Log full JSON at debug level, but don't include it in user-facing error
			logging.Debug("Failed to parse truncated JSON. Original length: %d, Fixed length: %d", len(jsonStr), len(fixedJSON))
			return nil, "", "", fmt.Errorf("parsing AI response: %w (response may be truncated or malformed)", err)
		}
		logging.Debug("Successfully parsed truncated response with %d recommendations", len(aiResp.Recommendations))
	}

	// Convert AI recommendations to TuningRecommendation
	recommendations := make([]TuningRecommendation, len(aiResp.Recommendations))
	for i, rec := range aiResp.Recommendations {
		recommendations[i] = TuningRecommendation{
			Parameter:        rec.Parameter,
			CurrentValue:     rec.CurrentValue,
			RecommendedValue: rec.RecommendedValue,
			Impact:           rec.Impact,
			Reason:           rec.Reason,
			Priority:         rec.Priority,
			CanApplyRuntime:  rec.CanApplyRuntime,
			SQLCommand:       rec.SQLCommand,
			RequiresRestart:  rec.RequiresRestart,
			ConfigFile:       rec.ConfigFile,
		}
	}

	// Handle missing fields in truncated responses
	tuningPotential := aiResp.TuningPotential
	if tuningPotential == "" && len(recommendations) > 0 {
		// Infer potential from recommendation count and priorities
		highPriorityCount := 0
		for _, rec := range recommendations {
			if rec.Priority == 1 {
				highPriorityCount++
			}
		}
		if highPriorityCount >= 3 {
			tuningPotential = "high"
		} else if highPriorityCount >= 1 {
			tuningPotential = "medium"
		} else {
			tuningPotential = "low"
		}
	}

	estimatedImpact := aiResp.EstimatedImpact
	if estimatedImpact == "" && len(recommendations) > 0 {
		estimatedImpact = fmt.Sprintf("%d tuning recommendations available (response may be truncated)", len(recommendations))
	}

	return recommendations, tuningPotential, estimatedImpact, nil
}
