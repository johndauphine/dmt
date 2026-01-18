package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/johndauphine/dmt/internal/logging"
)

// ErrorDiagnosis contains AI-generated analysis of a migration error.
type ErrorDiagnosis struct {
	Cause       string   `json:"cause"`
	Suggestions []string `json:"suggestions"`
	Confidence  string   `json:"confidence"` // high, medium, low
	Category    string   `json:"category"`   // type_mismatch, constraint, permission, connection, data_quality, other
}

// ErrorContext provides context about the error for AI diagnosis.
type ErrorContext struct {
	ErrorMessage string
	TableName    string
	TableSchema  string
	Columns      []Column
	SourceDBType string
	TargetDBType string
	TargetMode   string
}

// AIErrorDiagnoser uses AI to analyze migration errors and provide suggestions.
type AIErrorDiagnoser struct {
	aiMapper *AITypeMapper
	cache    map[string]*ErrorDiagnosis
	mu       sync.RWMutex
}

// NewAIErrorDiagnoser creates a new AI-powered error diagnoser.
func NewAIErrorDiagnoser(mapper *AITypeMapper) *AIErrorDiagnoser {
	return &AIErrorDiagnoser{
		aiMapper: mapper,
		cache:    make(map[string]*ErrorDiagnosis),
	}
}

// Diagnose analyzes an error and returns actionable suggestions.
func (d *AIErrorDiagnoser) Diagnose(ctx context.Context, errCtx *ErrorContext) (*ErrorDiagnosis, error) {
	if d.aiMapper == nil {
		return nil, fmt.Errorf("AI mapper not configured")
	}

	// Generate cache key from error message hash
	cacheKey := d.hashError(errCtx.ErrorMessage)

	// Check cache first
	d.mu.RLock()
	if cached, ok := d.cache[cacheKey]; ok {
		d.mu.RUnlock()
		logging.Debug("AI error diagnosis: cache hit for error hash %s", cacheKey[:8])
		return cached, nil
	}
	d.mu.RUnlock()

	// Build prompt
	prompt := d.buildPrompt(errCtx)

	logging.Debug("AI error diagnosis: analyzing error for table %s.%s", errCtx.TableSchema, errCtx.TableName)

	// Call AI
	response, err := d.aiMapper.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI diagnosis failed: %w", err)
	}

	// Parse response
	diagnosis, err := d.parseResponse(response)
	if err != nil {
		return nil, fmt.Errorf("parsing AI diagnosis: %w", err)
	}

	// Cache the result
	d.mu.Lock()
	d.cache[cacheKey] = diagnosis
	d.mu.Unlock()

	logging.Debug("AI error diagnosis: category=%s, confidence=%s", diagnosis.Category, diagnosis.Confidence)

	return diagnosis, nil
}

// hashError creates a short hash of the error message for caching.
func (d *AIErrorDiagnoser) hashError(errMsg string) string {
	hash := sha256.Sum256([]byte(errMsg))
	return hex.EncodeToString(hash[:16])
}

// buildPrompt constructs the AI prompt for error diagnosis.
func (d *AIErrorDiagnoser) buildPrompt(errCtx *ErrorContext) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration error analyst. Analyze this migration error and provide actionable suggestions.\n\n")

	sb.WriteString("=== ERROR ===\n")
	sb.WriteString(errCtx.ErrorMessage)
	sb.WriteString("\n\n")

	sb.WriteString("=== CONTEXT ===\n")
	sb.WriteString(fmt.Sprintf("Source DB: %s\n", errCtx.SourceDBType))
	sb.WriteString(fmt.Sprintf("Target DB: %s\n", errCtx.TargetDBType))
	sb.WriteString(fmt.Sprintf("Table: %s.%s\n", errCtx.TableSchema, errCtx.TableName))
	if errCtx.TargetMode != "" {
		sb.WriteString(fmt.Sprintf("Mode: %s\n", errCtx.TargetMode))
	}

	// Include column info if available (limited to relevant columns)
	if len(errCtx.Columns) > 0 {
		sb.WriteString("\nColumns (name: source_type):\n")
		maxCols := 20 // Limit to avoid token overflow
		for i, col := range errCtx.Columns {
			if i >= maxCols {
				sb.WriteString(fmt.Sprintf("  ... and %d more columns\n", len(errCtx.Columns)-maxCols))
				break
			}
			typeStr := col.DataType
			if col.MaxLength > 0 {
				typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.MaxLength)
			} else if col.Precision > 0 {
				if col.Scale > 0 {
					typeStr = fmt.Sprintf("%s(%d,%d)", col.DataType, col.Precision, col.Scale)
				} else {
					typeStr = fmt.Sprintf("%s(%d)", col.DataType, col.Precision)
				}
			}
			nullable := ""
			if !col.IsNullable {
				nullable = " NOT NULL"
			}
			sb.WriteString(fmt.Sprintf("  %s: %s%s\n", col.Name, typeStr, nullable))
		}
	}

	sb.WriteString("\n=== OUTPUT ===\n")
	sb.WriteString("Respond with ONLY a JSON object (no markdown, no explanation):\n")
	sb.WriteString(`{
  "cause": "brief root cause explanation (1-2 sentences)",
  "suggestions": ["actionable fix 1", "actionable fix 2", "actionable fix 3"],
  "confidence": "high|medium|low",
  "category": "type_mismatch|constraint|permission|connection|data_quality|other"
}`)

	return sb.String()
}

// parseResponse parses the AI response into an ErrorDiagnosis.
func (d *AIErrorDiagnoser) parseResponse(response string) (*ErrorDiagnosis, error) {
	// Clean up response
	response = strings.TrimSpace(response)
	response = strings.TrimPrefix(response, "```json")
	response = strings.TrimPrefix(response, "```")
	response = strings.TrimSuffix(response, "```")
	response = strings.TrimSpace(response)

	var diagnosis ErrorDiagnosis
	if err := json.Unmarshal([]byte(response), &diagnosis); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w (response: %s)", err, truncateString(response, 100))
	}

	// Validate required fields
	if diagnosis.Cause == "" {
		return nil, fmt.Errorf("missing cause in diagnosis")
	}
	if len(diagnosis.Suggestions) == 0 {
		return nil, fmt.Errorf("missing suggestions in diagnosis")
	}

	// Normalize confidence
	switch strings.ToLower(diagnosis.Confidence) {
	case "high", "medium", "low":
		diagnosis.Confidence = strings.ToLower(diagnosis.Confidence)
	default:
		diagnosis.Confidence = "medium"
	}

	// Normalize category
	switch strings.ToLower(diagnosis.Category) {
	case "type_mismatch", "constraint", "permission", "connection", "data_quality", "other":
		diagnosis.Category = strings.ToLower(diagnosis.Category)
	default:
		diagnosis.Category = "other"
	}

	return &diagnosis, nil
}

// CacheSize returns the number of cached diagnoses.
func (d *AIErrorDiagnoser) CacheSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.cache)
}

// ClearCache removes all cached diagnoses.
func (d *AIErrorDiagnoser) ClearCache() {
	d.mu.Lock()
	d.cache = make(map[string]*ErrorDiagnosis)
	d.mu.Unlock()
}
