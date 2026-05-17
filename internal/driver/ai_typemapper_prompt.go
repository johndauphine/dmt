package driver

import (
	"fmt"
	"strings"
)

// maxSampleValueLen is the maximum length of a single sample value in prompts.
const maxSampleValueLen = 100

// maxSamplesInPrompt is the maximum number of sample values to include in prompts.
const maxSamplesInPrompt = 5

// maxTotalSampleBytes is the maximum total bytes of sample data to include.
const maxTotalSampleBytes = 500

// sanitizeSampleValue cleans and truncates a sample value for inclusion in AI prompts.
// It redacts potential PII patterns and limits length.
func sanitizeSampleValue(value string) string {
	if value == "" {
		return value
	}

	// Truncate to max length
	if len(value) > maxSampleValueLen {
		value = value[:maxSampleValueLen] + "..."
	}

	// Redact potential email addresses
	if strings.Contains(value, "@") && strings.Contains(value, ".") {
		parts := strings.SplitN(value, "@", 2)
		if len(parts) == 2 && len(parts[0]) > 0 {
			value = "[EMAIL]@" + parts[1]
		}
	}

	// Redact potential SSN patterns (XXX-XX-XXXX)
	if len(value) == 11 && value[3] == '-' && value[6] == '-' {
		allDigits := true
		for i, c := range value {
			if i == 3 || i == 6 {
				continue
			}
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			value = "[SSN]"
		}
	}

	// Redact potential phone numbers (10+ consecutive digits)
	digitCount := 0
	for _, c := range value {
		if c >= '0' && c <= '9' {
			digitCount++
		}
	}
	if digitCount >= 10 && digitCount <= 15 {
		nonDigits := len(value) - digitCount
		if nonDigits <= 4 {
			value = "[PHONE]"
		}
	}

	return value
}

func (m *AITypeMapper) buildPrompt(info TypeInfo) string {
	var sb strings.Builder
	sb.WriteString("You are a database type mapping expert.\n\n")
	sb.WriteString("Based on DDL metadata only (no sample data), ")
	sb.WriteString(fmt.Sprintf("map this %s type to %s:\n", info.SourceDBType, info.TargetDBType))
	sb.WriteString(fmt.Sprintf("- Type: %s\n", info.DataType))
	if info.MaxLength > 0 {
		sb.WriteString(fmt.Sprintf("- Max length: %d\n", info.MaxLength))
	} else if info.MaxLength == -1 {
		sb.WriteString("- Max length: MAX\n")
	}
	if info.Precision > 0 {
		sb.WriteString(fmt.Sprintf("- Precision: %d\n", info.Precision))
	}
	if info.Scale > 0 {
		sb.WriteString(fmt.Sprintf("- Scale: %d\n", info.Scale))
	}

	// Sample values are no longer collected (privacy improvement)
	// Type mapping now works purely from DDL metadata

	// Add target database context
	switch info.TargetDBType {
	case "postgres":
		sb.WriteString("\nTarget: Standard PostgreSQL (no extensions installed).\n")
	case "mssql":
		sb.WriteString("\nTarget: SQL Server with full native type support.\n")
	case "mysql":
		sb.WriteString("\nTarget: MySQL 8.0+ or MariaDB 10.5+ with InnoDB engine.\n")
		sb.WriteString("Note: MySQL varchar has 65535 byte max (use TEXT for longer). Use utf8mb4 charset.\n")
	}

	sb.WriteString("\nReturn ONLY the ")
	sb.WriteString(info.TargetDBType)
	sb.WriteString(" type name (e.g., varchar(255), numeric(10,2), text).\n")
	sb.WriteString("No explanation, just the type.")

	return sb.String()
}
