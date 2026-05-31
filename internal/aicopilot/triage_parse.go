package aicopilot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

const (
	triageUnsafeAdvisorySuppressed = "Unsafe advisory text suppressed. Use read-only DMT commands and documented recovery procedures."
	triageUnsafeHeadingSuppressed  = "Unsafe advisory heading suppressed"
	triageCommandSuppressed        = "Destructive command suppressed. Use read-only DMT commands such as dmt diagnose, dmt status, dmt validate, or dmt analyze."
	triageDestructiveSuppressed    = "Destructive recommendation suppressed. Verify backups, gather operator confirmation, and use documented DMT recovery procedures before any target data change."
)

func ParseTriageReview(raw string) (*TriageReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review TriageReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI triage review JSON: %w", err)
	}
	review.Kind = normalizeTriageKind(review.Kind)
	review.Impact = normalizeTriageImpact(review.Impact)
	review.Summary = sanitizeTriageAdvisoryText(review.Summary, 800)
	review.Error = sanitizeTriageAdvisoryText(review.Error, 800)
	for i := range review.Findings {
		f := &review.Findings[i]
		f.Severity = normalizeSeverity(f.Severity)
		f.Category = sanitizeTriageHeadingText(f.Category, 80)
		f.Affected = sanitizeTriageHeadingText(f.Affected, 160)
		for j := range f.AffectedTables {
			f.AffectedTables[j] = sanitizeTriageHeadingText(f.AffectedTables[j], 160)
		}
		if len(f.AffectedTables) > 10 {
			f.AffectedTables = f.AffectedTables[:10]
		}
		for j := range f.DeterministicFacts {
			f.DeterministicFacts[j] = sanitizeTriageAdvisoryText(f.DeterministicFacts[j], 220)
		}
		f.DeterministicFacts = compactTriageTextList(f.DeterministicFacts, 10)
		f.LikelyCause = sanitizeTriageAdvisoryText(f.LikelyCause, 300)
		for j := range f.Hypotheses {
			f.Hypotheses[j].Confidence = normalizeConfidence(f.Hypotheses[j].Confidence)
			f.Hypotheses[j].Rationale = sanitizeTriageAdvisoryText(f.Hypotheses[j].Rationale, 600)
		}
		if len(f.Hypotheses) > 3 {
			f.Hypotheses = f.Hypotheses[:3]
		}
		for j := range f.SuggestedCommands {
			f.SuggestedCommands[j] = sanitizeTriageSuggestedCommand(f.SuggestedCommands[j])
		}
		f.SuggestedCommands = compactTriageTextList(f.SuggestedCommands, 5)
		if len(f.SuggestedCommands) > 5 {
			f.SuggestedCommands = f.SuggestedCommands[:5]
		}
		for j := range f.SuggestedConfigChanges {
			f.SuggestedConfigChanges[j] = sanitizeTriageSuggestedConfigChange(f.SuggestedConfigChanges[j])
		}
		f.SuggestedConfigChanges = compactTriageTextList(f.SuggestedConfigChanges, 5)
		if len(f.SuggestedConfigChanges) > 5 {
			f.SuggestedConfigChanges = f.SuggestedConfigChanges[:5]
		}
		f.ManualInspection = sanitizeTriageAdvisoryText(f.ManualInspection, 500)
		f.NextAction = sanitizeTriageNextAction(f.NextAction)
		f.Source = TriageFindingSourceAIAdvisory
	}
	for i := range review.Notes {
		review.Notes[i] = sanitizeTriageAdvisoryText(review.Notes[i], 400)
	}
	review.Notes = compactTriageTextList(review.Notes, 10)
	if len(review.Findings) > 5 {
		review.Findings = review.Findings[:5]
	}
	return &review, nil
}

func normalizeTriageKind(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case TriageKindMigrationFailure:
		return TriageKindMigrationFailure
	case TriageKindValidationMismatch:
		return TriageKindValidationMismatch
	default:
		return ""
	}
}

func normalizeTriageImpact(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case TriageImpactBlocked, TriageImpactAttention, TriageImpactInformational:
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return TriageImpactUnknown
	}
}

func normalizeConfidence(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "high", "medium", "low":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "low"
	}
}

func sanitizeTriageNextAction(action string) string {
	raw := action
	action = scrubTriageAdvisoryDisplayText(action, 600)
	if containsUnsafeDMTCommand(raw) {
		return triageUnsafeAdvisorySuppressed
	}
	if !containsDestructiveRecommendation(raw) || includesBackupAndConfirmation(raw) {
		return action
	}
	return triageDestructiveSuppressed
}

func sanitizeTriageAdvisoryText(text string, max int) string {
	raw := text
	text = scrubTriageAdvisoryDisplayText(text, max)
	if text == "" {
		return text
	}
	if isSafeTriageValidationReference(raw) {
		return text
	}
	if containsDestructiveRecommendation(raw) || containsUnsafeDMTCommand(raw) {
		return triageUnsafeAdvisorySuppressed
	}
	return text
}

func sanitizeTriageHeadingText(text string, max int) string {
	raw := text
	text = scrubTriageAdvisoryDisplayText(text, max)
	if text == "" {
		return text
	}
	if isSafeTriageValidationReference(raw) {
		return text
	}
	if containsDestructiveRecommendation(raw) || containsUnsafeDMTCommand(raw) {
		return triageUnsafeHeadingSuppressed
	}
	return text
}

func isSafeTriageValidationReference(s string) bool {
	value := strings.ToLower(strings.TrimSpace(s))
	value = strings.TrimPrefix(value, "validation.")
	switch value {
	case ValidationCategoryDeleteDrift,
		ValidationCategoryTypeCoercion,
		ValidationCategoryTimezoneDateHandling,
		ValidationCategoryWatermarkIssue,
		ValidationCategoryTargetTriggerDefaultBehavior,
		ValidationCategoryRowCountMismatch,
		ValidationCategoryNullMismatch,
		ValidationCategorySampleMismatch,
		ValidationCategoryValidationRuntime,
		ValidationCategoryUnknown:
		return true
	default:
		return false
	}
}

func sanitizeTriageSuggestedCommand(command string) string {
	raw := command
	command = scrubTriageAdvisoryDisplayText(command, 600)
	if command == "" || isReadOnlySuggestedCommand(raw) {
		return command
	}
	return triageCommandSuppressed
}

func compactTriageTextList(values []string, max int) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func sanitizeTriageSuggestedConfigChange(change string) string {
	raw := change
	change = sanitizeTriageNextAction(change)
	if reason := invalidConfigAssignmentInText(raw, aiConfigChangeContext{}); reason != "" {
		return "Invalid config suggestion suppressed. " + reason
	}
	return change
}

func containsDestructiveRecommendation(s string) bool {
	lower := normalizeRecommendationText(s)
	if containsDestructiveRecommendationPattern(lower) {
		return true
	}
	terms := destructiveRecommendationTerms()
	for _, term := range terms {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func containsDestructiveRecommendationPattern(normalized string) bool {
	tokens := strings.Fields(normalized)
	for i, token := range tokens {
		if !isDestructiveVerbToken(token) {
			continue
		}
		for j := i + 1; j < len(tokens) && j <= i+4; j++ {
			switch tokens[j] {
			case "target", "targets", "row", "rows", "table", "tables", "schema", "schemas":
				return true
			}
		}
	}
	return false
}

func isDestructiveVerbToken(token string) bool {
	switch token {
	case "clear", "clears", "cleared", "clearing",
		"delete", "deletes", "deleted", "deleting",
		"drop", "drops", "dropped", "dropping",
		"empty", "empties", "emptied", "emptying",
		"erase", "erases", "erased", "erasing",
		"purge", "purges", "purged", "purging",
		"recreate", "recreates", "recreated", "recreating",
		"reload", "reloads", "reloaded", "reloading",
		"remove", "removes", "removed", "removing",
		"replace", "replaces", "replaced", "replacing",
		"reset", "resets", "resetting",
		"truncate", "truncates", "truncated", "truncating",
		"wipe", "wipes", "wiped", "wiping":
		return true
	default:
		return false
	}
}

func destructiveRecommendationTerms() []string {
	return []string{
		" drop ",
		" drops ",
		" dropped ",
		" dropping ",
		" truncate ",
		" truncates ",
		" truncated ",
		" truncating ",
		" delete ",
		" deletes ",
		" deleted ",
		" deleting ",
		" wipe ",
		" wipes ",
		" wiped ",
		" wiping ",
		" purge ",
		" purges ",
		" purged ",
		" purging ",
		" recreate ",
		" recreates ",
		" recreated ",
		" recreating ",
		" overwrite ",
		" overwrites ",
		" overwritten ",
		" overwriting ",
		" erase ",
		" erases ",
		" erased ",
		" erasing ",
		" remove target ",
		" removes target ",
		" remove the target ",
		" removes the target ",
		" remove all target ",
		" removes all target ",
		" remove rows ",
		" removes rows ",
		" remove the rows ",
		" removes the rows ",
		" remove target rows ",
		" removes target rows ",
		" remove the target rows ",
		" removes the target rows ",
		" remove all rows ",
		" removes all rows ",
		" reset target ",
		" resets target ",
		" reset the target ",
		" resets the target ",
		" reset schema ",
		" resets schema ",
		" clear target ",
		" clears target ",
		" clear the target ",
		" clears the target ",
		" clear all target ",
		" clears all target ",
		" clear rows ",
		" clears rows ",
		" clear all rows ",
		" clears all rows ",
		" empty target ",
		" empties target ",
		" empty the target ",
		" empties the target ",
		" replace target ",
		" replaces target ",
		" replace the target ",
		" replaces the target ",
		" replace all target ",
		" replaces all target ",
		" replace rows ",
		" replaces rows ",
		" replace all rows ",
		" replaces all rows ",
		" reload target ",
		" reloads target ",
		" reload the target ",
		" reloads the target ",
		" reload all target ",
		" reloads all target ",
		" reload from source ",
		" reloads from source ",
	}
}

func firstDestructiveRecommendationIndex(normalized string) int {
	first := -1
	for _, term := range destructiveRecommendationTerms() {
		idx := strings.Index(normalized, term)
		if idx >= 0 && (first < 0 || idx < first) {
			first = idx
		}
	}
	return first
}

func includesBackupAndConfirmation(s string) bool {
	lower := normalizeRecommendationText(s)
	if containsNegatedSafetyToken(lower) {
		return false
	}
	if containsAny(lower, []string{
		" without backup ",
		" without backups ",
		" without confirmation ",
		" without operator approval ",
		" no backup ",
		" no backups ",
		" no confirmation ",
		" no approval ",
		" no operator confirmation ",
		" no operator approval ",
		" skip backup ",
		" skip backups ",
		" skip confirmation ",
		" before backup ",
		" before confirmation ",
		" before operator confirmation ",
		" before operator approval ",
		" pending backup ",
		" pending confirmation ",
		" backup pending ",
		" backups pending ",
		" confirmation pending ",
		" approval pending ",
		" backup verification and operator confirmation are pending ",
		" backup verification and operator confirmation are still pending ",
		" backup verification and operator confirmation has not been obtained ",
		" backup verification and operator confirmation have not been obtained ",
		" backup verification and operator confirmation remain pending ",
		" backup verification and operator confirmation remains pending ",
		" backups verified and operator confirmation are pending ",
		" backups verified and operator confirmation are still pending ",
		" backup verification pending ",
		" operator confirmation pending ",
		" backup missing ",
		" backups missing ",
		" confirmation missing ",
		" approval missing ",
		" not verified ",
		" not confirmed ",
		" not obtained ",
		" not required ",
		" can be skipped ",
		" may be skipped ",
		" or not ",
		" confirmation is not required ",
		" approval is not required ",
		" operator confirmation is not required ",
		" operator approval is not required ",
		" without operator confirmation ",
		" awaiting backup ",
		" awaiting confirmation ",
		" awaiting approval ",
	}) {
		return false
	}
	firstDestructive := firstDestructiveRecommendationIndex(lower)
	if firstDestructive <= 0 {
		return false
	}
	guard := lower[:firstDestructive+1]
	hasPositiveBackup := containsAny(guard, []string{
		" after backup ",
		" after backups ",
		" after verifying backup ",
		" after verifying backups ",
		" after backup verification ",
		" after backups are verified ",
		" after backup is verified ",
		" only after backup ",
		" only after backups ",
		" only after backups are verified ",
		" only after backup is verified ",
		" once backup ",
		" once backups ",
		" once backups are verified ",
		" once backup is verified ",
		" verified backup ",
		" verified backups ",
		" backups verified ",
	})
	hasPositiveConfirmation := containsAny(guard, []string{
		" after operator confirmation ",
		" after operator approval ",
		" after confirmation ",
		" after confirmation from operator ",
		" only after operator confirmation ",
		" only after operator approval ",
		" only after confirmation ",
		" only after confirmation from operator ",
		" once operator confirmation ",
		" once operator approval ",
		" once confirmation from operator ",
		" confirmation is obtained ",
		" confirmation has been obtained ",
		" operator confirmation is obtained ",
		" operator confirmation has been obtained ",
		" approval is obtained ",
		" approval has been obtained ",
		" operator approval is obtained ",
		" operator approval has been obtained ",
		" operator has confirmed ",
	})
	hasSequencing := containsAny(guard, []string{
		" after ",
		" only after ",
		" once ",
	})
	return hasSequencing && hasPositiveBackup && hasPositiveConfirmation
}

func containsNegatedSafetyToken(normalized string) bool {
	tokens := strings.Fields(normalized)
	for i, token := range tokens {
		if token != "no" {
			continue
		}
		for j := i + 1; j < len(tokens) && j <= i+4; j++ {
			switch tokens[j] {
			case "approval", "backup", "backups", "confirmation":
				return true
			}
		}
	}
	return false
}

func containsUnsafeDMTCommand(s string) bool {
	fields := strings.Fields(strings.TrimSpace(s))
	for i, field := range fields {
		token := strings.ToLower(trimTriageCommandToken(field))
		if !isDMTExecutableToken(token) {
			continue
		}
		if !isReadOnlySuggestedCommand(strings.Join(fields[i:], " ")) {
			return true
		}
	}
	lower := normalizeRecommendationText(s)
	return containsAny(lower, []string{
		" confirm backup ",
		" confirm backups ",
		" force resume ",
		" skip validation ",
		" bypass validation ",
	})
}

func isReadOnlySuggestedCommand(command string) bool {
	if containsShellControlOperator(command) {
		return false
	}
	fields := strings.Fields(strings.TrimSpace(command))
	if len(fields) == 0 {
		return true
	}
	if fields[0] == "$" {
		fields = fields[1:]
	}
	if len(fields) == 0 {
		return true
	}
	if strings.Contains(command, "\n") || strings.Contains(command, "\r") {
		return false
	}
	exe := strings.ToLower(trimTriageCommandToken(fields[0]))
	if !isDMTExecutableToken(exe) {
		return false
	}
	commandFound := false
	for i := 1; i < len(fields); i++ {
		token := strings.ToLower(trimTriageCommandToken(fields[i]))
		if token == "" {
			continue
		}
		if strings.HasPrefix(token, "-") {
			if readOnlyCommandFlagDisallowed(token) {
				return false
			}
			if cliFlagTakesValue(token) && !strings.Contains(token, "=") {
				i++
			}
			continue
		}
		switch token {
		case "diagnose", "status", "validate", "analyze":
			if commandFound {
				return false
			}
			commandFound = true
		default:
			return false
		}
	}
	return commandFound
}

func trimTriageCommandToken(token string) string {
	token = strings.Trim(token, "`\"',:;()[]")
	return strings.TrimRight(token, ".")
}

func isDMTExecutableToken(token string) bool {
	token = strings.ReplaceAll(token, `\`, "/")
	switch token {
	case "dmt", "./dmt", "dmt.exe", "./dmt.exe":
		return true
	default:
		return strings.HasSuffix(token, "/dmt") || strings.HasSuffix(token, "/dmt.exe")
	}
}

func readOnlyCommandFlagDisallowed(flag string) bool {
	flag = strings.TrimSpace(flag)
	switch {
	case flag == "--apply",
		flag == "--confirm-backup",
		flag == "--force-resume",
		strings.HasPrefix(flag, "--apply="),
		strings.HasPrefix(flag, "--confirm-backup="),
		strings.HasPrefix(flag, "--force-resume="):
		return true
	default:
		return false
	}
}

func containsShellControlOperator(command string) bool {
	return strings.Contains(command, "\n") ||
		strings.Contains(command, "\r") ||
		strings.Contains(command, ";") ||
		strings.Contains(command, "&") ||
		strings.Contains(command, "&&") ||
		strings.Contains(command, "||") ||
		strings.Contains(command, "|") ||
		strings.Contains(command, ">") ||
		strings.Contains(command, "<") ||
		strings.Contains(command, "`") ||
		strings.Contains(command, "$(")
}

func cliFlagTakesValue(flag string) bool {
	switch flag {
	case "-c", "--config", "--profile", "--secrets-file", "--state-file", "--timeout", "--run":
		return true
	default:
		return false
	}
}

func normalizeRecommendationText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte(' ')
		}
	}
	return " " + strings.Join(strings.Fields(b.String()), " ") + " "
}

func containsAny(s string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func formatInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func deterministicTriageImpact(payload TriagePayload) string {
	switch normalizeTriageKind(payload.Kind) {
	case TriageKindMigrationFailure:
		return TriageImpactBlocked
	case TriageKindValidationMismatch:
		if payload.ValidationMismatch != nil && (payload.ValidationMismatch.TimedOut || payload.ValidationMismatch.Error != "") {
			return TriageImpactBlocked
		}
		return TriageImpactAttention
	default:
		return TriageImpactUnknown
	}
}

func deterministicTriageSummary(payload TriagePayload) string {
	switch normalizeTriageKind(payload.Kind) {
	case TriageKindMigrationFailure:
		return "Deterministic migration failure facts are available for operator triage."
	case TriageKindValidationMismatch:
		return "Deterministic validation mismatch facts are available for operator triage."
	default:
		return "Deterministic triage facts are available for operator review."
	}
}

func scrubTriageFacts(facts []TriageFact) []TriageFact {
	out := make([]TriageFact, 0, len(facts))
	for _, fact := range facts {
		out = append(out, TriageFact{
			Category: scrubTriageText(fact.Category, 80),
			Affected: scrubTriageText(fact.Affected, 160),
			Detail:   scrubTriageText(fact.Detail, 500),
		})
	}
	return compactTriageFacts(out)
}

func scrubTriageError(err error) string {
	if err == nil {
		return ""
	}
	return scrubTriageText(logging.Scrub(err.Error()), 500)
}
