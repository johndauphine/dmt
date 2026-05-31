package aicopilot

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/internal/logging"
)

func ParseConfigReview(raw string) (*ConfigReview, error) {
	review, err := parseConfigReview(raw)
	if err != nil {
		return nil, err
	}
	normalizeConfigReview(review, nil)
	return review, nil
}

func parseConfigReview(raw string) (*ConfigReview, error) {
	body := extractJSONObject(strings.TrimSpace(raw))
	var review ConfigReview
	if err := json.Unmarshal([]byte(body), &review); err != nil {
		return nil, fmt.Errorf("parsing AI config review JSON: %w", err)
	}
	return &review, nil
}

func normalizeConfigReview(review *ConfigReview, payload *ConfigReviewPayload) {
	if review == nil {
		return
	}
	review.Summary = configReviewSafeGuidanceText(review.Summary, payload, 800)
	review.RefusalReason = configReviewSafeGuidanceText(review.RefusalReason, payload, 400)
	if len(review.PatchRecommendations) > 5 {
		review.PatchRecommendations = review.PatchRecommendations[:5]
	}
	for i := range review.PatchRecommendations {
		p := &review.PatchRecommendations[i]
		originalOperation := p.Operation
		rawPath := strings.TrimPrefix(strings.TrimSpace(p.Path), ".")
		rawPathUnsafe := configReviewPathTextUnsafe(rawPath)
		rawValueSensitive := valueContainsSensitiveConfigReviewKey(p.Value)
		rawValueUnsafe := valueContainsUnsafeConfigReviewGuidance(p.Value)
		p.Operation = normalizePatchOperation(p.Operation)
		p.Path = configReviewSafeText(rawPath, payload, 120)
		p.Value = sanitizeConfigReviewValue(p.Value, payload)
		p.Rationale = configReviewSafeGuidanceText(p.Rationale, payload, 600)
		p.Risk = configReviewSafeGuidanceText(p.Risk, payload, 300)
		p.WhenToApply = configReviewSafeGuidanceText(p.WhenToApply, payload, 300)
		p.ValidationErrors = sanitizeConfigReviewGuidanceStrings(p.ValidationErrors, payload, 5, 160)
		pathAllowed := configReviewPathAllowed(p.Path, payload)
		schemaErrors := validateAIConfigChange(p.Path, p.Value, configReviewChangeContext(payload))
		sensitivePath := isSensitiveConfigReviewPath(p.Path) || rawValueSensitive || valueContainsSensitiveConfigReviewKey(p.Value)
		if !patchOperationAllowed(originalOperation) {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, "operation normalized to set because only set, add, and remove are allowed")
		}
		for _, schemaError := range schemaErrors {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, schemaError)
		}
		if sensitivePath {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, "path targets sensitive connection or secret material")
		}
		if rawPathUnsafe {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, "path contains unsafe guidance or command text")
		}
		if rawValueUnsafe {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, "value contains unsafe guidance or command text")
		}
		if !pathAllowed {
			p.ValidationErrors = appendConfigPatchValidationError(p.ValidationErrors, "path is outside the safe config review allowlist")
		}
		if !pathAllowed || sensitivePath || rawPathUnsafe || rawValueUnsafe {
			p.Operation = "set"
			p.Value = "[REDACTED]"
			if rawPathUnsafe {
				p.Path = "[REDACTED]"
			}
			switch {
			case !pathAllowed && sensitivePath:
				p.Rationale = "Recommendation referenced sensitive material outside the safe config review allowlist and was redacted."
			case sensitivePath:
				p.Rationale = "Recommendation referenced sensitive connection or secret material and was redacted."
			case rawPathUnsafe:
				p.Rationale = "Recommendation path contained unsafe guidance or command text and was redacted."
			case rawValueUnsafe:
				p.Rationale = "Recommendation value contained unsafe guidance or command text and was redacted."
			default:
				p.Rationale = "Recommendation referenced a path outside the safe config review allowlist and was redacted."
			}
			p.Risk = "Do not apply without manual review."
			p.RequiresConfirmation = true
		}
		if configReviewPatchNeedsConfirmation(*p) {
			p.RequiresConfirmation = true
			if p.Risk == "" {
				p.Risk = "Requires explicit operator confirmation and verified target backup."
			} else if !strings.Contains(strings.ToLower(p.Risk), "backup") {
				p.Risk = limitText(p.Risk+" Requires verified target backup before applying.", 300)
			}
		}
		if len(schemaErrors) > 0 {
			p.RequiresConfirmation = true
		}
		if payload != nil && payload.Safety.RequiresReviewOnly {
			p.RequiresConfirmation = true
		}
		p.ValidationErrors = prioritizeConfigPatchValidationErrors(p.ValidationErrors)
		if len(p.ValidationErrors) > 5 {
			p.ValidationErrors = p.ValidationErrors[:5]
		}
	}
	review.Runbook.Title = configReviewSafeGuidanceText(review.Runbook.Title, payload, 120)
	review.Runbook.Summary = configReviewSafeGuidanceText(review.Runbook.Summary, payload, 500)
	review.Runbook.BeforeRun = sanitizeConfigReviewStrings(review.Runbook.BeforeRun, payload, 5, 300)
	review.Runbook.Run = sanitizeConfigReviewStrings(review.Runbook.Run, payload, 5, 300)
	review.Runbook.Validation = sanitizeConfigReviewStrings(review.Runbook.Validation, payload, 5, 300)
	review.Runbook.Rollback = sanitizeConfigReviewStrings(review.Runbook.Rollback, payload, 5, 300)
	review.Notes = sanitizeConfigReviewStrings(review.Notes, payload, 5, 300)
	if payload != nil {
		ensureConfigReviewRunbook(review, payload)
		review.Notes = removeUnsafeConfigReviewRunbookSteps(removeConfigReviewCommandSteps(review.Notes))
	}
}

func configReviewChangeContext(payload *ConfigReviewPayload) aiConfigChangeContext {
	var ctx aiConfigChangeContext
	if payload != nil {
		ctx.TargetMode = payload.Safety.TargetMode
	}
	return ctx
}

func normalizePatchOperation(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "set", "add", "remove":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "set"
	}
}

func patchOperationAllowed(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "set", "add", "remove":
		return true
	default:
		return false
	}
}

func sanitizeConfigReviewStrings(values []string, payload *ConfigReviewPayload, maxItems, maxLen int) []string {
	if len(values) > maxItems {
		values = values[:maxItems]
	}
	for i := range values {
		values[i] = configReviewSafeText(values[i], payload, maxLen)
	}
	return values
}

func sanitizeConfigReviewGuidanceStrings(values []string, payload *ConfigReviewPayload, maxItems, maxLen int) []string {
	if len(values) > maxItems {
		values = values[:maxItems]
	}
	for i := range values {
		values[i] = configReviewSafeGuidanceText(values[i], payload, maxLen)
	}
	return values
}

func sanitizeConfigReviewValue(v any, payload *ConfigReviewPayload) any {
	switch x := v.(type) {
	case string:
		return configReviewSafeText(x, payload, 240)
	case []any:
		if len(x) > 10 {
			x = x[:10]
		}
		for i := range x {
			x[i] = sanitizeConfigReviewValue(x[i], payload)
		}
		return x
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			safeKey := configReviewSafeText(k, payload, 160)
			out[safeKey] = sanitizeConfigReviewValue(val, payload)
		}
		return out
	default:
		if configReviewPrimitiveMatchesSensitiveValue(x, payload) {
			return "[REDACTED]"
		}
		return v
	}
}

func valueContainsSensitiveConfigReviewKey(v any) bool {
	switch x := v.(type) {
	case string:
		return configReviewStringLooksSensitive(x)
	case map[string]any:
		for k, val := range x {
			if isSensitiveConfigReviewPath(k) || valueContainsSensitiveConfigReviewKey(val) {
				return true
			}
		}
	case []any:
		for _, val := range x {
			if valueContainsSensitiveConfigReviewKey(val) {
				return true
			}
		}
	}
	return false
}

func valueContainsUnsafeConfigReviewGuidance(v any) bool {
	switch x := v.(type) {
	case string:
		return configReviewPatchValueStringUnsafe(x)
	case map[string]any:
		for key, val := range x {
			if configReviewPatchValueStringUnsafe(key) || valueContainsUnsafeConfigReviewGuidance(val) {
				return true
			}
		}
	case []any:
		for _, val := range x {
			if valueContainsUnsafeConfigReviewGuidance(val) {
				return true
			}
		}
	}
	return false
}

func configReviewPatchValueStringUnsafe(s string) bool {
	if strings.EqualFold(strings.TrimSpace(s), "drop_recreate") {
		return false
	}
	if containsDMTCommandText(s) {
		return true
	}
	if configReviewPatchValueBypassesSafetyChecks(s) {
		return true
	}
	if !configReviewRunbookStepUnsafe(s) {
		return false
	}
	return true
}

func configReviewPathTextUnsafe(s string) bool {
	return s != "" && (containsDMTCommandText(s) || configReviewRunbookStepUnsafe(s))
}

func configReviewPatchValueBypassesSafetyChecks(s string) bool {
	lower := normalizeConfigReviewFreeText(s)
	unsafe := []string{
		" skip validation ",
		" bypass validation ",
		" disable validation ",
		" ignore validation ",
		" turn off validation ",
		" turn validation off ",
		" without validation ",
		" run without validation ",
		" do not run validation ",
		" do not validate ",
		" skip preflight ",
		" bypass preflight ",
		" disable preflight ",
		" ignore preflight ",
		" turn off preflight ",
		" turn preflight off ",
		" without preflight ",
		" run without preflight ",
		" do not run preflight ",
	}
	for _, term := range unsafe {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func configReviewStringLooksSensitive(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "" {
		return false
	}
	if strings.Contains(lower, "://") ||
		strings.Contains(lower, "hooks.") ||
		strings.Contains(lower, "webhook") ||
		strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "authorization") ||
		strings.Contains(lower, "api_key") ||
		strings.Contains(lower, "apikey") ||
		strings.Contains(lower, "private key") ||
		strings.Contains(lower, "connection string") ||
		strings.Contains(lower, "token=") ||
		strings.Contains(lower, "secret") ||
		strings.Contains(lower, "password") ||
		strings.Contains(lower, "passwd") ||
		strings.Contains(lower, "dsn") {
		return true
	}
	return false
}

func configReviewPrimitiveMatchesSensitiveValue(v any, payload *ConfigReviewPayload) bool {
	if payload == nil {
		return false
	}
	text := fmt.Sprint(v)
	for _, sensitive := range payload.sensitiveValues {
		value := sensitive.Value
		if value == "" {
			continue
		}
		if text == value || strings.TrimSuffix(text, ".0") == value {
			return true
		}
	}
	return false
}

func configReviewSafeText(s string, payload *ConfigReviewPayload, max int) string {
	s = logging.Scrub(strings.TrimSpace(s))
	if payload != nil {
		values := append([]configReviewSensitiveValue(nil), payload.sensitiveValues...)
		sort.SliceStable(values, func(i, j int) bool {
			return len(values[i].Value) > len(values[j].Value)
		})
		for _, sensitive := range values {
			if sensitive.Value == "" {
				continue
			}
			s = redactConfigReviewSensitiveValue(s, sensitive)
		}
	}
	return limitText(s, max)
}

func redactConfigReviewSensitiveValue(s string, sensitive configReviewSensitiveValue) string {
	value := strings.TrimSpace(sensitive.Value)
	if value == "" {
		return s
	}
	if sensitive.Strict && len(value) >= 6 {
		return strings.ReplaceAll(s, value, logging.RedactedToken)
	}
	if !sensitive.Strict && len(value) < 6 {
		return s
	}
	return replaceConfigReviewBoundedValue(s, value)
}

func replaceConfigReviewBoundedValue(s, value string) string {
	if value == "" || !strings.Contains(s, value) {
		return s
	}
	var b strings.Builder
	searchStart := 0
	changed := false
	for {
		idx := strings.Index(s[searchStart:], value)
		if idx < 0 {
			break
		}
		idx += searchStart
		end := idx + len(value)
		if configReviewValueAtBoundary(s, idx, end) {
			b.WriteString(s[searchStart:idx])
			b.WriteString(logging.RedactedToken)
			searchStart = end
			changed = true
			continue
		}
		b.WriteString(s[searchStart : idx+1])
		searchStart = idx + 1
	}
	if !changed {
		return s
	}
	b.WriteString(s[searchStart:])
	return b.String()
}

func configReviewValueAtBoundary(s string, start, end int) bool {
	prevOK := start == 0 || !isConfigReviewIdentifierByte(s[start-1])
	nextOK := end >= len(s) || !isConfigReviewIdentifierByte(s[end])
	return prevOK && nextOK
}

func isConfigReviewIdentifierByte(b byte) bool {
	return b >= '0' && b <= '9' ||
		b >= 'A' && b <= 'Z' ||
		b >= 'a' && b <= 'z' ||
		b == '_'
}

func configReviewSafeGuidanceText(s string, payload *ConfigReviewPayload, max int) string {
	s = configReviewSafeText(s, payload, max)
	if configReviewRunbookStepUnsafe(s) || containsDMTCommandText(s) {
		return "Unsafe guidance suppressed. Use deterministic preflight, validation, and operator-reviewed config changes."
	}
	return s
}

func configReviewPathAllowed(path string, payload *ConfigReviewPayload) bool {
	if path == "" {
		return false
	}
	exactPaths := []string{
		"source.type", "source.schema", "source.ssl_mode", "source.encrypt", "source.packet_size",
		"target.type", "target.schema", "target.ssl_mode", "target.encrypt", "target.packet_size",
		"migration.target_mode", "migration.workers", "migration.chunk_size", "migration.upsert_merge_chunk_size", "migration.read_ahead_buffers",
		"migration.write_ahead_writers", "migration.parallel_readers", "migration.max_source_connections",
		"migration.max_target_connections", "migration.max_partitions", "migration.large_table_threshold",
		"migration.strict_consistency", "migration.create_indexes", "migration.create_foreign_keys",
		"migration.create_check_constraints", "migration.allow_partial", "migration.unmapped_type_action",
		"migration.approx_type_action", "migration.schema_contract", "migration.schema_evolution",
		"migration.deletes", "migration.validation", "migration.checkpoint_frequency", "migration.max_retries",
		"migration.history_retention_days", "migration.notify", "migration.runtime_tuning", "migration.runtime_tuning_interval",
	}
	if payload != nil && len(payload.Safety.AllowedPatchPaths) > 0 {
		exactPaths = payload.Safety.AllowedPatchPaths
	}
	for _, allowed := range exactPaths {
		if path == allowed || configReviewExpandablePath(allowed) && strings.HasPrefix(path, allowed+".") {
			return true
		}
	}
	return false
}

func isSensitiveConfigReviewPath(path string) bool {
	path = strings.ToLower(strings.TrimSpace(path))
	if path == "" {
		return false
	}
	for _, part := range strings.Split(path, ".") {
		normalized := strings.NewReplacer("_", "", "-", "").Replace(part)
		switch normalized {
		case "host", "port", "user", "username", "password", "database", "secret", "token", "apikey", "accesskey", "secretkey", "privatekey", "webhook", "webhookurl", "dsn", "connectionstring", "credential", "credentials", "krb5conf", "keytab", "realm", "spn":
			return true
		}
		if strings.Contains(normalized, "webhook") ||
			strings.Contains(normalized, "secret") ||
			strings.Contains(normalized, "token") ||
			strings.Contains(normalized, "auth") ||
			strings.Contains(normalized, "authorization") ||
			strings.Contains(normalized, "header") ||
			strings.Contains(normalized, "private") ||
			strings.Contains(normalized, "password") ||
			strings.Contains(normalized, "passwd") ||
			strings.Contains(normalized, "username") ||
			strings.Contains(normalized, "credential") ||
			strings.Contains(normalized, "url") ||
			strings.Contains(normalized, "uri") ||
			strings.Contains(normalized, "endpoint") ||
			strings.Contains(normalized, "apikey") ||
			strings.Contains(normalized, "api") && strings.Contains(normalized, "key") ||
			strings.Contains(normalized, "database") ||
			strings.Contains(normalized, "dsn") ||
			strings.Contains(normalized, "connectionstring") {
			return true
		}
	}
	return false
}

func configReviewPatchNeedsConfirmation(p ConfigPatchRecommendation) bool {
	if strings.EqualFold(p.Operation, "remove") {
		return true
	}
	if p.Path == "migration.target_mode" && strings.EqualFold(fmt.Sprint(p.Value), "drop_recreate") {
		return true
	}
	if p.Path == "migration.deletes" || strings.HasPrefix(p.Path, "migration.deletes.") {
		return true
	}
	return false
}

func appendConfigPatchValidationError(values []string, msg string) []string {
	for _, v := range values {
		if v == msg {
			return values
		}
	}
	return append(values, msg)
}

func prioritizeConfigPatchValidationErrors(values []string) []string {
	critical := map[string]bool{
		"operation normalized to set because only set, add, and remove are allowed": true,
		configChangeUnknownPathError:                           true,
		configChangeInvalidValueError:                          true,
		"path contains unsafe guidance or command text":        true,
		"path targets sensitive connection or secret material": true,
		"path is outside the safe config review allowlist":     true,
		"value contains unsafe guidance or command text":       true,
	}
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	appendIf := func(v string) {
		if v == "" || seen[v] {
			return
		}
		seen[v] = true
		out = append(out, v)
	}
	for _, v := range values {
		if critical[v] || strings.HasPrefix(v, configChangeUnknownPathError) || strings.HasPrefix(v, configChangeInvalidValueError) {
			appendIf(v)
		}
	}
	for _, v := range values {
		if !critical[v] && !strings.HasPrefix(v, configChangeUnknownPathError) && !strings.HasPrefix(v, configChangeInvalidValueError) {
			appendIf(v)
		}
	}
	return out
}

func ensureConfigReviewRunbook(review *ConfigReview, payload *ConfigReviewPayload) {
	if review == nil {
		return
	}
	if review.Runbook.Title == "" {
		review.Runbook.Title = "DMT migration review runbook"
	}
	if review.Runbook.Summary == "" {
		review.Runbook.Summary = "Operator-reviewed config recommendations with deterministic preflight, validation, and rollback gates."
	}
	commands := ConfigReviewCommands{
		Preflight: "dmt --config config.yaml preflight --ai-review",
		Run:       "dmt --config config.yaml run",
		Validate:  "dmt --config config.yaml validate",
		Resume:    "dmt --config config.yaml resume",
	}
	if payload != nil {
		commands = payload.Commands
		review.Runbook.BeforeRun = removeUnsafeConfigReviewRunbookSteps(removeConfigReviewCommandSteps(review.Runbook.BeforeRun))
		review.Runbook.Run = removeUnsafeConfigReviewRunbookSteps(removeConfigReviewCommandSteps(review.Runbook.Run))
		review.Runbook.Validation = removeUnsafeConfigReviewRunbookSteps(removeConfigReviewCommandSteps(review.Runbook.Validation))
		review.Runbook.Rollback = removeUnsafeConfigReviewRunbookSteps(removeConfigReviewCommandSteps(review.Runbook.Rollback))
	}
	review.Runbook.BeforeRun = ensureConfigReviewStep(review.Runbook.BeforeRun, "Run preflight: "+commands.Preflight, 5)
	if payload != nil && strings.EqualFold(payload.Safety.TargetMode, "drop_recreate") {
		review.Runbook.BeforeRun = ensureConfigReviewStep(review.Runbook.BeforeRun, "Verify target backups and operator confirmation before any drop_recreate run; pass --confirm-backup only after that check.", 5)
	}
	review.Runbook.Run = ensureConfigReviewStep(review.Runbook.Run, "Run migration: "+commands.Run, 5)
	review.Runbook.Validation = ensureConfigReviewStep(review.Runbook.Validation, "Validate results: "+commands.Validate, 5)
	review.Runbook.Rollback = ensureConfigReviewStep(review.Runbook.Rollback, "If validation fails, stop downstream cutover, inspect checkpoints, and resume only after correction: "+commands.Resume, 5)
	review.Notes = ensureConfigReviewStep(review.Notes, "Recommendations are advisory patches only; DMT does not apply them until an operator edits and reviews the config.", 5)
	review.Notes = ensureConfigReviewStep(review.Notes, "Keep credentials, AI API keys, and Slack webhook URLs in ~/.secrets/dmt-config.yaml or environment/file references, not in generated output.", 5)
}

func removeConfigReviewCommandSteps(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if containsDMTCommandText(value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func containsDMTCommandText(value string) bool {
	tokens := strings.Fields(normalizeConfigReviewFreeText(value))
	for i, token := range tokens {
		if token != "dmt" {
			continue
		}
		if i > 0 && tokens[i-1] == "run" {
			return true
		}
		for j := i + 1; j < len(tokens) && j <= i+8; j++ {
			switch tokens[j] {
			case "analyze", "diagnose", "preflight", "resume", "run", "status", "validate":
				return true
			}
		}
	}
	return false
}

func removeUnsafeConfigReviewRunbookSteps(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if configReviewRunbookStepUnsafe(value) {
			continue
		}
		out = append(out, value)
	}
	return out
}

func configReviewRunbookStepUnsafe(value string) bool {
	lower := normalizeConfigReviewFreeText(value)
	unsafe := []string{
		" drop ",
		" truncate ",
		" delete ",
		" wipe ",
		" purge ",
		" overwrite ",
		" remove target ",
		" clear target ",
		" skip validation ",
		" bypass validation ",
		" disable validation ",
		" ignore validation ",
		" ignore validation failures ",
		" turn off validation ",
		" turn validation off ",
		" without validation ",
		" run without validation ",
		" do not run validation ",
		" do not validate ",
		" do not validate results ",
		" dont run validation ",
		" skip preflight ",
		" bypass preflight ",
		" disable preflight ",
		" ignore preflight ",
		" ignore preflight failures ",
		" turn off preflight ",
		" turn preflight off ",
		" without preflight ",
		" run without preflight ",
		" do not run preflight ",
		" dont run preflight ",
	}
	for _, term := range unsafe {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func normalizeConfigReviewFreeText(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range strings.ToLower(value) {
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

func ensureConfigReviewStep(values []string, step string, maxItems int) []string {
	if maxItems <= 0 {
		return values
	}
	for _, v := range values {
		if strings.Contains(v, step) {
			return values
		}
	}
	if len(values) >= maxItems {
		withStep := append([]string{step}, values...)
		return withStep[:maxItems]
	}
	return append(values, step)
}

func configReviewExpandablePath(path string) bool {
	switch path {
	case "migration.schema_contract",
		"migration.schema_evolution",
		"migration.deletes",
		"migration.validation",
		"migration.notify":
		return true
	default:
		return false
	}
}
