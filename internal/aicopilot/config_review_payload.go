package aicopilot

import (
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmt/v5/internal/config"
)

func BuildConfigReviewPayload(cfg *config.Config, opts ConfigReviewOptions) ConfigReviewPayload {
	sensitiveValues := collectConfigReviewSensitiveValues(cfg)
	targetMode := ""
	if cfg != nil {
		targetMode = cfg.Migration.TargetMode
	}
	payload := ConfigReviewPayload{
		PromptVersion:   ConfigReviewPromptVersion,
		Task:            "Review an existing DMT configuration and return safe patch-style recommendations plus a concise operator runbook.",
		OperatorRequest: configReviewSafeText(opts.OperatorRequest, &ConfigReviewPayload{sensitiveValues: sensitiveValues}, 400),
		Config:          buildConfigSummary(cfg),
		Commands:        buildConfigReviewCommands(opts),
		Safety: ConfigReviewSafetyContext{
			AllowedPatchPaths: []string{
				"migration.target_mode",
				"migration.workers",
				"migration.chunk_size",
				"migration.upsert_merge_chunk_size",
				"migration.read_ahead_buffers",
				"migration.write_ahead_writers",
				"migration.parallel_readers",
				"migration.max_source_connections",
				"migration.max_target_connections",
				"migration.max_partitions",
				"migration.large_table_threshold",
				"migration.strict_consistency",
				"migration.create_indexes",
				"migration.create_foreign_keys",
				"migration.create_check_constraints",
				"migration.allow_partial",
				"migration.unmapped_type_action",
				"migration.approx_type_action",
				"migration.schema_contract",
				"migration.schema_evolution",
				"migration.deletes",
				"migration.validation",
				"migration.checkpoint_frequency",
				"migration.max_retries",
				"migration.history_retention_days",
				"migration.notify",
				"migration.runtime_tuning",
				"migration.runtime_tuning_interval",
				"source.type",
				"source.schema",
				"source.ssl_mode",
				"source.encrypt",
				"source.packet_size",
				"target.type",
				"target.schema",
				"target.ssl_mode",
				"target.encrypt",
				"target.packet_size",
			},
			DisallowedContent: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database",
				"target.host", "target.port", "target.user", "target.password", "target.database",
				"connection strings", "DSNs", "API keys", "Slack webhook URLs", "Kerberos keytab paths",
			},
			RequiresReviewOnly: true,
			TargetMode:         targetMode,
		},
		Redaction: RedactionSummary{
			OmittedFields: []string{
				"source.host", "source.port", "source.user", "source.password", "source.database",
				"source.krb5_conf", "source.keytab", "source.realm", "source.spn",
				"target.host", "target.port", "target.user", "target.password", "target.database",
				"target.krb5_conf", "target.keytab", "target.realm", "target.spn",
				"ai.api_key", "slack.webhook_url",
			},
			ScrubbedText: true,
		},
		sensitiveValues: sensitiveValues,
	}
	payload.Safety.RefusalReason = configReviewRefusalReason(payload.OperatorRequest)
	return payload
}

func buildConfigReviewCommands(opts ConfigReviewOptions) ConfigReviewCommands {
	base := "dmt"
	switch {
	case strings.TrimSpace(opts.ProfileName) != "":
		base += " --profile " + shellQuoteForReview(opts.ProfileName)
	case strings.TrimSpace(opts.ConfigPath) != "":
		base += " --config " + shellQuoteForReview(opts.ConfigPath)
	default:
		base += " --config config.yaml"
	}
	if strings.TrimSpace(opts.StateFile) != "" {
		base += " --state-file " + shellQuoteForReview(opts.StateFile)
	}
	return ConfigReviewCommands{
		Preflight: base + " preflight --ai-review",
		Run:       base + " run",
		Validate:  base + " validate",
		Resume:    base + " resume",
	}
}

func shellQuoteForReview(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return !isShellSafeReviewRune(r)
	}) == -1 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func isShellSafeReviewRune(r rune) bool {
	switch r {
	case '/', '.', '_', '-', ':':
		return true
	default:
		return r >= '0' && r <= '9' ||
			r >= 'A' && r <= 'Z' ||
			r >= 'a' && r <= 'z'
	}
}

func collectConfigReviewSensitiveValues(cfg *config.Config) []configReviewSensitiveValue {
	if cfg == nil {
		return nil
	}
	var values []configReviewSensitiveValue
	add := func(v any, strict bool) {
		s := strings.TrimSpace(fmt.Sprint(v))
		if s != "" && s != "0" && s != "<nil>" {
			values = append(values, configReviewSensitiveValue{Value: s, Strict: strict})
		}
	}
	add(cfg.Source.Host, false)
	add(cfg.Source.Port, false)
	add(cfg.Source.Database, false)
	add(cfg.Source.User, false)
	add(cfg.Source.Password, true)
	add(cfg.Source.Krb5Conf, false)
	add(cfg.Source.Keytab, false)
	add(cfg.Source.Realm, false)
	add(cfg.Source.SPN, false)
	add(cfg.Target.Host, false)
	add(cfg.Target.Port, false)
	add(cfg.Target.Database, false)
	add(cfg.Target.User, false)
	add(cfg.Target.Password, true)
	add(cfg.Target.Krb5Conf, false)
	add(cfg.Target.Keytab, false)
	add(cfg.Target.Realm, false)
	add(cfg.Target.SPN, false)
	if cfg.AI != nil {
		add(cfg.AI.APIKey, true)
	}
	if cfg.Slack != nil {
		add(cfg.Slack.WebhookURL, true)
	}
	sort.SliceStable(values, func(i, j int) bool {
		return len(values[i].Value) > len(values[j].Value)
	})
	out := values[:0]
	seen := map[string]int{}
	for _, v := range values {
		if idx, ok := seen[v.Value]; ok {
			out[idx].Strict = out[idx].Strict || v.Strict
			continue
		}
		seen[v.Value] = len(out)
		out = append(out, v)
	}
	return out
}

func configReviewRefusalReason(request string) string {
	request = strings.ToLower(strings.TrimSpace(request))
	if request == "" {
		return ""
	}
	unsafeNeedles := []string{
		"include password",
		"show password",
		"print password",
		"include secret",
		"show secret",
		"print secret",
		"api key",
		"webhook",
		"connection string",
		"dsn",
		"ignore all errors",
		"disable validation and preflight",
	}
	for _, needle := range unsafeNeedles {
		if strings.Contains(request, needle) {
			return "operator request asks for secrets or unsafe bypass guidance"
		}
	}
	ambiguousNeedles := []string{
		"do whatever",
		"anything necessary",
		"make it work somehow",
	}
	for _, needle := range ambiguousNeedles {
		if strings.Contains(request, needle) {
			return "operator request is too ambiguous for safe patch recommendations"
		}
	}
	return ""
}
