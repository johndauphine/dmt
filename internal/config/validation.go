package config

import (
	"fmt"
	"strings"
)

func (c *Config) validate() error {
	// Validate source
	//
	// File-based drivers (sqlite) don't use host — their connection
	// identity is the file path, carried on `database`. Network drivers
	// (mssql/postgres/mysql) require both host and database. Branch on
	// the canonical type before checking `host` so a sqlite source with
	// only `database: ./foo.db` validates cleanly.
	if !isFileBasedDriver(c.Source.Type) && c.Source.Host == "" {
		return fmt.Errorf("source.host is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source.database is required")
	}
	if !isValidDriverType(c.Source.Type) {
		return fmt.Errorf("source.type '%s' is not a valid driver type (supported: %v)", c.Source.Type, availableDriverTypes())
	}
	// Kerberos auth is descoped pending a verifiable test environment
	// (#251). The DSN-building functions in this file (SourceDSN /
	// TargetDSN) emit correct Kerberos DSNs, but the runtime drivers
	// use Dialect.BuildDSN(..., cfg.DSNOptions()) instead, and
	// DSNOptions doesn't carry auth/keytab/realm/SPN — so an
	// `auth: kerberos` config silently falls back to password auth.
	// Reject it at load time rather than letting that happen.
	if strings.EqualFold(c.Source.Auth, "kerberos") {
		return fmt.Errorf("source.auth: kerberos is not currently supported; tracking re-enable in #251")
	}

	// Validate target — same file-based-driver carve-out as source.
	if !isFileBasedDriver(c.Target.Type) && c.Target.Host == "" {
		return fmt.Errorf("target.host is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target.database is required")
	}
	if !isValidDriverType(c.Target.Type) {
		return fmt.Errorf("target.type '%s' is not a valid driver type (supported: %v)", c.Target.Type, availableDriverTypes())
	}
	// See source.auth note above — same descope applies to target. (#251)
	if strings.EqualFold(c.Target.Auth, "kerberos") {
		return fmt.Errorf("target.auth: kerberos is not currently supported; tracking re-enable in #251")
	}

	// Same-engine migration validation: prevent migration to the exact same database
	// Compare canonical driver names to handle aliases (e.g., "mssql" == "sqlserver")
	if canonicalDriverName(c.Source.Type) == canonicalDriverName(c.Target.Type) {
		// Use case-insensitive comparison for hostnames (RFC 1035)
		sameHost := strings.EqualFold(c.Source.Host, c.Target.Host)
		samePort := c.Source.Port == c.Target.Port
		sameDB := c.Source.Database == c.Target.Database
		if sameHost && samePort && sameDB {
			return fmt.Errorf("source and target cannot be the same database (%s:%d/%s)",
				c.Source.Host, c.Source.Port, c.Source.Database)
		}
	}

	// Validate migration settings
	if c.Migration.TargetMode != "drop_recreate" && c.Migration.TargetMode != "upsert" {
		return fmt.Errorf("migration.target_mode must be 'drop_recreate' or 'upsert'")
	}
	if err := c.validateDeletes(); err != nil {
		return err
	}
	switch c.Migration.UnmappedTypeAction {
	case "", "fail", "skip", "conservative-text":
		// Valid. Empty is allowed because applyDefaults sets it to
		// "fail" before validate runs in the normal load path; tests
		// that construct a Config directly and call validate may
		// leave it empty and that's OK.
	default:
		// Non-empty value that doesn't match is a user typo.
		// Fail loudly rather than silently misbehave (Copilot review
		// on PR #192 — without this, a typo'd action would default
		// to empty SQLType in the chain's handleUnmapped and produce
		// invalid DDL with no clear cause).
		return fmt.Errorf("migration.unmapped_type_action must be 'fail', 'skip', or 'conservative-text'; got %q",
			c.Migration.UnmappedTypeAction)
	}

	switch c.Migration.ApproxTypeAction {
	case "", "deterministic", "ai_fallback":
		// Valid. Empty intentionally falls through to NewFallbackChain
		// (#209) which picks ai_fallback when AI is configured,
		// deterministic when AI isn't — config doesn't know about AI
		// availability, the chain does. Explicit values override that
		// runtime decision. Issues #197, #209.
	default:
		return fmt.Errorf("migration.approx_type_action must be 'deterministic' or 'ai_fallback'; got %q",
			c.Migration.ApproxTypeAction)
	}

	if c.Migration.SchemaEvolution != nil {
		switch c.Migration.SchemaEvolution.AddedColumn {
		case "", SchemaEvolutionAuto, SchemaEvolutionLog, SchemaEvolutionFail,
			SchemaEvolutionDiscard, SchemaEvolutionDiscardValue:
			// Valid. Empty means the section-level default: auto.
		default:
			return fmt.Errorf("migration.schema_evolution.added_column must be 'auto', 'log', 'fail', 'discard_value', or 'discard'; got %q",
				c.Migration.SchemaEvolution.AddedColumn)
		}
		switch c.Migration.SchemaEvolution.NullabilityChange {
		case "", SchemaEvolutionAuto, SchemaEvolutionLog, SchemaEvolutionFail:
			// Valid. Empty means the section-level default: auto.
		default:
			return fmt.Errorf("migration.schema_evolution.nullability_change must be 'auto', 'log', or 'fail'; got %q",
				c.Migration.SchemaEvolution.NullabilityChange)
		}
		switch c.Migration.SchemaEvolution.TypeChange {
		case "", SchemaEvolutionAuto, SchemaEvolutionLog, SchemaEvolutionFail:
			// Valid. Empty means log-only; type evolution requires explicit opt-in.
		default:
			return fmt.Errorf("migration.schema_evolution.type_change must be 'auto', 'log', or 'fail'; got %q",
				c.Migration.SchemaEvolution.TypeChange)
		}
	}

	// Note: AI configuration is validated in the secrets package when loaded from ~/.secrets/dmt-config.yaml

	return nil
}
