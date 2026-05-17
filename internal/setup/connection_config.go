package setup

import "github.com/johndauphine/dmt/internal/config"

// SourceConnConfig returns a copy of s.Config with source-side placeholders
// resolved, suitable for the source connection test. The wizard keeps raw
// `${env:...}` / `${file:...}` values in s.Config so re-saving preserves
// them; connection tests need the resolved values to authenticate.
//
// Expansion is scoped to the source side and tolerates per-field failures
// (an unrelated target `${file:/missing}` must not break source auth).
// On per-field failure the placeholder survives unchanged so the eventual
// connect error mentions the actual credential the user needs to fix.
func (s *State) SourceConnConfig() config.Config {
	cfg := s.Config
	expandStringFields(
		&cfg.Source.Host,
		&cfg.Source.User,
		&cfg.Source.Password,
		&cfg.Source.Database,
		&cfg.Source.Schema,
		&cfg.Source.Krb5Conf,
		&cfg.Source.Keytab,
		&cfg.Source.Realm,
		&cfg.Source.SPN,
	)
	return cfg
}

// TargetConnConfig is SourceConnConfig's mirror for the target connection test.
func (s *State) TargetConnConfig() config.Config {
	cfg := s.Config
	expandStringFields(
		&cfg.Target.Host,
		&cfg.Target.User,
		&cfg.Target.Password,
		&cfg.Target.Database,
		&cfg.Target.Schema,
		&cfg.Target.Krb5Conf,
		&cfg.Target.Keytab,
		&cfg.Target.Realm,
		&cfg.Target.SPN,
	)
	return cfg
}

// expandStringFields resolves each string in-place. A per-field expansion
// failure leaves that field's placeholder unchanged so other fields still
// get the resolved value.
func expandStringFields(fields ...*string) {
	for _, f := range fields {
		if expanded, err := config.Expand(*f); err == nil {
			*f = expanded
		}
	}
}
