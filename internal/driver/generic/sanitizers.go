package generic

import "github.com/johndauphine/dmt/v5/internal/ident"

// identifierSanitizers are the writer-side identifier rewrite
// strategies selectable via quoting.identifier_sanitizer. The function
// must match what the deterministic type mapper applies to CREATE TABLE
// DDL for the same target, or writer-side DDL and introspection will
// look for tables that were created under a different name.
//
// Entries live in this literal (not per-file init funcs) for the same
// init-ordering reason as preflightStrategies.
var identifierSanitizers = map[string]func(string) string{
	"postgres": ident.SanitizePG,
}
