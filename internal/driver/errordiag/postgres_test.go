package errordiag

import (
	"strings"
	"testing"
)

// postgresCases pairs each pattern name with a representative real-world
// error string and an optional substring the diagnosis must contain.
// One entry per pattern in postgresPatterns — the coverage test below
// fails if a pattern is added without a corresponding test row.
var postgresCases = []struct {
	pattern   string
	errMsg    string
	mustMatch string // optional substring expected in Cause or first Suggestion
	mustCateg string // optional expected Category
}{
	{
		pattern:   "postgres_not_accepting_connections",
		errMsg:    `pinging postgres database: server error: FATAL: the database system is not yet accepting connections (SQLSTATE 57P03)`,
		mustMatch: "crash recovery",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_duplicate_key",
		errMsg:    `pq: duplicate key value violates unique constraint "users_email_key"`,
		mustMatch: "users_email_key",
		mustCateg: "constraint",
	},
	{
		pattern:   "pg_fk_violation",
		errMsg:    `pq: insert or update on table "orders" violates foreign key constraint "fk_orders_users"`,
		mustMatch: "fk_orders_users",
		mustCateg: "constraint",
	},
	{
		pattern:   "pg_fk_unimplementable",
		errMsg:    `pq: foreign key constraint "fk_x" cannot be implemented`,
		mustMatch: "fk_x",
		mustCateg: "constraint",
	},
	{
		pattern:   "pg_not_null_violation",
		errMsg:    `pq: null value in column "email" of relation "users" violates not-null constraint`,
		mustMatch: "email",
		mustCateg: "data_quality",
	},
	{
		pattern:   "pg_value_too_long",
		errMsg:    `pq: value too long for type character varying(50)`,
		mustMatch: "character varying",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "pg_invalid_input_syntax",
		errMsg:    `pq: invalid input syntax for type integer: "abc"`,
		mustMatch: "abc",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "pg_relation_exists",
		errMsg:    `pq: relation "users" already exists`,
		mustMatch: "users",
		mustCateg: "other",
	},
	{
		pattern:   "pg_relation_missing",
		errMsg:    `pq: relation "orders" does not exist`,
		mustMatch: "orders",
		mustCateg: "other",
	},
	{
		pattern:   "pg_column_missing",
		errMsg:    `pq: column "email" does not exist`,
		mustMatch: "email",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "pg_column_exists",
		errMsg:    `pq: column "email" of relation "users" already exists`,
		mustMatch: "email",
		mustCateg: "other",
	},
	{
		pattern:   "pg_permission_schema",
		errMsg:    `pq: permission denied for schema public`,
		mustMatch: "public",
		mustCateg: "permission",
	},
	{
		pattern:   "pg_permission_table",
		errMsg:    `pq: permission denied for table users`,
		mustMatch: "users",
		mustCateg: "permission",
	},
	{
		pattern:   "pg_must_be_owner",
		errMsg:    `pq: must be owner of table users`,
		mustMatch: "users",
		mustCateg: "permission",
	},
	{
		pattern:   "pg_password_auth",
		errMsg:    `pq: password authentication failed for user "dmt"`,
		mustMatch: "dmt",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_no_pghba_entry",
		errMsg:    `pq: no pg_hba.conf entry for host "10.0.0.5", user "dmt", database "prod", SSL off`,
		mustMatch: "10.0.0.5",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_database_missing",
		errMsg:    `pq: database "prod" does not exist`,
		mustMatch: "prod",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_connection_refused",
		errMsg:    `dial tcp 10.0.0.5:5432: connect: connection refused`,
		mustMatch: "refused",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_connection_timeout",
		errMsg:    `dial tcp 10.0.0.5:5432: i/o timeout`,
		mustMatch: "timeout",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_out_of_memory",
		errMsg:    `pq: out of memory`,
		mustMatch: "memory",
		mustCateg: "other",
	},
	{
		pattern:   "pg_disk_full",
		errMsg:    `pq: could not extend file "base/16384/16385": No space left on device`,
		mustMatch: "disk",
		mustCateg: "other",
	},
	{
		pattern:   "pg_invalid_encoding",
		errMsg:    `pq: invalid byte sequence for encoding "UTF8": 0x80`,
		mustMatch: "UTF8",
		mustCateg: "data_quality",
	},
	{
		pattern:   "pg_tx_aborted",
		errMsg:    `pq: current transaction is aborted, commands ignored until end of transaction block`,
		mustMatch: "earlier",
		mustCateg: "other",
	},
	{
		pattern:   "pg_deadlock",
		errMsg:    `pq: deadlock detected`,
		mustMatch: "deadlock",
		mustCateg: "other",
	},
	{
		pattern:   "pg_statement_timeout",
		errMsg:    `pq: canceling statement due to statement timeout`,
		mustMatch: "statement_timeout",
		mustCateg: "other",
	},
	{
		pattern:   "pg_admin_termination",
		errMsg:    `pq: terminating connection due to administrator command`,
		mustMatch: "terminated",
		mustCateg: "connection",
	},
	{
		pattern:   "pg_ssl_required",
		errMsg:    `pq: SSL is not enabled on the server`,
		mustMatch: "SSL",
		mustCateg: "connection",
	},
}

func TestPostgresPatterns_AllCasesMatch(t *testing.T) {
	for _, tc := range postgresCases {
		t.Run(tc.pattern, func(t *testing.T) {
			m, ok := Lookup("postgres", tc.errMsg)
			if !ok {
				t.Fatalf("expected Lookup to match for pattern %q with msg %q", tc.pattern, tc.errMsg)
			}
			if m.PatternName != tc.pattern {
				t.Fatalf("matched the wrong pattern: got %q, want %q (error: %q)", m.PatternName, tc.pattern, tc.errMsg)
			}
			assertDiagnosisShape(t, m.Diagnosis, tc.pattern)
			if tc.mustCateg != "" && m.Diagnosis.Category != tc.mustCateg {
				t.Errorf("category mismatch: got %q, want %q", m.Diagnosis.Category, tc.mustCateg)
			}
			if tc.mustMatch != "" {
				combined := m.Diagnosis.Cause + " " + strings.Join(m.Diagnosis.Suggestions, " ")
				if !strings.Contains(combined, tc.mustMatch) {
					t.Errorf("diagnosis should mention %q; got cause=%q suggestions=%v",
						tc.mustMatch, m.Diagnosis.Cause, m.Diagnosis.Suggestions)
				}
			}
		})
	}
}

func TestPostgresPatterns_EveryCatalogPatternHasACase(t *testing.T) {
	covered := make(map[string]struct{}, len(postgresCases))
	for _, tc := range postgresCases {
		covered[tc.pattern] = struct{}{}
	}
	for _, p := range postgresPatterns {
		if _, ok := covered[p.Name]; !ok {
			t.Errorf("pattern %q has no test case in postgresCases (add one or remove the pattern)", p.Name)
		}
	}
}
