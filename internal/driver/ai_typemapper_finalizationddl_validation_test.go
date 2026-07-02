package driver

import (
	"strings"
	"testing"
)

func TestValidateFinalizationDDLResponse(t *testing.T) {
	req := func(typ DDLType) FinalizationDDLRequest {
		return FinalizationDDLRequest{
			Table:        &Table{Name: "users"},
			TargetDBType: "postgres",
			Type:         typ,
		}
	}

	tests := []struct {
		name           string
		ddl            string
		typ            DDLType
		validatePrefix string
		wantErr        string // substring; "" means expect success
	}{
		// --- injection: multi-statement must be rejected (#561) ---
		{
			name:    "index with trailing DROP rejected",
			ddl:     "CREATE INDEX idx_users_email ON users (email); DROP TABLE users;",
			typ:     DDLTypeIndex,
			wantErr: "exactly one SQL statement",
		},
		{
			name:           "check constraint with trailing DROP rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (1=1); DROP TABLE users;",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "exactly one SQL statement",
		},
		{
			name:           "trailing DROP without final semicolon still rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (1=1); DROP TABLE users",
			typ:            DDLTypeForeignKey,
			validatePrefix: "ALTER TABLE",
			wantErr:        "exactly one SQL statement",
		},

		// --- injection: comment-hidden trailing statement (#561 review) ---
		// The apostrophe/paren inside the comment used to open a fake quote
		// region that swallowed the real ';', hiding the DROP from the scanner.
		{
			name:    "line comment hiding trailing DROP rejected",
			ddl:     "CREATE INDEX idx ON users (email) -- it's fine\n; DROP TABLE users",
			typ:     DDLTypeIndex,
			wantErr: "must not contain SQL comments",
		},
		{
			name:           "block comment hiding trailing DROP rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (age > 0) /* don't */ ; DROP TABLE users",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "must not contain SQL comments",
		},
		{
			name:           "mysql hash comment hiding trailing DROP rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (age > 0) # nope\n; DROP TABLE users",
			typ:            DDLTypeForeignKey,
			validatePrefix: "ALTER TABLE",
			wantErr:        "must not contain SQL comments",
		},
		{
			name:    "decoy target table in comment rejected",
			ddl:     "CREATE INDEX idx /*ON users*/ ON evil (x)",
			typ:     DDLTypeIndex,
			wantErr: "must not contain SQL comments",
		},

		// --- injection: dialect string-escape desync (#561 re-review) ---
		// No comment, so the comment gate doesn't fire; the validator's quote
		// model diverges from the DB's and would otherwise hide the real ';'.
		{
			name:           "mysql backslash-escaped quote hiding DROP rejected",
			ddl:            `ALTER TABLE users ADD CONSTRAINT c CHECK (note <> '\''); DROP TABLE users`,
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "backslash escape",
		},
		{
			name:           "postgres dollar-quote hiding DROP rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (note <> $$'$$); DROP TABLE users",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "dollar-quoting",
		},
		{
			name:           "tagged dollar-quote rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (note <> $tag$x$tag$); DROP TABLE users",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "dollar-quoting",
		},

		// --- injection: wrong target table must be rejected ---
		{
			name:    "index targeting another table rejected",
			ddl:     "CREATE INDEX idx ON secrets (email)",
			typ:     DDLTypeIndex,
			wantErr: "expected \"users\"",
		},
		{
			name:           "ALTER TABLE targeting another table rejected",
			ddl:            "ALTER TABLE secrets ADD CONSTRAINT c CHECK (x > 0)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "expected \"users\"",
		},

		// --- wrong statement kind ---
		{
			name:    "non-CREATE-INDEX for index type rejected",
			ddl:     "ALTER TABLE users ADD CONSTRAINT c CHECK (1=1)",
			typ:     DDLTypeIndex,
			wantErr: "valid CREATE INDEX",
		},
		{
			name:           "non-ALTER-TABLE for FK type rejected",
			ddl:            "CREATE INDEX idx ON users (email)",
			typ:            DDLTypeForeignKey,
			validatePrefix: "ALTER TABLE",
			wantErr:        "valid ALTER TABLE",
		},
		{
			name:    "empty response rejected",
			ddl:     "   ",
			typ:     DDLTypeIndex,
			wantErr: "response is empty",
		},
		{
			name:           "unterminated string literal rejected",
			ddl:            "ALTER TABLE users ADD CONSTRAINT c CHECK (note <> 'oops)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
			wantErr:        "unterminated",
		},

		// --- valid single statements must pass (don't over-restrict) ---
		{
			name: "plain index accepted",
			ddl:  "CREATE INDEX idx ON users (email)",
			typ:  DDLTypeIndex,
		},
		{
			name: "unique filtered index accepted",
			ddl:  "CREATE UNIQUE INDEX idx ON users (email) WHERE deleted_at IS NULL",
			typ:  DDLTypeIndex,
		},
		{
			name: "index with trailing semicolon accepted",
			ddl:  "CREATE INDEX idx ON users (email);",
			typ:  DDLTypeIndex,
		},
		{
			name: "schema-qualified index accepted",
			ddl:  "CREATE INDEX idx ON public.users (email)",
			typ:  DDLTypeIndex,
		},
		{
			name: "quoted-identifier index accepted",
			ddl:  `CREATE INDEX idx ON "users" (email)`,
			typ:  DDLTypeIndex,
		},
		{
			name: "index with USING clause accepted",
			ddl:  "CREATE INDEX idx ON users USING gin (data)",
			typ:  DDLTypeIndex,
		},
		{
			name: "index ON ONLY partitioned table accepted",
			ddl:  "CREATE INDEX idx ON ONLY users (email)",
			typ:  DDLTypeIndex,
		},
		{
			name:           "foreign key accepted",
			ddl:            "ALTER TABLE users ADD CONSTRAINT fk_users_org FOREIGN KEY (org_id) REFERENCES orgs (id)",
			typ:            DDLTypeForeignKey,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "check constraint accepted",
			ddl:            "ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "schema-qualified ALTER TABLE accepted",
			ddl:            "ALTER TABLE public.users ADD CONSTRAINT chk_age CHECK (age > 0)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "ALTER TABLE ONLY accepted",
			ddl:            "ALTER TABLE ONLY users ADD CONSTRAINT chk_age CHECK (age > 0)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "semicolon inside string literal is not multi-statement",
			ddl:            "ALTER TABLE users ADD CONSTRAINT chk_note CHECK (note <> 'a;b')",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "comment markers inside a string literal are allowed",
			ddl:            "ALTER TABLE users ADD CONSTRAINT chk_note CHECK (note <> '-- not a comment /* nor this */')",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
		{
			name:           "lone dollar sign in identifier is allowed",
			ddl:            "ALTER TABLE users ADD CONSTRAINT chk_amt CHECK (amount$ > 0)",
			typ:            DDLTypeCheckConstraint,
			validatePrefix: "ALTER TABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := req(tt.typ)
			err := validateFinalizationDDLResponse(tt.ddl, r, tt.validatePrefix)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected success, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}
