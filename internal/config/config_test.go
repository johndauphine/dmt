package config

import (
	"strings"
	"testing"
)

func TestMSSQLDSNURLEncoding(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		password string
		database string
		wantUser string
		wantPass string
		wantDB   string
	}{
		{
			name:     "plain credentials",
			user:     "admin",
			password: "secret",
			database: "mydb",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "password with @",
			user:     "admin",
			password: "pass@word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%40word",
			wantDB:   "mydb",
		},
		{
			name:     "password with colon",
			user:     "admin",
			password: "pass:word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%3Aword",
			wantDB:   "mydb",
		},
		{
			name:     "password with slash",
			user:     "admin",
			password: "pass/word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%2Fword",
			wantDB:   "mydb",
		},
		{
			name:     "user with @",
			user:     "user@domain",
			password: "secret",
			database: "mydb",
			wantUser: "user%40domain",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "database with spaces",
			user:     "admin",
			password: "secret",
			database: "my database",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "my+database", // QueryEscape uses + for spaces
		},
		{
			name:     "complex password",
			user:     "admin",
			password: "P@ss:w/rd?123",
			database: "mydb",
			wantUser: "admin",
			wantPass: "P%40ss%3Aw%2Frd%3F123",
			wantDB:   "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			dsn := cfg.buildMSSQLDSN("localhost", 1433, tt.database, tt.user, tt.password,
				"true", false, "", "", "", "", "")

			// Check that encoded values appear in DSN
			if !strings.Contains(dsn, tt.wantUser+":") {
				t.Errorf("MSSQL DSN missing encoded user %q in %q", tt.wantUser, dsn)
			}
			if !strings.Contains(dsn, ":"+tt.wantPass+"@") {
				t.Errorf("MSSQL DSN missing encoded password %q in %q", tt.wantPass, dsn)
			}
			if !strings.Contains(dsn, "database="+tt.wantDB) {
				t.Errorf("MSSQL DSN missing encoded database %q in %q", tt.wantDB, dsn)
			}
		})
	}
}

func TestPostgresDSNURLEncoding(t *testing.T) {
	tests := []struct {
		name     string
		user     string
		password string
		database string
		wantUser string
		wantPass string
		wantDB   string
	}{
		{
			name:     "plain credentials",
			user:     "admin",
			password: "secret",
			database: "mydb",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "password with @",
			user:     "admin",
			password: "pass@word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%40word",
			wantDB:   "mydb",
		},
		{
			name:     "password with colon",
			user:     "admin",
			password: "pass:word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%3Aword",
			wantDB:   "mydb",
		},
		{
			name:     "password with slash",
			user:     "admin",
			password: "pass/word",
			database: "mydb",
			wantUser: "admin",
			wantPass: "pass%2Fword",
			wantDB:   "mydb",
		},
		{
			name:     "user with @",
			user:     "user@domain",
			password: "secret",
			database: "mydb",
			wantUser: "user%40domain",
			wantPass: "secret",
			wantDB:   "mydb",
		},
		{
			name:     "database with spaces",
			user:     "admin",
			password: "secret",
			database: "my database",
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "my%20database", // PathEscape uses %20 for spaces
		},
		{
			name:     "complex password",
			user:     "admin",
			password: "P@ss:w/rd?123",
			database: "mydb",
			wantUser: "admin",
			wantPass: "P%40ss%3Aw%2Frd%3F123",
			wantDB:   "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			dsn := cfg.buildPostgresDSN("localhost", 5432, tt.database, tt.user, tt.password,
				"disable", "", "")

			// Check that encoded values appear in DSN
			if !strings.Contains(dsn, tt.wantUser+":") {
				t.Errorf("Postgres DSN missing encoded user %q in %q", tt.wantUser, dsn)
			}
			if !strings.Contains(dsn, ":"+tt.wantPass+"@") {
				t.Errorf("Postgres DSN missing encoded password %q in %q", tt.wantPass, dsn)
			}
			if !strings.Contains(dsn, "/"+tt.wantDB+"?") {
				t.Errorf("Postgres DSN missing encoded database %q in %q", tt.wantDB, dsn)
			}
		})
	}
}

func TestMSSQLKerberosEncoding(t *testing.T) {
	cfg := &Config{}

	// Test MSSQL Kerberos with special chars
	dsn := cfg.buildMSSQLDSN("localhost", 1433, "my database", "user@REALM.COM", "",
		"true", false, "kerberos", "/path/to/krb5.conf", "", "REALM.COM", "MSSQLSvc/host:1433")

	// database is QueryEscaped (+ for spaces)
	if !strings.Contains(dsn, "database=my+database") {
		t.Errorf("MSSQL Kerberos DSN missing encoded database in %q", dsn)
	}
	// username in query param is QueryEscaped
	if !strings.Contains(dsn, "krb5-username=user%40REALM.COM") {
		t.Errorf("MSSQL Kerberos DSN missing encoded username in %q", dsn)
	}
	// SPN with special chars
	if !strings.Contains(dsn, "ServerSPN=MSSQLSvc%2Fhost%3A1433") {
		t.Errorf("MSSQL Kerberos DSN missing encoded SPN in %q", dsn)
	}
}

func TestPostgresKerberosEncoding(t *testing.T) {
	cfg := &Config{}

	// Test Postgres Kerberos with special chars
	dsn := cfg.buildPostgresDSN("localhost", 5432, "my database", "user@REALM.COM", "",
		"disable", "kerberos", "prefer")

	// database is PathEscaped (%20 for spaces)
	if !strings.Contains(dsn, "/my%20database?") {
		t.Errorf("Postgres Kerberos DSN missing encoded database in %q", dsn)
	}
	// user in userinfo is QueryEscaped
	if !strings.Contains(dsn, "user%40REALM.COM@") {
		t.Errorf("Postgres Kerberos DSN missing encoded user in %q", dsn)
	}
}
