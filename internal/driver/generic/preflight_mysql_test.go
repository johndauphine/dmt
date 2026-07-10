package generic

import (
	"context"
	"testing"

	"github.com/johndauphine/dmt/internal/driver"
)

// TestParseMySQLVersion covers the SHOW VERSION() string shapes the
// preflight check has to handle: bare MySQL semver, vendor-suffixed
// MySQL, and MariaDB. mysqlPFParseMySQLVersion returns ok=false rather than
// guessing — better to warn than to assert version compatibility on a
// malformed input.
func TestParseMySQLVersion(t *testing.T) {
	cases := []struct {
		in        string
		wantMajor int
		wantMinor int
		wantOK    bool
	}{
		{in: "8.0.35", wantMajor: 8, wantMinor: 0, wantOK: true},
		{in: "5.7.43", wantMajor: 5, wantMinor: 7, wantOK: true},
		{in: "5.7", wantMajor: 5, wantMinor: 7, wantOK: true},
		{in: "8.0.35-mysql", wantMajor: 8, wantMinor: 0, wantOK: true},
		{in: "10.6.12-MariaDB", wantMajor: 10, wantMinor: 6, wantOK: true},
		// MariaDB sometimes emits e.g. "5.5.5-10.4.22-MariaDB" via the
		// fake-major version-string compatibility hack. mysqlPFParseMySQLVersion
		// returns the FIRST major.minor it sees — caller is expected to
		// detect "MariaDB" in the raw string and adjust the floor.
		{in: "5.5.5-10.4.22-MariaDB", wantMajor: 5, wantMinor: 5, wantOK: true},
		{in: "", wantOK: false},
		{in: "garbage", wantOK: false},
		{in: "8", wantOK: false},
		{in: "x.y.z", wantOK: false},
		{in: "  8.0.35  ", wantMajor: 8, wantMinor: 0, wantOK: true},
	}
	for _, tc := range cases {
		major, minor, ok := mysqlPFParseMySQLVersion(tc.in)
		if ok != tc.wantOK {
			t.Errorf("mysqlPFParseMySQLVersion(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if major != tc.wantMajor || minor != tc.wantMinor {
			t.Errorf("mysqlPFParseMySQLVersion(%q) = (%d, %d), want (%d, %d)",
				tc.in, major, minor, tc.wantMajor, tc.wantMinor)
		}
	}
}

// TestGrantsInclude covers the SHOW GRANTS parse paths the privilege
// check relies on: ALL PRIVILEGES (and the bare "ALL ON" variant) must
// match every probe; specific verb lists must match by exact-verb token
// (so "SELECT" doesn't match "SELECT_NEXTVAL"-style hypothetical
// privileges); and the absence of the verb must return false rather than
// erroring out. Scope-aware matching (#228 Copilot review): a grant on
// `other_db`.* must NOT satisfy a check against `mydb` — preflight has
// to surface false-positive risk on cross-DB grants.
func TestGrantsInclude(t *testing.T) {
	cases := []struct {
		name   string
		grants []string
		priv   string
		dbName string
		want   bool
	}{
		{
			name:   "global ALL PRIVILEGES matches any scope",
			grants: []string{"GRANT ALL PRIVILEGES ON *.* TO 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "mydb",
			want:   true,
		},
		{
			name:   "bare ALL ON matching db",
			grants: []string{"GRANT ALL ON `db`.* TO 'dmt'@'%'"},
			priv:   "CREATE",
			dbName: "db",
			want:   true,
		},
		{
			name:   "verb list matches present, matching scope",
			grants: []string{"GRANT SELECT, INSERT, UPDATE ON `db`.* TO 'dmt'@'%'"},
			priv:   "INSERT",
			dbName: "db",
			want:   true,
		},
		{
			name:   "scope mismatch rejects grant on other db",
			grants: []string{"GRANT ALL PRIVILEGES ON `other_db`.* TO 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "mydb",
			want:   false,
		},
		{
			name:   "verb list rejects absent verb",
			grants: []string{"GRANT SELECT, INSERT ON `db`.* TO 'dmt'@'%'"},
			priv:   "DROP",
			dbName: "db",
			want:   false,
		},
		{
			name: "match across multiple grant lines",
			grants: []string{
				"GRANT SELECT ON `db`.* TO 'dmt'@'%'",
				"GRANT INSERT, UPDATE ON `db`.* TO 'dmt'@'%'",
			},
			priv:   "UPDATE",
			dbName: "db",
			want:   true,
		},
		{
			name:   "verb prefix match is not allowed",
			grants: []string{"GRANT SELECT_FOO ON `db`.* TO 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "db",
			want:   false,
		},
		{
			name:   "empty grants returns false",
			grants: nil,
			priv:   "SELECT",
			dbName: "db",
			want:   false,
		},
		{
			name:   "case-insensitive verb matching",
			grants: []string{"grant select, insert on `db`.* to 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "db",
			want:   true,
		},
		{
			name:   "empty dbName falls back to scope-less match",
			grants: []string{"GRANT SELECT ON `other_db`.* TO 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "",
			want:   true,
		},
		{
			name:   "table-scoped grant matches the db scope",
			grants: []string{"GRANT SELECT ON `db`.`tbl` TO 'dmt'@'%'"},
			priv:   "SELECT",
			dbName: "db",
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mysqlPFGrantsInclude(tc.grants, tc.priv, tc.dbName)
			if got != tc.want {
				t.Errorf("mysqlPFGrantsInclude(%v, %q, %q) = %v, want %v", tc.grants, tc.priv, tc.dbName, got, tc.want)
			}
		})
	}
}

func TestMySQLLockTablesPreflightGate(t *testing.T) {
	tests := []struct {
		name string
		req  driver.PreFlightRequest
	}{
		{name: "target", req: driver.PreFlightRequest{Side: driver.PreFlightSideTarget, StrictConsistency: true, ParallelReaders: 4}},
		{name: "relaxed", req: driver.PreFlightRequest{Side: driver.PreFlightSideSource, ParallelReaders: 4}},
		{name: "single reader", req: driver.PreFlightRequest{Side: driver.PreFlightSideSource, StrictConsistency: true, ParallelReaders: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A nil DB proves the gate returns before attempting any probe.
			if got := mysqlPFCheckLockTablesMySQL(context.Background(), nil, tc.req); len(got) != 0 {
				t.Fatalf("findings = %+v, want none", got)
			}
		})
	}
}

func TestMySQLLockTablesPreflightTableSelection(t *testing.T) {
	tests := []struct {
		name               string
		includes, excludes []string
		want               bool
	}{
		{name: "all by default", want: true},
		{name: "include exact case insensitive", includes: []string{"EVENTS"}, want: true},
		{name: "include glob", includes: []string{"ev*"}, want: true},
		{name: "not included", includes: []string{"orders"}},
		{name: "excluded wins", includes: []string{"ev*"}, excludes: []string{"events"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mysqlPFTableSelected("events", tc.includes, tc.excludes); got != tc.want {
				t.Fatalf("selected = %t, want %t", got, tc.want)
			}
		})
	}
}
