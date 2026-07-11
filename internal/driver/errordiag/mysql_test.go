package errordiag

import (
	"strings"
	"testing"
)

var mysqlCases = []struct {
	pattern   string
	errMsg    string
	mustMatch string
	mustCateg string
}{
	{
		pattern:   "mysql_invalid_connection",
		errMsg:    `writing chunk: inserting batch into comments: invalid connection`,
		mustMatch: "closed the connection",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_duplicate_entry",
		errMsg:    `Error 1062 (23000): Duplicate entry 'alice@x.com' for key 'users.email_unique'`,
		mustMatch: "email_unique",
		mustCateg: "constraint",
	},
	{
		pattern:   "mysql_null_violation",
		errMsg:    `Error 1048: Column 'email' cannot be null`,
		mustMatch: "email",
		mustCateg: "data_quality",
	},
	{
		pattern:   "mysql_data_too_long",
		errMsg:    `Error 1406: Data too long for column 'name' at row 1`,
		mustMatch: "name",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mysql_incorrect_string",
		errMsg:    `Error 1366: Incorrect string value: '\xF0\x9F\x98\x80' for column 'bio' at row 1`,
		mustMatch: "utf8mb4",
		mustCateg: "data_quality",
	},
	{
		pattern:   "mysql_incorrect_value",
		errMsg:    `Error 1366: Incorrect integer value: 'abc' for column 'age' at row 1`,
		mustMatch: "abc",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mysql_out_of_range",
		errMsg:    `Error 1264: Out of range value for column 'age' at row 1`,
		mustMatch: "age",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mysql_table_exists",
		errMsg:    `Error 1050: Table 'users' already exists`,
		mustMatch: "users",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_table_missing",
		errMsg:    `Error 1146: Table 'prod.orders' doesn't exist`,
		mustMatch: "prod.orders",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_unknown_column",
		errMsg:    `Error 1054: Unknown column 'email' in 'field list'`,
		mustMatch: "email",
		mustCateg: "type_mismatch",
	},
	{
		pattern:   "mysql_database_access_denied",
		errMsg:    `Error 1044 (42000): Access denied for user 'dmt'@'%' to database 'prod'`,
		mustMatch: "LOCK TABLES",
		mustCateg: "permission",
	},
	{
		pattern:   "mysql_database_access_denied",
		errMsg:    `Error 1044 (42000): Access denied for user 'dmt'@'%' to database 'target'`,
		mustMatch: "failed operation",
		mustCateg: "permission",
	},
	{
		pattern:   "mysql_command_denied",
		errMsg:    `Error 1142 (42000): LOCK TABLES command denied to user 'dmt'@'%' for table 'events'`,
		mustMatch: "strict lock-window",
		mustCateg: "permission",
	},
	{
		pattern:   "mysql_command_denied",
		errMsg:    `Error 1142 (42000): INSERT command denied to user 'dmt'@'%' for table 'events'`,
		mustMatch: "denied command privilege",
		mustCateg: "permission",
	},
	{
		pattern:   "mysql_access_denied",
		errMsg:    `Error 1045: Access denied for user 'dmt'@'10.0.0.5' (using password: YES)`,
		mustMatch: "dmt",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_privilege_required",
		errMsg:    `Error 1227: Access denied; you need (at least one of) the SUPER privilege(s) for this operation`,
		mustMatch: "SUPER",
		mustCateg: "permission",
	},
	{
		pattern:   "mysql_connection_refused",
		errMsg:    `dial tcp 10.0.0.5:3306: connect: connection refused`,
		mustMatch: "mysqld",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_unknown_database",
		errMsg:    `Error 1049: Unknown database 'prod'`,
		mustMatch: "prod",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_max_allowed_packet",
		errMsg:    `Error 1153: Got a packet bigger than 'max_allowed_packet' bytes`,
		mustMatch: "max_allowed_packet",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_lock_wait_timeout",
		errMsg:    `Error 1205: Lock wait timeout exceeded; try restarting transaction`,
		mustMatch: "strict source reads",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_deadlock",
		errMsg:    `Error 1213: Deadlock found when trying to get lock; try restarting transaction`,
		mustMatch: "deadlock",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_fk_cant_add",
		errMsg:    `Error 1215: Cannot add foreign key constraint`,
		mustMatch: "Foreign-key",
		mustCateg: "constraint",
	},
	{
		pattern:   "mysql_fk_parent_violation",
		errMsg:    "Error 1451: Cannot delete or update a parent row: a foreign key constraint fails (`prod`.`orders`, CONSTRAINT `fk_orders_users` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`))",
		mustMatch: "users",
		mustCateg: "constraint",
	},
	{
		pattern:   "mysql_fk_child_violation",
		errMsg:    "Error 1452: Cannot add or update a child row: a foreign key constraint fails (`prod`.`orders`, CONSTRAINT `fk_orders_users` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`))",
		mustMatch: "users",
		mustCateg: "constraint",
	},
	{
		pattern:   "mysql_key_too_long",
		errMsg:    `Error 1071: Specified key was too long; max key length is 767 bytes`,
		mustMatch: "767",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_table_full",
		errMsg:    `Error 1114: The table 'huge' is full`,
		mustMatch: "huge",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_server_gone",
		errMsg:    `Error 2006: MySQL server has gone away`,
		mustMatch: "wait_timeout",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_lost_connection",
		errMsg:    `Error 2013: Lost connection to MySQL server during query`,
		mustMatch: "network",
		mustCateg: "connection",
	},
	{
		pattern:   "mysql_row_too_large",
		errMsg:    `Error 1118: Row size too large (> 8126)`,
		mustMatch: "InnoDB",
		mustCateg: "other",
	},
	{
		pattern:   "mysql_lock_table_full",
		errMsg:    `Error 1206: The total number of locks exceeds the lock table size`,
		mustMatch: "innodb_buffer_pool_size",
		mustCateg: "other",
	},
}

func TestMySQLPatterns_AllCasesMatch(t *testing.T) {
	for _, tc := range mysqlCases {
		t.Run(tc.pattern, func(t *testing.T) {
			m, ok := Lookup("mysql", tc.errMsg)
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

func TestMySQLPatterns_EveryCatalogPatternHasACase(t *testing.T) {
	covered := make(map[string]struct{}, len(mysqlCases))
	for _, tc := range mysqlCases {
		covered[tc.pattern] = struct{}{}
	}
	for _, p := range mysqlPatterns {
		if _, ok := covered[p.Name]; !ok {
			t.Errorf("pattern %q has no test case in mysqlCases (add one or remove the pattern)", p.Name)
		}
	}
}
